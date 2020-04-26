package simpledb;

import java.io.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int maxPages;

    private ConcurrentHashMap<PageId, Page> bufferPool;
    private LinkedHashMap<PageId, Integer> ordering; // Keeps track of access order for pages, for eviction
    private TransactionManager transactionManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxPages = numPages;
        bufferPool = new ConcurrentHashMap<PageId, Page>();
        ordering = new LinkedHashMap<PageId, Integer>(16, 0.75f, true);
        transactionManager = new TransactionManager();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    // Helper function for JoinOptimizer calculations
    public int getNumPages() {
        return maxPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
            if (perm != Permissions.READ_ONLY && perm != Permissions.READ_WRITE) {
                throw new DbException("Improper Permission");
            }

            transactionManager.grantLock(tid, pid, perm);

            Page p;
            if (bufferPool.containsKey(pid)) {
                p = bufferPool.get(pid);
                ordering.put(pid, null); // update access order
            }
            else {
                HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
                if (pid.getPageNumber() < f.numPages()) {
                    p = f.readPage(pid);
                }
                else {
                    // design choice, instead of returning null, will return an empty page and add to BP
                    try {
                        p = new HeapPage(((HeapPageId) pid), HeapPage.createEmptyPageData());
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        throw new DbException("Creating new page failed");
                    }
                }
                if (this.maxPages == bufferPool.size()) {
                    this.evictPage();
                }
                bufferPool.put(pid, p);
                ordering.put(pid, null);
            }

            return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        transactionManager.unlock(tid, pid, true);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return transactionManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException 
    {
        Set<PageId> pids = transactionManager.getTransactionPages(tid);
        if (pids != null) {
            for (PageId pid : pids) {
                if (commit) {
                    flushPage(pid);
                }
                else {
                    discardPage(pid);
                }
            }    
        }
        
        transactionManager.unlockAll(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        /* Get DbFile of the table to add the tuple to & insert tuple */
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirty_pages = f.insertTuple(tid, t);
        
        /* Mark page(s) dirty */
        for (Page page : dirty_pages) {
            page.markDirty(true, tid);
            bufferPool.put(page.getId(), page); // put {key,value}
            ordering.put(page.getId(), null);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        /* Get DbFile of the table to add the tuple to & insert tuple */
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirty_pages = f.deleteTuple(tid, t);

        /* Mark page(s) dirty */
        for (Page page : dirty_pages) {
            page.markDirty(true, tid);
            bufferPool.put(page.getId(), page); // put {key,value}
            ordering.put(page.getId(), null);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : bufferPool.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        bufferPool.remove(pid);
        ordering.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (!bufferPool.containsKey(pid)) {
            return;
        }

        Page page = bufferPool.get(pid);
        if (page.isDirty() == null) {
            return;
        }

        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        Set<PageId> pids = transactionManager.getTransactionPages(tid);
        for (PageId pid : pids) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
    	for (PageId pid : ordering.keySet()) {
    		try {
                if (bufferPool.get(pid).isDirty() == null) {
                    flushPage(pid);
                    bufferPool.remove(pid);
                    ordering.remove(pid);
                    return;
                }
    		}
    		catch (IOException e) {
                e.printStackTrace();
    			throw new DbException("IOException while trying to flush page");
    		}
    	}
    	throw new DbException("No pages evicted");
    }

}
