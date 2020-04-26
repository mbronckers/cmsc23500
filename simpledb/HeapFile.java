package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    
    private TupleDesc td;
    private File f;
    private int table_id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.table_id = f.hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return table_id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) throws IllegalArgumentException {
        if (pid.getPageNumber() < 0 || pid.getPageNumber() > (this.numPages() - 1)) {
            throw new IllegalArgumentException("Page number out of bounds");
        }
        if (this.getId() != pid.getTableId()) {
            throw new IllegalArgumentException("Page is not in this table");
        }
        
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
			raf.seek((long) (pid.getPageNumber() * BufferPool.getPageSize()));
			byte[] buffer = new byte[BufferPool.getPageSize()];
            raf.read(buffer);
            raf.close();
			return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), buffer);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
        }
        
		throw new IllegalArgumentException("Page is not in this table");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try {
            RandomAccessFile hf = new RandomAccessFile(this.f, "rw");
            PageId pid = page.getId();
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            hf.seek(offset);
            hf.write(page.getPageData(), 0, BufferPool.getPageSize());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(((double) f.length()) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
        if (this.td.equals(t.getTupleDesc()) != true) {
            throw new DbException("TupleDesc not equal to file TupleDesc");
        }

        HeapPage hp = null;
        PageId temp_pid = null;
        
        /* Get free pages */
        for (int i = 0; i < this.numPages(); i++) {
            temp_pid = new HeapPageId(this.getId(), i);
            hp = (HeapPage) Database.getBufferPool().getPage(tid, temp_pid, Permissions.READ_WRITE);
            
            /* There are empty slots, insert tuple */
            if (hp.getNumEmptySlots() > 0) {
                hp.insertTuple(t);
                return new ArrayList<Page>(Arrays.asList(hp));
            }
        }

        /* No empty pages, create one with sync lock*/
        synchronized(this) {
            HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
            hp = new HeapPage(pid, HeapPage.createEmptyPageData());
            hp.insertTuple(t);
            try {
                int page_size = BufferPool.getPageSize();
                int offset = page_size * this.numPages();
                byte[] byte_stream = hp.getPageData();
                RandomAccessFile hf = new RandomAccessFile(this.f, "rw");
                
                hf.seek(offset);
                hf.write(byte_stream);
                hf.close();

            } catch (IOException e) {
                throw e;
            }    
        }
        
        return new ArrayList<Page>(Arrays.asList(hp));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

        if (t.getRecordId().getPageId().getTableId() != this.table_id) {
            throw new DbException("Tuple not a member of this file");
        } else if (t.getRecordId().getPageId().getPageNumber() < 0 || 
                   t.getRecordId().getPageId().getPageNumber() >= numPages()) {

            throw new DbException("Page number out of bounds");
        }

        /* Deleting tuple from associated heap page using BufferPool.getPage() */
        PageId pid = t.getRecordId().getPageId();
        HeapPage hp = (HeapPage) (Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE));
        hp.deleteTuple(t);

        return new ArrayList<Page>(Arrays.asList(hp));
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

