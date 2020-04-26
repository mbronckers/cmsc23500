package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {

    private HeapFile f;
    private int pageNum;
    private TransactionId tid;
    private Iterator<Tuple> iter;

    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() 
        throws DbException, TransactionAbortedException {
        pageNum = 0;
        iter = ((HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), pageNum), Permissions.READ_ONLY))).iterator();
    }

    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    @Override
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
            if (iter != null) {
                while (true) {
                    if (iter.hasNext()) {
                        return true;
                    }
                    if (pageNum == (f.numPages() - 1)) {
                        break;
                    }
                    pageNum++;
                    iter = ((HeapPage) (Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), pageNum), Permissions.READ_ONLY))).iterator();
                }
            }
            return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
        if (iter == null){
            throw new NoSuchElementException("Iterator not open");
        }
        else if (!this.hasNext()) {
            throw new NoSuchElementException("No more tuples");
        }
        return iter.next();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.open();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close() {
        iter = null;
        pageNum = 0;
    }
}