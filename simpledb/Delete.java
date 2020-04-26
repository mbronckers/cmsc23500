package simpledb;

import java.io.*;
import java.util.*;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId trans_id;
    private OpIterator child;
    private TupleDesc td;
    private boolean fetched;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.trans_id = t;
        this.child = child;
        this.fetched = false;

        /* Specify the num of fields, the types of fields, & the names of fields
           for the TupleDesc ield names and number of */
        String[] field_names = new String[1];
        field_names[0] = "No. of deleted records";
        Type[] type = new Type[] {Type.INT_TYPE};

        this.td = new TupleDesc(type, field_names);

    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.fetched) return null;

        Tuple ret = new Tuple(this.td);  // the return tuple
        BufferPool bp = Database.getBufferPool();
        int count = 0;

        try {
            fetched = true;

            /* Delete tuples from child operator */
            while (child.hasNext()) {
                Tuple del = child.next();  // tuple to be deleted
                bp.deleteTuple(this.trans_id, del);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* Set single field to number of deleted tuples */ 
        ret.setField(0, new IntField(count));
        
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
