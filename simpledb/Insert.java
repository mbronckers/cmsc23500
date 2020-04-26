package simpledb;

import java.io.*;
import java.util.*;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId trans_id;
    private OpIterator child;
    private int table_id;
    private TupleDesc td;
    private boolean inserted = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        
        TupleDesc temp = Database.getCatalog().getTupleDesc(tableId);
        if (child.getTupleDesc().equals(temp) == false) {
            throw new DbException("Child td does not equal table td");
        }

        this.trans_id = t;
        this.child = child;
        this.table_id = tableId;

        /* Specify the num of fields, the types of fields, & the names of fields
           for the TupleDesc ield names and number of */
        String[] field_names = new String[1];
        field_names[0] = "No. of inserted records";
        Type[] type = new Type[] {Type.INT_TYPE};

        this.td = new TupleDesc(type, field_names);
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.inserted) return null;
        
        BufferPool bp = Database.getBufferPool();
        int count = 0;

        /* Insert tuples read from child into table_id */
        this.child.open();
        while (this.child.hasNext()) {
            
            Tuple next = child.next();
            
            try {
                bp.insertTuple(this.trans_id, this.table_id, next);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.child.close();
        this.inserted = true;

        /* Create single field tuple with no. of inserted records */
        Tuple ret = new Tuple(this.td);
        ret.setField(0, new IntField(count));

        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
