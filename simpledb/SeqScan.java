package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int table_id;
    private String table_alias;
    private DbFileIterator it;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.table_id = tableid;
        this.table_alias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        Catalog c = Database.getCatalog();
        return c.getTableName(table_id);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        return table_alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.table_id = tableid;
        this.table_alias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.it = Database.getCatalog().getDatabaseFile(this.table_id).iterator(this.tid);
        this.it.open();   
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc t = Database.getCatalog().getDatabaseFile(this.table_id).getTupleDesc();
        
        int num_fields = t.numFields();
        String[] prefixed_fields = new String[num_fields];
        Type[] types = new Type[num_fields];

        /* Prefix the fields with the table alias */
        for (int i = 0; i < num_fields; i++) {
            types[i] = t.getFieldType(i);

            // what to do if table_alias is null ??
            prefixed_fields[i] = this.table_alias + "." + t.getFieldName(i); 
        }

        /* Return new TupleDesc with the original types and new prefixed fields */
        return new TupleDesc(types, prefixed_fields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (this.it != null) return this.it.hasNext();
        throw new IllegalStateException("Iterator has not been opened yet or is already closed");
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (this.it != null) return this.it.next();
        throw new IllegalStateException("Iterator has not been opened yet or is already closed");
    }

    public void close() {
        if (this.it != null) this.it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if (this.it != null) this.it.rewind();
        else {   
            throw new IllegalStateException("Iterator has not been opened yet or is already closed");
        }
    }
}
