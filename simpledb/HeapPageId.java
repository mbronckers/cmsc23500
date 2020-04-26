package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private int table_id;
    private int page_number;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.table_id = tableId;
        this.page_number = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return this.table_id;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        return this.page_number;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        String table_id = String.valueOf(this.table_id);
        String page_number = String.valueOf(this.page_number);
        return (table_id + page_number).hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if (o instanceof HeapPageId) {
            HeapPageId temp = (HeapPageId) o; // have to construct a HeapPageId instance to access members
            return (temp.getPageNumber() == this.page_number && temp.getTableId() == this.table_id);
        }
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
