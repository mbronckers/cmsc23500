package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    private int num_tuples;
    private int io_page_cost;
    
    private static HashMap<String, Integer> maxs, mins;
    private static HashMap<String, IntHistogram> int_histograms;
    private static HashMap<String, StringHistogram> str_histograms;

    private static DbFile file;
    private TupleDesc td;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.td = Database.getCatalog().getTupleDesc(tableid);
        this.io_page_cost = ioCostPerPage;

        this.maxs = new HashMap<String, Integer>();
        this.mins = new HashMap<String, Integer>();

        this.int_histograms = new HashMap<String, IntHistogram>();
        this.str_histograms = new HashMap<String, StringHistogram>();

        /* Iterator over tuples and set mins/maxs for int type fields */ 
        TransactionId transaction_id = new TransactionId();
        DbFileIterator iter = file.iterator(transaction_id); 
        set_range(iter, this.td);

        init_histograms(td);
        fill_histograms(iter, td);
    }

    /**
     * Initializes histograms for each field_name (INT_TYPE or STRING_TYPE) in
     * the tuple schema
     *
     * @param td Tuple descriptor to access tuple schema info
     *
     * @return void
     */
    private void init_histograms(TupleDesc td) {
        for (int i = 0; i < td.numFields(); i++) {
            
            String field_name = td.getFieldName(i);

            /* Initialize appropriate histogram type & add to HashMap */
            if (td.getFieldType(i).equals(Type.INT_TYPE)) {

                IntHistogram inth = new IntHistogram(NUM_HIST_BINS, 
                                                    mins.get(field_name),
                                                    maxs.get(field_name));

                this.int_histograms.put(field_name, inth);

            } else if (td.getFieldType(i).equals(Type.STRING_TYPE)) {

                StringHistogram strh = new StringHistogram(NUM_HIST_BINS);

                this.str_histograms.put(field_name, strh);

            }
        }
    }

    /**
     * Fills in the histograms for each field_name (INT_TYPE or STRING_TYPE) in
     * the tuple schema by iterating over the tuples
     *
     * @param iter Iterator to iterate over the tuples in file
     * @param td Tuple descriptor to access tuple schema info
     *
     * @return void
     */
    private void fill_histograms(DbFileIterator iter, TupleDesc td) {
        try {
            iter.open();
            while (iter.hasNext()) {
                Tuple temp = iter.next();

                for (int i = 0; i < td.numFields(); i++) {

                    if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                        /* Get field value and field name */
                        String field_name = td.getFieldName(i);
                        int value = ((IntField) temp.getField(i)).getValue();

                        /* Insert into appropriate histogram */
                        this.int_histograms.get(field_name).addValue(value);
                    
                    } else if (td.getFieldType(i).equals(Type.STRING_TYPE)) {
                        /* Get field value and field name */
                        String field_name = td.getFieldName(i);
                        String value = ((StringField) temp.getField(i)).getValue();

                        /* Insert into appropriate histogram */
                        this.str_histograms.get(field_name).addValue(value);
                    }

                }

            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Iterates over file's tuples and updates the mins/maxs for
     * each of integer type fields accordingly
     *
     * @param iter Iterator for file
     * @param td Tuple descriptor to access tuple schema info
     *
     * @return void
     */
    private void set_range(DbFileIterator iter, TupleDesc td) {
        Tuple temp;

        /* Iterate over file's tuples and fill in maxs/mins accordingly */
        try {
            iter.open();
            while (iter.hasNext()) {
                temp = iter.next();
                this.num_tuples++;

                int num_fields = td.numFields();
                
                /* Iterate over field types and only check int types */
                for (int i = 0; i < num_fields; i++) {
                    if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                        String field_name = td.getFieldName(i);
                        int field_value = ((IntField) temp.getField(i)).getValue();

                        set_min(field_name, field_value);
                        set_max(field_name, field_value);
                    }
                }
            }
            iter.close();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Helper function to set_range to update min accordingly
     *
     * @param key Fieldname to check min value for
     * @param value Current tuple's value to cross-reference against current min
     *
     * @return void
     */
    private void set_min(String key, int value) {
        /* Field name does not exist yet in mins */
        if (!this.mins.containsKey(key)) {
            this.mins.put(key, value);
        } 
        /* Existing min present, update as necessary */
        else {
            int current_min = this.mins.get(key);
            if (value < current_min) this.mins.put(key, value);
        }
    }

    /**
     * Helper function to set_range to update max accordingly
     *
     * @param key Fieldname to check max value for
     * @param value Current tuple's value to cross-reference against current max
     *
     * @return void
     */
    private void set_max(String key, int value) {
        /* Field name does not exist yet in maxs */
        if (!this.maxs.containsKey(key)) {
            this.maxs.put(key, value);
        } 
        /* Existing max present, update as necessary */
        else {
            int current_max = this.maxs.get(key);
            if (value > current_max) this.maxs.put(key, value);
        }
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        int num_pages = ((HeapFile) file).numPages();
        return num_pages * this.io_page_cost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (this.num_tuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        String field_name = this.td.getFieldName(field);
        
        if (constant.getType().equals(Type.INT_TYPE)) {
            IntHistogram h = this.int_histograms.get(field_name);
            int value = ((IntField) constant).getValue();

            return h.estimateSelectivity(op, value);

        } else if (constant.getType().equals(Type.STRING_TYPE)) {
            StringHistogram h = this.str_histograms.get(field_name);
            String value = ((StringField) constant).getValue();

            return h.estimateSelectivity(op, value);
        }

        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return num_tuples;
    }

    public int distinctTuples(String field_name) {
        IntHistogram h = this.int_histograms.get(field_name);
        if (h != null) {
            return h.getDistinctTuples();
        }
        
        StringHistogram str = this.str_histograms.get(field_name);
        if (str != null) {
            return str.getDistinctTuples();
        }

        return -1;
    }
}
