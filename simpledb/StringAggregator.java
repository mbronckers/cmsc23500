package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Tuple> groups;
    private Tuple noGroup;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield != NO_GROUPING) {
            groups = new HashMap<Field, Tuple>();
            noGroup = null;
        }
        else {
            groups = null;
            noGroup = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupGB = tup.getField(gbfield);

        if (noGroup == null) {
            Tuple in = new Tuple(new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE})); 
            in.setField(0, tupGB);
            if (groups.containsKey(tupGB)) {
                Tuple prev = groups.get(tupGB);
                IntField prevA = (IntField) prev.getField(afield);
                in.setField(1, new IntField(prevA.getValue() + 1));
            }
            else {
                in.setField(1, new IntField(1));
            }
            groups.put(tupGB, in);
        }
        else {
            if (noGroup.getField(0) != null) {
                IntField prevA = (IntField) noGroup.getField(afield);
                noGroup.setField(0, new IntField(prevA.getValue() + 1));
            }
            else {
                noGroup.setField(0, new IntField(1));
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> ts = new ArrayList<Tuple>();
        TupleDesc td;
        
        if (gbfield != NO_GROUPING) {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            ts.addAll(groups.values());
        } 
        else {
            td = new TupleDesc(new Type[] {Type.INT_TYPE});
            if (noGroup.getField(0) != null) {
                ts.add(noGroup);
            }
        }
        
        return new TupleIterator(td, ts);
    }

}
