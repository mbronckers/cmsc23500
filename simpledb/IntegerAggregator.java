package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Tuple> groups;
    private HashMap<Field, Integer>groupsCount;
    private Tuple noGroup;
    private int noGroupCount;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield != NO_GROUPING) {
            groups = new HashMap<Field, Tuple>();
            if (what == Op.AVG) {
                groupsCount = new HashMap<Field, Integer>();
            }
            else {
                groupsCount = null;
            }
            noGroup = null;
            noGroupCount = -1;
        }
        else {
            groups = null;
            groupsCount = null;
            noGroup = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            if (what == Op.AVG) {
                noGroupCount = 0;
            }
            else {
                noGroupCount = -1;
            }
        }   
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField tupA = (IntField) tup.getField(afield);

        if (noGroup == null) {
            Field tupGB = tup.getField(gbfield);
            Tuple in = new Tuple(new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE})); 
            in.setField(0, tupGB);
            if (groups.containsKey(tupGB)) {
                Tuple prev = groups.get(tupGB);
                IntField prevA = (IntField) prev.getField(afield);
                switch (what) {
                    case MIN:
                        if (prevA.getValue() > tupA.getValue()) {
                            in.setField(1, tupA);
                        }
                        break;
                    case MAX:
                        if (prevA.getValue() < tupA.getValue()) {
                            in.setField(1, tupA);
                        }
                        break;   
                    case SUM:
                        in.setField(1, new IntField(tupA.getValue() + prevA.getValue()));
                        break;
                    case AVG:
                        in.setField(1, new IntField(prevA.getValue() + tupA.getValue()));
                        groupsCount.put(tupGB, groupsCount.get(tupGB) + 1); 
                        break;
                    case COUNT:
                        in.setField(1, new IntField(prevA.getValue() + 1));
                        break;
                }
            }
            else {
                if (what == Op.COUNT) { // Set field to 1 for counting
                    in.setField(1, new IntField(1));
                }
                else { // all other cases set to tupA
                    in.setField(1, tupA);
                    if (what == Op.AVG) {
                        groupsCount.put(tupGB, 1);
                    }
                }
            }
            
            if (in.getField(1) != null) {
                groups.put(tupGB, in);
            }
        }
        else {
            if (noGroup.getField(0) != null) { // previous tuples have been aggregated
                IntField prevA = (IntField) noGroup.getField(0);
                switch (what) {
                    case MIN:
                        if (prevA.getValue() > tupA.getValue()) {
                            noGroup.setField(0, tupA);
                        }
                        break;
                    case MAX:
                        if (prevA.getValue() < tupA.getValue()) {
                            noGroup.setField(0, tupA);
                        }
                        break;   
                    case SUM:
                        noGroup.setField(0, new IntField(tupA.getValue() + prevA.getValue()));
                        break;
                    case AVG:
                        noGroup.setField(0, new IntField(prevA.getValue() + tupA.getValue()));
                        noGroupCount += 1;
                        break;
                    case COUNT:
                        noGroup.setField(0, new IntField(prevA.getValue() + 1));
                        break;
                }   
            }
            else { // first time adding tuple in
                if (what == Op.COUNT) { // Set field to 1 for counting
                    noGroup.setField(0, new IntField(1));
                }
                else { // all other cases set to tupA
                    noGroup.setField(0, tupA);
                    if (what == Op.AVG) {
                        noGroupCount += 1;
                    }
                }
            }     
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> ts = new ArrayList<Tuple>();
        TupleDesc td;
        
        if (gbfield != NO_GROUPING) {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            if (what == Op.AVG) {
                Collection<Tuple> res = groups.values();
                Iterator<Tuple> iter = res.iterator();
                while (iter.hasNext()) {
                    Tuple temp = iter.next();
                    IntField tempA = (IntField) temp.getField(1);
                    Field tempGB = temp.getField(0);
                    Tuple in = new Tuple(temp.getTupleDesc());
                    in.setField(0, tempGB);
                    in.setField(1, new IntField(tempA.getValue() / groupsCount.get(tempGB)));
                    ts.add(in);
                }
            }
            else {
                ts.addAll(groups.values());
            }
        } 
        else {
            td = new TupleDesc(new Type[] {Type.INT_TYPE});
            if (noGroup.getField(0) != null) {
                if (what == Op.AVG){
                    IntField prevA = (IntField) noGroup.getField(0);
                    noGroup.setField(0, new IntField(prevA.getValue() / noGroupCount));
                }
                ts.add(noGroup);
            }
        }
        
        return new TupleIterator(td, ts);
    }

}
