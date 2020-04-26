package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate pred;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple cached;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        pred = p;
        this.child1 = child1;
        this.child2 = child2;
        cached = new Tuple(child1.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        return pred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        int f1 = pred.getField1();
        TupleDesc td = child1.getTupleDesc();
        return td.getFieldName(f1);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        int f2 = pred.getField2();
        TupleDesc td = child2.getTupleDesc();
        return td.getFieldName(f2);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
        cached = null;
    }

    public void close() {
        child2.close();
        child1.close();
        super.close();
        cached = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        cached = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (child1.hasNext() || cached != null) {
            Tuple left;
            if (cached == null) {
                cached = child1.next();
            }
            left = cached;
    		while (child2.hasNext()) {
                Tuple right;
                right = child2.next();
    			if (pred.filter(left, right)) {
                    Tuple full = new Tuple(this.getTupleDesc());
                    Iterator<Field> iterLeft = left.fields();
                    Iterator<Field> iterRight = right.fields();
                    int index = 0;
                    while (iterLeft.hasNext()) {
                        full.setField(index, iterLeft.next());
                        index += 1;
                    }
                    while (iterRight.hasNext()) {
                        full.setField(index, iterRight.next());
                        index += 1;
                    }
                    return full;
                }
            }
            cached = null;
    		child2.rewind();
    	}
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children = new OpIterator[] {child1, child2};
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}