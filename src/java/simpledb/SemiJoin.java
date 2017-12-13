package simpledb;

import java.util.NoSuchElementException;

/**
 * Created by aditisri on 12/12/17.
 */
public class SemiJoin extends Operator {
    private static final long serialVersionUID = 1L;
    private final JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple currentTuple1 = null; // current tuple pointed by the left child iterator
    public SemiJoin(JoinPredicate p, OpIterator child1, OpIterator child2){
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        super.close();
        child1.close();
        child2.close();
    }



    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (currentTuple1 == null && child1.hasNext()) {
            // Should only be called at the first fetch or after rewind call
            currentTuple1 = child1.next();
        }
        while (currentTuple1 != null) {
            if (!child2.hasNext()){
                if (child1.hasNext()){
                    child2.rewind();
                    currentTuple1 = child1.next();
                } else {
                    currentTuple1 = null;
                    return null;
                }
            }
            while (child2.hasNext()) {
                final Tuple t2 = child2.next();
                if (p.filter(currentTuple1, t2)) {
                    return currentTuple1;
                }
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[0];
    }

    @Override
    public void setChildren(OpIterator[] children) {
        assert children.length == 2 : "Incorrent length of OpIterator[]!";
        child1 = children[0];
        child2 = children[1];
    }

    @Override
    public TupleDesc getTupleDesc() {
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        currentTuple1 = null;

    }
}
