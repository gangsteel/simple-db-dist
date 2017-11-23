package querytree;

import global.Global;
import networking.NodeServer;
import simpledb.*;

public class QJoin implements QueryTree {

    private final QueryTree child1;
    private final QueryTree child2;
    private final JoinPredicate joinPredicate;

    QJoin(QueryTree child1, QueryTree child2, int colNum1, Predicate.Op op, int colNum2) {
        this.child1 = child1;
        this.child2 = child2;
        this.joinPredicate = new JoinPredicate(colNum1, op, colNum2);
    }

    public OpIterator getRootOp(){
        return new Join(joinPredicate, child1.getRootOp(), child2.getRootOp());
    }

    @Override
    public String toString() {
        return String.format("JOIN(%s, %s, %d %s %d)", child1.toString(), child2.toString(), joinPredicate.getField1(),
                joinPredicate.getOperator().toString(), joinPredicate.getField2());
    }

    @Override
    public boolean equals(Object obj) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public int hashCode() {
        throw new RuntimeException("not implemented");
    }

}
