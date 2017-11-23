package querytree;

import networking.NodeServer;
import simpledb.*;

class QFilter implements QueryTree {
    
    private final QueryTree child;
    private final Predicate predicate;

    QFilter(QueryTree child, int colNum, Predicate.Op pred, Field operand) {
        this.child = child;
        this.predicate = new Predicate(colNum, pred, operand);
    }

    public OpIterator getRootOp(NodeServer node){
        return new Filter(predicate, child.getRootOp(node));
    }
    
    @Override
    public String toString() {
        return getRootOp(null).toString();
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
