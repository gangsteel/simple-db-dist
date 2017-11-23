package querytree;

import networking.NodeServer;
import simpledb.OpIterator;

class QAggregate implements QueryTree {
    
    public enum Agg {
        MIN, MAX, SUM, AVG, COUNT;
        
        public String toString()
        {
            if (this==MIN)
                return "MIN";
            if (this==MAX)
                return "MAX";
            if (this==SUM)
                return "SUM";
            if (this==AVG)
                return "AVG";
            if (this==COUNT)
                return "COUNT";
            throw new IllegalStateException("impossible to reach here");
        }
    }
    
    private final QueryTree child;
    private final int colNum;
    private final Agg aggregator;
    
    QAggregate(QueryTree child, int colNum, Agg aggregator) {
        this.child = child;
        this.colNum = colNum;
        this.aggregator = aggregator;
    }

    @Override
    public OpIterator getRootOp() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
    
    @Override
    public String toString() {
        return "AGGREGATE(" + child + "," + colNum + "," + aggregator + ")";
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
