package querytree;

import networking.NodeServer;
import simpledb.Aggregator;
import simpledb.OpIterator;
import simpledb.Aggregate;

public class QAggregate implements QueryTree {
    
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

    // TODO: change parser to handle Aggregator.Op
    private final QueryTree child;
    private final int colNum;
    private final Aggregator.Op aggregator;
    
    QAggregate(QueryTree child, int colNum, Aggregator.Op aggregator) {
        this.child = child;
        this.colNum = colNum;
        this.aggregator = aggregator;
    }

    @Override
    public OpIterator getRootOp() {
        return new Aggregate(this.child.getRootOp(), this.colNum, Aggregator.NO_GROUPING, aggregator);
    }

    @Override
    public String getRootType(){
        return "AGGREGATE";
    }

    public Aggregator.Op getAggregator(){
        return this.aggregator;
    }

    @Override
    public void setIsGlobal(boolean isGlobal) {
        child.setIsGlobal(isGlobal);
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
