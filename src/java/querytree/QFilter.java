package querytree;

class QFilter implements QueryTree {
    
    private final QueryTree child;
    private final Pred pred;
    private final int colNum;
    private final int operand;
    
    public enum Pred {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, NOT_EQUALS;

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == NOT_EQUALS)
                return "!=";
            throw new IllegalStateException("impossible to reach here");
        }
    }
    
    QFilter(QueryTree child, int colNum, Pred pred, int operand) {
        this.child = child;
        this.pred = pred;
        this.colNum = colNum;
        this.operand = operand;
    }
    
    @Override
    public String toString() {
        return "FILTER(" + child + "," + colNum + pred + operand + ")";
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