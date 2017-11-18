package querytree;

/**
 * Abstract recursive data type representing a query tree structure
 * that can be converted from a String command and to a String command
 * that can be converted back
 */
public interface QueryTree {
    /**
     * QueryTree = QScan(tableName: String)
     *             + QFilter(child: QueryTree, colNumber: int, predicate: enum, operand: int)
     *             + QAggregate(child: QueryTree, colNumber: int, aggregator: enum)
     *             + QJoin(left: QueryTree, right: QueryTree, colL: int, colR: int, jpredicate: ?)
     * The class names are prefixed with letter "Q" to avoid conflict with simpledb classes
     */
    
    // TODO: static factory methods
    public static QueryTree scan(String tableName) {
        return new QScan(tableName);
    }
    
    public static QueryTree filter(QueryTree child, int colNum, QFilter.Pred pred, int operand) {
        return new QFilter(child, colNum, pred, operand);
    }
    
    public static QueryTree aggregate(QueryTree child, int colNum, QAggregate.Agg aggregator) {
        return new QAggregate(child, colNum, aggregator);
    }
    
    /**
     * Overriding the toString method, the returning String
     * must be parsable by QueryParser.parse and turn back to equal QueryTree:
     * a.equals(QueryPaser.parse(a.toString())) must be true.
     * (Maybe this constraint can be relaxed?)
     * @return the parsable String representing this QueryTree
     */
    @Override
    public String toString();
    
    @Override
    public boolean equals(Object obj);
    
    @Override
    public int hashCode();
}
