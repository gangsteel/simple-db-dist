package querytree;

import networking.NodeServer;
import simpledb.*;

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
    /**
     * useSimpleDb is whether or not to run the SimpleDB version without networking.
     */
    public static QueryTree scan(NodeServer node, String name, String alias, boolean useSimpleDb) {
        // TODO: Alias must be the same as name? It seems that eventually, we pass in the alias to the other nodes.
        return new QScan(node, name, alias, useSimpleDb);
    }

    public static QueryTree scan(NodeServer node, String name, String alias) {
        // TODO: Alias must be the same as name? It seems that eventually, we pass in the alias to the other nodes.
        return new QScan(node, name, alias, false);
    }
    
    public static QueryTree filter(QueryTree child, int colNum, Predicate.Op pred, Field operand) {
        return new QFilter(child, colNum, pred, operand);
    }
    
    public static QueryTree aggregate(QueryTree child, int colNum, Aggregator.Op aggregator) {
        return new QAggregate(child, colNum, aggregator);
    }

    public static QueryTree join(QueryTree child1, QueryTree child2, int colNum1, Predicate.Op op, int colNum2) {
        return new QJoin(child1, child2, colNum1, op, colNum2);
    }

    public static QueryTree hashJoin(QueryTree child1, QueryTree child2, int colNum1, Predicate.Op op, int colNum2){
        return new QHashJoin(child1, child2, colNum1, op, colNum2);
    }

    public OpIterator getRootOp();

    public void setIsGlobal(boolean isGlobal);

    public String getRootType();






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
