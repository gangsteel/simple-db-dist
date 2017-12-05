package querytree;
import edu.mit.eecs.parserlib.Parser;

import java.io.File;
import java.io.IOException;
import java.util.List;

import edu.mit.eecs.parserlib.ParseTree;
import edu.mit.eecs.parserlib.UnableToParseException;

import networking.NodeServer;
import querytree.QAggregate.Agg;
import simpledb.IntField;
import simpledb.Predicate;

/**
 * Utility class parsing incoming TCP commands
 */
public class QueryParser {

    private QueryParser() {
    } // This should be a static class

    private enum QueryGrammar {COMMANDS, SCAN, FILTER, AGGREGATE, WORDS, NUMBER, PRED, AGGREGATOR, WHITESPACE, JOIN};

    private static final Parser<QueryGrammar> PARSER = makeParser();

    private static Parser<QueryGrammar> makeParser() {
        final File grammarFile = new File("src/java/querytree/Query.g");
        try {
            return Parser.compile(grammarFile, QueryGrammar.COMMANDS);
        } catch (UnableToParseException | IOException e) {
            throw new RuntimeException("cannot read the file or grammar has syntax error.");
        }
    }

    public static QueryTree parse(NodeServer node, String command) throws UnableToParseException {
        return parse(node, command, false);
    }

    public static QueryTree parse(NodeServer node, String command, boolean useSimpleDb) throws UnableToParseException {
        final ParseTree<QueryGrammar> parseTree = PARSER.parse(command);
        return makeQueryTree(node, parseTree, useSimpleDb);
    }

    private static QueryTree makeQueryTree(NodeServer node, final ParseTree<QueryGrammar> tree, boolean useSimpleDb) {
        switch (tree.name()) {
            case COMMANDS: {
                return makeQueryTree(node, tree.children().get(0), useSimpleDb);
            }
            case SCAN: {
                final String tableName = tree.children().get(0).text();
                return QueryTree.scan(node, tableName, tableName, useSimpleDb);
            }
            case FILTER: {
                final List<ParseTree<QueryGrammar>> children = tree.children();
                final QueryTree child = makeQueryTree(node, children.get(0), useSimpleDb);
                final int colNum = Integer.parseInt(children.get(1).text());
                final Predicate.Op pred = convertSignToPred(children.get(2).text());
                final int operand = Integer.parseInt(children.get(3).text());
                return QueryTree.filter(child, colNum, pred, new IntField(operand));
            }
            case AGGREGATE: {
                final List<ParseTree<QueryGrammar>> children = tree.children();
                final QueryTree child = makeQueryTree(node, children.get(0), useSimpleDb);
                final int colNum = Integer.parseInt(children.get(1).text());
                final Agg agg = convertNameToAgg(children.get(2).text());
                return QueryTree.aggregate(child, colNum, agg);
            }
            case JOIN: {
                final List<ParseTree<QueryGrammar>> children = tree.children();
                final QueryTree child1 = makeQueryTree(node, children.get(0), useSimpleDb);
                final QueryTree child2 = makeQueryTree(node, children.get(1), useSimpleDb);
                final int colNum1 = Integer.parseInt(children.get(2).text());
                final Predicate.Op op = convertSignToPred(children.get(3).text());
                final int colNum2 = Integer.parseInt(children.get(4).text());
                return QueryTree.join(child1, child2, colNum1, op, colNum2);
            }
            default:
                throw new AssertionError("should never get here or not implemented:" + tree);
        }
    }

    private static Predicate.Op convertSignToPred(String sign) {
        switch (sign) {
            case "=":
                return Predicate.Op.EQUALS;
            case ">":
                return Predicate.Op.GREATER_THAN;
            case "<":
                return Predicate.Op.LESS_THAN;
            case "<=":
                return Predicate.Op.LESS_THAN_OR_EQ;
            case ">=":
                return Predicate.Op.GREATER_THAN_OR_EQ;
            case "!=":
                return Predicate.Op.NOT_EQUALS;
            default:
                throw new AssertionError("should never get here");
        }
    }

    private static Agg convertNameToAgg(String name) {
        switch (name) {
            case "MIN":
                return Agg.MIN;
            case "MAX":
                return Agg.MAX;
            case "SUM":
                return Agg.SUM;
            case "AVG":
                return Agg.AVG;
            case "COUNT":
                return Agg.COUNT;
            default:
                throw new AssertionError("should never get here");
        }
    }
}