package querytree;
import edu.mit.eecs.parserlib.Parser;

import java.io.File;
import java.io.IOException;
import java.util.List;

import edu.mit.eecs.parserlib.ParseTree;
import edu.mit.eecs.parserlib.UnableToParseException;

import querytree.QFilter.Pred;
import querytree.QAggregate.Agg;

/**
 * Utility class parsing incoming TCP commands
 */
public class QueryParser {

    // FIXME: made this public so we could instantiate from head node
    public QueryParser() {} // This should be a static class
    
    private enum QueryGrammar {COMMANDS, SCAN, FILTER, AGGREGATE, WORDS, NUMBER, PRED, AGGREGATOR, WHITESPACE};
    
    private static final Parser<QueryGrammar> PARSER = makeParser();
    
    private static Parser<QueryGrammar> makeParser() {
        final File grammarFile = new File("src/java/querytree/Query.g");
        try {
            return Parser.compile(grammarFile, QueryGrammar.COMMANDS);
        } catch (UnableToParseException | IOException e) {
            throw new RuntimeException("cannot read the file or grammar has syntax error.");
        }
    }
    
    public static QueryTree parse(String command) throws UnableToParseException {
        final ParseTree<QueryGrammar> parseTree = PARSER.parse(command);
        return makeQueryTree(parseTree);
    }
    
    private static QueryTree makeQueryTree(final ParseTree<QueryGrammar> tree) {
        switch (tree.name()) {
        case COMMANDS:
        {
            return makeQueryTree(tree.children().get(0));
        }
        case SCAN:
        {
            final String tableName = tree.children().get(0).text();
            return QueryTree.scan(tableName);
        }
        case FILTER:
        {
            final List<ParseTree<QueryGrammar>> children = tree.children();
            final QueryTree child = makeQueryTree(children.get(0));
            final int colNum = Integer.parseInt(children.get(1).text());
            final Pred pred = convertSignToPred(children.get(2).text());
            final int operand = Integer.parseInt(children.get(3).text());
            return QueryTree.filter(child, colNum, pred, operand);
        }
        case AGGREGATE:
        {
            final List<ParseTree<QueryGrammar>> children = tree.children();
            final QueryTree child = makeQueryTree(children.get(0));
            final int colNum = Integer.parseInt(children.get(1).text());
            final Agg agg = convertNameToAgg(children.get(2).text());
            return QueryTree.aggregate(child, colNum, agg);
        }
        default:
            throw new AssertionError("should never get here or not implemented:" + tree);
        }
    }
    
    private static Pred convertSignToPred(String sign) {
        switch (sign) {
        case "=":
            return Pred.EQUALS;
        case ">":
            return Pred.GREATER_THAN;
        case "<":
            return Pred.LESS_THAN;
        case "<=":
            return Pred.LESS_THAN_OR_EQ;
        case ">=":
            return Pred.GREATER_THAN_OR_EQ;
        case "!=":
            return Pred.NOT_EQUALS;
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

    public void start(){

    }
}
