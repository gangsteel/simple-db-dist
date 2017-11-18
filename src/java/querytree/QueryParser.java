package querytree;
import edu.mit.eecs.parserlib.Parser;

import java.io.File;
import java.io.IOException;

import edu.mit.eecs.parserlib.ParseTree;
import edu.mit.eecs.parserlib.UnableToParseException;

/**
 * Utility class parsing incoming TCP commands
 */
public class QueryParser {
    
    private QueryParser() {} // This should be a static class
    
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
        default:
            throw new AssertionError("should never get here or not implemented:" + tree);
        }
    }
}
