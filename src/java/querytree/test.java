package querytree;

import edu.mit.eecs.parserlib.UnableToParseException;

/**
 * Just a temporary test run to do simple tests
 */
public class test {
    public static void main(String[] argv) {
        try {
            QueryTree testTree = QueryParser.parse("  SCAN( _a-b.2c012   )");
            System.out.println(testTree);
        } catch (UnableToParseException e) {
            throw new RuntimeException(e);
        }
    }
}
