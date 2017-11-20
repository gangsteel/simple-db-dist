package querytree;

import edu.mit.eecs.parserlib.UnableToParseException;

/**
 * Just a temporary test run to do simple tests
 */
public class test {
    public static void main(String[] argv) {
        try {
            QueryTree testTree = QueryParser.parse(" AGGREGATE( SCAN( _a-b.2c012   ) , 3, COUNT)");
            System.out.println(testTree);
            testTree = QueryParser.parse(" FILTER  ( SCAN( _a-b.2c012   ) , 3>5)");
            System.out.println(testTree);
            testTree = QueryParser.parse("  FILTER  (  AGGREGATE ( FILTER  ( SCAN ( _a-b.2c012   ) , 3   != 5  ) , 5, AVG  ), 8 <=4 )   " );
            System.out.println(testTree);
        } catch (UnableToParseException e) {
            throw new RuntimeException(e);
        }
    }
}
