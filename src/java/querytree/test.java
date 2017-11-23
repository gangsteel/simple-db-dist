package querytree;

import edu.mit.eecs.parserlib.UnableToParseException;
import networking.NodeServer;

import java.io.IOException;

/**
 * Just a temporary test run to do simple tests
 */
public class test {
    public static void main(String[] argv) {
        try {
            QueryTree testTree = QueryParser.parse(null, " AGGREGATE( SCAN( _a-b.2c012   ) , 3, COUNT)");
            System.out.println(testTree);
            testTree = QueryParser.parse(null, " FILTER  ( SCAN( _a-b.2c012   ) , 3>5)");
            System.out.println(testTree);
            testTree = QueryParser.parse(null, "  FILTER  (  AGGREGATE ( FILTER  ( SCAN ( _a-b.2c012   ) , 3   != 5  ) , 5, AVG  ), 8 <=4 )   " );
            System.out.println(testTree);
            testTree = QueryParser.parse(null, " JOIN ( SCAN ( table1 ), SCAN ( table2 ), 3 <= 5 )");
            System.out.println(testTree);
        } catch (UnableToParseException e) {
            throw new RuntimeException(e);
        }
    }
}
