package networking;

import distributeddb.GlobalSeqScan;
import global.Global;
import querytree.QueryTree;
import simpledb.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by aditisri on 11/28/17.
 */
public class AddRemoveTest {
    private static final Logger LOGGER = Logger.getLogger(AddRemoveTest.class.getName());
    public static  void main(String[] args) throws IOException, DbException, TransactionAbortedException{
        List<String> tableNames = Arrays.asList("table1", "table2", "table3", "table4");
        List<Integer> ports = new ArrayList<>(Arrays.asList(8000, 8001, 8002));


        // Set up the Nodes
        List<NodeServer> nodes = new ArrayList<>();
        LOGGER.log(Level.INFO, "Setting up nodes on ports: " + ports);
        for (int port : ports) {
            // Create a Node and set its database
            NodeServer node = new NodeServer(port);

            // Create partitioned tables
            for (int i = 0; i < 2; i++) {
                String tableName = tableNames.get(i);
                HeapFile tablePart = Utility.createEmptyTempHeapFile(3);
                node.addTable(tablePart, tableName);
                Tuple t = new Tuple(tablePart.getTupleDesc());
                t.setField(0, new IntField(port));
                t.setField(1, new IntField(port+1));
                t.setField(2, new IntField(port+2));
                Tuple t2 = new Tuple(tablePart.getTupleDesc());
                t2.setField(0, new IntField(port+3));
                t2.setField(1, new IntField(port+4));
                t2.setField(2, new IntField(port+5));
                Database.getBufferPool().insertTuple(Global.TRANSACTION_ID, tablePart.getId(), t);
                Database.getBufferPool().insertTuple(Global.TRANSACTION_ID, tablePart.getId(), t2);
            }
            for (int i = 2; i < 4; i++) {
                String tableName = tableNames.get(i);
                HeapFile tablePart = Utility.createEmptyTempHeapFile(3);
                node.addTable(tablePart, tableName);
                Tuple t = new Tuple(tablePart.getTupleDesc());
                t.setField(0, new IntField(port-8000));
                t.setField(1, new IntField(port-8000+1));
                t.setField(2, new IntField(port-8000+2));
                Tuple t2 = new Tuple(tablePart.getTupleDesc());
                t2.setField(0, new IntField(port-8000+3));
                t2.setField(1, new IntField(port-8000+4));
                t2.setField(2, new IntField(port-8000+5));
                Database.getBufferPool().insertTuple(Global.TRANSACTION_ID, tablePart.getId(), t);
                Database.getBufferPool().insertTuple(Global.TRANSACTION_ID, tablePart.getId(), t2);
            }

            // Store to our list of nodes
            nodes.add(node);
        }

        // Give a reference to all other nodes
        LOGGER.log(Level.INFO, "Giving each node references to each other.");
        for (NodeServer node : nodes) {
            for (NodeServer other : nodes) {
                if (node != other) {
                    node.addReference(new Machine(Global.LOCALHOST, other.getPort()));
                }
            }
        }

        LOGGER.log(Level.INFO, "Making nodes listen.");
        // Make all of the nodes listen
        for (NodeServer node : nodes) {
            node.startListen();
        }

        // Create a head node
        HeadNode headNode = new HeadNode();

        // Pass reference to all other nodes to the headNode
        LOGGER.log(Level.INFO, "Giving HeadNode reference to each Node.");
        for (NodeServer node : nodes) {
            headNode.addChildNode(Global.LOCALHOST, node.getPort());
        }

        // Make the queries manually and pass them to the HeadNode. Note that we use null NodeServer because the query
        // does not correspond to any NodeServer. Since the toString() method does not depend on a NodeServer, and all
        // HeadNode does is relay this String to other NodeServers, this works.
        LOGGER.log(Level.INFO, "Handling query...");
        String tableName1 = "table1";
        String tableName2 = "table2";
        String tableName3 = "table3";
        String tableName4 = "table4";
        QueryTree leftJoin = QueryTree.join(QueryTree.scan(null, tableName1, tableName1), QueryTree.scan(null, tableName2, tableName2),
                0, Predicate.Op.EQUALS, 2);
        QueryTree rightJoin = QueryTree.join(QueryTree.scan(null, tableName3, tableName3), QueryTree.scan(null, tableName4, tableName4),
                2, Predicate.Op.LESS_THAN, 0);
        QueryTree query = QueryTree.join(leftJoin, rightJoin, 0, Predicate.Op.GREATER_THAN, 0);
        headNode.processQuery(query);

        LOGGER.log(Level.INFO, "Starting removal");
        headNode.removeChildNode(Global.LOCALHOST, ports.get(0));
        nodes.remove(0);
        ports.remove(0);
//        QueryTree scan1 = QueryTree.scan(nodes.get(0), tableName1, tableName1);
//        LOGGER.log(Level.INFO, "Performing scan of table1");
//        headNode.processQuery(scan1);

        // Left Join should output:
        // 8002 8003 8004 8000 8001 8002
        // 8003 8004 8005 8001 8002 8003
        // 8004 8005 8006 8002 8003 8004
        // 8005 8006 8007 8003 8004 8005

        // Right Join should output:
        // 0 1 2 3 4 5
        // 0 1 2 4 5 6
        // 0 1 2 5 6 7
        // 1 2 3 4 5 6
        // 1 2 3 5 6 7
        // 2 3 4 5 6 7

        // Therefore there should be 24 total outputs, which there are if you count them.

        // Finished!
//        LOGGER.log(Level.INFO, "Finished!");
//        System.exit(1);

    }
}
