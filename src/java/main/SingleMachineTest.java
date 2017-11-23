package main;

import global.Global;
import networking.HeadNode;
import networking.Machine;
import networking.NodeServer;
import querytree.QueryTree;
import simpledb.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example main function for testing our Distributed Database on a single machine.
 */
public class SingleMachineTest {

    private static final Logger LOGGER = Logger.getLogger(SingleMachineTest.class.getName());

    public static void main(String[] args) throws IOException, DbException, TransactionAbortedException {
        List<String> tableNames = Arrays.asList("table1", "table2");
        List<Integer> ports = Arrays.asList(8000, 8001, 8002);

        // Set up the Nodes
        List<NodeServer> nodes = new ArrayList<>();
        LOGGER.log(Level.INFO, "Setting up nodes on ports: " + ports);
        for (int port : ports) {
            // Create a Node and set its database
            NodeServer node = new NodeServer(port);

            // Create partitioned tables
            for (String tableName : tableNames) {
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
        String tableName = "table1";
        QueryTree query = QueryTree.filter(QueryTree.scan(null, tableName, tableName), 1, Predicate.Op.EQUALS, new IntField(8001));
        headNode.processQuery(query);

        // Finished!
        LOGGER.log(Level.INFO, "Finished!");
        System.exit(1);
    }

}
