package networking;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.mit.eecs.parserlib.UnableToParseException;
import querytree.QueryParser;
import querytree.QueryTree;
import simpledb.*;

/**
 * Server class interact with the including database of the node and
 * communicate with outside (the head node, or direct communication from a user)
 * through TCP protocol
 */
public class NodeServer {

    private static final Logger LOGGER = Logger.getLogger(NodeServer.class.getName());

    private String id;
    private final List<Machine> references;
    private final ServerSocket serverSocket;
    private final int port;
    
    public NodeServer(int portNumber) throws IOException {
        id = "" + portNumber;
        references = new ArrayList<>();
        serverSocket = new ServerSocket(portNumber);
        //TODO: change simpledb so we pass instances of database around
        port = portNumber;
    }

    /** Catalog-like Methods. We use this to reference a table associated with this Node. **/

    public void addTable(DbFile table, String tableName) {
        Database.getCatalog().addTable(table, getStoredTableName(tableName));
    }

    public String getTableName(int tableId) {
        return getStoredTableName(Database.getCatalog().getTableName(tableId));
    }

    public int getTableId(String tableName) {
        return Database.getCatalog().getTableId(getStoredTableName(tableName));
    }

    private String getStoredTableName(String tableName) {
        return id + "." + tableName;
    }

    /** End Catalog-like Method **/

    public void addReference(Machine machine) {
        references.add(machine);
    }

    public int getPort() {
        return port;
    }

    /**
     * Start listening to the port and accepting income connections
     * @throws IOException if an error occurs waiting for a connection
     */
    public void startListen() throws IOException {
        new Thread(new ConnnectionListening()).start();
    }
    
    /**
     * handle connection between this node and client
     * @param socket the socket connecting between this node and the client
     * @throws IOException if the connection encounters an error
     */
    private void handleConnection(Socket socket) throws IOException {
        LOGGER.log(Level.INFO, "Client from " + socket.getInetAddress().toString() + ":"
                + socket.getPort() + " is connected. Local port: " + socket.getLocalPort() + ".");
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // TODO: should we autoFlush?

        try {
            for (String line = in.readLine(); line != null; line = in.readLine()) {
                if (line.equals("exit")) {
                    break; // A way to debug individual server
                }
                try {
                    QueryTree qt = QueryParser.parse(line);
                    processQuery(qt, out);
                    // Some trash for now. TODO: run the query and return the results
                } catch (UnableToParseException e) {
                    out.println("Unable to parse your command!"); // TODO: More information
                }
            }
        } finally {
            System.out.println("Client from " + socket.getInetAddress().toString() + ":"
                    + socket.getPort() + " is leaving.");
            out.close();
            in.close();
        }
    }

    private void processQuery(QueryTree queryTree, PrintWriter outputStream){
        OpIterator op = queryTree.getRootOp(this);
        try {
            op.open();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        try {
            while (op.hasNext()) {
                Tuple t = op.next();
                outputStream.println(t.toString());
            }
            outputStream.println("END");
        }
        catch(DbException e){
            System.out.println("There was an error processing query");
            e.printStackTrace();
            //TODO: do error handling
        }
        catch(TransactionAbortedException e){
            System.out.println("Transaction aborted while processing query");
            e.printStackTrace();
            //TODO: do error handling

        }
    }
    
    public static void main(String[] args) {
        // TODO: Make the main to accept a port number?
        final int port = 4444;
        try {
            NodeServer server = new NodeServer(port);
            server.startListen();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }

    private class ConnnectionListening implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    final Socket socket = serverSocket.accept();

                    Thread handler = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                try {
                                    handleConnection(socket);
                                } finally {
                                    socket.close();
                                }
                            } catch (IOException ioe) {
                                ioe.printStackTrace(); // but do not stop serving
                            }
                        }
                    });

                    handler.start();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
    }

}