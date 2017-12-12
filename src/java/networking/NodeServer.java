package networking;

import java.awt.*;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import distributeddb.GlobalSeqScan;
import distributeddb.Profiler;
import edu.mit.eecs.parserlib.UnableToParseException;
import global.Global;
import global.Utils;
import querytree.QAggregate;
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
    private List<Machine> references;
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

    private String getBaseTableName(String fullTableName){
        return fullTableName.replaceFirst("^" + this.id + ".", "");
    }

    /** End Catalog-like Method **/

    public void addReference(Machine machine) {
        references.add(machine);
    }
    
    /**
     * Add all machines by a String with format of ip:port;ip:port;...;ip:port
     * @param machines the String representing all the references
     */
    private void addAllReferences(String machines) {
        if (machines.equals("")) {
            return;
        }
        final String[] machineArray = machines.split(";");
        references = new ArrayList<>();
        for (String machine : machineArray) {
            final Matcher matcher = Pattern.compile(Global.IP_PORT_REGEX).matcher(machine);
            if (!matcher.matches()) {
                throw new RuntimeException("Wrong format of IP and port: #.#.#.#:#");
            }
            final Machine m = new Machine(matcher.group(1), Integer.parseInt(matcher.group(2)));
            addReference(m);
            LOGGER.log(Level.INFO, "Fellow child node " + m + " added.");
        }
    }

    public void removeReference(Machine machine){
        references.remove(machine);
    }

    public List<Machine> getReferences() {
        return new ArrayList<>(references);
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
    private void handleConnection(Socket socket) throws IOException,DbException, TransactionAbortedException {
        LOGGER.log(Level.INFO, "Client from " + socket.getInetAddress().toString() + ":"
                + socket.getPort() + " is connected. Local port: " + socket.getLocalPort() + ".");
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true); // TODO: should we autoFlush?
        Global.PROFILER.incrementType(Profiler.Type.PROCESSING, 50);

        try {
            for (String line = in.readLine(); line != null; line = in.readLine()) {
                if (line.equals("exit")) {
                    break; // A way to debug individual server
                } else if (line.startsWith("machines:")) {
                    addAllReferences(line.replaceFirst("machines:",""));
                    break;
                }
                if (line.startsWith("WRITE")){
                    String[] reqArr = line.split(" ");
                    String tableName= reqArr[reqArr.length-1];
                    String tupleStr = "";
                    boolean inTuple = false;
                    for(String s: reqArr){
                        if (s.equals("TABLE")){
                            inTuple = false;
                        }
                        if (inTuple){
                            tupleStr += " " + s;
                        }
                        if (s.equals("TUPLE")){
                            inTuple = true;
                        }
                    }
                    tupleStr = tupleStr.trim();
                    TupleDesc td = Database.getCatalog().getTupleDesc(Database.getCatalog().getTableId(tableName));
                    Tuple tup = Utils.stringToTuple(td, tupleStr);
                    int tableId = Database.getCatalog().getTableId(tableName);
                    Database.getBufferPool().insertTuple(Global.TRANSACTION_ID, tableId, tup);
                }
                else if (line.startsWith("DELETE NODE")){
                    String[] reqArr = line.split(" ");
                    String ip = reqArr[2];
                    String port = reqArr[3];
                    this.removeReference(new Machine(ip, Integer.parseInt(port)));
                    if (this.id.equals(port)){
                        this.terminate();
                    }
                }
                else {
                    try{
                        QueryTree qt = QueryParser.parse(this, line);
                        long t1 = System.nanoTime();
                        processQuery(qt, out);
                        long t2 = System.nanoTime();
                        System.out.println("processing time " + (t2-t1));
                        Global.PROFILER.incrementType(Profiler.Type.PROCESSING, t2-t1);
                    }catch (UnableToParseException e) {
                            out.println("Unable to parse your command!"); // TODO: More information
                    }
                }
                out.println("DONE");

            }
        } finally {
            LOGGER.log(Level.INFO, "Client from " + socket.getInetAddress().toString() + ":"
                    + socket.getPort() + " is leaving.");
            out.close();
            in.close();
        }
    }


    private void terminate() throws DbException, TransactionAbortedException{
        int numServers = this.references.size();
        List<Machine> sortedMachines = new ArrayList<>(this.references);
        Iterator<Integer> tableIdIterator = Database.getCatalog().tableIdIterator();
        Collections.sort(sortedMachines, new Comparator<Machine>(){
            @Override
            public int compare(Machine o1, Machine o2) {
                return (o1.port > o2.port) ? 1 : (o1.port < o2.port) ? -1 : 0;
            }
        });

        while(tableIdIterator.hasNext()){
            int tableId = tableIdIterator.next();
            String tableName = Database.getCatalog().getTableName(tableId);

            if (tableName.startsWith(this.id)){
                //TODO: idk what to use for alias. I just used name. Probs ok.
                String baseTableName = this.getBaseTableName(tableName);

                OpIterator scan = new SeqScan(Global.TRANSACTION_ID, tableId, baseTableName);
                scan.open();
                while(scan.hasNext()){
                    Tuple t = scan.next();
                    //TODO: do we want to hash on some particular attribute. Can maintain a map indicating which attribute we're hashing on
                    Machine destinaton = sortedMachines.get(t.hashCode()%(numServers-1));
                    Thread thread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Socket s = new Socket(destinaton.ipAddress, destinaton.port);
                                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                                out.println("WRITE " + "TUPLE " + t.fastToString() + " TABLE " + baseTableName);
                                out.println("DONE");
                                for (String line = in.readLine(); !line.equals("DONE"); in.readLine()) {
                                    //TODO: do something here probably
                                    System.out.println(line);
                                }
                                in.close();
                                out.close();
                                s.close();
                            }
                            catch(IOException e){
                                //TODO:handle error
                                e.printStackTrace();
                            }

                        }
                    });
                    thread.start();

                }
            }
        }




    }

    private boolean isAvgQuery(QueryTree queryTree){
        if (queryTree.getRootType() == "AGGREGATE"){
            QAggregate aggQuery = (QAggregate) queryTree;
            if(aggQuery.getAggregator() == Aggregator.Op.AVG){
                return true;
            }
        }
        return false;
    }

    private void processQuery(QueryTree queryTree, PrintWriter outputStream){
        if(isAvgQuery(queryTree)){
            QAggregate aggQuery = (QAggregate) queryTree;
            QueryTree q1 = QueryTree.aggregate(aggQuery.getChild(), aggQuery.getColNum(), Aggregator.Op.COUNT);
            QueryTree q2 = QueryTree.aggregate(aggQuery.getChild(), aggQuery.getColNum(), Aggregator.Op.SUM);
            OpIterator op1 = q1.getRootOp();
            OpIterator op2 = q2.getRootOp();
            int count = 0;
            int sum = 0;
            try {
                op1.open();
                op2.open();
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
            try {
                if (op1.hasNext() && op2.hasNext()){
                    count = ((IntField) op1.next().getField(0)).getValue();
                    sum = ((IntField) op2.next().getField(0)).getValue();
                    Type[] typeArr = {Type.INT_TYPE, Type.INT_TYPE};
                    String[] fieldArr = {"count", "sum"};
                    Tuple t = new Tuple(new TupleDesc(typeArr, fieldArr));
                    t.setField(0, new IntField(count));
                    t.setField(1, new IntField(sum));
                    outputStream.println(t.fastToString());
                    outputStream.println("END");
                }
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }

        }
        else {
            OpIterator op = queryTree.getRootOp();
            try {
                op.open();
            } catch (DbException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
            try {
                while (op.hasNext()) {
                    long t1 = System.nanoTime();
                    Tuple t = op.next();
                    long t2 = System.nanoTime();
                    System.out.println("processing " + (t2-t1));
                    Global.PROFILER.incrementType(Profiler.Type.PROCESSING, t2-t1);
                    outputStream.println("TIME: " + System.nanoTime());
                    outputStream.print(t.fastToString() + System.lineSeparator());
                }
                outputStream.println("END");
            } catch (DbException e) {
                LOGGER.log(Level.INFO, "There was an error processing query");
                e.printStackTrace();
                //TODO: do error handling
            } catch (TransactionAbortedException e) {
                LOGGER.log(Level.INFO, "Transaction aborted while processing query");
                e.printStackTrace();
                //TODO: do error handling

            }

        }
    }
    
    public static void main(String[] args) {
        final int port = Integer.parseInt(args[0]);
        try {
            NodeServer server = new NodeServer(port);
            Database.getCatalog().loadSchema("config/child/" + port + "/catalog.txt");
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
                                LOGGER.log(Level.INFO, "error io exception");
                            }
                            catch(DbException e){
                                e.printStackTrace();
                                LOGGER.log(Level.INFO, "error dbexception");
                            }
                            catch(TransactionAbortedException e){
                                e.printStackTrace();
                                LOGGER.log(Level.INFO, "error trans aborted exception");
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
