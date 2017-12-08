package networking;

import global.Global;
import querytree.QueryParser;
import querytree.QueryTree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.mit.eecs.parserlib.UnableToParseException;


/**
 * The class running on the head node, accepting user command (CLI),
 * and communicate with the nodes through TCP protocol.<br>
 * Note: This class is not supposed to use anything from package simpledb, the only
 * way to communicate with it should be a network protocol. (Correct it if this is wrong)
 */
public class HeadNode {
    
    private final List<String> childrenIps = new ArrayList<>();
    private final List<Integer> childrenPorts = new ArrayList<>();
    private static final Logger LOGGER = Logger.getLogger(HeadNode.class.getName());

    public HeadNode(){
    }
    
    /**
     * Add a child node to the head node
     * @param childIp the child IP address represented by a String
     * @param childPort the port of the child node
     */
    public void addChildNode(String childIp, int childPort) {
        childrenIps.add(childIp);
        childrenPorts.add(childPort);
    }
    
    /**
     * Broadcast the existence of all the children to all the children
     */
    public void broadcastChilds() {
        final List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < childrenIps.size(); i++){
            final String ip = childrenIps.get(i);
            final int port = childrenPorts.get(i);
            
            final int currentIndex = i; // To mute java error
            
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    Socket s;
                    try {
                        s = new Socket(ip, port);
                        PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                        String message = "machines:";
                        for (int j = 0; j < childrenIps.size(); j++) {
                            if (j != currentIndex) {
                                message = message + childrenIps.get(j) + ":" + childrenPorts.get(j) + ";";
                            }
                        }
                        out.println(message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    
                }
            });
            
            t.start();
            workers.add(t);
        }
        workers.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public void removeChildNode(String childIp, int childPort){
        for(int i=0; i< childrenPorts.size(); i++){
            Thread t = new Thread(new DeleteNodeRequest(childrenIps.get(i), childrenPorts.get(i), childIp, childPort));
            t.start();
        }
        childrenIps.remove(childIp);
        childrenPorts.remove(new Integer(childPort));
    }

    /**
     * Start the command line interface to accept client typing queries
     * @throws IOException if the I/O is interrupted
     */
    public void getInput() throws IOException{
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("SimpleDist> ");
            final String input = in.readLine();
            if (input.equals("exit")) {
                return;
            } else if (input.equals("help")) {
                System.out.println("Hello there! The programmer are still helping themselves.");
                // TODO: Some help messages
            } else {
                try {
                    QueryTree qt = QueryParser.parse(null, input); // null Node does not affect the string
                    processQuery(qt);
                } catch (UnableToParseException e) {
                    System.out.println("Wrong syntax. Type 'help' for help.");
                }
            }
        }
    }

    /**
     * Handles the given query. This will relay the request to all nodes and collect their responses. This method waits
     * for all nodes to finish their response before terminating.
     * @param queryTree query
     * @return
     */
    public void processQuery(QueryTree queryTree){
        final long startTime = System.nanoTime();
        AggregateResult aggResult = new AggregateResult(queryTree);

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < childrenIps.size(); i++){
            final String ip = childrenIps.get(i);
            final int port = childrenPorts.get(i);
            Thread t = new Thread(new NodeRequestWorker(ip, port, queryTree, new Function<String, Void>() {
                @Override
                public Void apply(String s) {
                    System.out.println(s);
                    if (queryTree.getRootType() == "AGGREGATE"){
                        aggResult.merge(s);
                    }
//                    result.merge(s);
                    return null;
                }
            }));
            t.start();
            workers.add(t);
        }
        workers.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        if(queryTree.getRootType() == "AGGREGATE"){
            System.out.println("Aggregate result: ");
            aggResult.printResult();
        }
        final long endTime = System.nanoTime();
        final long duration = endTime - startTime;
        System.out.println("Query time: " + (double)duration / 1000000.0 + "ms.");
    }

    public void addChildNodesFromFile(String fileName) {
        try ( final BufferedReader fileReader = new BufferedReader(new FileReader(new File(fileName))) ) {
            String line;
            while ( (line = fileReader.readLine()) != null ) {
                final Matcher matcher = Pattern.compile(Global.IP_PORT_REGEX).matcher(line);
                if (!matcher.matches()) {
                    throw new RuntimeException("Wrong format of IP and port: #.#.#.#:#");
                }
                // System.out.println(matcher.group(1)); System.out.println(matcher.group(2));
                addChildNode(matcher.group(1), Integer.parseInt(matcher.group(2)));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found in config/head directory!");
        } catch (IOException e) {
            throw new RuntimeException("IO error during reading the file");
        }
    }

    /**
     * The main function to start head node and accepting client's requests
     * @param args String array with length of 1, representing the file name for
     * head node configuration. The file should be located in config/head directory
     */
    public static void main(String[] args) {
        final String fileName = "config/head/" + args[0];
        HeadNode head = new HeadNode();
        head.addChildNodesFromFile(fileName);
        try {
            head.broadcastChilds();
            head.getInput();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
