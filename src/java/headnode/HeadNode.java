package headnode;

import querytree.QueryParser;
import querytree.QueryTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import edu.mit.eecs.parserlib.UnableToParseException;

/**
 * The class running on the head node, accepting user command (CLI),
 * and communicate with the nodes through TCP protocol.<br>
 * Note: This class is not supposed to use anything from package simpledb, the only
 * way to communicate with it should be a network protocol. (Correct it if this is wrong)
 */
public class HeadNode {
    
    private static final String LOCALHOST = "127.0.0.1";

    private final List<String> childrenIps = new ArrayList<>();
    private final List<Integer> childrenPorts = new ArrayList<>();
    private Result result;

    public HeadNode(){
        this.result = null;
    }
    
    /**
     * Add a child node to the head node
     * @param childIp the child IP address represented by a String
     * @param childPort the port of the child node
     */
    public void addChildNode(String childIp, int childPort) {
        // TODO: check format of the child IP
        childrenIps.add(childIp);
        childrenPorts.add(childPort);
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
                    QueryTree qt = QueryParser.parse(input);
                    processQuery(qt);
                } catch (UnableToParseException e) {
                    System.out.println("Wrong syntax. Type 'help' for help.");
                }
            }
        }
    }

    public Result processQuery(QueryTree queryTree){
        this.result = new Result(queryTree);
        for (int i = 0; i < childrenIps.size(); i++){
            final String ip = childrenIps.get(i);
            final int port = childrenPorts.get(i);
            Thread t = new Thread(new ChildConnection(ip, port, queryTree));
            t.start();
        }
        return this.result;
    }



    private class ChildConnection implements Runnable {
        private final String childIp;
        private final int childPort;
        private final QueryTree queryTree;
        
        public ChildConnection(String childIp, int childPort, QueryTree queryTree){
            this.childIp = childIp;
            this.childPort = childPort;
            this.queryTree = queryTree;
        }
        
        @Override
        public void run() {
            try {
            Socket s = new Socket(childIp, childPort);
            // TODO: Maybe some timeout here
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println(queryTree.toString());
            
            for (String line = in.readLine(); !line.equals("END"); line = in.readLine()) {
                // TODO: synchronized control
                System.out.println(line);
                result.merge(line);
            }
            } catch (IOException e) {
                throw new RuntimeException(e);
                // TODO: Some error handling
            }
        }
    }

    /**
     * The main function to start head node and accepting client's requests
     * @param args TODO
     */
    public static void main(String[] args) {
        // TODO: command line arguments for children ips/ports parse format: #.#.#.#:# using regex
        HeadNode head = new HeadNode();
        head.addChildNode(LOCALHOST, 4444); //TODO: Just for test purpose
        try {
            head.getInput();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
