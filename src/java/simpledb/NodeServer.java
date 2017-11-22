package simpledb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import edu.mit.eecs.parserlib.UnableToParseException;
import querytree.QueryParser;
import querytree.QueryTree;

/**
 * Server class interact with the including database of the node and
 * communicate with outside (the head node, or direct communication from a user)
 * through TCP protocol
 */
public class NodeServer {
    private final ServerSocket serverSocket;
    
    public NodeServer(int portNumber) throws IOException {
        serverSocket = new ServerSocket(portNumber);
    }
    
    /**
     * Start listening to the port and accepting income connections
     * @throws IOException if an error occurs waiting for a connection
     */
    public void startListen() throws IOException {
        while (true) {
            final Socket socket = serverSocket.accept();
            
            Thread handler = new Thread(new Runnable(){
                @Override
                public void run() {
                    try {
                        try {
                            handleConnection(socket);
                        }
                        finally {
                            socket.close();
                        }
                    } catch (IOException ioe) {
                        ioe.printStackTrace(); // but do not stop serving
                    }
                }
            });
            
            handler.start();
        }
    }
    
    /**
     * handle connection between this node and client
     * @param socket the socket connecting between this node and the client
     * @throws IOException if the connection encounters an error
     */
    private void handleConnection(Socket socket) throws IOException {
        System.err.println("Client from " + socket.getInetAddress().toString() + ":"
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
                    // Some trash for now. TODO: run the query and return the results
                    out.println("1,2,3");
                    out.println("4,5,6");
                    out.println("7,8,9");
                    out.println("It's garbage now!");
                } catch (UnableToParseException e) {
                    out.println("Unable to parse your command!"); // TODO: More information
                }
            }
        } finally {
            System.err.println("Client from " + socket.getInetAddress().toString() + ":"
                    + socket.getPort() + " is leaving.");
            out.close();
            in.close();
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

}
