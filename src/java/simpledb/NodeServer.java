package simpledb;

import java.net.Socket;

/**
 * Server class interact with the including database of the node and
 * communicate with outside (the head node, or direct communication from a user)
 * through TCP protocol
 */
public class NodeServer {
    private final int portNumber;
    
    public NodeServer(int portNumber) {
        // More inputs? Like the table maybe
        this.portNumber = portNumber;
    }
    
    /**
     * Start listening to the port and accepting income connections
     */
    public void startListen() {
        throw new RuntimeException("not implemented");
    }
    
    /**
     * handle connection between this node and client
     * @param socket the socket connecting between this node and the client
     */
    private void handleConnection(Socket socket) {
        // Need 1) a parser; 2) I/O through socket
        throw new RuntimeException("not implemented");
    }

}
