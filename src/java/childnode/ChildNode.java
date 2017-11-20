package childnode;

import com.sun.tools.doclets.internal.toolkit.util.DocFinder;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import querytree.QueryTree;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by aditisri on 11/18/17.
 */
public class ChildNode {

    private InetAddress address;
    private int port;
    private ServerSocket serverSocket;
    private Socket clientSocket;
    //FIXME: Is ths right??? Do we want to assign dynamically or on creation?
    public ChildNode(InetAddress address, int port){
        this.address = address;
        this.port = port;
        // if ths socket can't be opened, set to null
        this.serverSocket = this.getServerSocket();
        this.clientSocket = this.getClientSocket();
        this.listenForRequests();

    }

    private void listenForRequests(){
        try {
            ObjectInputStream in = new ObjectInputStream(this.clientSocket.getInputStream());
            QueryTree queryTree = (QueryTree) in.readObject();
            processQuery(queryTree);
        }
        catch(IOException e){
            e.printStackTrace();
            System.out.println("Couldn't read query tree port " + this.port);
        }
        catch(ClassNotFoundException e){
            e.printStackTrace();
            System.out.println("Couldn't read query tree port " + this.port);
        }
    }

    private void processQuery(QueryTree queryTree){
        try {
            ObjectOutputStream out = new ObjectOutputStream(this.clientSocket.getOutputStream());
        }
        catch(IOException e){
            System.out.println("Coudldn't write to socket port " + this.port);
            return;
        }
        /* whenever you hit a valid tuple, write it to the out stream */

    }

    public InetAddress getAddress(){
        return this.address;
    }

    public int getPort(){
        return this.port;
    }

    public boolean serverSocketIsOpen(){
        return this.serverSocket != null;
    }

    private ServerSocket getServerSocket() {
        try {
            return new ServerSocket(this.port);
        }
        catch(IOException e){
            System.out.println("Couldn't open socket at port " + this.port);
            return null;
        }
    }

    private Socket getClientSocket(){
        if (this.serverSocketIsOpen()){
            try {
                return this.serverSocket.accept();
            }
            catch(IOException e){
                System.out.println("Couldn't accept connection on port " + this.port);
                return null;
            }
        }
        return null;
    }




}
