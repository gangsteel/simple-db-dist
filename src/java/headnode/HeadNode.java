package headnode;

import childnode.ChildNode;
import querytree.QueryParser;
import querytree.QueryTree;
import simpledb.LogicalPlan;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;

/**
 * The class running on the head node, accepting user command (CLI),
 * and communicate with the nodes through TCP protocol.<br>
 * Note: This class is not supposed to use anything from package simpledb, the only
 * way to communicate with it should be a network protocol. (Correct it if this is wrong)
 */
public class HeadNode {

    private class ChildConnection implements Runnable{
        private ChildNode child;
        private QueryTree queryTree;
        public ChildConnection(ChildNode child, QueryTree queryTree){
            this.child = child;
            this.queryTree = queryTree;
        }
        @Override
        public void run() {
            try {
                Socket s = new Socket("localhost", this.child.getPort());
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                out.writeObject(this.queryTree);
                ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                //TODO: set timeout for reading
                int count = 0;
                while (count < 100) {
                    //TODO: cast this to whatever it's supposed to be
                    Object o = in.readObject();
                    synchronized(this) {
                        mergeResults(o);
                    }
                    count ++;
                }
            }
            catch(IOException e){
                e.printStackTrace();
                return;
            }
            catch(ClassNotFoundException e){
                e.printStackTrace();
                return;
            }
            finally{
                synchronized (this){
                    numComplete++;
                    if (numComplete == children.size()){
                        finishProcessing();

                    }
                }

            }
        }
    }

    //FIXME: I'm not entirely sure why we have so many separate packages?
    //TODO: look into rpc frameworks
    private final List<ChildNode> children = new ArrayList<>();
    private QueryParser parser;
    private int numComplete;
    public HeadNode(){
        this.parser = new QueryParser();
        this.numComplete = 0;
    }

    public String getInput(){
        //TODO: Make sure input is valid... maybe do this in the QueryParser
        Scanner s = new Scanner(System.in);
        String query = "";
        while(s.hasNextLine()){
            query += s.nextLine();
        }
        return query;
    }

    // maintain a list of child nodes and
    public QueryTree generateTree(String query){
        QueryTree tree = this.parser.parse(query);
        return tree;
    }

    public void processQuery(){
        //TODO: make query plan class or figure out how this is handled in simpledb... Logical plan??
        this.numComplete  = 0;
        String query = this.getInput();
        QueryTree queryTree = this.generateTree(query);
        for (ChildNode node: this.children){
            Thread t = new Thread(new ChildConnection(node, queryTree));
            t.start();
        }
    }

    public void mergeResults(Object o){

    }

    public void finishProcessing(){

    }

    // Constructor
    // Command line user interface

    // TODO: for writing (later)
    public ChildNode determineDestinationNode(){
        return null;
    }

}
