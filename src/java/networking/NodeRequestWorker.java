package networking;

import querytree.QueryTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.function.Function;

public class NodeRequestWorker implements Runnable {
    private final Function<String, Void> lineHandler;
    private final Function<Void, Void> requestFinishedHandler;
    private final String childIp;
    private final int childPort;
    private final QueryTree queryTree;

    public NodeRequestWorker(String childIp, int childPort, QueryTree queryTree){
        this(childIp, childPort, queryTree, new Function<String, Void>() {
            @Override
            public Void apply(String s) {
                System.out.println(s);
                return null;
            }
        }, new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                return null;
            }
        });
    }

    public NodeRequestWorker(String childIp, int childPort, QueryTree queryTree, Function<String, Void> lineHandler){
        this(childIp, childPort, queryTree, lineHandler, new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                return null;
            }
        });
    }

    public NodeRequestWorker(String childIp, int childPort, QueryTree queryTree, Function<String, Void> lineHandler,
                             Function<Void, Void> requestFinishedHandler){
        this.lineHandler = lineHandler;
        this.childIp = childIp;
        this.childPort = childPort;
        this.queryTree = queryTree;
        this.requestFinishedHandler = requestFinishedHandler;
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
                lineHandler.apply(line);
            }
            requestFinishedHandler.apply(null);
        } catch (IOException e) {
            throw new RuntimeException(e);
            // TODO: Some error handling
        }
    }
}
