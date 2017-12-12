package performance;

import global.Global;
import net.sf.antcontrib.antserver.server.Server;
import simpledb.IntField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Created by aditisri on 12/11/17.
 */

/**
 * Meant to help figure out how to improve performance. Can test latency with different number of tuples and different tuple sizes
 */
public class LatencyTest {
    private long totalLatency;
    private int numTuples;
    private int numFields;
    private int numChildNodes;
    private int[] ports;
    private String randomTupleString = null;
    private boolean isComplete;
    private List<ServerSocket> sockets;

    public LatencyTest(int numTuples, int numFields, int numChildNodes){
        this.totalLatency = 0;
        this.numTuples = numTuples;
        this.numFields = numFields;
        this.numChildNodes = numChildNodes;
        this.ports = new int[numChildNodes];
        this.isComplete = false;
        setRandomTupleString();
        setPorts();
        this.sockets = new ArrayList<>();
    }

    public boolean isComplete(){
        return this.isComplete;
    }

    public synchronized void closeServerSockets(){
        for (ServerSocket s:sockets){
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        sockets = new ArrayList<>();
    }

    private void setPorts(){
        for (int i = 0; i<numChildNodes; i++){
            ports[i] = 8000+i+1;
        }
    }

    private void setRandomTupleString() {
        Type[] types = new Type[numFields];
        for (int i=0; i<numFields;i++){
            types[i] = Type.INT_TYPE;
        }
        TupleDesc td = new TupleDesc(types);
        Tuple testTuple = new Tuple(td);
        for (int i = 0; i < numFields; i++) {
            testTuple.setField(i, new IntField(i));
        }
        randomTupleString = testTuple.toString();
    }

    public synchronized void incrementLatency(long delta) {
        totalLatency += delta;
    }

    public long getAverageLatency() {
        return totalLatency / numTuples;
    }

    public void startServer(int port) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket server = new ServerSocket(port);
                    sockets.add(server);
                    while (true) {
                        Socket sSocket = server.accept();
                        Thread handler = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    BufferedReader in = new BufferedReader(new InputStreamReader(sSocket.getInputStream()));
                                    PrintWriter out = new PrintWriter(sSocket.getOutputStream(), true);
                                    for (int i = 0; i < numTuples/numChildNodes; i++) {
                                        out.println(randomTupleString);
                                    }
                                    out.println("DONE");
                                    out.close();
                                    in.close();
                                    sSocket.close();

                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                            }
                        });
                        handler.start();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    public void printStats() {
        System.out.println("Total Latency(rtt) " + totalLatency + "ns");
        System.out.println("Avg. Latency(rtt) " + getAverageLatency() + "ns");
    }

    public void initializeServers(){
        for (int port: ports){
            startServer(port);
        }
    }

    public void runTest(){
        initializeServers();
        Thread[] threads = new Thread[ports.length];
        int counter = 0;
        for (int port : ports) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Socket cSocket = new Socket(Global.LOCALHOST, port);
                        PrintWriter out = new PrintWriter(cSocket.getOutputStream(), true);
                        long startTime = System.nanoTime();
                        out.println("START");
                        BufferedReader in = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));

                        for (String line = in.readLine(); !line.equals("DONE"); line = in.readLine()) {
                            // just wait
                        }

                        long endTime = System.nanoTime();
                        incrementLatency(endTime - startTime);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            threads[counter] = t;
            counter ++;
            t.start();
        }

        for (Thread t: threads){
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        closeServerSockets();
        this.isComplete = true;
        printStats();
        writeStatsToFile();
    }

    public void writeStatsToFile(){
        String stats = "" + numTuples + " " + numFields + " " + numChildNodes + " " + getAverageLatency() + " " + totalLatency;
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("src/java/performance/latencyTestResults.txt", true));
            bw.write(stats);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public static void main(String[] args) {
        List<LatencyTest> tests = new ArrayList<>();
        new LatencyTest(1, 2, 1);
        // Increase number of tuples
//        tests.add(new LatencyTest(1, 10, 1));
//        tests.add(new LatencyTest(10, 10, 1));
//        tests.add(new LatencyTest(100, 10, 1));
//        tests.add(new LatencyTest(1000, 10, 1));
//        tests.add(new LatencyTest(10000, 10, 1));

//        // Increase number of fields
//        tests.add(new LatencyTest(1000, 1, 1));
//        tests.add(new LatencyTest(1000, 10, 1));
//        tests.add(new LatencyTest(1000, 100, 1));
//        tests.add(new LatencyTest(1000, 1000, 1));
//        tests.add(new LatencyTest(1000, 10000, 1));
//
//        // Increase number of child nodes
//        tests.add(new LatencyTest(10000, 100, 1));
//        tests.add(new LatencyTest(10000, 100, 5));
//        tests.add(new LatencyTest(10000, 100, 10));
//        tests.add(new LatencyTest(10000, 100, 20));
        tests.add(new LatencyTest(10000, 100, 50));


        for (LatencyTest test : tests){
            test.runTest();
            while(!test.isComplete()){
                //just wait
            }
        }

        System.exit(1);
    }
}


