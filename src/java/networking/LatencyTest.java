package networking;

import global.Global;
import simpledb.IntField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by aditisri on 12/11/17.
 */

/**
 * Meant to help figure out how to improve performance. Can test latency with different number of tuples and different tuple sizes
 */
public class LatencyTest {
    static long totalLatency = 0;
    static int numTuples = 10000;
    static int numFields = 1000;
    static int numChildNodes = 3;
    static int[] ports = new int[numChildNodes];
    static String randomTupleString = null;

    public static void setPorts(){
        for (int i = 0; i<numChildNodes; i++){
            ports[i] = 8000+i+1;
        }
    }

    public static void setRandomTupleString() {
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

    public static synchronized void incrementLatency(long delta) {
        totalLatency += delta;
    }

    public static long getAverageLatency(int numTuples) {
        return totalLatency / numTuples;
    }

    public static void startServer(int port) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket server = new ServerSocket(port);
                    while (true) {
                        Socket sSocket = server.accept();
                        Thread handler = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    BufferedReader in = new BufferedReader(new InputStreamReader(sSocket.getInputStream()));
                                    PrintWriter out = new PrintWriter(sSocket.getOutputStream(), true);
                                    for (int i = 0; i < numTuples; i++) {
                                        out.println(randomTupleString);
                                    }
                                    out.println("DONE");

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

    public static void printStats() {
        System.out.println("Total Latency(rtt) " + totalLatency + "ns");
        System.out.println("Avg. Latency(rtt) " + getAverageLatency(numTuples) + "ns");
    }


    public static void main(String[] args) {
        setRandomTupleString();
        setPorts();

        for(int port:ports){
            startServer(port);
        }

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
                            System.out.println(line);
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
        printStats();
        System.exit(1);
    }
}


