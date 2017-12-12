package performance;

import global.Global;
import global.Utils;
import simpledb.IntField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

import java.io.*;
import java.lang.instrument.Instrumentation;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by aditisri on 12/12/17.
 */
public class OutputStreamStringTest {
    private Tuple testTuple;
    private int numFields;
    private int port;
    public OutputStreamStringTest(int numFields, int port){
        this.numFields = numFields;
        this.port = port;
        testTuple = null;
        setRandomTuple();


    }

    private void setRandomTuple() {
        Type[] types = new Type[numFields];
        for (int i=0; i<numFields;i++){
            types[i] = Type.INT_TYPE;
        }
        TupleDesc td = new TupleDesc(types);
        testTuple = new Tuple(td);
        for (int i = 0; i < numFields; i++) {
            testTuple.setField(i, new IntField(i));
        }
    }

    class ObjectStreamServerHandler implements Runnable{
        private Socket s;
        public ObjectStreamServerHandler(Socket s){
            this.s = s;
        }

        @Override
        public void run() {
            try {
                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                out.writeObject(testTuple);
                out.writeObject("DONE");
                out.close();
                s.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    class StringServerHandler implements Runnable{
        private Socket s;
        public StringServerHandler(Socket s){
            this.s = s;
        }

        @Override
        public void run() {
            try {
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println(testTuple.fastToString());
                out.println("DONE");
                out.close();
                s.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    class ObjectStreamClientHandler implements Runnable{
        private Socket s;
        public ObjectStreamClientHandler(Socket s){
            this.s = s;
        }

        @Override
        public void run() {
             try {
                 ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 long startTime = System.nanoTime();
                 out.writeObject("START");
                 ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                    try {
                        for (Object line = in.readObject(); line != null; line = in.readObject()) {
                            if (line instanceof String && line.equals("DONE")){
                                break;
                            }
                            Tuple sentTup = (Tuple) line;
                        }
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }

                    long endTime = System.nanoTime();
                    writeResultToFile(endTime-startTime, false);

                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }
    class StringClientHandler implements Runnable{
        private Socket s;
        public StringClientHandler(Socket s){
            this.s = s;
        }

        @Override
        public void run() {
            try {
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                long startTime = System.nanoTime();

                out.println("START");
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                for (String line = in.readLine(); !line.equals("DONE"); line = in.readLine()) {
                    Tuple t = Utils.stringToTuple(testTuple.getTupleDesc(), line);
                }

                long endTime = System.nanoTime();
                writeResultToFile(endTime-startTime, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void startServer(int port, boolean tupleIsString) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket server = new ServerSocket(port);
                    while (true) {
                        Socket sSocket = server.accept();
                        Thread handler = null;
                        if(tupleIsString){
                            handler = new Thread(new StringServerHandler(sSocket));
                        }
                        else{
                            handler = new Thread(new ObjectStreamServerHandler(sSocket));
                        }
                        handler.start();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }
    public void runTest(boolean tupleIsString){
        this.startServer(port, tupleIsString);
        try {
            Socket cSocket = new Socket(Global.LOCALHOST, port);
            Thread t = null;
            if(tupleIsString){
                t = new Thread(new StringClientHandler(cSocket));
            }
            else{
                t = new Thread(new ObjectStreamClientHandler(cSocket));
            }
            t.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeResultToFile(long time, boolean tupleIsString){
        String results = "" + this.numFields + " " + tupleIsString + " " + time;
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("src/java/performance/streamResults.txt", true));
            bw.write(results);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args){
        OutputStreamStringTest test = new OutputStreamStringTest(60000, 8001);
        test.runTest(true);

    }
}



