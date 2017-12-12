package performance;

import simpledb.IntField;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

import javax.net.ssl.SSLContext;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by aditisri on 12/11/17.
 */
public class FastSerializationTest {
    private int numFields;
    private Tuple testTuple;

    public FastSerializationTest(int numFields){
        this.numFields = numFields;
        this.testTuple = generateTuple();
    }

    public Tuple generateTuple(){
        Type[] types = new Type[numFields];
        for (int i = 0; i<numFields; i++){
            types[i] = Type.INT_TYPE;
        }
        TupleDesc td = new TupleDesc(types);
        Tuple t = new Tuple(td);
        for (int i =0; i<numFields; i++){
            t.setField(i, new IntField(i));
        }
        return t;
    }

    public long slowTime(){
        long start = System.nanoTime();
        testTuple.toString();
        long end = System.nanoTime();
        return end-start;
    }

    public long fastTime(){
        long start = System.nanoTime();
        testTuple.fastToString();
        long end = System.nanoTime();
        return end-start;
    }

    public void writeResultsToFile(){
        String results = numFields + " " + slowTime() + " " + fastTime();
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("src/java/performance/toStringResults.txt", true));
            bw.write(results);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void runTest(){
        writeResultsToFile();
    }


    public static void main(String[] args){
        List<FastSerializationTest> tests = new ArrayList<>();
        tests.add(new FastSerializationTest(1));
        tests.add(new FastSerializationTest(10));
        tests.add(new FastSerializationTest(100));
        tests.add(new FastSerializationTest(1000));
        tests.add(new FastSerializationTest(10000));
        for(FastSerializationTest test : tests){
            test.runTest();
        }

    }
}
