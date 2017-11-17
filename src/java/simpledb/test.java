package simpledb;
import java.io.*;

public class test {

    public static void main(String[] argv) {

        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test1");
        
        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), descriptor);
        Database.getCatalog().addTable(table2, "test2");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");
        Filter sf1 = new Filter(
                new Predicate(0, Predicate.Op.LESS_THAN, new IntField(100)), ss1);
        
        Join j = new Join(
                new JoinPredicate(0, Predicate.Op.EQUALS, 0), sf1, ss2);

        try {
            printOp(new SeqScan(tid, table1.getId(), "tt1"));
            printOp(new SeqScan(tid, table2.getId(), "tt2"));
            printOp(j);
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void printOp(OpIterator j) throws DbException, TransactionAbortedException {
        System.out.println(j.getTupleDesc());
        j.open();
        while (j.hasNext()) {
            Tuple tup = j.next();
            System.out.println(tup);
        }
        j.close();
    }

}

