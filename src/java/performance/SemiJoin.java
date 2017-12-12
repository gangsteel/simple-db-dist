package performance;

import distributeddb.GlobalSeqScan;
import distributeddb.Profiler;
import global.Global;
import networking.Machine;
import networking.NodeRequestWorker;
import networking.NodeServer;
import querytree.QueryTree;
import simpledb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * Created by aditisri on 12/12/17.
 */
public class SemiJoin {
    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private NodeServer node;
    private String tableName;
    private String tableAlias;

    // Fields for outputting tuples
    private DbFileIterator tupleIterator;
    private SemiJoin.TupleQueue queue;

    public SemiJoin(TransactionId tid, NodeServer node, String tableName, String tableAlias){
        this.tid = tid;
        this.node = node;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.tupleIterator = Database.getCatalog().getDatabaseFile(Database.getCatalog().getTableId(tableName)).iterator(tid);

    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        tupleIterator.open();
        resetQueuing();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        final TupleDesc originTd = Database.getCatalog().getTupleDesc(Database.getCatalog().getTableId(tableName));
        // Now addTuple the prefix
        final int length = originTd.numFields();
        final Type[] typeAr = new Type[length];
        final String[] fieldAr = new String[length];
        for (int i = 0; i < length; i++) {
            typeAr[i] = originTd.getFieldType(i);
            fieldAr[i] = tableAlias + "." + originTd.getFieldName(i);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return tupleIterator.hasNext() || queue.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (tupleIterator.hasNext()) {
            Tuple t = tupleIterator.next();
            return t;
        }
        else {
            Tuple t = queue.next();
            //System.out.println("GlobalSeqScan: " + t);
            return t;
        }
    }

    public void close() {
        tupleIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        tupleIterator.rewind();
        resetQueuing();
    }

    private void resetQueuing() {
        queue = new SemiJoin.TupleQueue();
        List<Machine> references = node.getReferences();
        for (Machine machine : references) {
            NodeRequestWorker worker = new NodeRequestWorker(machine.ipAddress, machine.port,
                    QueryTree.scan(null, tableName, tableAlias), new Function<String, Void>() {
                @Override
                public Void apply(String s) {
                    // TODO: fix the parsing
                    long tend = System.nanoTime();
                    if (s.startsWith("TIME")){
                        long start = Long.parseLong(s.split(" ")[1]);
                        Global.PROFILER.incrementType(Profiler.Type.TRANSFER, tend-start);
                    }
                    String[] parsed = s.split(" ");
                    Tuple t = new Tuple(Database.getCatalog().getTupleDesc(Database.getCatalog().getTableId(tableName)));

                    for (int i = 0; i < parsed.length; i++) {
                        t.setField(i, new IntField(Integer.parseInt(parsed[i])));
                    }
                    try {
                        queue.addTuple(t);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
            Thread thread = new Thread(worker);
            queue.addWorker(thread);
            long t1 = System.nanoTime();
            thread.start();
            long t2 = System.nanoTime();
            Global.PROFILER.incrementType(Profiler.Type.SOCKET, t2-t1);
        }
    }

    private class TupleQueue {
        private final BlockingQueue<Tuple> queue;
        private List<Thread> workers;

        public TupleQueue() {
            queue = new LinkedBlockingQueue<>(100);
            workers = new ArrayList<>();
        }

        public void addTuple(Tuple tuple) throws InterruptedException {
            queue.put(tuple);
        }

        public void addWorker(Thread thread) {
            workers.add(thread);
        }

        public boolean hasNext() {
            while (queue.isEmpty() && !allWorkersDone()) {
                // block while it is possible for the queue to fetch tuples and it has no tuples
            }
            if (!queue.isEmpty()) return true;
            return !allWorkersDone();
        }

        public Tuple next() {
            return queue.poll();
        }

        private boolean allWorkersDone() {
            for (Thread worker : workers) {
                if (worker.isAlive()) return false;
            }
            return true;
        }
    }
}
