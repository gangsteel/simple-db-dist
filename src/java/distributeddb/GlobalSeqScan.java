package distributeddb;

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

public class GlobalSeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private NodeServer node;
    private String tableName;
    private String tableAlias;

    // Fields for outputting tuples
    private DbFileIterator tupleIterator;
    private TupleQueue queue;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public GlobalSeqScan(TransactionId tid, NodeServer node, String tableName, String tableAlias) {
        this.tid = tid;
        this.node = node;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.tupleIterator = Database.getCatalog().getDatabaseFile(node.getTableId(tableName)).iterator(tid);
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
        final TupleDesc originTd = Database.getCatalog().getTupleDesc(node.getTableId(tableName));
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
        queue = new TupleQueue();
        List<Machine> references = node.getReferences();
        for (Machine machine : references) {
            NodeRequestWorker worker = new NodeRequestWorker(machine.ipAddress, machine.port,
                    QueryTree.scan(null, tableName, tableAlias), new Function<String, Void>() {
                @Override
                public Void apply(String s) {
                    // TODO: fix the parsing
                    String[] parsed = s.split(" ");
                    Tuple t = new Tuple(Database.getCatalog().getTupleDesc(node.getTableId(tableName)));
                    for (int i = 0; i < parsed.length; i++) {
                        t.setField(i, new IntField(Integer.parseInt(parsed[i])));
                    }
                    queue.addTuple(t);
                    return null;
                }
            });
            Thread thread = new Thread(worker);
            queue.addWorker(thread);
            thread.start();
        }
    }

    private class TupleQueue {
        private final BlockingQueue<Tuple> queue;
        private List<Thread> workers;

        public TupleQueue() {
            queue = new LinkedBlockingQueue<>(100);
            workers = new ArrayList<>();
        }

        public void addTuple(Tuple tuple) {
            queue.add(tuple);
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
