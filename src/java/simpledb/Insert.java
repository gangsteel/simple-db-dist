package simpledb;

import java.io.IOException;
import java.util.Arrays;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private OpIterator child;
    private final int tableId;
    private int numInserted = 0;
    private TupleIterator result;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }
    
    /**
     * Insert all the Tuples to the File
     * @throws TransactionAbortedException if transcation aborted
     * @throws DbException when there are problems opening/accessing the database.
     */
    private void insertAll() throws DbException, TransactionAbortedException {
        child.open();
        while(child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(t, tableId, child.next());
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("I/O error detected reading the file");
            }
            numInserted++;
        }
        child.close();
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        insertAll();
        final Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(numInserted));
        result = new TupleIterator(getTupleDesc(), Arrays.asList(tuple));
        result.open();
    }

    public void close() {
        super.close();
        result.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        result.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (result.hasNext()) {
            return result.next();
        } else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        assert children.length == 1 : "Incorrent length of OpIterator[]!";
        child = children[0];
    }
}
