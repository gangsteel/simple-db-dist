package simpledb;

import java.io.IOException;
import java.util.Arrays;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId t;
    private OpIterator child;
    private int numDeleted = 0;
    private TupleIterator result;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }
    
    /**
     * Delete all the Tuples in the File
     * @throws DbException when there are problems opening/accessing the database.
     * @throws TransactionAbortedException if transcation aborted
     */
    private void deleteAll() throws DbException, TransactionAbortedException {
        child.open();
        while(child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(t, child.next());
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("I/O error detected reading the file");
            }
            numDeleted++;
        }
        child.close();
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        deleteAll();
        final Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(numDeleted));
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
