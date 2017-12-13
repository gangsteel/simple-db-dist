package simpledb;

import java.util.NoSuchElementException;

/**
 * Created by aditisri on 12/12/17.
 */
public class SeqColScan implements OpIterator{
    private SeqScan fullScan;
    private int colNum;
    public SeqColScan(TransactionId tid, int tableid, String tableAlias, int colNum){
        this.fullScan = new SeqScan(tid, tableid, tableAlias);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.fullScan.open();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return this.fullScan.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Tuple nextTup = fullScan.next();
        Field relevantField = nextTup.getField(colNum);
        Type[] type = {nextTup.getTupleDesc().getFieldType(colNum)};
        TupleDesc td = new TupleDesc(type);
        Tuple colTup = new Tuple(td);
        colTup.setField(0, relevantField);
        return colTup;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        fullScan.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return fullScan.getTupleDesc();
    }

    @Override
    public void close() {
        fullScan.close();
    }
}
