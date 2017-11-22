package querytree;

import simpledb.OpIterator;
import simpledb.SeqScan;
import simpledb.TransactionId;

class QScan implements QueryTree {
    
    private final int tableid;
    private final String tableAlias;

    QScan(String alias, int id) {
        this.tableid = id;
        this.tableAlias = alias;
    }

    public int getTableId(){
        return 0;
    }


    public OpIterator getRootOp(){
        return new SeqScan(new TransactionId(), this.tableid, this.tableAlias);
    }


    @Override
    public String toString() {
        return "SCAN(" + tableid + ")";
    }
    
    @Override
    public boolean equals(Object obj) {
        throw new RuntimeException("not implemented");
    }
    
    @Override
    public int hashCode() {
        throw new RuntimeException("not implemented");
    }
}
