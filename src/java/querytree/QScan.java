package querytree;

import global.Global;
import main.SingleMachineTest;
import networking.NodeServer;
import simpledb.*;

class QScan implements QueryTree {
    private final String tableName;
    private final String tableAlias;

    QScan(String name, String alias) {
        this.tableName = name;
        this.tableAlias = alias;
    }

    public OpIterator getRootOp(NodeServer node){
        return new SeqScan(Global.TRANSACTION_ID, node.getTableId(tableName), this.tableAlias);
    }

    @Override
    public String toString() {
        return "SCAN(" + tableAlias + ")";
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
