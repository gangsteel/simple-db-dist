package querytree;

import distributeddb.GlobalSeqScan;
import global.Global;
import main.SingleMachineTest;
import networking.NodeServer;
import simpledb.*;

class QScan implements QueryTree {
    private final NodeServer node;
    private final String tableName;
    private final String tableAlias;
    private boolean isGlobal;

    QScan(NodeServer node, String name, String alias) {
        this.node = node;
        this.tableName = name;
        this.tableAlias = alias;
    }

    public OpIterator getRootOp(){
        if (!isGlobal) {
            return new SeqScan(Global.TRANSACTION_ID, Database.getCatalog().getTableId(tableName), this.tableAlias);
        }
        else {
            return new GlobalSeqScan(Global.TRANSACTION_ID, node, tableName, tableAlias);
        }
    }

    @Override
    public String getRootType(){
        return "SCAN";
    }

    @Override
    public void setIsGlobal(boolean isGlobal) {
        this.isGlobal = isGlobal;
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
