package querytree;

import distributeddb.GlobalSeqScan;
import global.Global;
import networking.NodeServer;
import performance.GlobalColSeqScan;
import simpledb.Database;
import simpledb.OpIterator;
import simpledb.SeqColScan;
import simpledb.SeqScan;

/**
 * Created by aditisri on 12/12/17.
 */
public class QSemiScan implements QueryTree{

    private final NodeServer node;
    private final String tableName;
    private final String tableAlias;
    private final boolean useSimpleDb;
    private boolean isGlobal;
    private int colNum;

    QSemiScan(NodeServer node, String name, String alias, int colNum, boolean useSimpleDb) {
        this.node = node;
        this.tableName = name;
        this.tableAlias = alias;
        this.useSimpleDb = useSimpleDb;
        this.colNum = colNum;
    }

    public OpIterator getRootOp(){
        if (!isGlobal || useSimpleDb) {
            return new SeqColScan(Global.TRANSACTION_ID, Database.getCatalog().getTableId(tableName), this.tableAlias, this.colNum);
        }
        else {
            return new GlobalColSeqScan(Global.TRANSACTION_ID, node, tableName, tableAlias, colNum);
        }
    }

    @Override
    public String getRootType(){
        return "SEMISCAN";
    }

    @Override
    public void setIsGlobal(boolean isGlobal) {
        this.isGlobal = isGlobal;
    }


    @Override
    public String toString() {
        return "SEMISCAN(" + tableAlias + ")";
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
