package querytree;

import querytree.QueryTree;
import simpledb.DbException;
import simpledb.OpIterator;
import simpledb.TransactionAbortedException;

/**
 * Basic query processor on a QTree. This only runs with partitions stored on a single partition. Furthermore, the QTree
 * must not have any GlobalSeqScans in it.
 */
public class QTreeProcessor {

    public static void processQuery(QueryTree qt) throws DbException, TransactionAbortedException {
        final long startTime = System.nanoTime();
        OpIterator it = qt.getRootOp();
        it.open();
        while (it.hasNext()) {
            System.out.println(it.next());
        }
        final long endTime = System.nanoTime();
        final long duration = endTime - startTime;
        System.out.println("Query time: " + (double)duration / 1000000.0 + "ms.");
    }

}
