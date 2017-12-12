package querytree;

import simpledb.HashEquiJoin;
import simpledb.JoinPredicate;
import simpledb.OpIterator;
import simpledb.Predicate;

/**
 * Created by aditisri on 12/12/17.
 */
public class QHashJoin implements QueryTree{
    private final QueryTree child1;
    private final QueryTree child2;
    private final JoinPredicate joinPredicate;
    public QHashJoin(QueryTree child1, QueryTree child2, int colNum1, Predicate.Op op, int colNum2){
        this.child1 = child1;
        this.child2 = child2;
        this.joinPredicate = new JoinPredicate(colNum1, op, colNum2);
    }


    @Override
    public OpIterator getRootOp() {
        return new HashEquiJoin(joinPredicate, child1.getRootOp(), child2.getRootOp());
    }

    @Override
    //TODO: just copied for now might need to change later maybe always set to false? never need global for hash join
    public void setIsGlobal(boolean isGlobal) {
        if (isGlobal) {
            child1.setIsGlobal(true);
            child2.setIsGlobal(true);
        }
        else {
            //child1.setIsGlobal(false);
            //child2.setIsGlobal(true);
            child1.setIsGlobal(true);
            child2.setIsGlobal(false);
            // Possible improvement: switch the global seq between left and right
        }

    }

    @Override
    public String getRootType() {
        return "HASH_JOIN";
    }
}
