package networking;

import global.Utils;
import querytree.QueryTree;
import simpledb.*;
import querytree.QAggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by aditisri on 11/20/17.
 */
public class AggregateResult {
    private List<Tuple> hits;
    private QueryTree queryTree;
    private int count;
    private int min;
    private int max;
    private int sum;
    private static final Logger LOGGER = Logger.getLogger(AggregateResult.class.getName());

    public AggregateResult(QueryTree queryTree){
        this.hits = new ArrayList<>();
        this.queryTree = queryTree;
        this.count = 0;
        this.min = Integer.MAX_VALUE;
        this.max = Integer.MIN_VALUE;
        this.sum = 0;
    }

    public void merge(String stringTup){
        LOGGER.log(Level.INFO, stringTup);
        QAggregate aggTree = (QAggregate) this.queryTree;
        TupleDesc td = null;
        if (aggTree.getAggregator() == Aggregator.Op.AVG){
            Type[] typeArr = {Type.INT_TYPE, Type.INT_TYPE};
            String[] fieldArr = {"count", "sum"};
            td = new TupleDesc(typeArr, fieldArr);
        }
        else{
            Type[] typeArr = {Type.INT_TYPE};
            String[] fieldArr = {"aggField"};
            td = new TupleDesc(typeArr, fieldArr);
        }
        Tuple t = Utils.stringToTuple(td, stringTup);
        int value = ((IntField) t.getField(0)).getValue();
        switch(aggTree.getAggregator()){
            case COUNT:
                this.count += value;
                break;
            case MIN:
                this.min = Math.min(this.min, value);
                break;
            case MAX:
                this.max = Math.max(this.max, value);
                break;
            case SUM:
                this.sum += value;
                break;
            case AVG:
                int sum = ((IntField)t.getField(1)).getValue();
                this.count += value;
                this.sum += sum;
            }
        }

    public void printResult(){
        QAggregate aggTree = (QAggregate) this.queryTree;
        TupleDesc td = this.queryTree.getRootOp().getTupleDesc();
//        int aggVal = 0;
        switch(aggTree.getAggregator()){
            case COUNT:
                System.out.println(this.count);
                break;
            case MIN:
                System.out.println(this.min);
                break;
            case MAX:
                System.out.println(this.max);
                break;
            case SUM:
                System.out.println(this.sum);
                break;
            case AVG:
                System.out.println(this.sum/this.count);
        }

//        Tuple resultTuple = new Tuple(td);
//        resultTuple.setField(0, new IntField(aggVal));
//        List<Tuple> resultSet = new ArrayList<>();
//        resultSet.add(resultTuple);
    }






}
