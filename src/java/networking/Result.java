package networking;

import com.sun.deploy.util.ArrayUtil;
import global.Utils;
import querytree.QueryTree;
import simpledb.*;
import querytree.QAggregate;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by aditisri on 11/20/17.
 */
public class Result {
    private List<Tuple> hits;
    private QueryTree queryTree;
    private int count;
    private int min;
    private int max;
    private int sum;

    public Result(QueryTree queryTree){
        this.hits = new ArrayList<>();
        this.queryTree = queryTree;
        this.count = 0;
        this.min = Integer.MAX_VALUE;
        this.max = Integer.MIN_VALUE;
        this.sum = 0;
    }

    public void merge(String stringTup){
        TupleDesc td = this.queryTree.getRootOp().getTupleDesc();
        Tuple t = Utils.stringToTuple(td, stringTup);
        int value = ((IntField) t.getField(0)).getValue();
        QAggregate aggTree = (QAggregate) this.queryTree;
        if(this.queryTree.getRootType() == "AGGREGATE"){
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
                    int count = ((IntField)t.getField(1)).getValue();
                    this.count += count;
                    this.sum += value;

            }
        }
        else {
            this.hits.add(t);
        }
    }

    public List<Tuple> getResult(){
        if(this.queryTree.getRootType() == "AGGREGATE"){
            QAggregate aggTree = (QAggregate) this.queryTree;
            TupleDesc td = this.queryTree.getRootOp().getTupleDesc();
            int aggVal = 0;
            switch(aggTree.getAggregator()){
                case COUNT:
                    aggVal = this.count;
                    break;
                case MIN:
                    aggVal = this.min;
                    break;
                case MAX:
                    aggVal = this.max;
                    break;
                case SUM:
                    aggVal = this.sum;
                    break;
                case AVG:
                    aggVal = this.sum/this.count;
            }

            Tuple resultTuple = new Tuple(td);
            resultTuple.setField(0, new IntField(aggVal));
            List<Tuple> resultSet = new ArrayList<>();
            resultSet.add(resultTuple);
            return resultSet;
        }
        else{
            return this.hits;
        }
    }






}
