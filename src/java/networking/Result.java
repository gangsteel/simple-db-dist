package networking;

import querytree.QueryTree;
import simpledb.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by aditisri on 11/20/17.
 */
public class Result {
    private List<Tuple> hits;
    public Result(QueryTree queryTree){
        this.hits = new ArrayList<>();

    }

    public void merge(String stringTup){
        Tuple t = stringToTuple(stringTup);
        this.hits.add(t);
    }

    public Tuple stringToTuple(String res){
        //TODO: make a getTupleDesc function in QueryTree. Use that to parse out structure.
        return null;

    }




}
