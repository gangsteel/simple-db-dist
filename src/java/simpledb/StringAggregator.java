package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> results; // The results stored in a Map object
    private TupleDesc td = null; // the TupleDesc of tuples

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Unsupported operation");
        }
        this.results = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (td == null) {
            td = tup.getTupleDesc();
        }
        
        Field groupingField;
        if (gbfield == Aggregator.NO_GROUPING) {
            groupingField = new StringField("ALL", 3);
        } else {
            groupingField = tup.getField(gbfield);
        }
        
        if (results.containsKey(groupingField)) {
            // Update the key-value
            final int newValue = results.get(groupingField) + 1;
            results.put(groupingField, newValue);
        } else {
            // Create the key-value
            results.put(groupingField, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // Make the TupleDesc
        TupleDesc tupleDesc;
        if (gbfield != Aggregator.NO_GROUPING){
            final String groupName = td.getFieldName(gbfield);
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{groupName, what.toString()});
        } else {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{what.toString()});
        }
        
        // Get the List of Tuples
        final List<Tuple> tuples = new ArrayList<>();
        for (final Field key : results.keySet()) {
            final int value = results.get(key);
            if (gbfield != Aggregator.NO_GROUPING){
                final Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(value));
                tuples.add(tuple);
            } else {
                final Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(value));
                tuples.add(tuple);
            }
        }
        
        return new TupleIterator(tupleDesc, tuples);
    }

}
