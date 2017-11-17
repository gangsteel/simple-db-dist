package simpledb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> results; // The results stored in a Map object
    private final List<Map<Field,Integer>> count_sum; // Auxiliary Maps for average calculation
    private TupleDesc td = null; // the TupleDesc of tuples

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.results = new ConcurrentHashMap<>();
        if (what == Op.AVG) {
            this.count_sum = Arrays.asList(new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        } else {
            this.count_sum = null;
        }
    }
    
    /**
     * Get the updated value based on the operation type
     * @param oldValue the old value stored in the Map object
     * @param fieldValue the integer value of the field
     * @param what the type of operator, only supports MIN, MAX, SUM, and COUNT
     * @return the updated value to be stored in the Map object
     */
    private static int getUpdatedValue(int oldValue, int fieldValue, Op what) {
        if (what == Op.MIN) {
            return (fieldValue < oldValue) ? fieldValue : oldValue;
        } else if (what == Op.MAX) {
            return (fieldValue > oldValue) ? fieldValue : oldValue;
        } else if (what == Op.SUM) {
            return oldValue + fieldValue;
        } else if (what == Op.COUNT) {
            return oldValue + 1;
        } else {
            throw new IllegalArgumentException("Unsupported operator");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        assert tup.getField(afield).getType() == Type.INT_TYPE : "wrong type!";
        if (td == null) {
            td = tup.getTupleDesc();
        }
        
        final IntField aggField = (IntField)tup.getField(afield);
        final int aggValue = aggField.getValue();
        Field groupingField;
        if (gbfield == Aggregator.NO_GROUPING) {
            groupingField = new StringField("ALL", 3);
        } else {
            groupingField = tup.getField(gbfield);
        }
        
        if (results.containsKey(groupingField)) {
            // Update the key-value
            if (what == Op.AVG) {
                final Map<Field, Integer> countMap = count_sum.get(0);
                final Map<Field, Integer> sumMap = count_sum.get(1);
                final int newCount = getUpdatedValue(countMap.get(groupingField), aggValue, Op.COUNT);
                final int newSum = getUpdatedValue(sumMap.get(groupingField), aggValue, Op.SUM);
                results.put(groupingField, newSum/newCount);
                countMap.put(groupingField, newCount);
                sumMap.put(groupingField, newSum);
            } else if (what == Op.MAX || what == Op.MIN || what == Op.SUM || what == Op.COUNT) {
                final int newValue = getUpdatedValue(results.get(groupingField), aggValue, what);
                results.put(groupingField, newValue);
            } else {
                throw new IllegalArgumentException("Unsupported operator");
            }
        } else {
            // Create the key-value
            if (what == Op.AVG) {
                results.put(groupingField, aggValue);
                count_sum.get(0).put(groupingField, 1);
                count_sum.get(1).put(groupingField, aggValue);
            } else if (what == Op.MAX || what == Op.MIN || what == Op.SUM) {
                results.put(groupingField, aggValue);
            } else if (what == Op.COUNT) {
                results.put(groupingField, 1);
            } else {
                throw new IllegalArgumentException("Unsupported operator");
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // Make the TupleDesc
        final String aggName = td.getFieldName(afield);
        TupleDesc tupleDesc;
        if (gbfield != Aggregator.NO_GROUPING){
            final String groupName = td.getFieldName(gbfield);
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{groupName, what.toString() + "(" + aggName + ")"});
        } else {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{what.toString() + "(" + aggName + ")"});
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
