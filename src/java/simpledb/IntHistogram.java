package simpledb;

import java.util.ArrayList;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    
    private final int buckets;
    private final double min;
    private final double max;
    private final double binWidth;
    private final List<Integer> distribution; // number of tuples in each bucket
    private int nTup; // total number of tuples

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.min = min - 0.5;
        this.max = max + 0.5;
        this.binWidth = (this.max - this.min) / (double)buckets;
        distribution = new ArrayList<>();
        for (int i = 0; i < buckets; i++) {
            distribution.add(0);
        }
        nTup = 0;
    }
    
    /**
     * Get the bin index of the given value v
     * @return the bin index of v, if v == max, return buckets - 1
     */
    private int getBinIndex(double v) {
        assert (v <= max && v >= min) : "value out of bound";
        return (v == max) ? buckets - 1 : (int)((v - min) / binWidth);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        nTup++;
    	final int binIndex = getBinIndex(v);
    	final int nTupBin = distribution.get(binIndex) + 1;
    	distribution.set(binIndex, nTupBin);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        final int binIndex;
        switch (op) {
        case EQUALS:
        case LIKE:
            return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v) +
                    estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - 1.0;
        case NOT_EQUALS:
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        case GREATER_THAN:
            if (v + 0.5 > max) return 0;
            else if (v - 0.5 < min) return 1;
            
            binIndex = getBinIndex(v + 0.5);
            double numTupGreater = ((min + (binIndex + 1) * binWidth) - v - 0.5) / binWidth
                    * distribution.get(binIndex);
            for (int i = binIndex + 1; i < buckets; i++) {
                numTupGreater += distribution.get(i);
            }
            return numTupGreater / (double)nTup;
        case LESS_THAN:
            if (v + 0.5 > max) return 1;
            else if (v - 0.5 < min) return 0;
            
            binIndex = getBinIndex(v - 0.5);
            double numTupLesser = (v - 0.5 - (min + binIndex * binWidth)) / binWidth
                    * distribution.get(binIndex);
            for (int i = 0; i < binIndex; i++) {
                numTupLesser += distribution.get(i);
            }
            return numTupLesser / (double)nTup;
        case LESS_THAN_OR_EQ:
            return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        case GREATER_THAN_OR_EQ:
            return 1.0 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        default: throw new IllegalStateException("impossible to reach here");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return 1.0 / (max - min);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String outputString = "Total tups within [" + min + "," + max + "]: " 
                + nTup + ". Distribution: " + distribution;
        return outputString;
    }
}
