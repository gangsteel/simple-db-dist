package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private final int tupleNum; // total tuple number
    private final int pageNumApprox; // approximated total page number
    private final int ioCostPerPage;
    private final TupleDesc td;
    private final Map<Integer, IntHistogram> intHistograms;
    private final Map<Integer, StringHistogram> stringHistograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        intHistograms = new HashMap<>();
        stringHistograms = new HashMap<>();
        final DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        td = file.getTupleDesc();
        int numberTuples = 0;
        try {
            final TransactionId tid = new TransactionId();
            final DbFileIterator iterator = file.iterator(tid);
            iterator.open();
            // Firstly, we need to get the min/max value of integer fields
            final List<Integer> minValues = new ArrayList<>();
            final List<Integer> maxValues = new ArrayList<>();
            if (!iterator.hasNext()) {
                throw new IllegalArgumentException("No tuples in the table!");
            }
            final Tuple firstTuple = iterator.next();
            // Use the first tuple to initialize the ArrayLists to get place holders
            for (int i = 0; i < td.numFields(); i++) {
                if (td.getFieldType(i) == Type.INT_TYPE) {
                    final Field field = firstTuple.getField(i);
                    assert (field instanceof IntField) : "Wrong field type";
                    minValues.add( ((IntField)field).getValue() );
                    maxValues.add( ((IntField)field).getValue() );
                } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                    minValues.add(0);
                    maxValues.add(0);
                } else {
                    throw new IllegalStateException("impossible to reach here");
                }
            }
            assert (minValues.size() == td.numFields()) && (maxValues.size() == td.numFields()) : "wrong list length";
            
            // Loop through all the tuples and get max/min values
            while(iterator.hasNext()) {
                final Tuple current = iterator.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        // Only update the ArrayLists if it's an INT_TYPE
                        final Field field = current.getField(i);
                        assert (field instanceof IntField) : "Wrong field type";
                        final int fieldValue = ((IntField)field).getValue();
                        if (fieldValue < minValues.get(i)) {
                            minValues.set(i, fieldValue);
                        }
                        if (fieldValue > maxValues.get(i)) {
                            maxValues.set(i, fieldValue);
                        }
                    }
                }
            }
            // Now, create and update the histograms
            for (int i = 0; i < td.numFields(); i++) {
                if (td.getFieldType(i) == Type.INT_TYPE) {
                    intHistograms.put(i,
                            new IntHistogram(NUM_HIST_BINS, minValues.get(i), maxValues.get(i)));
                } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                    stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
                } else {
                    throw new IllegalStateException("impossible to reach here");
                }
            }
            iterator.rewind();
            while (iterator.hasNext()) {
                numberTuples++;
                final Tuple current = iterator.next();
                for (int i = 0; i < td.numFields(); i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        final IntHistogram hist = intHistograms.get(i);
                        final Field field = current.getField(i);
                        assert (field instanceof IntField) : "Wrong field type";
                        hist.addValue(((IntField)field).getValue());
                    } else if (td.getFieldType(i) == Type.STRING_TYPE) {
                        final StringHistogram hist = stringHistograms.get(i);
                        final Field field = current.getField(i);
                        assert (field instanceof StringField) : "Wrong field type";
                        hist.addValue(((StringField)field).getValue());
                    } else {
                        throw new IllegalStateException("impossible to reach here");
                    }
                }
            }
            iterator.close();
            // complete the transaction
            Database.getBufferPool().transactionComplete(tid);
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Error detected reading the file");
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Error detected reading the file");
        }
        tupleNum = numberTuples;
        final int tupleNumPerPage = (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);
        pageNumApprox = (int)Math.ceil((double)tupleNum / (double)tupleNumPerPage);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return pageNumApprox * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) Math.round(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        switch (op) {
        case EQUALS:
        case LIKE:
            if (td.getFieldType(field) == Type.INT_TYPE) {
                return intHistograms.get(field).avgSelectivity();
            } else if (td.getFieldType(field) == Type.STRING_TYPE) {
                return stringHistograms.get(field).avgSelectivity();
            } else {
                throw new IllegalStateException("impossible to reach here");
            }
        case NOT_EQUALS:
            return 1.0 - avgSelectivity(field, Predicate.Op.EQUALS);
        case GREATER_THAN:
        case LESS_THAN:
        case LESS_THAN_OR_EQ:
        case GREATER_THAN_OR_EQ:
            return 0.5;
        default: throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (td.getFieldType(field) == Type.INT_TYPE) {
            assert (constant instanceof IntField) : "constant not the same with field type";
            return intHistograms.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (td.getFieldType(field) == Type.STRING_TYPE) {
            assert (constant instanceof StringField) : "constant not the same with field type";
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        } else {
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return tupleNum;
    }

}
