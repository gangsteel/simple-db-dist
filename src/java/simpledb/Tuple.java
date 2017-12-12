package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private RecordId recId; // RecordId is immutable type
    private Field[] fields;
    private TupleDesc td; // TupleDesc is immutable type

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.td = td;
        fields = new Field[td.numFields()];
        // TODO: recId? It is null now..
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return this.fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        String finalString = "";
        for (Field f : fields) {
            finalString += f + " ";
        }
        return finalString.substring(0, finalString.length()-1);
    }

    /**
     * Uses StringBuilder to generate string version of Tuple for performance improvement
     * @return
     */

    public String fastToString(){
        StringBuilder s = new StringBuilder();
        for (Field f: fields){
            s.append(f + " ");
        }
        return s.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return Arrays.asList(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.td = td;
        fields = new Field[td.numFields()];
    }
    
    /**
     * Merge two Tuple objects together to make a new one, used in Join class
     * TODO: RecordId problem not solved here.
     * 
     * @param t1 the first Tuple object
     * @param t2 the second Tuple object
     * @return a new Tuple object whose TupleDesc is TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()),
     * and the fields are merged accordingly.
     */
    public static Tuple merge(Tuple t1, Tuple t2) {
        final TupleDesc td1 = t1.getTupleDesc();
        final TupleDesc td2 = t2.getTupleDesc();
        final Tuple result = new Tuple(TupleDesc.merge(td1, td2));
        
        // Start to set fields
        final int td1num = td1.numFields();
        final int td2num = td2.numFields();
        assert td1num + td2num == result.getTupleDesc().numFields() : "assert failure";
        
        for (int i = 0; i < td1num + td2num; i++){
            if (i < td1num){
                result.setField(i, t1.getField(i));
            } else {
                result.setField(i, t2.getField(i - td1num));
            }
        }
        return result;
    }
}
