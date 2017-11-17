package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        
        @Override
        public boolean equals(Object o) {
            if ( !(o instanceof TDItem) ) return false;
            TDItem oItem = (TDItem)o;
            return oItem.fieldType.equals(this.fieldType) && oItem.fieldName.equals(this.fieldName);
        }
        
        @Override
        public int hashCode() {
            return fieldName.hashCode();
        }

        @Override
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;
    private final List<TDItem> items;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null. The null entries will appear as empty strings in the resulting tuple
     *            description
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert typeAr.length == fieldAr.length : "Length of typeAr and fieldAr should be equal.";
        assert typeAr.length > 0 : "Length must be positive.";
        
        final int tupleLength = typeAr.length;
        final List<TDItem> newItems = new ArrayList<>();
        
        for (int i = 0; i < tupleLength; i++){
            String field = fieldAr[i];
            if (field == null) {
                field = "";
            }
            newItems.add(new TDItem(typeAr[i], field));
        }
        
        items = Collections.unmodifiableList(newItems);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * The anonymous fields will appear as empty Strings.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, emptyStringArray(typeAr.length));
    }
    
    /**
     * Utility static method giving a String array of ""s.
     * @param length the length of the String array
     * @return a String array with ""s of length length
     */
    private static String[] emptyStringArray(int length) {
        String[] stringArray = new String[length];
        Arrays.fill(stringArray, "");
        return stringArray;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getFieldName(i).equals(name)) return i;
        }
        throw new NoSuchElementException("cannot find the name" + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for (int i = 0; i < this.numFields(); i++) {
            totalSize += this.getFieldType(i).getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        final int mergedLength = td1.numFields() + td2.numFields();
        String[] mergedFieldAr = new String[mergedLength];
        Type[] mergedTypeAr = new Type[mergedLength];
        for (int i = 0; i < mergedLength; i++){
            if (i < td1.numFields()) {
                mergedFieldAr[i] = td1.getFieldName(i);
                mergedTypeAr[i] = td1.getFieldType(i);
            } else {
                int indexIn2 = i - td1.numFields();
                mergedFieldAr[i] = td2.getFieldName(indexIn2);
                mergedTypeAr[i] = td2.getFieldType(indexIn2);
            }
        }
        return new TupleDesc(mergedTypeAr, mergedFieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        if ( !(o instanceof TupleDesc) ) return false;
        TupleDesc that = (TupleDesc)o;
        final int length = this.numFields();
        if ( that.numFields() != length ) return false;
        for (int i = 0; i < length; i++) {
            if ( !that.getFieldType(i).equals(this.getFieldType(i)) ) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.numFields();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // some code goes here
        return items.toString();
    }
}
