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

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> fieldsDesc;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return fieldsDesc.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length < 1) {
            throw new IllegalArgumentException("typeAr must contain at least one element");
        }
        else if (fieldAr.length < 1) {
            throw new IllegalArgumentException("fieldAr must contain at least one element");
        }
        else if (fieldAr.length != typeAr.length) {
            throw new IllegalArgumentException("Must provide equal number of fields and names");
        }

        fieldsDesc = new ArrayList<TDItem> (typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            fieldsDesc.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr.length < 1) {
            throw new IllegalArgumentException("typeAr must contain at least one element");
        }

        fieldsDesc = new ArrayList<TDItem> (typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            fieldsDesc.add(new TDItem(typeAr[i], null));
        }    
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fieldsDesc.size();
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
        if (i < 0 || i > (this.numFields() - 1)) {
            throw new NoSuchElementException("Index out of bounds");
        }

        return fieldsDesc.get(i).fieldName;
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
        if (i < 0 || i > (this.numFields() - 1)) {
            throw new NoSuchElementException("Index out of bounds");
        }

        return fieldsDesc.get(i).fieldType;
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
        Iterator<TDItem> iter = this.iterator();
        int index = 0;

        while (iter.hasNext()) {
            TDItem temp = iter.next();
            if (temp.fieldName == null) {
                continue;
            }
            if (temp.fieldName.equals(name)) {
                return index;
            }
            index++;
        }

        throw new NoSuchElementException("Field with given name doesn't exist");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        Iterator<TDItem> iter = this.iterator();
        int total = 0;

        while (iter.hasNext()) {
            TDItem temp = iter.next();
            total += temp.fieldType.getLen();
        }

        return total;
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
        Iterator<TDItem> iter1 = td1.iterator();
        Iterator<TDItem> iter2 = td2.iterator();
        int total = td1.numFields() + td2.numFields();
        Type[] fieldAr = new Type[total];
        String[] nameAr = new String[total];
        int i = 0;

        while (iter1.hasNext()) {
            TDItem temp = iter1.next();
            fieldAr[i] = temp.fieldType;
            nameAr[i] = temp.fieldName;
            i++;
        }

        while (iter2.hasNext()) {
            TDItem temp = iter2.next();
            fieldAr[i] = temp.fieldType;
            nameAr[i] = temp.fieldName;
            i++;
        }

        return new TupleDesc(fieldAr, nameAr);
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

    public boolean equals(Object o) {
        if (o != null && o instanceof TupleDesc) {
            TupleDesc temp = (TupleDesc) o;
            if (this.numFields() == temp.numFields()) {
                for (int i = 0; i < this.numFields(); i++) {
                    if (!this.getFieldType(i).equals(temp.getFieldType(i))) {
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }

    public int hashCode() {
        return this.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder s = new StringBuilder();
        Iterator<TDItem> iter = this.iterator();

        while (iter.hasNext()) {
            s.append(iter.next().toString());
            s.append(",");
        }

        String full = s.toString();
        return full.substring(0, full.length() - 1);
    }
}
