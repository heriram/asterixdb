package edu.uci.ics.asterix.om.adm;


import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * A ADMArray is an ordered sequence of values. Its external text form is a
 * string wrapped in square brackets with commas separating the values. The
 * internal form is an object having <code>get</code> and <code>opt</code>
 * methods for accessing the values by index, and <code>put</code> methods for
 * adding or replacing values. The values can be any of these types:
 * <code>Boolean</code>, <code>ADMArray</code>, <code>ADMObject</code>,
 * <code>Number</code>, <code>String</code>, or the
 * <code>ADMObject.NULL object</code>.
 * <p>
 * The constructor can convert a ADM text into a Java object. The
 * <code>toString</code> method converts to ADM text.
 * <p>
 * A <code>get</code> method returns a value if one can be found, and throws an
 * exception if one cannot be found. An <code>opt</code> method returns a
 * default value instead of throwing an exception, and so is useful for
 * obtaining optional values.
 * <p>
 * The generic <code>get()</code> and <code>opt()</code> methods return an
 * object which you can cast or query for type. There are also typed
 * <code>get</code> and <code>opt</code> methods that do type checking and type
 * coercion for you.
 * <p>
 * The texts produced by the <code>toString</code> methods strictly conform to
 * ADM syntax rules. The constructors are more forgiving in the texts they will
 * accept:
 * <ul>
 * <li>An extra <code>,</code>&nbsp;<small>(comma)</small> may appear just
 * before the closing bracket.</li>
 * <li>The <code>null</code> value will be inserted when there is <code>,</code>
 * &nbsp;<small>(comma)</small> elision.</li>
 * <li>Strings may be quoted with <code>'</code>&nbsp;<small>(single
 * quote)</small>.</li>
 * <li>Strings do not need to be quoted at all if they do not begin with a quote
 * or single quote, and if they do not contain leading or trailing spaces, and
 * if they do not contain any of these characters:
 * <code>{ } [ ] / \ : , #</code> and if they do not look like numbers and
 * if they are not the reserved words <code>true</code>, <code>false</code>, or
 * <code>null</code>.</li>
 * </ul>
 *
 * @author AsterixDB team
 * @version 2015-03-31
 */
abstract public class ADMArray {

    /**
     * The arrayList where the ADMArray's properties are kept.
     */
    protected ArrayList<Object> myArrayList;
    
    /**
     * To be used to construct an empty ADMArray class.
     */
    protected void init() {
    	this.myArrayList = new ArrayList<Object>();
    }



    /**
     * To be used to construct a ADMArray from a Collection.
     *
     * @param collection
     *            A Collection.
     */
    protected void initCollection(Collection<Object> collection) {
        this.myArrayList = new ArrayList<Object>();
        if (collection != null) {
            Iterator<Object> iter = collection.iterator();
            while (iter.hasNext()) {
                this.myArrayList.add(ADMObject.wrap(iter.next()));
            }
        }
    }

    /**
     * To be used to construct a ADMArray from an array
     *
     * @throws ADMException
     *             If not an array.
     */
    @SuppressWarnings("unchecked")
	protected void init(Object array) throws ADMException {
    	if (array instanceof Collection) {
    		initCollection((Collection<Object>)array);
    		return;
    	}
    	
        init();
        if (array.getClass().isArray()) {
            int length = Array.getLength(array);
            for (int i = 0; i < length; i += 1) {
                this.put(ADMObject.wrap(Array.get(array, i)));
            }
        } else {
            throw new ADMException(
                    "ADMArray initial value should be a string or collection or array.");
        }
    }

    /**
     * Get the object value associated with an index.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return An object value.
     * @throws ADMException
     *             If there is no value for the index.
     */
    public Object get(int index) throws ADMException {
        Object object = this.opt(index);
        if (object == null) {
            throw new ADMException("ADMArray[" + index + "] not found.");
        }
        return object;
    }

    /**
     * Get the boolean value associated with an index. The string values "true"
     * and "false" are converted to boolean.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The truth.
     * @throws ADMException
     *             If there is no value for the index or if the value is not
     *             convertible to boolean.
     */
    public boolean getBoolean(int index) throws ADMException {
        Object object = this.get(index);
        if (object.equals(Boolean.FALSE)
                || (object instanceof String && ((String) object)
                        .equalsIgnoreCase("false"))) {
            return false;
        } else if (object.equals(Boolean.TRUE)
                || (object instanceof String && ((String) object)
                        .equalsIgnoreCase("true"))) {
            return true;
        }
        throw new ADMException("ADMArray[" + index + "] is not a boolean.");
    }

    /**
     * Get the double value associated with an index.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The value.
     * @throws ADMException
     *             If the key is not found or if the value cannot be converted
     *             to a number.
     */
    public double getDouble(int index) throws ADMException {
        Object object = this.get(index);
        try {
            return object instanceof Number ? ((Number) object).doubleValue()
                    : Double.parseDouble((String) object);
        } catch (Exception e) {
            throw new ADMException("ADMArray[" + index + "] is not a number.");
        }
    }

    /**
     * Get the int value associated with an index.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The value.
     * @throws ADMException
     *             If the key is not found or if the value is not a number.
     */
    public int getInt(int index) throws ADMException {
        Object object = this.get(index);
        try {
            return object instanceof Number ? ((Number) object).intValue()
                    : Integer.parseInt((String) object);
        } catch (Exception e) {
            throw new ADMException("ADMArray[" + index + "] is not a number.");
        }
    }

    /**
     * Get the ADMArray associated with an index.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return A ADMArray value.
     * @throws ADMException
     *             If there is no value for the index. or if the value is not a
     *             ADMArray
     */
    public ADMArray getADMArray(int index) throws ADMException {
        Object object = this.get(index);
        if (object instanceof ADMArray) {
            return (ADMArray) object;
        }
        throw new ADMException("ADMArray[" + index + "] is not a ADMArray.");
    }

    /**
     * Get the ADMObject associated with an index.
     *
     * @param index
     *            subscript
     * @return A ADMObject value.
     * @throws ADMException
     *             If there is no value for the index or if the value is not a
     *             ADMObject
     */
    public ADMObject getADMObject(int index) throws ADMException {
        Object object = this.get(index);
        if (object instanceof ADMObject) {
            return (ADMObject) object;
        }
        throw new ADMException("ADMArray[" + index + "] is not a ADMObject.");
    }

    /**
     * Get the long value associated with an index.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The value.
     * @throws ADMException
     *             If the key is not found or if the value cannot be converted
     *             to a number.
     */
    public long getLong(int index) throws ADMException {
        Object object = this.get(index);
        try {
            return object instanceof Number ? ((Number) object).longValue()
                    : Long.parseLong((String) object);
        } catch (Exception e) {
            throw new ADMException("ADMArray[" + index + "] is not a number.");
        }
    }

    /**
     * Get the string associated with an index.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return A string value.
     * @throws ADMException
     *             If there is no string value for the index.
     */
    public String getString(int index) throws ADMException {
        Object object = this.get(index);
        if (object instanceof String) {
            return (String) object;
        }
        throw new ADMException("ADMArray[" + index + "] not a string.");
    }

    /**
     * Determine if the value is null.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return true if the value at the index is null, or if there is no value.
     */
    public boolean isNull(int index) {
        return ADMObject.NULL.equals(this.opt(index));
    }

    /**
     * Make a string from the contents of this ADMArray. The
     * <code>separator</code> string is inserted between each element. Warning:
     * This method assumes that the data structure is acyclical.
     *
     * @param separator
     *            A string that will be inserted between the elements.
     * @return a string.
     * @throws ADMException
     *             If the array contains an invalid number.
     */
    public String join(String separator) throws ADMException {
        int len = this.length();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < len; i += 1) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(ADMObject.valueToString(this.myArrayList.get(i)));
        }
        return sb.toString();
    }

    /**
     * Get the number of elements in the ADMArray, included nulls.
     *
     * @return The length (or size).
     */
    public int length() {
        return this.myArrayList.size();
    }

    /**
     * Get the optional object value associated with an index.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return An object value, or null if there is no object at that index.
     */
    public Object opt(int index) {
        return (index < 0 || index >= this.length()) ? null : this.myArrayList
                .get(index);
    }

    /**
     * Get the optional boolean value associated with an index. It returns false
     * if there is no value at that index, or if the value is not Boolean.TRUE
     * or the String "true".
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The truth.
     */
    public boolean optBoolean(int index) {
        return this.optBoolean(index, false);
    }

    /**
     * Get the optional boolean value associated with an index. It returns the
     * defaultValue if there is no value at that index or if it is not a Boolean
     * or the String "true" or "false" (case insensitive).
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @param defaultValue
     *            A boolean default.
     * @return The truth.
     */
    public boolean optBoolean(int index, boolean defaultValue) {
        try {
            return this.getBoolean(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get the optional double value associated with an index. NaN is returned
     * if there is no value for the index, or if the value is not a number and
     * cannot be converted to a number.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The value.
     */
    public double optDouble(int index) {
        return this.optDouble(index, Double.NaN);
    }

    /**
     * Get the optional double value associated with an index. The defaultValue
     * is returned if there is no value for the index, or if the value is not a
     * number and cannot be converted to a number.
     *
     * @param index
     *            subscript
     * @param defaultValue
     *            The default value.
     * @return The value.
     */
    public double optDouble(int index, double defaultValue) {
        try {
            return this.getDouble(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get the optional int value associated with an index. Zero is returned if
     * there is no value for the index, or if the value is not a number and
     * cannot be converted to a number.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The value.
     */
    public int optInt(int index) {
        return this.optInt(index, 0);
    }

    /**
     * Get the optional int value associated with an index. The defaultValue is
     * returned if there is no value for the index, or if the value is not a
     * number and cannot be converted to a number.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @param defaultValue
     *            The default value.
     * @return The value.
     */
    public int optInt(int index, int defaultValue) {
        try {
            return this.getInt(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get the optional ADMArray associated with an index.
     *
     * @param index
     *            subscript
     * @return A ADMArray value, or null if the index has no value, or if the
     *         value is not a ADMArray.
     */
    public ADMArray optADMArray(int index) {
        Object o = this.opt(index);
        return o instanceof ADMArray ? (ADMArray) o : null;
    }

    /**
     * Get the optional ADMObject associated with an index. Null is returned if
     * the key is not found, or null if the index has no value, or if the value
     * is not a ADMObject.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return A ADMObject value.
     */
    public ADMObject optADMObject(int index) {
        Object o = this.opt(index);
        return o instanceof ADMObject ? (ADMObject) o : null;
    }

    /**
     * Get the optional long value associated with an index. Zero is returned if
     * there is no value for the index, or if the value is not a number and
     * cannot be converted to a number.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return The value.
     */
    public long optLong(int index) {
        return this.optLong(index, 0);
    }

    /**
     * Get the optional long value associated with an index. The defaultValue is
     * returned if there is no value for the index, or if the value is not a
     * number and cannot be converted to a number.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @param defaultValue
     *            The default value.
     * @return The value.
     */
    public long optLong(int index, long defaultValue) {
        try {
            return this.getLong(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get the optional string value associated with an index. It returns an
     * empty string if there is no value at that index. If the value is not a
     * string and is not null, then it is coverted to a string.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @return A String value.
     */
    public String optString(int index) {
        return this.optString(index, "");
    }

    /**
     * Get the optional string associated with an index. The defaultValue is
     * returned if the key is not found.
     *
     * @param index
     *            The index must be between 0 and length() - 1.
     * @param defaultValue
     *            The default value.
     * @return A String value.
     */
    public String optString(int index, String defaultValue) {
        Object object = this.opt(index);
        return ADMObject.NULL.equals(object) ? defaultValue : object
                .toString();
    }

    /**
     * Append a boolean value. This increases the array's length by one.
     *
     * @param value
     *            A boolean value.
     * @return this.
     */
    public ADMArray put(boolean value) {
        this.put(value ? Boolean.TRUE : Boolean.FALSE);
        return this;
    }

    /**
     * Put a value in the ADMArray, where the value will be a ADMArray which
     * is produced from a Collection.
     *
     * @param value
     *            A Collection value.
     * @return this.
     */
    abstract public ADMArray put(Collection<Object> value);
    
    /**
     * Append a double value. This increases the array's length by one.
     *
     * @param value
     *            A double value.
     * @throws ADMException
     *             if the value is not finite.
     * @return this.
     */
    public ADMArray put(double value) throws ADMException {
        Double d = new Double(value);
        ADMObject.testValidity(d);
        this.put(d);
        return this;
    }

    /**
     * Append an int value. This increases the array's length by one.
     *
     * @param value
     *            An int value.
     * @return this.
     */
    public ADMArray put(int value) {
        this.put(new Integer(value));
        return this;
    }

    /**
     * Append an long value. This increases the array's length by one.
     *
     * @param value
     *            A long value.
     * @return this.
     */
    public ADMArray put(long value) {
        this.put(new Long(value));
        return this;
    }

    /**
     * Put a value in the ADMArray, where the value will be a ADMObject which
     * is produced from a Map.
     *
     * @param value
     *            A Map value.
     * @return this.
     */
    public ADMArray put(Map<String, Object> value) {
        this.put(new ADMObject(value));
        return this;
    }

    /**
     * Append an object value. This increases the array's length by one.
     *
     * @param value
     *            An object value. The value should be a Boolean, Double,
     *            Integer, ADMArray, ADMObject, Long, or String, or the
     *            ADMObject.NULL object.
     * @return this.
     */
    public ADMArray put(Object value) {
        this.myArrayList.add(value);
        return this;
    }

    /**
     * Put or replace a boolean value in the ADMArray. If the index is greater
     * than the length of the ADMArray, then null elements will be added as
     * necessary to pad it out.
     *
     * @param index
     *            The subscript.
     * @param value
     *            A boolean value.
     * @return this.
     * @throws ADMException
     *             If the index is negative.
     */
    public ADMArray put(int index, boolean value) throws ADMException {
        this.put(index, value ? Boolean.TRUE : Boolean.FALSE);
        return this;
    }

    /**
     * Put a value in the ADMArray, where the value will be a ADMArray which
     * is produced from a Collection.
     *
     * @param index
     *            The subscript.
     * @param value
     *            A Collection value.
     * @return this.
     * @throws ADMException
     *             If the index is negative or if the value is not finite.
     */
    abstract public ADMArray put(int index, Collection<Object> value);
    

    /**
     * Put or replace a double value. If the index is greater than the length of
     * the ADMArray, then null elements will be added as necessary to pad it
     * out.
     *
     * @param index
     *            The subscript.
     * @param value
     *            A double value.
     * @return this.
     * @throws ADMException
     *             If the index is negative or if the value is not finite.
     */
    public ADMArray put(int index, double value) throws ADMException {
        this.put(index, new Double(value));
        return this;
    }

    /**
     * Put or replace an int value. If the index is greater than the length of
     * the ADMArray, then null elements will be added as necessary to pad it
     * out.
     *
     * @param index
     *            The subscript.
     * @param value
     *            An int value.
     * @return this.
     * @throws ADMException
     *             If the index is negative.
     */
    public ADMArray put(int index, int value) throws ADMException {
        this.put(index, new Integer(value));
        return this;
    }

    /**
     * Put or replace a long value. If the index is greater than the length of
     * the ADMArray, then null elements will be added as necessary to pad it
     * out.
     *
     * @param index
     *            The subscript.
     * @param value
     *            A long value.
     * @return this.
     * @throws ADMException
     *             If the index is negative.
     */
    public ADMArray put(int index, long value) throws ADMException {
        this.put(index, new Long(value));
        return this;
    }

    /**
     * Put a value in the ADMArray, where the value will be a ADMObject that
     * is produced from a Map.
     *
     * @param index
     *            The subscript.
     * @param value
     *            The Map value.
     * @return this.
     * @throws ADMException
     *             If the index is negative or if the the value is an invalid
     *             number.
     */
    public ADMArray put(int index, Map<String, Object> value) throws ADMException {
        this.put(index, new ADMObject(value));
        return this;
    }

    /**
     * Put or replace an object value in the ADMArray. If the index is greater
     * than the length of the ADMArray, then null elements will be added as
     * necessary to pad it out.
     *
     * @param index
     *            The subscript.
     * @param value
     *            The value to put into the array. The value should be a
     *            Boolean, Double, Integer, ADMArray, ADMObject, Long, or
     *            String, or the ADMObject.NULL object.
     * @return this.
     * @throws ADMException
     *             If the index is negative or if the the value is an invalid
     *             number.
     */
    public ADMArray put(int index, Object value) throws ADMException {
        ADMObject.testValidity(value);
        if (index < 0) {
            throw new ADMException("ADMArray[" + index + "] not found.");
        }
        if (index < this.length()) {
            this.myArrayList.set(index, value);
        } else {
            while (index != this.length()) {
                this.put(ADMObject.NULL);
            }
            this.put(value);
        }
        return this;
    }

    /**
     * Remove an index and close the hole.
     *
     * @param index
     *            The index of the element to be removed.
     * @return The value that was associated with the index, or null if there
     *         was no value.
     */
    public Object remove(int index) {
        return index >= 0 && index < this.length()
            ? this.myArrayList.remove(index)
            : null;
    }

    /**
     * Determine if two ADMArrays are similar.
     * They must contain similar sequences.
     *
     * @param other The other ADMArray
     * @return true if they are equal
     */
    public boolean similar(Object other) {
        if (!(other instanceof ADMArray)) {
            return false;
        }
        int len = this.length();
        if (len != ((ADMArray)other).length()) {
            return false;
        }
        for (int i = 0; i < len; i += 1) {
            Object valueThis = this.get(i);
            Object valueOther = ((ADMArray)other).get(i);
            if (valueThis instanceof ADMObject) {
                if (!((ADMObject)valueThis).similar(valueOther)) {
                    return false;
                }
            } else if (valueThis instanceof ADMArray) {
                if (!((ADMArray)valueThis).similar(valueOther)) {
                    return false;
                }
            } else if (!valueThis.equals(valueOther)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Produce a ADMObject by combining a ADMArray of names with the values of
     * this ADMArray.
     *
     * @param names
     *            A ADMArray containing a list of key strings. These will be
     *            paired with the values.
     * @return A ADMObject, or null if there are no names or if this ADMArray
     *         has no values.
     * @throws ADMException
     *             If any of the names are null.
     */
    public ADMObject toADMObject(ADMArray names) throws ADMException {
        if (names == null || names.length() == 0 || this.length() == 0) {
            return null;
        }
        ADMObject jo = new ADMObject();
        for (int i = 0; i < names.length(); i += 1) {
            jo.put(names.getString(i), this.opt(i));
        }
        return jo;
    }

    /**
     * Make a ADM text of this ADMArray. For compactness, no unnecessary
     * whitespace is added. If it is not possible to produce a syntactically
     * correct ADM text then null will be returned instead. This could occur if
     * the array contains an invalid number.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return a printable, displayable, transmittable representation of the
     *         array.
     */
    public String toString() {
        try {
            return this.toString(0);
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Make a prettyprinted ADM text of this ADMArray. Warning: This method
     * assumes that the data structure is acyclical.
     *
     * @param indentFactor
     *            The number of spaces to add to each level of indentation.
     * @return a printable, displayable, transmittable representation of the
     *         object, beginning with <code>[</code>&nbsp;<small>(left
     *         bracket)</small> and ending with <code>]</code>
     *         &nbsp;<small>(right bracket)</small>.
     * @throws ADMException
     */
    public String toString(int indentFactor) throws ADMException {
        StringWriter sw = new StringWriter();
        synchronized (sw.getBuffer()) {
            return this.write(sw, indentFactor, 0).toString();
        }
    }

    /**
     * Write the contents of the ADMArray as ADM text to a writer. For
     * compactness, no whitespace is added.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return The writer.
     * @throws ADMException
     */
    public Writer write(Writer writer) throws ADMException {
        return this.write(writer, 0, 0);
    }

    /**
     * Write the contents of the ADMArray as ADM text to a writer. For
     * compactness, no whitespace is added.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @param indentFactor
     *            The number of spaces to add to each level of indentation.
     * @param indent
     *            The indention of the top level.
     * @return The writer.
     * @throws ADMException
     */
    abstract Writer write(Writer writer, int indentFactor, int indent);
  
}
