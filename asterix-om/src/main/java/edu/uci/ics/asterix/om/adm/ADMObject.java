package edu.uci.ics.asterix.om.adm;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ResourceBundle;
import java.util.Set;

/**
 * A ADMObject is an unordered collection of name/value pairs. Its external
 * form is a string wrapped in curly braces with colons between the names and
 * values, and commas between the values and names. The internal form is an
 * object having <code>get</code> and <code>opt</code> methods for accessing
 * the values by name, and <code>put</code> methods for adding or replacing
 * values by name. The values can be any of these types: <code>Boolean</code>, <code>ADMArray</code>, <code>ADMObject</code>, <code>Number</code>, <code>String</code>, or the <code>ADMObject.NULL</code> object. A
 * ADMObject constructor can be used to convert an external form ADM text
 * into an internal form whose values can be retrieved with the <code>get</code> and <code>opt</code> methods, or to convert values into a
 * ADM text using the <code>put</code> and <code>toString</code> methods. A <code>get</code> method returns a value if one can be found, and throws an
 * exception if one cannot be found. An <code>opt</code> method returns a
 * default value instead of throwing an exception, and so is useful for
 * obtaining optional values.
 * <p>
 * The generic <code>get()</code> and <code>opt()</code> methods return an object, which you can cast or query for type. There are also typed <code>get</code> and <code>opt</code> methods that do type checking and type coercion for you. The opt methods differ from the get methods in that they do not throw. Instead, they return a specified value, such as null.
 * <p>
 * The <code>put</code> methods add or replace values in an object. For example,
 *
 * <pre>
 * myString = new ADMObject().put(&quot;ADM&quot;, &quot;Hello, World!&quot;).toString();
 * </pre>
 *
 * produces the string <code>{"ADM": "Hello, World"}</code>.
 * <p>
 * The texts produced by the <code>toString</code> methods strictly conform to the ADM syntax rules. The constructors are more forgiving in the texts they will accept:
 * <ul>
 * <li>An extra <code>,</code>&nbsp;<small>(comma)</small> may appear just before the closing brace.</li>
 * <li>Strings may be quoted with <code>'</code>&nbsp;<small>(single quote)</small>.</li>
 * <li>Strings do not need to be quoted at all if they do not begin with a quote or single quote, and if they do not contain leading or trailing spaces, and if they do not contain any of these characters: <code>{ } [ ] / \ : , #</code> and if they do not look like numbers and if they are not the reserved words <code>true</code>, <code>false</code>, or <code>null</code>.</li>
 * </ul>
 *
 */
public class ADMObject {
    public static final Character[] WHITE_SPACES = new Character[] { '\b', '\f', '\t', '\n', '\r', ' ' };
    public static Set<Character> whiteSpaceCharSet;
    static {
        whiteSpaceCharSet = new HashSet<Character>();
        whiteSpaceCharSet.addAll(Arrays.asList(WHITE_SPACES));
    }

    /**
     * ADMObject.NULL is equivalent to the value that JavaScript calls null,
     * whilst Java's null is equivalent to the value that JavaScript calls
     * undefined.
     */
    private static final class Null {

        /**
         * There is only intended to be a single instance of the NULL object,
         * so the clone method returns itself.
         *
         * @return NULL.
         */
        @Override
        protected final Object clone() {
            return this;
        }

        /**
         * A Null object is equal to the null value and to itself.
         *
         * @param object
         *            An object to test for nullness.
         * @return true if the object parameter is the ADMObject.NULL object or
         *         null.
         */
        @Override
        public boolean equals(Object object) {
            return object == null || object == this;
        }

        /**
         * Get the "null" string value.
         *
         * @return The string "null".
         */
        public String toString() {
            return "null";
        }
    }

    /**
     * The map where the ADMObject's properties are kept.
     */
    private final Map<String, Object> map;

    /**
     * It is sometimes more convenient and less ambiguous to have a <code>NULL</code> object than to use Java's <code>null</code> value. <code>ADMObject.NULL.equals(null)</code> returns <code>true</code>. <code>ADMObject.NULL.toString()</code> returns <code>"null"</code>.
     */
    public static final Object NULL = new Null();

    /**
     * Construct an empty ADMObject.
     */
    public ADMObject() {
        this.map = new HashMap<String, Object>();
    }

    /**
     * Construct a ADMObject from a subset of another ADMObject. An array of
     * strings is used to identify the keys that should be copied. Missing keys
     * are ignored.
     *
     * @param jo
     *            A ADMObject.
     * @param names
     *            An array of strings.
     * @throws ADMException
     * @exception ADMException
     *                If a value is a non-finite number or if a name is
     *                duplicated.
     */
    public ADMObject(ADMObject jo, String[] names) {
        this();
        for (int i = 0; i < names.length; i += 1) {
            try {
                this.putOnce(names[i], jo.opt(names[i]));
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Construct a ADMObject from a ADMTokener.
     *
     * @param x
     *            A ADMTokener object containing the source string.
     * @throws ADMException
     *             If there is a syntax error in the source string or a
     *             duplicated key.
     */
    public ADMObject(ADMTokener x) throws ADMException {
        this();
        char c;
        String key;

        if (x.nextClean() != '{') {
            throw x.syntaxError("An ADMObject text must begin with '{'");
        }
        for (;;) {
            c = x.nextClean();
            switch (c) {
                case 0:
                    throw x.syntaxError("An ADMObject text must end with '}'");
                case '}':
                    return;
                default:
                    x.back();
                    key = x.nextValue().toString();
            }

            // The key is followed by ':'.

            c = x.nextClean();
            if (c != ':') {
                throw x.syntaxError("Expected a ':' after a key");
            }
            this.putOnce(key, x.nextValue());

            // Pairs are separated by ','.

            switch (x.nextClean()) {
                case ';':
                case ',':
                    if (x.nextClean() == '}') {
                        return;
                    }
                    x.back();
                    break;
                case '}':
                    return;
                default:
                    throw x.syntaxError("Expected a ',' or '}'");
            }
        }
    }

    /**
     * Construct a ADMObject from a Map.
     *
     * @param map
     *            A map object that can be used to initialize the contents of
     *            the ADMObject.
     * @throws ADMException
     */
    public ADMObject(Map<String, Object> map) {
        this.map = new HashMap<String, Object>();
        if (map != null) {
            Iterator<Entry<String, Object>> i = map.entrySet().iterator();
            while (i.hasNext()) {
                Entry<String, Object> entry = i.next();
                Object value = entry.getValue();
                if (value != null) {
                    this.map.put(entry.getKey(), wrap(value));
                }
            }
        }
    }

    /**
     * Construct a ADMObject from an Object using bean getters. It reflects on
     * all of the public methods of the object. For each of the methods with no
     * parameters and a name starting with <code>"get"</code> or <code>"is"</code> followed by an uppercase letter, the method is invoked,
     * and a key and the value returned from the getter method are put into the
     * new ADMObject.
     * The key is formed by removing the <code>"get"</code> or <code>"is"</code> prefix. If the second remaining character is not upper case, then the
     * first character is converted to lower case.
     * For example, if an object has a method named <code>"getName"</code>, and
     * if the result of calling <code>object.getName()</code> is <code>"Larry Fine"</code>, then the ADMObject will contain <code>"name": "Larry Fine"</code>.
     *
     * @param bean
     *            An object that has getter methods that should be used to make
     *            a ADMObject.
     */
    public ADMObject(Object bean) {
        this();
        this.populateMap(bean);
    }

    /**
     * Construct a ADMObject from an Object, using reflection to find the
     * public members. The resulting ADMObject's keys will be the strings from
     * the names array, and the values will be the field values associated with
     * those keys in the object. If a key is not found or not visible, then it
     * will not be copied into the new ADMObject.
     *
     * @param object
     *            An object that has fields that should be used to make a
     *            ADMObject.
     * @param names
     *            An array of strings, the names of the fields to be obtained
     *            from the object.
     */
    public ADMObject(Object object, String names[]) {
        this();
        Class c = object.getClass();
        for (int i = 0; i < names.length; i += 1) {
            String name = names[i];
            try {
                this.putOpt(name, c.getField(name).get(object));
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Construct a ADMObject from a source ADM text string. This is the most
     * commonly used ADMObject constructor.
     *
     * @param source
     *            A string beginning with <code>{</code>&nbsp;<small>(left
     *            brace)</small> and ending with <code>}</code> &nbsp;<small>(right brace)</small>.
     * @exception ADMException
     *                If there is a syntax error in the source string or a
     *                duplicated key.
     */
    public ADMObject(String source) throws ADMException {
        this(new ADMTokener(source));
    }

    /**
     * Construct a ADMObject from a ResourceBundle.
     *
     * @param baseName
     *            The ResourceBundle base name.
     * @param locale
     *            The Locale to load the ResourceBundle for.
     * @throws ADMException
     *             If any ADMExceptions are detected.
     */
    public ADMObject(String baseName, Locale locale) throws ADMException {
        this();
        ResourceBundle bundle = ResourceBundle.getBundle(baseName, locale, Thread.currentThread()
                .getContextClassLoader());

        // Iterate through the keys in the bundle.

        Enumeration<String> keys = bundle.getKeys();
        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();
            if (key != null) {

                // Go through the path, ensuring that there is a nested ADMObject for each
                // segment except the last. Add the value using the last segment's name into
                // the deepest nested ADMObject.

                String[] path = ((String) key).split("\\.");
                int last = path.length - 1;
                ADMObject target = this;
                for (int i = 0; i < last; i += 1) {
                    String segment = path[i];
                    ADMObject nextTarget = target.optADMObject(segment);
                    if (nextTarget == null) {
                        nextTarget = new ADMObject();
                        target.put(segment, nextTarget);
                    }
                    target = nextTarget;
                }
                target.put(path[last], bundle.getString((String) key));
            }
        }
    }

    /**
     * Accumulate values under a key. It is similar to the put method except
     * that if there is already an object stored under the key then a ADMArray
     * is stored under the key to hold all of the accumulated values. If there
     * is already a ADMArray, then the new value is appended to it. In
     * contrast, the put method replaces the previous value.
     * If only one value is accumulated that is not a ADMArray, then the result
     * will be the same as using put. But if multiple values are accumulated,
     * then the result will be like append.
     *
     * @param key
     *            A key string.
     * @param value
     *            An object to be accumulated under the key.
     * @return this.
     * @throws ADMException
     *             If the value is an invalid number or if the key is null.
     */
    public ADMObject accumulate(String key, Object value) throws ADMException {
        testValidity(value);
        Object object = this.opt(key);
        if (object == null) {
            if (value instanceof ADMUnorderedArray)
                this.put(key, new ADMUnorderedArray().put(value));
            else if (value instanceof ADMOrderedArray)
                this.put(key, new ADMOrderedArray().put(value));
            else
                this.put(key, value);
        } else if (object instanceof ADMUnorderedArray) {
            ((ADMUnorderedArray) object).put(value);
        } else if (object instanceof ADMOrderedArray) {
            ((ADMOrderedArray) object).put(value);
        } else {
            // Use ADMUnorderedArray as the default array
            this.put(key, new ADMUnorderedArray().put(object).put(value));
        }
        return this;
    }

    /**
     * Append values to the array under a key. If the key does not exist in the
     * ADMObject, then the key is put in the ADMObject with its value being a
     * ADMArray containing the value parameter. If the key was already
     * associated with a ADMArray, then the value parameter is appended to it.
     *
     * @param key
     *            A key string.
     * @param value
     *            An object to be accumulated under the key.
     * @return this.
     * @throws ADMException
     *             If the key is null or if the current value associated with
     *             the key is not a ADMArray.
     */
    public ADMObject append(String key, Object value) throws ADMException {
        testValidity(value);
        Object object = this.opt(key);
        if (object == null) {
            if (value instanceof ADMOrderedArray) {
                this.put(key, new ADMOrderedArray().put(value));
            } else {
                this.put(key, new ADMUnorderedArray().put(value));
            }
        } else if (object instanceof ADMUnorderedArray) {
            this.put(key, ((ADMUnorderedArray) object).put(value));
        } else if (object instanceof ADMOrderedArray) {
            this.put(key, ((ADMOrderedArray) object).put(value));
        } else {
            throw new ADMException("ADMObject[" + key + "] is not an ADMArray.");
        }
        return this;
    }

    /**
     * Produce a string from a double. The string "null" will be returned if the
     * number is not finite.
     *
     * @param d
     *            A double.
     * @return A String.
     */
    public static String doubleToString(double d) {
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            return "null";
        }

        // Shave off trailing zeros and decimal point, if possible.

        String string = Double.toString(d);
        if (string.indexOf('.') > 0 && string.indexOf('e') < 0 && string.indexOf('E') < 0) {
            while (string.endsWith("0")) {
                string = string.substring(0, string.length() - 1);
            }
            if (string.endsWith(".")) {
                string = string.substring(0, string.length() - 1);
            }
        }
        return string;
    }

    /**
     * Get the value object associated with a key.
     *
     * @param key
     *            A key string.
     * @return The object associated with the key.
     * @throws ADMException
     *             if the key is not found.
     */
    public Object get(String key) throws ADMException {
        if (key == null) {
            throw new ADMException("Null key.");
        }
        Object object = this.opt(key);
        if (object == null) {
            throw new ADMException("ADMObject[" + quote(key) + "] not found.");
        }
        return object;
    }

    /**
     * Get the boolean value associated with a key.
     *
     * @param key
     *            A key string.
     * @return The truth.
     * @throws ADMException
     *             if the value is not a Boolean or the String "true" or
     *             "false".
     */
    public boolean getBoolean(String key) throws ADMException {
        Object object = this.get(key);
        if (object.equals(Boolean.FALSE) || (object instanceof String && ((String) object).equalsIgnoreCase("false"))) {
            return false;
        } else if (object.equals(Boolean.TRUE)
                || (object instanceof String && ((String) object).equalsIgnoreCase("true"))) {
            return true;
        }
        throw new ADMException("ADMObject[" + quote(key) + "] is not a Boolean.");
    }

    /**
     * Get the double value associated with a key.
     *
     * @param key
     *            A key string.
     * @return The numeric value.
     * @throws ADMException
     *             if the key is not found or if the value is not a Number
     *             object and cannot be converted to a number.
     */
    public double getDouble(String key) throws ADMException {
        Object object = this.get(key);
        try {
            return object instanceof Number ? ((Number) object).doubleValue() : Double.parseDouble((String) object);
        } catch (Exception e) {
            throw new ADMException("ADMObject[" + quote(key) + "] is not a number.");
        }
    }

    /**
     * Get the int value associated with a key.
     *
     * @param key
     *            A key string.
     * @return The integer value.
     * @throws ADMException
     *             if the key is not found or if the value cannot be converted
     *             to an integer.
     */
    public int getInt(String key) throws ADMException {
        Object object = this.get(key);
        try {
            return object instanceof Number ? ((Number) object).intValue() : Integer.parseInt((String) object);
        } catch (Exception e) {
            throw new ADMException("ADMObject[" + quote(key) + "] is not an int.");
        }
    }

    /**
     * Get the ADMArray value associated with a key.
     *
     * @param key
     *            A key string.
     * @return A ADMArray which is the value.
     * @throws ADMException
     *             if the key is not found or if the value is not a ADMArray.
     */
    public AbstractADMArray getADMArray(String key) throws ADMException {
        Object object = this.get(key);
        if (object instanceof ADMOrderedArray) {
            return (ADMOrderedArray) object;
        } else if (object instanceof ADMUnorderedArray) {
            return (ADMUnorderedArray) object;
        }

        throw new ADMException("ADMObject[" + quote(key) + "] is not an ADMArray.");
    }

    /**
     * Get the ADMObject value associated with a key.
     *
     * @param key
     *            A key string.
     * @return A ADMObject which is the value.
     * @throws ADMException
     *             if the key is not found or if the value is not a ADMObject.
     */
    public ADMObject getADMObject(String key) throws ADMException {
        Object object = this.get(key);
        if (object instanceof ADMObject) {
            return (ADMObject) object;
        }
        throw new ADMException("ADMObject[" + quote(key) + "] is not a ADMObject.");
    }

    /**
     * Get the long value associated with a key.
     *
     * @param key
     *            A key string.
     * @return The long value.
     * @throws ADMException
     *             if the key is not found or if the value cannot be converted
     *             to a long.
     */
    public long getLong(String key) throws ADMException {
        Object object = this.get(key);
        try {
            return object instanceof Number ? ((Number) object).longValue() : Long.parseLong((String) object);
        } catch (Exception e) {
            throw new ADMException("ADMObject[" + quote(key) + "] is not a long.");
        }
    }

    /**
     * Get an array of field names from a ADMObject.
     *
     * @return An array of field names, or null if there are no names.
     */
    public static String[] getNames(ADMObject jo) {
        int length = jo.length();
        if (length == 0) {
            return null;
        }
        Iterator<String> iterator = jo.keys();
        String[] names = new String[length];
        int i = 0;
        while (iterator.hasNext()) {
            names[i] = iterator.next();
            i += 1;
        }
        return names;
    }

    /**
     * Get an array of field names from an Object.
     *
     * @return An array of field names, or null if there are no names.
     */
    public static String[] getNames(Object object) {
        if (object == null) {
            return null;
        }
        Class klass = object.getClass();
        Field[] fields = klass.getFields();
        int length = fields.length;
        if (length == 0) {
            return null;
        }
        String[] names = new String[length];
        for (int i = 0; i < length; i += 1) {
            names[i] = fields[i].getName();
        }
        return names;
    }

    /**
     * Get the string associated with a key.
     *
     * @param key
     *            A key string.
     * @return A string which is the value.
     * @throws ADMException
     *             if there is no string value for the key.
     */
    public String getString(String key) throws ADMException {
        Object object = this.get(key);
        if (object instanceof String) {
            return (String) object;
        }
        throw new ADMException("ADMObject[" + quote(key) + "] not a string.");
    }

    /**
     * Determine if the ADMObject contains a specific key.
     *
     * @param key
     *            A key string.
     * @return true if the key exists in the ADMObject.
     */
    public boolean has(String key) {
        return this.map.containsKey(key);
    }

    /**
     * Increment a property of a ADMObject. If there is no such property,
     * create one with a value of 1. If there is such a property, and if it is
     * an Integer, Long, Double, or Float, then add one to it.
     *
     * @param key
     *            A key string.
     * @return this.
     * @throws ADMException
     *             If there is already a property with this name that is not an
     *             Integer, Long, Double, or Float.
     */
    public ADMObject increment(String key) throws ADMException {
        Object value = this.opt(key);
        if (value == null) {
            this.put(key, 1);
        } else if (value instanceof Integer) {
            this.put(key, (Integer) value + 1);
        } else if (value instanceof Long) {
            this.put(key, (Long) value + 1);
        } else if (value instanceof Double) {
            this.put(key, (Double) value + 1);
        } else if (value instanceof Float) {
            this.put(key, (Float) value + 1);
        } else {
            throw new ADMException("Unable to increment [" + quote(key) + "].");
        }
        return this;
    }

    /**
     * Determine if the value associated with the key is null or if there is no
     * value.
     *
     * @param key
     *            A key string.
     * @return true if there is no value associated with the key or if the value
     *         is the ADMObject.NULL object.
     */
    public boolean isNull(String key) {
        return ADMObject.NULL.equals(this.opt(key));
    }

    /**
     * Get an enumeration of the keys of the ADMObject.
     *
     * @return An iterator of the keys.
     */
    public Iterator<String> keys() {
        return this.keySet().iterator();
    }

    /**
     * Get a set of keys of the ADMObject.
     *
     * @return A keySet.
     */
    public Set<String> keySet() {
        return this.map.keySet();
    }

    /**
     * Get the number of keys stored in the ADMObject.
     *
     * @return The number of keys in the ADMObject.
     */
    public int length() {
        return this.map.size();
    }

    /**
     * Produce a ADMUnorderedArray containing the names of the elements of this
     * ADMObject.
     *
     * @return A ADMUnorderedArray containing the key strings, or null if the ADMObject
     *         is empty.
     */
    public ADMUnorderedArray names() {
        ADMUnorderedArray aa = new ADMUnorderedArray();
        Iterator<String> keys = this.keys();
        while (keys.hasNext()) {
            aa.put(keys.next());
        }
        return aa.length() == 0 ? null : aa;
    }

    /**
     * Produce a string from a Number.
     *
     * @param number
     *            A Number
     * @return A String.
     * @throws ADMException
     *             If n is a non-finite number.
     */
    public static String numberToString(Number number) throws ADMException {
        if (number == null) {
            throw new ADMException("Null pointer");
        }
        testValidity(number);

        // Shave off trailing zeros and decimal point, if possible.

        String string = number.toString();
        if (string.indexOf('.') > 0 && string.indexOf('e') < 0 && string.indexOf('E') < 0) {
            while (string.endsWith("0")) {
                string = string.substring(0, string.length() - 1);
            }
            if (string.endsWith(".")) {
                string = string.substring(0, string.length() - 1);
            }
        }
        return string;
    }

    /**
     * Get an optional value associated with a key.
     *
     * @param key
     *            A key string.
     * @return An object which is the value, or null if there is no value.
     */
    public Object opt(String key) {
        return key == null ? null : this.map.get(key);
    }

    /**
     * Get an optional boolean associated with a key. It returns false if there
     * is no such key, or if the value is not Boolean.TRUE or the String "true".
     *
     * @param key
     *            A key string.
     * @return The truth.
     */
    public boolean optBoolean(String key) {
        return this.optBoolean(key, false);
    }

    /**
     * Get an optional boolean associated with a key. It returns the
     * defaultValue if there is no such key, or if it is not a Boolean or the
     * String "true" or "false" (case insensitive).
     *
     * @param key
     *            A key string.
     * @param defaultValue
     *            The default.
     * @return The truth.
     */
    public boolean optBoolean(String key, boolean defaultValue) {
        try {
            return this.getBoolean(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get an optional double associated with a key, or NaN if there is no such
     * key or if its value is not a number. If the value is a string, an attempt
     * will be made to evaluate it as a number.
     *
     * @param key
     *            A string which is the key.
     * @return An object which is the value.
     */
    public double optDouble(String key) {
        return this.optDouble(key, Double.NaN);
    }

    /**
     * Get an optional double associated with a key, or the defaultValue if
     * there is no such key or if its value is not a number. If the value is a
     * string, an attempt will be made to evaluate it as a number.
     *
     * @param key
     *            A key string.
     * @param defaultValue
     *            The default.
     * @return An object which is the value.
     */
    public double optDouble(String key, double defaultValue) {
        try {
            return this.getDouble(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get an optional int value associated with a key, or zero if there is no
     * such key or if the value is not a number. If the value is a string, an
     * attempt will be made to evaluate it as a number.
     *
     * @param key
     *            A key string.
     * @return An object which is the value.
     */
    public int optInt(String key) {
        return this.optInt(key, 0);
    }

    /**
     * Get an optional int value associated with a key, or the default if there
     * is no such key or if the value is not a number. If the value is a string,
     * an attempt will be made to evaluate it as a number.
     *
     * @param key
     *            A key string.
     * @param defaultValue
     *            The default.
     * @return An object which is the value.
     */
    public int optInt(String key, int defaultValue) {
        try {
            return this.getInt(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get an optional ADMArray associated with a key. It returns null if there
     * is no such key, or if its value is not a ADMArray.
     *
     * @param key
     *            A key string.
     * @return A ADMArray which is the value.
     */
    public AbstractADMArray optADMArray(String key) {
        Object o = this.opt(key);

        if (o instanceof ADMUnorderedArray)
            return (ADMUnorderedArray) o;
        else if (o instanceof ADMOrderedArray)
            return (ADMOrderedArray) o;

        return null;
    }

    /**
     * Get an optional ADMObject associated with a key. It returns null if
     * there is no such key, or if its value is not a ADMObject.
     *
     * @param key
     *            A key string.
     * @return A ADMObject which is the value.
     */
    public ADMObject optADMObject(String key) {
        Object object = this.opt(key);
        return object instanceof ADMObject ? (ADMObject) object : null;
    }

    /**
     * Get an optional long value associated with a key, or zero if there is no
     * such key or if the value is not a number. If the value is a string, an
     * attempt will be made to evaluate it as a number.
     *
     * @param key
     *            A key string.
     * @return An object which is the value.
     */
    public long optLong(String key) {
        return this.optLong(key, 0);
    }

    /**
     * Get an optional long value associated with a key, or the default if there
     * is no such key or if the value is not a number. If the value is a string,
     * an attempt will be made to evaluate it as a number.
     *
     * @param key
     *            A key string.
     * @param defaultValue
     *            The default.
     * @return An object which is the value.
     */
    public long optLong(String key, long defaultValue) {
        try {
            return this.getLong(key);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get an optional string associated with a key. It returns an empty string
     * if there is no such key. If the value is not a string and is not null,
     * then it is converted to a string.
     *
     * @param key
     *            A key string.
     * @return A string which is the value.
     */
    public String optString(String key) {
        return this.optString(key, "");
    }

    /**
     * Get an optional string associated with a key. It returns the defaultValue
     * if there is no such key.
     *
     * @param key
     *            A key string.
     * @param defaultValue
     *            The default.
     * @return A string which is the value.
     */
    public String optString(String key, String defaultValue) {
        Object object = this.opt(key);
        return NULL.equals(object) ? defaultValue : object.toString();
    }

    private void populateMap(Object bean) {
        Class klass = bean.getClass();

        // If klass is a System class then set includeSuperClass to false.

        boolean includeSuperClass = klass.getClassLoader() != null;

        Method[] methods = includeSuperClass ? klass.getMethods() : klass.getDeclaredMethods();
        for (int i = 0; i < methods.length; i += 1) {
            try {
                Method method = methods[i];
                if (Modifier.isPublic(method.getModifiers())) {
                    String name = method.getName();
                    String key = "";
                    if (name.startsWith("get")) {
                        if ("getClass".equals(name) || "getDeclaringClass".equals(name)) {
                            key = "";
                        } else {
                            key = name.substring(3);
                        }
                    } else if (name.startsWith("is")) {
                        key = name.substring(2);
                    }
                    if (key.length() > 0 && Character.isUpperCase(key.charAt(0))
                            && method.getParameterTypes().length == 0) {
                        if (key.length() == 1) {
                            key = key.toLowerCase();
                        } else if (!Character.isUpperCase(key.charAt(1))) {
                            key = key.substring(0, 1).toLowerCase() + key.substring(1);
                        }

                        Object result = method.invoke(bean, (Object[]) null);
                        if (result != null) {
                            this.map.put(key, wrap(result));
                        }
                    }
                }
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Put a key/boolean pair in the ADMObject.
     *
     * @param key
     *            A key string.
     * @param value
     *            A boolean which is the value.
     * @return this.
     * @throws ADMException
     *             If the key is null.
     */
    public ADMObject put(String key, boolean value) throws ADMException {
        this.put(key, value ? Boolean.TRUE : Boolean.FALSE);
        return this;
    }

    /**
     * Put a key/value pair in the ADMObject, where the value will be a
     * ADMArray which is produced from a Collection.
     *
     * @param key
     *            A key string.
     * @param value
     *            A Collection value.
     * @return this.
     * @throws ADMException
     */
    public ADMObject put(String key, Collection<Object> value) throws ADMException {
        this.put(key, new ADMUnorderedArray(value));
        return this;
    }

    /**
     * Put a key/double pair in the ADMObject.
     *
     * @param key
     *            A key string.
     * @param value
     *            A double which is the value.
     * @return this.
     * @throws ADMException
     *             If the key is null or if the number is invalid.
     */
    public ADMObject put(String key, double value) throws ADMException {
        this.put(key, new Double(value));
        return this;
    }

    /**
     * Put a key/int pair in the ADMObject.
     *
     * @param key
     *            A key string.
     * @param value
     *            An int which is the value.
     * @return this.
     * @throws ADMException
     *             If the key is null.
     */
    public ADMObject put(String key, int value) throws ADMException {
        this.put(key, new Integer(value));
        return this;
    }

    /**
     * Put a key/long pair in the ADMObject.
     *
     * @param key
     *            A key string.
     * @param value
     *            A long which is the value.
     * @return this.
     * @throws ADMException
     *             If the key is null.
     */
    public ADMObject put(String key, long value) throws ADMException {
        this.put(key, new Long(value));
        return this;
    }

    /**
     * Put a key/value pair in the ADMObject, where the value will be a
     * ADMObject which is produced from a Map.
     *
     * @param key
     *            A key string.
     * @param value
     *            A Map value.
     * @return this.
     * @throws ADMException
     */
    public ADMObject put(String key, Map<String, Object> value) throws ADMException {
        this.put(key, new ADMObject(value));
        return this;
    }

    /**
     * Put a key/value pair in the ADMObject. If the value is null, then the
     * key will be removed from the ADMObject if it is present.
     *
     * @param key
     *            A key string.
     * @param value
     *            An object which is the value. It should be of one of these
     *            types: Boolean, Double, Integer, ADMArray, ADMObject, Long,
     *            String, or the ADMObject.NULL object.
     * @return this.
     * @throws ADMException
     *             If the value is non-finite number or if the key is null.
     */
    public ADMObject put(String key, Object value) throws ADMException {
        if (key == null) {
            throw new NullPointerException("Null key.");
        }
        if (value != null) {
            testValidity(value);
            this.map.put(key, value);
        } else {
            this.remove(key);
        }
        return this;
    }

    /**
     * Put a key/value pair in the ADMObject, but only if the key and the value
     * are both non-null, and only if there is not already a member with that
     * name.
     *
     * @param key
     *            string
     * @param value
     *            object
     * @return this.
     * @throws ADMException
     *             if the key is a duplicate
     */
    public ADMObject putOnce(String key, Object value) throws ADMException {
        if (key != null && value != null) {
            if (this.opt(key) != null) {
                throw new ADMException("Duplicate key \"" + key + "\"");
            }
            this.put(key, value);
        }
        return this;
    }

    /**
     * Put a key/value pair in the ADMObject, but only if the key and the value
     * are both non-null.
     *
     * @param key
     *            A key string.
     * @param value
     *            An object which is the value. It should be of one of these
     *            types: Boolean, Double, Integer, ADMArray, ADMObject, Long,
     *            String, or the ADMObject.NULL object.
     * @return this.
     * @throws ADMException
     *             If the value is a non-finite number.
     */
    public ADMObject putOpt(String key, Object value) throws ADMException {
        if (key != null && value != null) {
            this.put(key, value);
        }
        return this;
    }

    /**
     * Produce a string in double quotes with backslash sequences in all the
     * right places. A backslash will be inserted within </, producing <\/,
     * allowing ADM text to be delivered in HTML. In ADM text, a string cannot
     * contain a control character or an unescaped quote or backslash.
     *
     * @param string
     *            A String
     * @return A String correctly formatted for insertion in a ADM text.
     */
    public static String quote(String string) {
        StringWriter sw = new StringWriter();
        synchronized (sw.getBuffer()) {
            try {
                return quote(string, sw).toString();
            } catch (IOException ignored) {
                // will never happen - we are writing to a string writer
                return "";
            }
        }
    }

    private static String escape(String string) {
        StringBuilder escaped = new StringBuilder();
        char[] string_chars = string.toCharArray();
        char prev_c;
        char c = 0;
        for (int i = 0; i < string_chars.length; i++) {
            prev_c = c;
            c = string_chars[i];
            switch (c) {
                case '\\':
                case '"':
                    escaped.append('\\');
                    escaped.append(c);
                    break;
                case '/':
                    /*if (prev_c == '<') {
                        escaped.append('\\');
                    }*/
                    escaped.append(c);
                    break;
                case '\b':
                case '\t':
                case '\n':
                case '\f':
                case '\r':
                    if (c != prev_c) {
                        escaped.append(c);
                    }
                    break;
                default:
                    if (c < ' ' || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) {
                        ; // Ignore/skip
                    } else
                        escaped.append(c);
            }
        }

        return escaped.toString();
    }

    public static Writer quote(String string, Writer w /*, boolean escape*/) throws IOException {
        if (string == null || string.length() == 0) {
            w.write("\"\"");
            return w;
        }

        char b;
        char c = 0;
        String hhhh;
        int i;
        int len = string.length();

        w.write('"');
        w.write(escape(string));
        //if (escape) {
        /*for (i = 0; i < len; i += 1) {
            b = c;
            c = string.charAt(i);

            switch (c) {
                case '\\':
                case '"':
                    w.write('\\');
                    w.write(c);
                    break;
                case '/':
                    if (b == '<') {
                        w.write('\\');
                    }
                    w.write(c);
                    break;
                case '\b':
                    w.write("\\b");
                    break;
                case '\t':
                    w.write("\\t");
                    break;
                case '\n':
                    w.write("\\n");
                    break;
                case '\f':
                    w.write("\\f");
                    break;
                case '\r':
                    w.write("\\r");
                    break;
                default:
                    if (c < ' ' || (c >= '\u0080' && c < '\u00a0') || (c >= '\u2000' && c < '\u2100')) {
                        w.write("\\u");
                        hhhh = Integer.toHexString(c);
                        w.write("0000", 0, 4 - hhhh.length());
                        w.write(hhhh);
                    } else {
                        w.write(c);
                    }
            }
        }*/
        // } else 
        //    w.write(string);
        w.write('"');
        return w;
    }

    /**
     * Remove a name and its value, if present.
     *
     * @param key
     *            The name to be removed.
     * @return The value that was associated with the name, or null if there was
     *         no value.
     */
    public Object remove(String key) {
        return this.map.remove(key);
    }

    /**
     * Determine if two ADMObjects are similar.
     * They must contain the same set of names which must be associated with
     * similar values.
     *
     * @param other
     *            The other ADMObject
     * @return true if they are equal
     */
    public boolean similar(Object other) {
        try {
            if (!(other instanceof ADMObject)) {
                return false;
            }
            Set<String> set = this.keySet();
            if (!set.equals(((ADMObject) other).keySet())) {
                return false;
            }
            Iterator<String> iterator = set.iterator();
            while (iterator.hasNext()) {
                String name = iterator.next();
                Object valueThis = this.get(name);
                Object valueOther = ((ADMObject) other).get(name);
                if (valueThis instanceof ADMObject) {
                    if (!((ADMObject) valueThis).similar(valueOther)) {
                        return false;
                    }
                } else if (valueThis instanceof ADMOrderedArray) {
                    if (!((ADMOrderedArray) valueThis).similar(valueOther)) {
                        return false;
                    }
                } else if (valueThis instanceof ADMUnorderedArray) {
                    if (!((ADMUnorderedArray) valueThis).similar(valueOther)) {
                        return false;
                    }
                } else if (!valueThis.equals(valueOther)) {
                    return false;
                }
            }
            return true;
        } catch (Throwable exception) {
            return false;
        }
    }

    /**
     * Try to convert a string into a number, boolean, or null. If the string
     * can't be converted, return the string.
     *
     * @param string
     *            A String.
     * @return A simple ADM value.
     */
    public static Object stringToValue(String string) {
        Double d;
        if (string.equals("")) {
            return string;
        }
        if (string.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        }
        if (string.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        }
        if (string.equalsIgnoreCase("null")) {
            return ADMObject.NULL;
        }

        /*
         * If it might be a number, try converting it. If a number cannot be
         * produced, then the value will just be a string.
         */

        char b = string.charAt(0);
        if ((b >= '0' && b <= '9') || b == '-') {
            try {
                if (string.indexOf('.') > -1 || string.indexOf('e') > -1 || string.indexOf('E') > -1) {
                    d = Double.valueOf(string);
                    if (!d.isInfinite() && !d.isNaN()) {
                        return d;
                    }
                } else {
                    Long myLong = new Long(string);
                    if (string.equals(myLong.toString())) {
                        if (myLong == myLong.intValue()) {
                            return myLong.intValue();
                        } else {
                            return myLong;
                        }
                    }
                }
            } catch (Exception ignore) {
            }
        }
        return string;
    }

    /**
     * Throw an exception if the object is a NaN or infinite number.
     *
     * @param o
     *            The object to test.
     * @throws ADMException
     *             If o is a non-finite number.
     */
    public static void testValidity(Object o) throws ADMException {
        if (o != null) {
            if (o instanceof Double) {
                if (((Double) o).isInfinite() || ((Double) o).isNaN()) {
                    throw new ADMException("ADM does not allow non-finite numbers.");
                }
            } else if (o instanceof Float) {
                if (((Float) o).isInfinite() || ((Float) o).isNaN()) {
                    throw new ADMException("ADM does not allow non-finite numbers.");
                }
            }
        }
    }

    /**
     * Produce a ADMArray containing the values of the members of this
     * ADMObject.
     *
     * @param names
     *            A ADMUnorderedArray containing a list of key strings. This determines
     *            the sequence of the values in the result.
     * @return A ADMUnorderedArray of values.
     * @throws ADMException
     *             If any of the values are non-finite numbers.
     */
    public ADMUnorderedArray toADMArray(ADMUnorderedArray names) throws ADMException {
        if (names == null || names.length() == 0) {
            return null;
        }
        ADMUnorderedArray aa = new ADMUnorderedArray();
        for (int i = 0; i < names.length(); i += 1) {
            aa.put(this.opt(names.getString(i)));
        }
        return aa;
    }

    /**
     * Make a ADM text of this ADMObject. For compactness, no whitespace is
     * added. If this would not result in a syntactically correct ADM text,
     * then null will be returned instead.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return a printable, displayable, portable, transmittable representation
     *         of the object, beginning with <code>{</code>&nbsp;<small>(left
     *         brace)</small> and ending with <code>}</code>&nbsp;<small>(right
     *         brace)</small>.
     */
    public String toString() {
        try {
            return this.toString(0);
        } catch (Exception e) {
            System.err.println("SEVERE: Error found when trying to generate an ADM string.");
            return "";
        }
    }

    /**
     * Make a prettyprinted ADM text of this ADMObject.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @param indentFactor
     *            The number of spaces to add to each level of indentation.
     * @return a printable, displayable, portable, transmittable representation
     *         of the object, beginning with <code>{</code>&nbsp;<small>(left
     *         brace)</small> and ending with <code>}</code>&nbsp;<small>(right
     *         brace)</small>.
     * @throws ADMException
     *             If the object contains an invalid number.
     */
    public String toString(int indentFactor) throws ADMException {
        StringWriter w = new StringWriter();
        synchronized (w.getBuffer()) {
            return this.write(w, indentFactor, 0).toString();
        }
    }

    /**
     * Make a ADM text of an Object value. If the object has an
     * value.toADMString() method, then that method will be used to produce the
     * ADM text. The method is required to produce a strictly conforming text.
     * If the object does not contain a toADMString method (which is the most
     * common case), then a text will be produced by other means. If the value
     * is an array or Collection, then a ADMArray will be made from it and its
     * toADMString method will be called. If the value is a MAP, then a
     * ADMObject will be made from it and its toADMString method will be
     * called. Otherwise, the value's toString method will be called, and the
     * result will be quoted.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @param value
     *            The value to be serialized.
     * @return a printable, displayable, transmittable representation of the
     *         object, beginning with <code>{</code>&nbsp;<small>(left
     *         brace)</small> and ending with <code>}</code>&nbsp;<small>(right
     *         brace)</small>.
     * @throws ADMException
     *             If the value is or contains an invalid number.
     */
    public static String valueToString(Object value) throws ADMException {
        if (value == null || value.equals(NULL)) {
            return "null";
        }
        if (value instanceof ADMString) {
            Object object;
            try {
                object = ((ADMString) value).toADMString();
            } catch (Exception e) {
                throw new ADMException(e);
            }
            if (object instanceof String) {
                return (String) object;
            }
            throw new ADMException("Bad value from toADMString: " + object);
        }
        if (value instanceof Number) {
            return numberToString((Number) value);
        }
        if (value instanceof Boolean || value instanceof ADMObject || value instanceof ADMUnorderedArray
                || value instanceof ADMOrderedArray) {
            return value.toString();
        }
        if (value instanceof Map) {
            return new ADMObject((Map<String, Object>) value).toString();
        }
        if (value instanceof Collection) { // Assumming a collection is not always ordered
            return new ADMUnorderedArray((Collection<Object>) value).toString();
        }
        if (value.getClass().isArray()) { // Assumming an array is always ordered
            return new ADMOrderedArray(value).toString();
        }
        return quote(value.toString());
    }

    /**
     * Wrap an object, if necessary. If the object is null, return the NULL
     * object. If it is an array or collection, wrap it in a ADMArray. If it is
     * a map, wrap it in a ADMObject. If it is a standard property (Double,
     * String, et al) then it is already wrapped. Otherwise, if it comes from
     * one of the java packages, turn it into a string. And if it doesn't, try
     * to wrap it in a ADMObject. If the wrapping fails, then null is returned.
     *
     * @param object
     *            The object to wrap
     * @return The wrapped value
     */
    public static Object wrap(Object object) {
        try {
            if (object == null) {
                return NULL;
            }
            if (object instanceof ADMObject || object instanceof ADMUnorderedArray || object instanceof ADMOrderedArray
                    || NULL.equals(object) || object instanceof ADMString || object instanceof Byte
                    || object instanceof Character || object instanceof Short || object instanceof Integer
                    || object instanceof Long || object instanceof Boolean || object instanceof Float
                    || object instanceof Double || object instanceof String) {
                return object;
            }

            if (object instanceof Collection) {
                return new ADMUnorderedArray((Collection<Object>) object);
            }
            if (object.getClass().isArray()) {
                return new ADMOrderedArray(object);
            }
            if (object instanceof Map) {
                return new ADMObject((Map<String, Object>) object);
            }
            Package objectPackage = object.getClass().getPackage();
            String objectPackageName = objectPackage != null ? objectPackage.getName() : "";
            if (objectPackageName.startsWith("java.") || objectPackageName.startsWith("javax.")
                    || object.getClass().getClassLoader() == null) {
                return object.toString();
            }
            return new ADMObject(object);
        } catch (Exception exception) {
            return null;
        }
    }

    /**
     * Write the contents of the ADMObject as ADM text to a writer. For
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

    static final Writer writeValue(Writer writer, Object value, int indentFactor, int indent) throws ADMException,
            IOException {
        if (value == null || value.equals(NULL)) {
            writer.write("null");
        } else if (value instanceof ADMObject) {
            ((ADMObject) value).write(writer, indentFactor, indent);
        } else if (value instanceof ADMUnorderedArray) {
            ((ADMUnorderedArray) value).write(writer, indentFactor, indent);
        } else if (value instanceof ADMOrderedArray) {
            ((ADMOrderedArray) value).write(writer, indentFactor, indent);
        } else if (value instanceof Map) {
            new ADMObject((Map<String, Object>) value).write(writer, indentFactor, indent);
        } else if (value instanceof Collection) {
            new ADMUnorderedArray((Collection<Object>) value).write(writer, indentFactor, indent);
        } else if (value.getClass().isArray()) {
            new ADMOrderedArray(value).write(writer, indentFactor, indent);
        } else if (value instanceof Number) {
            writer.write(numberToString((Number) value));
        } else if (value instanceof Boolean) {
            writer.write(value.toString());
        } else if (value instanceof ADMString) {
            Object o;
            try {
                o = ((ADMString) value).toADMString();
            } catch (Exception e) {
                throw new ADMException(e);
            }
            writer.write(o != null ? o.toString() : quote(value.toString()));
        } else {
            quote(value.toString(), writer);
        }
        return writer;
    }

    static final void indent(Writer writer, int indent) throws IOException {
        for (int i = 0; i < indent; i += 1) {
            writer.write(' ');
        }
    }

    /**
     * Write the contents of the ADMObject as ADM text to a writer. For
     * compactness, no whitespace is added.
     * <p>
     * Warning: This method assumes that the data structure is acyclical.
     *
     * @return The writer.
     * @throws ADMException
     */
    Writer write(Writer writer, int indentFactor, int indent) throws ADMException {
        try {
            boolean commanate = false;
            final int length = this.length();
            Iterator<String> keys = this.keys();
            writer.write('{');

            if (length == 1) {
                Object key = keys.next();
                writer.write(quote(key.toString()));
                writer.write(':');
                if (indentFactor > 0) {
                    writer.write(' ');
                }
                writeValue(writer, this.map.get(key), indentFactor, indent);
            } else if (length != 0) {
                final int newindent = indent + indentFactor;
                while (keys.hasNext()) {
                    Object key = keys.next();
                    if (commanate) {
                        writer.write(',');
                    }
                    if (indentFactor > 0) {
                        writer.write('\n');
                    }
                    indent(writer, newindent);
                    writer.write(quote(key.toString()));
                    writer.write(':');
                    if (indentFactor > 0) {
                        writer.write(' ');
                    }
                    writeValue(writer, this.map.get(key), indentFactor, newindent);
                    commanate = true;
                }
                if (indentFactor > 0) {
                    writer.write('\n');
                }
                indent(writer, indent);
            }
            writer.write('}');
            return writer;
        } catch (IOException exception) {
            throw new ADMException(exception);
        }
    }

}
