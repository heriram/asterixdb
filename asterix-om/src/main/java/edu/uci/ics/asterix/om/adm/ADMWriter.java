package edu.uci.ics.asterix.om.adm;

import java.io.IOException;
import java.io.Writer;

/**
 * ADMWriter provides a quick and convenient way of producing ADM text.
 * The texts produced strictly conform to ADM syntax rules. No whitespace is
 * added, so the results are ready for transmission or storage. Each instance of
 * ADMWriter can produce one ADM text.
 * <p>
 * A ADMWriter instance provides a <code>value</code> method for appending
 * values to the
 * text, and a <code>key</code>
 * method for adding keys before values in objects. There are <code>array</code>
 * and <code>endArray</code> methods that make and bound array values, and
 * <code>object</code> and <code>endObject</code> methods which make and bound
 * object values. All of these methods return the ADMWriter instance,
 * permitting a cascade style. For example, <pre>
 * new ADMWriter(myWriter)
 *     .object()
 *         .key("ADM")
 *         .value("Hello, World!")
 *     .endObject();</pre> which writes <pre>
 * {"ADM":"Hello, World!"}</pre>
 * <p>
 * The first method called must be <code>array</code> or <code>object</code>.
 * There are no methods for adding commas or colons. ADMWriter adds them for
 * you. Objects and arrays can be nested up to 20 levels deep.
 * <p>
 * This can sometimes be easier than using a ADMObject to build a string.
 *
 */
public class ADMWriter {
    private static final int maxdepth = 200;

    /**
     * The comma flag determines if a comma should be output before the next
     * value.
     */
    private boolean comma;

    /**
     * The current mode. Values:
     * 'a' (array),
     * 'u' (unordered array),
     * 'd' (done),
     * 'i' (initial),
     * 'k' (key),
     * 'o' (object).
     */
    protected char mode;

    /**
     * The object/array stack.
     */
    private final ADMObject stack[];

    /**
     * The stack top index. A value of 0 indicates that the stack is empty.
     */
    private int top;

    /**
     * The writer that will receive the output.
     */
    protected Writer writer;

    /**
     * Make a fresh ADMWriter. It can be used to build one ADM text.
     */
    public ADMWriter(Writer w) {
        this.comma = false;
        this.mode = 'i';
        this.stack = new ADMObject[maxdepth];
        this.top = 0;
        this.writer = w;
    }

    /**
     * Append a value.
     * @param string A string value.
     * @return this
     * @throws ADMException If the value is out of sequence.
     */
    private ADMWriter append(String string) throws ADMException {
        if (string == null) {
            throw new ADMException("Null pointer");
        }
        if (this.mode == 'o' || this.mode == 'a' || this.mode == 'u') {
            try {
                if (this.comma && (this.mode == 'a' || this.mode == 'u')) {
                    this.writer.write(',');
                }
                this.writer.write(string);
            } catch (IOException e) {
                throw new ADMException(e);
            }
            if (this.mode == 'o') {
                this.mode = 'k';
            }
            this.comma = true;
            return this;
        }
        throw new ADMException("Value out of sequence.");
    }

    /**
     * Begin appending a new array. All values until the balancing
     * <code>endArray</code> will be appended to this array. The
     * <code>endArray</code> method must be called to mark the array's end.
     * @return this
     * @throws ADMException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public ADMWriter array() throws ADMException {
        return array(true);
    }

    /**
     * Begin appending a new array. All values until the balancing
     * <code>endArray</code> will be appended to this array. The
     * <code>endArray</code> method must be called to mark the array's end.
     * @return this
     * @throws ADMException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public ADMWriter unorderedArray() throws ADMException {
        return array(false);
    }

    /**
     * Begin appending a new array. All values until the balancing
     * <code>endArray</code> will be appended to this array. The
     * <code>endArray</code> method must be called to mark the array's end.
     * @return this
     * @throws ADMException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public ADMWriter array(boolean isOrdered) throws ADMException {
        if (this.mode == 'i' || this.mode == 'o' || this.mode == 'a' || this.mode == 'u') {
            this.push(null);
            if (isOrdered)
                this.append("[");
            else this.append("{{");
            this.comma = false;
            return this;
        }
        throw new ADMException("Misplaced array.");
    }

    /**
     * End something.
     * @param mode Mode
     * @param c Closing character
     * @return this
     * @throws ADMException If unbalanced.
     */
    private ADMWriter end(char mode, char c) throws ADMException {
        return this.end(mode, String.valueOf(c));
    }

    /**
     * End something.
     * @param mode Mode
     * @param s Closing string -- e.g., '}}'
     * @return this
     * @throws ADMException If unbalanced.
     */
    private ADMWriter end(char mode, String s) throws ADMException {
        if (this.mode != mode) {
            throw new ADMException((mode == 'a' || this.mode == 'u')
                    ? "Misplaced endArray."
                    : "Misplaced endObject.");
        }
        this.pop(mode);
        try {
            this.writer.write(s);
        } catch (IOException e) {
            throw new ADMException(e);
        }
        this.comma = true;
        return this;
    }

    /**
     * End an array. This method most be called to balance calls to
     * <code>array</code>.
     * @return this
     * @throws ADMException If incorrectly nested.
     */
    public ADMWriter endArray() throws ADMException {
        return this.end('a', ']');
    }

    /**
     * End an array. This method most be called to balance calls to
     * <code>array</code>.
     * @return this
     * @throws ADMException If incorrectly nested.
     */
    public ADMWriter endUnorderedArray() throws ADMException {
        return this.end('a', "}}");
    }

    /**
     * End an object. This method most be called to balance calls to
     * <code>object</code>.
     * @return this
     * @throws ADMException If incorrectly nested.
     */
    public ADMWriter endObject() throws ADMException {
        return this.end('k', '}');
    }

    /**
     * Append a key. The key will be associated with the next value. In an
     * object, every value must be preceded by a key.
     * @param string A key string.
     * @return this
     * @throws ADMException If the key is out of place. For example, keys
     *  do not belong in arrays or if the key is null.
     */
    public ADMWriter key(String string) throws ADMException {
        if (string == null) {
            throw new ADMException("Null key.");
        }
        if (this.mode == 'k') {
            try {
                this.stack[this.top - 1].putOnce(string, Boolean.TRUE);
                if (this.comma) {
                    this.writer.write(',');
                }
                this.writer.write(ADMObject.quote(string));
                this.writer.write(':');
                this.comma = false;
                this.mode = 'o';
                return this;
            } catch (IOException e) {
                throw new ADMException(e);
            }
        }
        throw new ADMException("Misplaced key.");
    }


    /**
     * Begin appending a new object. All keys and values until the balancing
     * <code>endObject</code> will be appended to this object. The
     * <code>endObject</code> method must be called to mark the object's end.
     * @return this
     * @throws ADMException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public ADMWriter object() throws ADMException {
        if (this.mode == 'i') {
            this.mode = 'o';
        }
        if (this.mode == 'o' || this.mode == 'a' || this.mode == 'u') {
            this.append("{");
            this.push(new ADMObject());
            this.comma = false;
            return this;
        }
        throw new ADMException("Misplaced object.");

    }


    /**
     * Pop an array or object scope.
     * @param c The scope to close.
     * @throws ADMException If nesting is wrong.
     */
    private void pop(char c) throws ADMException {
        if (this.top <= 0) {
            throw new ADMException("Nesting error.");
        }
        char m = this.stack[this.top - 1] == null ? 'a' : 'k';
        if (m != c) {
            throw new ADMException("Nesting error.");
        }
        this.top -= 1;
        this.mode = this.top == 0
            ? 'd'
            : this.stack[this.top - 1] == null
            ? 'a'
            : 'k';
    }

    /**
     * Push an array or object scope.
     * @param ao The scope to open.
     * @throws ADMException If nesting is too deep.
     */
    private void push(ADMObject ao) throws ADMException {
        if (this.top >= maxdepth) {
            throw new ADMException("Nesting too deep.");
        }
        this.stack[this.top] = ao;
        this.mode = ao == null ? 'a' : 'k';
        this.top += 1;
    }


    /**
     * Append either the value <code>true</code> or the value
     * <code>false</code>.
     * @param b A boolean.
     * @return this
     * @throws ADMException
     */
    public ADMWriter value(boolean b) throws ADMException {
        return this.append(b ? "true" : "false");
    }

    /**
     * Append a double value.
     * @param d A double.
     * @return this
     * @throws ADMException If the number is not finite.
     */
    public ADMWriter value(double d) throws ADMException {
        return this.value(new Double(d));
    }

    /**
     * Append a long value.
     * @param l A long.
     * @return this
     * @throws ADMException
     */
    public ADMWriter value(long l) throws ADMException {
        return this.append(Long.toString(l));
    }


    /**
     * Append an object value.
     * @param object The object to append. It can be null, or a Boolean, Number,
     *   String, ADMObject, or ADMArray, or an object that implements ADMString.
     * @return this
     * @throws ADMException If the value is out of sequence.
     */
    public ADMWriter value(Object object) throws ADMException {
        return this.append(ADMObject.valueToString(object));
    }
}
