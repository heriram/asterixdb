package edu.uci.ics.asterix.om.adm;


import java.io.IOException;
import java.io.Writer;
import java.util.Collection;

/**
 * A ADMOrderedArray is an ordered sequence of values. Its external text form is a
 * string wrapped in square brackets with commas separating the values. The
 * internal form is an object having <code>get</code> and <code>opt</code>
 * methods for accessing the values by index, and <code>put</code> methods for
 * adding or replacing values. The values can be any of these types:
 * <code>Boolean</code>, <code>ADMOrderedArray</code>, <code>ADMObject</code>,
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
 * @author ADM.org
 * @version 2014-05-03
 */
public class ADMOrderedArray extends ADMArray{
    
	public ADMOrderedArray(Collection<Object> value) {
		this.init(value);
	}

	public ADMOrderedArray() {
		this.init();
	}
	
    /**
     * Construct a ADMArray from an array
     *
     * @throws ADMException
     *             If not an array.
     */
    public ADMOrderedArray(Object array) {
    	this.init(array);
    }
	
    /**
     * Construct a ADMOrderedArray from a ADMTokener.
     *
     * @param x
     *            A ADMTokener
     * @throws ADMException
     *             If there is a syntax error.
     */
    public ADMOrderedArray(ADMTokener x) throws ADMException {
        super();

        
        if ( x.nextClean() != '[' ) {
        	x.back();
    		throw x.syntaxError("A ADMOrderedArray text must start with '[' or '{{'");
        }
        
        if (x.nextClean() != ']') {
            x.back();
            for (;;) {
                if (x.nextClean() == ',') {
                    x.back();
                    this.myArrayList.add(ADMObject.NULL);
                } else {
                    x.back();
                    this.myArrayList.add(x.nextValue());
                }
                switch (x.nextClean()) {
                case ',':
                    if ( x.nextClean() == ']') {
                        return;
                    }
                    x.back();
                    break;
                case ']':
                    return;
                default:
                    throw x.syntaxError("Expected a ',' or ']'");
                }
            }
        }
    }

    /**
     * Construct a ADMOrderedArray from a source ADM text.
     *
     * @param source
     *            A string that begins with <code>[</code>&nbsp;<small>(left
     *            bracket)</small> and ends with <code>]</code>
     *            &nbsp;<small>(right bracket)</small>.
     * @throws ADMException
     *             If there is a syntax error.
     */
    public ADMOrderedArray(String source) throws ADMException {
        this(new ADMTokener(source));
    }

    /**
     * Put a value in the ADMArray, where the value will be a ADMArray which
     * is produced from a Collection.
     *
     * @param value
     *            A Collection value.
     * @return this.
     */
    public ADMArray put(Collection<Object> value) {
        this.put(new ADMOrderedArray(value));
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
    public ADMArray put(int index, Collection<Object> value) throws ADMException {
        this.put(index, new ADMOrderedArray(value));
        return this;
    }

    /**
     * Write the contents of the ADMOrderedArray as ADM text to a writer. For
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
    @Override
    Writer write(Writer writer, int indentFactor, int indent)
            throws ADMException {
        try {
            boolean commanate = false;
            int length = this.length();
            
            writer.write('[');
            
            if (length == 1) {
                ADMObject.writeValue(writer, this.myArrayList.get(0),
                        indentFactor, indent);
            } else if (length != 0) {
                final int newindent = indent + indentFactor;

                for (int i = 0; i < length; i += 1) {
                    if (commanate) {
                        writer.write(',');
                    }
                    if (indentFactor > 0) {
                        writer.write('\n');
                    }
                    ADMObject.indent(writer, newindent);
                    ADMObject.writeValue(writer, this.myArrayList.get(i),
                            indentFactor, newindent);
                    commanate = true;
                }
                if (indentFactor > 0) {
                    writer.write('\n');
                }
                ADMObject.indent(writer, indent);
            }
            
            writer.write(']');
            
            return writer;
        } catch (IOException e) {
            throw new ADMException(e);
        }
    }
}
