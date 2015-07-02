package edu.uci.ics.asterix.om.adm;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;

public class ADMUnorderedArray extends ADMArray {
	
	public ADMUnorderedArray() {
		this.init();
	}
	
	public ADMUnorderedArray(Collection<Object> value) {
		this.init(value);
	}
	    
    /**
     * Construct a ADMArray from an array
     *
     * @throws ADMException
     *             If not an array.
     */
    public ADMUnorderedArray(Object array) {
    	this.init(array);
    }
    
	/**
	 * Construct a ADMArray from a ADMTokener.
	 *
	 * @param x
	 *            A ADMTokener
	 * @throws ADMException
	 *             If there is a syntax error.
	 */
	 public ADMUnorderedArray(ADMTokener x) throws ADMException {
		 this();
	        
		 if (x.nextClean() != '{') {
			x.back();
 			throw x.syntaxError("An Open ADMArray text must start with '{{'");
		 } else if(x.nextClean() != '{') {
			x.back(); // Because of the first check
			x.back();
			throw x.syntaxError("An Open ADMArray text must start with '{{'");
		 } 
        
        if (x.nextClean() != '}') {
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
                	char c = x.nextClean();
                    if ( (c == '}' && x.nextClean()=='}')) {
                        return;
                    }
                    x.back();
                    break;
                case '}':
                	if (x.nextClean() == '}') {
                		return;
                	}
                default:
                    throw x.syntaxError("Expected a ',' or '}}'");
                }
            }
        }
    }

	/**
     * Construct a ADMArray from a source ADM text.
     *
     * @param source
     *            A string that begins with <code>[</code>&nbsp;<small>(left
     *            bracket)</small> and ends with <code>]</code>
     *            &nbsp;<small>(right bracket)</small>.
     * @throws ADMException
     *             If there is a syntax error.
     */
    public ADMUnorderedArray(String source) throws ADMException {
        this(new ADMTokener(source));
    }
    
    /**
     * Put a value in the ADMArray, where the value will be a ADMArray which
     * is produced from a Collection.
     *
     * @param value
     *            A Collection value.
     * @return this.
     * @throws ADMException
     *             If the index is negative or if the value is not finite.
     */
    public ADMArray put(Collection<Object> value) {
        this.put(new ADMUnorderedArray(value));
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
        this.put(index, new ADMUnorderedArray(value));
        return this;
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
    @Override
    Writer write(Writer writer, int indentFactor, int indent)
            throws ADMException {
        try {
            boolean commanate = false;
            int length = this.length();
            
            writer.write("{{");

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
            
            writer.write("}}");
            return writer;
        } catch (IOException e) {
            throw new ADMException(e);
        }
    }

}
