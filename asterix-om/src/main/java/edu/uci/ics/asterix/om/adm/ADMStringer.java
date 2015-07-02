package edu.uci.ics.asterix.om.adm;


import java.io.StringWriter;

/**
 * ADMStringer provides a quick and convenient way of producing ADM text.
 * The texts produced strictly conform to ADM syntax rules. No whitespace is
 * added, so the results are ready for transmission or storage. Each instance of
 * ADMStringer can produce one ADM text.
 * <p>
 * A ADMStringer instance provides a <code>value</code> method for appending
 * values to the
 * text, and a <code>key</code>
 * method for adding keys before values in objects. There are <code>array</code>
 * and <code>endArray</code> methods that make and bound array values, and
 * <code>object</code> and <code>endObject</code> methods which make and bound
 * object values. All of these methods return the ADMWriter instance,
 * permitting cascade style. For example, <pre>
 * myString = new ADMStringer()
 *     .object()
 *         .key("ADM")
 *         .value("Hello, World!")
 *     .endObject()
 *     .toString();</pre> which produces the string <pre>
 * {"ADM":"Hello, World!"}</pre>
 * <p>
 * The first method called must be <code>array</code> or <code>object</code>.
 * There are no methods for adding commas or colons. ADMStringer adds them for
 * you. Objects and arrays can be nested up to 20 levels deep.
 * <p>
 * This can sometimes be easier than using a ADMObject to build a string.
 * @author ADM.org
 * @version 2008-09-18
 */
public class ADMStringer extends ADMWriter {
    /**
     * Make a fresh ADMStringer. It can be used to build one ADM text.
     */
    public ADMStringer() {
        super(new StringWriter());
    }

    /**
     * Return the ADM text. This method is used to obtain the product of the
     * ADMStringer instance. It will return <code>null</code> if there was a
     * problem in the construction of the ADM text (such as the calls to
     * <code>array</code> were not properly balanced with calls to
     * <code>endArray</code>).
     * @return The ADM text.
     */
    public String toString() {
        return this.mode == 'd' ? this.writer.toString() : "";
    }
}
