package edu.uci.ics.asterix.om.adm;
/**
 * The <code>ADMString</code> interface allows a <code>toADMString()</code>
 * method so that a class can change the behavior of
 * <code>ADMObject.toString()</code>, <code>ADMArray.toString()</code>,
 * and <code>ADMWriter.value(</code>Object<code>)</code>. The
 * <code>toADMString</code> method will be used instead of the default behavior
 * of using the Object's <code>toString()</code> method and quoting the result.
 */
public interface ADMString {
    /**
     * The <code>toADMString</code> method allows a class to produce its own ADM
     * serialization.
     *
     * @return A strictly syntactically correct ADM text.
     */
    public String toADMString();
}
