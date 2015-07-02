package edu.uci.ics.asterix.om.adm;

/**
 * The ADMException is thrown by the ADM.org classes when things are amiss.
 * This class is made based on the JSON Java package
 *
 * @author Heri Ramampiaro
 * @version 2014-10-16
 */
public class ADMException extends RuntimeException {
    private static final long serialVersionUID = 0;
    private Throwable cause;

    /**
     * Constructs a ADMException with an explanatory message.
     *
     * @param message
     *            Detail about the reason for the exception.
     */
    public ADMException(String message) {
        super(message);
    }

    /**
     * Constructs a new ADMException with the specified cause.
     * @param cause The cause.
     */
    public ADMException(Throwable cause) {
        super(cause.getMessage());
        this.cause = cause;
    }

    /**
     * Returns the cause of this exception or null if the cause is nonexistent
     * or unknown.
     *
     * @return the cause of this exception or null if the cause is nonexistent
     *          or unknown.
     */
    @Override
    public Throwable getCause() {
        return this.cause;
    }
}
