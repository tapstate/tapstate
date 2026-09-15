package io.tapstate.core.sql;

/**
 * The SQL did not parse, or did not validate against the tables it was given.
 *
 * <p>Carries no error code on purpose, and is not the diagnostic anybody is meant to read. This
 * module derives; the validation layer above it owns the user-facing vocabulary and turns this
 * into a coded diagnostic with the position information a person can act on. A code assigned here
 * would put that vocabulary in the wrong ring and point the dependency the wrong way.
 */
public final class SqlFrontEndException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SqlFrontEndException(String message, Throwable cause) {
        super(message, cause);
    }
}
