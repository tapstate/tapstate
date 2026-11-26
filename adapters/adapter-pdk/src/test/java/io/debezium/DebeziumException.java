package io.debezium;

/** Models the exception type loaded independently by the MySQL connector. */
public final class DebeziumException extends RuntimeException {
    public DebeziumException(String message) {
        super(message);
    }
}
