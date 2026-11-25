package io.tapstate.core.logging;

import java.util.Objects;

/**
 * A resumable position in one node-local pipeline log ring. The generation identifies one lifetime
 * of a retained pipeline buffer and the sequence identifies one appended line within that lifetime.
 * A generation changes when a buffer is replaced, so a cursor from an evicted buffer or a previous
 * process lifetime is never mistaken for a position in a new ring.
 *
 * @param generation the opaque buffer lifetime identifier
 * @param sequence   the append sequence within that generation, starting at one
 */
public record LogCursor(String generation, long sequence) {

    private static final char SEPARATOR = ':';

    public LogCursor {
        Objects.requireNonNull(generation, "generation");
        if (generation.isBlank()) {
            throw new IllegalArgumentException("generation must not be blank");
        }
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must not be negative");
        }
    }

    /** Encodes this opaque cursor for the {@code after} HTTP and websocket query parameter. */
    public String token() {
        return generation + SEPARATOR + sequence;
    }

    /** Decodes a cursor previously returned by {@link #token()}. */
    public static LogCursor parse(String token) {
        Objects.requireNonNull(token, "token");
        int separator = token.lastIndexOf(SEPARATOR);
        if (separator < 1 || separator == token.length() - 1) {
            throw new IllegalArgumentException("cursor must be generation:sequence");
        }
        try {
            return new LogCursor(token.substring(0, separator), Long.parseLong(token.substring(separator + 1)));
        } catch (NumberFormatException invalidSequence) {
            throw new IllegalArgumentException("cursor sequence must be a number", invalidSequence);
        }
    }
}
