package io.tapstate.core.event;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A binary column's value: the bytes, and the tag the source put on them.
 *
 * <p>The frozen conversion surface has a box of its own for this, and that box is not a value - it
 * declares neither equality nor a hash, so two of them holding the same bytes are equal only when they
 * are the same object. A row value has to behave like one. A join key built from a binary column has to
 * match a key built from the same bytes on the other side, and the key's hash decides which member it
 * routes to, so an identity hash sends the two halves of one join to two places and the document never
 * fills in - with no error, which is the whole difficulty with it.
 *
 * <p>Translated at the boundary that already knows that box, so nothing downstream has to. What travels
 * is this, and the way back out builds the box again.
 *
 * <p><b>The tag travels with the bytes.</b> A document store puts one on a binary column to say what
 * the bytes are - a uuid is the ordinary case - and a target of the same kind writes it back. Bytes
 * alone would arrive tagged as whatever the default is, which is a different column from the one that
 * was read.
 *
 * <p>The array is held rather than copied, in and out: a row's values are passed along this way
 * everywhere else, and a copy per access would be one per row on the path a binary column takes.
 */
public record Bytes(byte tag, byte[] value) implements Serializable {

    private static final long serialVersionUID = 1L;

    public Bytes {
        value = value == null ? new byte[0] : value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Bytes bytes && tag == bytes.tag && Arrays.equals(value, bytes.value);
    }

    @Override
    public int hashCode() {
        return 31 * Byte.hashCode(tag) + Arrays.hashCode(value);
    }

    /** Length rather than content: a row's bytes are not something a log line should spell out. */
    @Override
    public String toString() {
        return "Bytes[tag=" + tag + ", length=" + value.length + "]";
    }
}
