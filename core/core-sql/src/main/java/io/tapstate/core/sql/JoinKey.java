package io.tapstate.core.sql;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * The one value a set of join columns is matched by.
 *
 * <p>Two properties of this encoding are correctness rather than tidiness, and both fail the same
 * way when they are wrong: the join emits rows that look entirely ordinary, so nothing reports
 * anything and nobody finds out.
 *
 * <ul>
 *   <li><b>Each column's bytes are preceded by their length</b>, so the boundary between two
 *       columns cannot be moved. Concatenating the columns -- with a separator or without one --
 *       lets two different sets of column values produce one key, and the surplus matches that
 *       follow are indistinguishable from real ones.
 *   <li><b>A null in any column poisons the whole key.</b> SQL evaluates {@code NULL = NULL} to
 *       unknown, so a row with a null in its key matches nothing; a hash table treats null as an
 *       ordinary key and happily matches every such row with every other. A poisoned key is equal
 *       to nothing but itself, so a carrier that builds a table from one side and probes it from
 *       the other gets the required answer without having to know the rule.
 * </ul>
 *
 * <p>A value contributes its plain text, with two exceptions that would otherwise split values a
 * database considers equal: a byte string contributes its own bytes rather than an identity, and
 * an exact number drops trailing zeros so that 1.0 and 1.00 land on one key. Values that a
 * database would compare only after coercing one of them -- a 64-bit 1 against a floating point
 * 1.0 -- do not land on one key; the coercion belongs upstream, where the column types are known.
 */
public final class JoinKey {

    /** The encoded columns, or null once a null column poisoned the key. */
    private final byte[] encoded;

    private JoinKey(byte[] encoded) {
        this.encoded = encoded;
    }

    /** The key these column values match by, in the order the join's key pairs name them. */
    public static JoinKey of(List<?> values) {
        List<byte[]> parts = new ArrayList<>(values.size());
        int length = 0;
        for (Object value : values) {
            if (value == null) {
                return new JoinKey(null);
            }
            byte[] part = bytesOf(value);
            parts.add(part);
            length += Integer.BYTES + part.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        for (byte[] part : parts) {
            buffer.putInt(part.length).put(part);
        }
        return new JoinKey(buffer.array());
    }

    /** Whether this key can match anything at all: false once a null column poisoned it. */
    public boolean matchable() {
        return encoded != null;
    }

    /**
     * The name this key is filed under where state is kept by name rather than by object -- a store
     * behind a map, an index bucket, a mirror entry.
     *
     * <p>Injective for the same reason equality is: the encoding above cannot let two different sets
     * of column values produce one string, and the transcription below cannot let two different byte
     * strings produce one either. Two keys sharing a name is one row reading and overwriting another's
     * state, with the right shape and nothing reporting it.
     *
     * <p>A poisoned key has no name. It matches nothing by definition, so nothing about it is ever
     * stored or looked up, and giving it one would be inventing an identity for a row that has none.
     */
    public String name() {
        if (encoded == null) {
            throw new IllegalStateException(
                    "a key with a null column matches nothing, so it is never filed under a name");
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(encoded);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof JoinKey key
                && encoded != null
                && key.encoded != null
                && Arrays.equals(encoded, key.encoded);
    }

    @Override
    public int hashCode() {
        // A poisoned key needs a hash that no other poisoned key shares, or two of them land in
        // one bucket and the equality above is all that stands between them -- which is fine, but
        // only because it is asked. Identity gives that without inventing a value.
        return encoded == null ? System.identityHashCode(this) : Arrays.hashCode(encoded);
    }

    @Override
    public String toString() {
        return encoded == null ? "JoinKey[poisoned]" : "JoinKey[" + encoded.length + " bytes]";
    }

    private static byte[] bytesOf(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.stripTrailingZeros().toPlainString().getBytes(StandardCharsets.UTF_8);
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }
}
