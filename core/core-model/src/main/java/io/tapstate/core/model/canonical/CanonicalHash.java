package io.tapstate.core.model.canonical;

import io.tapstate.core.model.Resource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The content hash of a resource: lower-hex SHA-256 over its canonical structure. The hash is a
 * function of the canonical tree alone, so re-hashing an unchanged resource re-hashes identically —
 * this is the idempotency key that lets a re-apply of unchanged content be a no-op.
 *
 * <p>The structure, not the text. A resource's identity must not move when the way it is written down
 * moves: layout, quoting and line breaks are presentation, and binding the version identity to them
 * would make every reformatting of the canonical form a version change for every stored resource.
 * That is also what lets the store keep the structure and nothing else — the hash can be taken from
 * what is stored rather than from a rendering of it.
 *
 * <p>Identity stays the top-level id; this hash decides no-op vs a new revision, never storage
 * keying. SHA-256 via the built-in JDK provider is native-image safe (no reflection, no runtime
 * classpath scanning), so it holds across the fat-jar server and the native CLI alike.
 */
public final class CanonicalHash {

    private static final CanonicalWriter WRITER = new CanonicalWriter();

    private CanonicalHash() {
    }

    /**
     * Lower-hex SHA-256 (64 characters) over {@code resource}'s canonical structure.
     *
     * <p>The structure is encoded with a length in front of every piece, which is what makes two
     * different trees unable to encode alike: without it {@code {a: "bc"}} and {@code {ab: "c"}} run
     * together into the same characters. Numbers encode by how they are written rather than by the Java
     * class they arrived as — the number is what was meant, and a store that hands an int back as a
     * long must not change a resource's identity by doing so.
     */
    public static String of(Resource resource) {
        Objects.requireNonNull(resource, "resource");
        StringBuilder encoded = new StringBuilder();
        encode(WRITER.tree(resource), encoded);
        return ofText(encoded.toString());
    }

    /** Lower-hex SHA-256 of {@code text}'s UTF-8 bytes (64 characters). */
    public static String ofText(String text) {
        Objects.requireNonNull(text, "text");
        byte[] digest = sha256().digest(text.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }

    private static void encode(Object value, StringBuilder out) {
        switch (value) {
            case null -> out.append("N0:");
            case Map<?, ?> map -> {
                out.append('M').append(map.size()).append(';');
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    sized(String.valueOf(entry.getKey()), out);
                    encode(entry.getValue(), out);
                }
            }
            case List<?> list -> {
                out.append('L').append(list.size()).append(';');
                for (Object item : list) {
                    encode(item, out);
                }
            }
            case String s -> tagged('S', s, out);
            case Boolean b -> tagged('B', b.toString(), out);
            // Every number under one tag, written the way it is written down. Two numbers that the
            // canonical form shows identically are the same resource, whatever Java classes they
            // arrived as -- which is what keeps an identity from changing when a store hands an int
            // back as a long.
            case Number n -> tagged('#', n.toString(), out);
            // Anything else is a writer that grew a new value type without deciding how it is
            // identified. Crashing is the answer: folding it into one of the cases above would
            // silently give two different resources the same identity.
            default -> throw new IllegalStateException(
                    "canonical tree holds a value of an unhashable type: " + value.getClass().getName());
        }
    }

    private static void tagged(char tag, String text, StringBuilder out) {
        out.append(tag);
        sized(text, out);
    }

    private static void sized(String text, StringBuilder out) {
        out.append(text.getBytes(StandardCharsets.UTF_8).length).append(':').append(text);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a mandated JDK algorithm; its absence is a broken runtime, not a user error.
            throw new IllegalStateException("SHA-256 is required but unavailable", e);
        }
    }
}
