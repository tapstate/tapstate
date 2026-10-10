package io.tapstate.spi.store;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Immutable deployment inputs, hashed with sorted, length-prefixed UTF-8 fields. */
public record ExecutionProfile(int formatVersion, Map<String, String> attributes) {
    public ExecutionProfile {
        if (formatVersion < 1) {
            throw new IllegalArgumentException("profile format version must be positive");
        }
        Objects.requireNonNull(attributes, "attributes");
        TreeMap<String, String> sorted = new TreeMap<>();
        attributes.forEach((key, value) -> {
            if (key == null || key.isBlank() || value == null || value.isBlank()) {
                throw new IllegalArgumentException("profile attributes must be present and nonblank");
            }
            sorted.put(key, value);
        });
        if (sorted.isEmpty()) {
            throw new IllegalArgumentException("profile attributes must not be empty");
        }
        attributes = Collections.unmodifiableMap(sorted);
    }

    public String hash() {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(formatVersion).array());
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(attributes.size()).array());
            attributes.forEach((key, value) -> {
                field(digest, key);
                field(digest, value);
            });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", unavailable);
        }
    }

    private static void field(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
