package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.ConnectionConfig;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The key one live connector instance is filed under: the connector id plus a content hash of the
 * exact settings it was opened with.
 *
 * <p>The settings are in the key, not the connection's identity, because an instance is only
 * interchangeable with another opened against the same settings. Keyed by identity, a connection
 * whose settings were changed keeps being served by an instance still pointed at the old database —
 * and answers from it without any error at all. Two different connections configured identically are
 * the same instance to open, and share one.
 *
 * <p>The settings are hashed, not spelled out, because they carry credentials, and this key is held
 * in a map, named in diagnostics and read while debugging.
 */
final class ConnectionFingerprint {

    private ConnectionFingerprint() {
    }

    /** The key for {@code config}: {@code <connector-id>:<lower-hex sha-256 of the settings>}. */
    static String of(ConnectionConfig config) {
        return config.connectorId() + ":" + sha256(canonical(List.of(config.settings(), config.nodeConfig())));
    }

    /**
     * A rendering of {@code settings} that two equal settings always share and two different settings
     * never do. Map entries are sorted, since the order settings were written in is not part of what
     * they configure; list elements are not, since their order is. Every scalar is length-prefixed, so
     * no two different structures can render to the same text.
     */
    private static String canonical(Object value) {
        StringBuilder out = new StringBuilder();
        write(value, out);
        return out.toString();
    }

    private static void write(Object value, StringBuilder out) {
        switch (value) {
            case null -> out.append('~');
            case Map<?, ?> map -> {
                List<String> keys = new ArrayList<>();
                map.keySet().forEach(key -> keys.add(String.valueOf(key)));
                Collections.sort(keys);
                out.append('{');
                for (String key : keys) {
                    scalar(key, out);
                    out.append('=');
                    write(valueOf(map, key), out);
                    out.append(';');
                }
                out.append('}');
            }
            case Iterable<?> items -> {
                out.append('[');
                for (Object item : items) {
                    write(item, out);
                    out.append(';');
                }
                out.append(']');
            }
            default -> scalar(String.valueOf(value), out);
        }
    }

    /** The entry under {@code key}, found by the rendered key so a non-string key still resolves. */
    private static Object valueOf(Map<?, ?> map, String key) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (key.equals(String.valueOf(entry.getKey()))) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static void scalar(String text, StringBuilder out) {
        out.append(text.length()).append(':').append(text);
    }

    private static String sha256(String text) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every Java platform; its absence is not a diagnosable condition.
            throw new IllegalStateException(e);
        }
        StringBuilder hex = new StringBuilder(64);
        for (byte b : digest.digest(text.getBytes(StandardCharsets.UTF_8))) {
            hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return hex.toString();
    }
}
