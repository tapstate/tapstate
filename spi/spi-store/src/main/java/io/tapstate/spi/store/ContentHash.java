package io.tapstate.spi.store;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The content hash that keys a connector artifact in the distribution store: the lower-hex SHA-256 of
 * the artifact bytes. One definition so every side that hashes an artifact — the store that files it,
 * the registrar that pre-checks an id conflict against it — agrees on the identity, rather than each
 * re-deriving it and risking a silent divergence.
 */
public final class ContentHash {

    private ContentHash() {
    }

    /** The lower-hex SHA-256 of {@code bytes}. */
    public static String of(byte[] bytes) {
        return HexFormat.of().formatHex(sha256().digest(bytes));
    }

    /** The lower-hex SHA-256 of a stream, read with bounded memory without closing it. */
    public static String of(InputStream bytes) throws IOException {
        MessageDigest digest = sha256();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = bytes.read(buffer)) != -1) {
            digest.update(buffer, 0, read);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a required JDK algorithm; its absence is a broken runtime, not a user error.
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
