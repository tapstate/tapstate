package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;

/** Bounded transfers verify the actual bytes before a caller publishes their destination. */
public final class ArtifactStreams {
    private ArtifactStreams() {}

    /** Compatibility for byte-array stores: one exact allocation, without a growable-buffer copy. */
    public static byte[] readVerified(InputStream source, String hash, long length) throws IOException {
        if (hash == null || !hash.matches("[0-9a-f]{64}") || length < 0 || length > Integer.MAX_VALUE - 8L)
            throw unreadable("connector-artifact");
        final byte[] bytes;
        try { bytes = new byte[(int) length]; }
        catch (OutOfMemoryError unavailable) { throw unreadable(hash); }
        copyVerified(source, new OutputStream() {
            private int position;
            @Override public void write(int value) {
                if (position == bytes.length) throw unreadable(hash);
                bytes[position++] = (byte) value;
            }
            @Override public void write(byte[] block, int offset, int count) {
                Objects.checkFromIndexSize(offset, count, block.length);
                if (count > bytes.length - position) throw unreadable(hash);
                System.arraycopy(block, offset, bytes, position, count); position += count;
            }
        }, hash, length);
        return bytes;
    }

    /**
     * Copies exactly the declared length and checks SHA-256, without closing either stream.
     * The destination can contain partial bytes on failure and must remain unpublished until return.
     */
    public static void copyVerified(InputStream source, OutputStream target, String hash, long length) throws IOException {
        Objects.requireNonNull(source, "source"); Objects.requireNonNull(target, "target");
        if (hash == null || !hash.matches("[0-9a-f]{64}") || length < 0) throw unreadable("connector-artifact");
        MessageDigest digest = ContentHash.sha256();
        byte[] readBuffer = new byte[8192];
        byte[] captured = new byte[readBuffer.length];
        long remaining = length;
        while (remaining > 0) {
            int count = source.read(readBuffer, 0, (int) Math.min(readBuffer.length, remaining));
            if (count == -1) throw unreadable(hash);
            if (count == 0) continue;
            // Source callbacks may retain their read buffer; guards and writes see a private snapshot.
            System.arraycopy(readBuffer, 0, captured, 0, count);
            digest.update(captured, 0, count);
            target.write(captured, 0, count);
            remaining -= count;
        }
        if (source.read() != -1 || !hash.equals(HexFormat.of().formatHex(digest.digest()))) throw unreadable(hash);
    }

    private static TapstateException unreadable(String hash) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", hash, "field", "artifact"), null);
    }
}
