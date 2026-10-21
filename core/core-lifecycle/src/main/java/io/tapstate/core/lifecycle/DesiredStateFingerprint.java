package io.tapstate.core.lifecycle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Deterministic complete-intent identity shared by admission writers and their store-side guards. */
public final class DesiredStateFingerprint {
    private DesiredStateFingerprint() { }

    public static String of(DesiredState desired) {
        Objects.requireNonNull(desired, "desired");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            field(digest, "cluster-desired-intent-v1");
            field(digest, desired.pipelineId());
            field(digest, desired.targetState().name());
            field(digest, desired.revision());
            field(digest, Boolean.toString(desired.purgeState()));
            field(digest, desired.assemblyRevision());
            field(digest, Boolean.toString(desired.reassemble()));
            field(digest, desired.rebuiltAtStateEpoch() == null ? null
                    : Long.toString(desired.rebuiltAtStateEpoch()));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException missing) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", missing);
        }
    }

    private static void field(MessageDigest digest, String value) {
        if (value == null) {
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(-1).array());
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
