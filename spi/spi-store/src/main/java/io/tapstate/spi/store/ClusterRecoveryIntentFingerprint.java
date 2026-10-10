package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.DesiredState;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Stable complete desired-intent identity; assembly-only equality cannot authorize recovery. */
public final class ClusterRecoveryIntentFingerprint {
    private ClusterRecoveryIntentFingerprint() {}

    public static String of(String incarnation, DesiredState desired) {
        ClusterRecoveryKey.required(incarnation, "incarnation");
        Objects.requireNonNull(desired, "desired");
        StringBuilder encoded = new StringBuilder("recovery-intent:1;");
        field(encoded, incarnation);
        field(encoded, desired.pipelineId());
        field(encoded, desired.targetState().name());
        field(encoded, desired.revision());
        field(encoded, Boolean.toString(desired.purgeState()));
        field(encoded, desired.assemblyRevision());
        field(encoded, Boolean.toString(desired.reassemble()));
        field(encoded, desired.rebuiltAtStateEpoch() == null ? null : desired.rebuiltAtStateEpoch().toString());
        return ContentHash.of(encoded.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void field(StringBuilder encoded, String value) {
        if (value == null) {
            encoded.append("-1:");
        } else {
            encoded.append(value.length()).append(':').append(value);
        }
        encoded.append(';');
    }
}
