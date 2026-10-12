package io.tapstate.control.core;

import java.time.Instant;

/** Credential-free runtime status accepted by the current Cloud C2 API. */
public record CloudRuntimeStatus(
        String runtimeVersion,
        long uptimeMillis,
        int activePipelines,
        Instant lastErrorAt) {

    public CloudRuntimeStatus {
        if (runtimeVersion == null || runtimeVersion.isBlank()) {
            throw new IllegalArgumentException("runtimeVersion must be non-blank");
        }
        if (uptimeMillis < 0) {
            throw new IllegalArgumentException("uptimeMillis must not be negative");
        }
        if (activePipelines < 0) {
            throw new IllegalArgumentException("activePipelines must not be negative");
        }
    }
}
