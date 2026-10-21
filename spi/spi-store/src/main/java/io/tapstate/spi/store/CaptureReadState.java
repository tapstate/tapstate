package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** Latest physical reader proof; a resolved anchor alone does not certify reader initialization. */
public record CaptureReadState(CaptureReadAttempt attempt, String resolvedAnchor,
        Instant anchorResolvedAt, Instant firstDeliveredAt, boolean failed, String failureCode,
        java.util.Map<String, Object> failureParams, String disposition, Instant failedAt) {
    public CaptureReadState(CaptureReadAttempt attempt, String resolvedAnchor, Instant anchorResolvedAt,
            Instant firstDeliveredAt, boolean failed, String failureCode) {
        this(attempt, resolvedAnchor, anchorResolvedAt, firstDeliveredAt, failed, failureCode, java.util.Map.of(), null, null);
    }
    public CaptureReadState {
        Objects.requireNonNull(attempt, "attempt");
        failureParams = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(failureParams));
        if (!failed && (failureCode != null || !failureParams.isEmpty() || disposition != null || failedAt != null)) {
            throw new IllegalArgumentException("healthy reader state cannot contain a failure");
        }
        if ((resolvedAnchor == null) != (anchorResolvedAt == null) || (firstDeliveredAt != null && resolvedAnchor == null)) {
            throw new IllegalArgumentException("resolved anchor and its observation travel together");
        }
    }
    public boolean accepted() {
        return resolvedAnchor != null && firstDeliveredAt != null && !failed;
    }
}
