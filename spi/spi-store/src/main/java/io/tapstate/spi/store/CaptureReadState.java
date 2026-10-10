package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** Latest physical reader proof; a resolved anchor alone does not certify reader initialization. */
public record CaptureReadState(CaptureReadAttempt attempt, String resolvedAnchor,
        Instant anchorResolvedAt, Instant firstDeliveredAt, boolean failed, String failureCode) {
    public CaptureReadState {
        Objects.requireNonNull(attempt, "attempt");
        if ((resolvedAnchor == null) != (anchorResolvedAt == null)) {
            throw new IllegalArgumentException("resolved anchor and its observation travel together");
        }
    }
    public boolean accepted() {
        return resolvedAnchor != null && firstDeliveredAt != null && !failed;
    }
}
