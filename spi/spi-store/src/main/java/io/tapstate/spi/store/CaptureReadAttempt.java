package io.tapstate.spi.store;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Store-allocated physical reader version, scoped to its claim, chain generation and served selection. */
public record CaptureReadAttempt(String miningChainId, long chainEpoch, long version,
        WorkloadClaimFence captureClaim, List<String> tables, Kind requestedKind,
        String requestedToken, Instant requestedInstant, Instant allocatedAt) {
    public enum Kind { RESUME, PRESENT, EARLIEST, AT }
    public CaptureReadAttempt {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(captureClaim, "captureClaim");
        Objects.requireNonNull(requestedKind, "requestedKind");
        Objects.requireNonNull(allocatedAt, "allocatedAt");
        tables = List.copyOf(tables);
        if (miningChainId.isBlank() || chainEpoch < 1 || version < 1 || tables.isEmpty()
                || captureClaim.key().type() != WorkloadClaimType.CAPTURE || captureClaim.profileGeneration() < 1
                || (requestedKind == Kind.RESUME && (requestedToken == null || requestedToken.isBlank()))
                || (requestedKind == Kind.AT && requestedInstant == null)) {
            throw new IllegalArgumentException("capture read attempt requires real authority and requested coordinates");
        }
    }
}
