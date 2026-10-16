package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** Proven source startup for an exact prepared execution; a read is consumed through a conditional store write. */
public record CaptureStartupProof(WorkloadClaimFence pipelineClaim, CaptureResumeWitness witness,
        ClusterRecoveryPosition requestedPosition, Instant preparedAt, Instant acceptedAt,
        CaptureReadState readerState) {
    /** The logical source entry the qualified reader/load has accepted for this consumer. */
    public ClusterRecoveryPosition acceptedPosition() {
        return requestedPosition;
    }

    public CaptureStartupProof {
        Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        Objects.requireNonNull(witness, "witness");
        Objects.requireNonNull(requestedPosition, "requestedPosition");
        Objects.requireNonNull(preparedAt, "preparedAt");
        Objects.requireNonNull(acceptedAt, "acceptedAt");
    }
}
