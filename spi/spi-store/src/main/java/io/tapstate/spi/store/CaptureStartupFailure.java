package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** A coded failure of the exact physical reader observed after this execution attached. */
public record CaptureStartupFailure(WorkloadClaimFence pipelineClaim, CaptureResumeWitness witness,
        ClusterRecoveryPosition requestedPosition, Instant preparedAt, CaptureReadState readerState) {
    public CaptureStartupFailure {
        Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        Objects.requireNonNull(witness, "witness");
        Objects.requireNonNull(preparedAt, "preparedAt");
        Objects.requireNonNull(readerState, "readerState");
        if (!pipelineClaim.key().clusterId().equals(readerState.attempt().captureClaim().key().clusterId())
                || pipelineClaim.profileGeneration() != readerState.attempt().captureClaim().profileGeneration()) {
            throw new IllegalArgumentException("reader failure authority must belong to this execution profile");
        }
        if (!readerState.failed() || readerState.failureCode() == null || readerState.failedAt() == null
                || readerState.disposition() == null) {
            throw new IllegalArgumentException("a qualified startup failure requires the actual coded reader failure");
        }
    }
    public String code() { return readerState.failureCode(); }
    public Map<String, Object> params() { return readerState.failureParams(); }
    public String disposition() { return readerState.disposition(); }
    public Instant failedAt() { return readerState.failedAt(); }
    public String connectorId() { return witness.connectorId(); }
    public String captureId() { return readerState.attempt().captureClaim().key().resourceId(); }
}
