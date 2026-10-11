package io.tapstate.runtime.probe;

import java.time.Instant;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Serializable coordinates for one preview; canonical resources remain private to the runtime call. */
public record PipelinePreviewRequest(
        String runId,
        String principal,
        String pipelineId,
        String outputId,
        int rootLimit,
        String sampleId,
        String candidateHash,
        List<String> canonicalResources,
        Instant deadline) implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int MAX_ROOT_LIMIT = 100;
    public static final int MAX_SAMPLE_ID_LENGTH = 128;

    public PipelinePreviewRequest {
        requireText(runId, "runId");
        requireText(principal, "principal");
        requireText(pipelineId, "pipelineId");
        requireText(candidateHash, "candidateHash");
        if (outputId != null && outputId.isBlank()) {
            throw new IllegalArgumentException("outputId must be non-blank when provided");
        }
        if (sampleId != null && (sampleId.isBlank() || sampleId.length() > MAX_SAMPLE_ID_LENGTH)) {
            throw new IllegalArgumentException(
                    "sampleId must be non-blank and no longer than " + MAX_SAMPLE_ID_LENGTH);
        }
        if (rootLimit < 1 || rootLimit > MAX_ROOT_LIMIT) {
            throw new IllegalArgumentException("rootLimit must be between 1 and " + MAX_ROOT_LIMIT);
        }
        canonicalResources = List.copyOf(canonicalResources);
        if (canonicalResources.isEmpty()) {
            throw new IllegalArgumentException("canonicalResources must not be empty");
        }
        Objects.requireNonNull(deadline, "deadline");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
    }
}
