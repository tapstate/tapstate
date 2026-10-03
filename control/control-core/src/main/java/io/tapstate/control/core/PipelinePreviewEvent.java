package io.tapstate.control.core;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One HTTP-neutral, sequenced event in a Pipeline preview session. */
public record PipelinePreviewEvent(
        String runId, String candidateHash, long seq, String kind, Instant at, Map<String, Object> payload) {

    public PipelinePreviewEvent {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(candidateHash, "candidateHash");
        if (seq < 0) {
            throw new IllegalArgumentException("seq must be non-negative");
        }
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(at, "at");
        payload = payload == null ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }
}
