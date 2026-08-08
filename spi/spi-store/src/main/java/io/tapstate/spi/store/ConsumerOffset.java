package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One consumer pipeline's cursor into a mining chain's change stream. It carries two quantities of
 * different lifetime: {@code perTableSeq} — the run-local read cursor into each per-table ring (a
 * table-to-sequence map; not stable across a restart, because a re-mine allocates a fresh sequence
 * space) — and {@code sinkAcked} — the source position durably acked to the pipeline's sink (stable
 * across a restart; the quantity a source-read-offset advance is bounded by). The acked position is
 * absent until the pipeline's sink first acks a change.
 *
 * <p>The acked position is a pair, and both halves are needed for different reasons. The token is what
 * a read resumes from and the only half a connector understands. The order is the engine's own record of
 * where that token sat, and it is what any comparison runs on — bounding a source-read advance by the
 * slowest consumer is a comparison, and a token is opaque by contract, with only equality defined on it.
 * A stored token whose order was dropped can no longer be ranked against anything.
 */
public record ConsumerOffset(String pipelineId, Map<String, Long> perTableSeq, ChainPosition sinkAcked) {

    public ConsumerOffset {
        if (pipelineId == null || pipelineId.isBlank()) {
            throw new IllegalArgumentException("consumer offset pipelineId must be non-blank");
        }
        if (perTableSeq == null) {
            throw new IllegalArgumentException("consumer offset perTableSeq must be set");
        }
        perTableSeq = Collections.unmodifiableMap(new LinkedHashMap<>(perTableSeq));
    }

    /** The acked token, or null when the sink has acked nothing yet — what a read resumes from. */
    public String sinkAckedSrcpos() {
        return sinkAcked == null ? null : sinkAcked.token();
    }
}
