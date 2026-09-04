package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One consumer pipeline's own state on a mining chain — everything the chain records that belongs to a
 * single pipeline rather than to the chain. It carries quantities of three lifetimes: {@code perTableSeq}
 * — the run-local read cursor into each per-table ring (a table-to-sequence map; not stable across a
 * restart, because a re-mine allocates a fresh sequence space) — {@code sinkAcked} — the source position
 * durably acked to the pipeline's sink (stable across a restart; the quantity a source-read-offset advance
 * is bounded by) — and {@code snapshotCompletedTables} — the tables whose initial load this pipeline's
 * sink has confirmed. The acked position is absent until the pipeline's sink first acks a change.
 *
 * <p>The acked position is a pair, and both halves are needed for different reasons. The token is what
 * a read resumes from and the only half a connector understands. The order is the engine's own record of
 * where that token sat, and it is what any comparison runs on — bounding a source-read advance by the
 * slowest consumer is a comparison, and a token is opaque by contract, with only equality defined on it.
 * A stored token whose order was dropped can no longer be ranked against anything.
 *
 * <p><strong>Snapshot completion is one pipeline's answer, never the chain's.</strong> A chain is keyed by
 * the physical source coordinate and deliberately excludes the table subset, so two pipelines reading one
 * database share a chain by construction — and each writes to a target of its own. "Has this table's
 * initial load landed" is therefore a question per pipeline, and a chain-level answer to it hands the
 * second pipeline the first one's answer: it skips a load it never did, and every row of that table sits
 * in a target that never received it, with the run healthy and nothing logged. What the chain does share
 * is the mining — the source's change log read once for everyone on it — and the initial load is not part
 * of that.
 *
 * <p>A table is listed once a sink has confirmed its rows — <em>written</em>, not merely read, and
 * certainly not merely started. The distinction is the whole value of the field: a table read and never
 * written looks finished to whoever read it, and a run that skipped it on that basis would leave every row
 * of it that has not changed since absent from the target for good, because the tail only replays what
 * changed after the seam. So the mark is the sink's to make and no reader's. Membership is a set: marking a
 * table already listed changes nothing.
 *
 * <p>The lists and maps are unmodifiable defensive copies. A pure value over {@code java..} only (rule R2):
 * positions travel as opaque tokens, never as a connector type.
 */
public record ConsumerOffset(
        String pipelineId,
        Map<String, Long> perTableSeq,
        ChainPosition sinkAcked,
        List<String> snapshotCompletedTables) {

    public ConsumerOffset {
        if (pipelineId == null || pipelineId.isBlank()) {
            throw new IllegalArgumentException("consumer offset pipelineId must be non-blank");
        }
        if (perTableSeq == null) {
            throw new IllegalArgumentException("consumer offset perTableSeq must be set");
        }
        if (snapshotCompletedTables == null) {
            throw new IllegalArgumentException("consumer offset snapshotCompletedTables must be set");
        }
        perTableSeq = Collections.unmodifiableMap(new LinkedHashMap<>(perTableSeq));
        snapshotCompletedTables = List.copyOf(snapshotCompletedTables);
    }

    /** A cursor with no table's initial load confirmed yet — the shape a pipeline has before its first ack. */
    public ConsumerOffset(String pipelineId, Map<String, Long> perTableSeq, ChainPosition sinkAcked) {
        this(pipelineId, perTableSeq, sinkAcked, List.of());
    }

    /** The acked token, or null when the sink has acked nothing yet — what a read resumes from. */
    public String sinkAckedSrcpos() {
        return sinkAcked == null ? null : sinkAcked.token();
    }
}
