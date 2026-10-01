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
 * is bounded by) — and the snapshot state: {@code snapshotCompletedTables}, the tables whose initial load
 * this pipeline's sink has confirmed, plus {@code cdcStartPosition} and {@code snapshotEpoch}, the seam and
 * generation at which this pipeline's load began -- or, with generation zero, where a direct tail that loads
 * nothing first began on a chain other pipelines are on. The acked position is absent until the pipeline's
 * sink first acks a change.
 *
 * <p><strong>On a chain carrying several tables, {@code sinkAcked} is the chain's contiguous prefix, not the
 * latest acknowledgement.</strong> One source log feeds every table of the chain, and the tables land
 * independently: a quiet table's change can be written while an earlier change of another table is still in
 * flight. So what each table's sink confirmed is kept in {@code sinkAckedByTable}, in that table's own ring
 * generation and sequence, and {@code sinkAcked} moves only once every change up to a point of the source log
 * has landed for every table this pipeline selects. A table's sequence never ranks another table's change.
 *
 * <p>{@code selectedTables} is the tables this pipeline currently reads through the chain's shared ring,
 * across every source of the pipeline that reads it, and {@code selectedTablesEpoch} the ring generation the
 * selection was made in. A pipeline reading the chain only through a direct tail of its own selects nothing
 * from the ring, which is an empty selection: the chain's reader owes it nothing. Both are absent on a record
 * written before selections were recorded; such a consumer is treated as selecting every table, which is the
 * reading that can only hold the chain back, never let it skip.
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
 * <p><strong>The snapshot seam is one pipeline's answer too.</strong> Every pipeline on a chain runs its
 * own load, and the seam says where that load began. Storing one seam on the shared chain lets a pipeline
 * whose restart reads no tables adopt whichever other pipeline recorded the chain first. Once the source
 * has aged past that position, the restart cannot be served. The seam and generation move together because
 * a resumed load needs both the position its tail replays from and the generation its rows remain pinned
 * to.
 *
 * <p>The lists and maps are unmodifiable defensive copies. A pure value over {@code java..} only (rule R2):
 * positions travel as opaque tokens, never as a connector type.
 */
public record ConsumerOffset(
        String pipelineId,
        Map<String, Long> perTableSeq,
        ChainPosition sinkAcked,
        List<String> snapshotCompletedTables,
        String cdcStartPosition,
        long snapshotEpoch,
        List<String> selectedTables,
        Long selectedTablesEpoch,
        Map<String, ChainPosition> sinkAckedByTable) {

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
        if (snapshotEpoch < 0) {
            throw new IllegalArgumentException(
                    "consumer offset snapshotEpoch must not be negative, got " + snapshotEpoch);
        }
        if ((selectedTables == null) != (selectedTablesEpoch == null)) {
            throw new IllegalArgumentException(
                    "consumer offset selectedTables and selectedTablesEpoch are recorded together");
        }
        if (selectedTablesEpoch != null && selectedTablesEpoch < 1) {
            throw new IllegalArgumentException(
                    "consumer offset selectedTablesEpoch must be positive, got " + selectedTablesEpoch);
        }
        if (selectedTables != null && selectedTables.stream().anyMatch(table -> table == null || table.isBlank())) {
            throw new IllegalArgumentException("consumer offset selectedTables must name every table");
        }
        if (sinkAckedByTable == null) {
            throw new IllegalArgumentException("consumer offset sinkAckedByTable must be set");
        }
        sinkAckedByTable.forEach((table, position) -> {
            if (table == null || table.isBlank() || position == null || position.order() == null) {
                throw new IllegalArgumentException(
                        "consumer offset table acknowledgements need a named table and an ordered position");
            }
        });
        perTableSeq = Collections.unmodifiableMap(new LinkedHashMap<>(perTableSeq));
        snapshotCompletedTables = List.copyOf(snapshotCompletedTables);
        selectedTables = selectedTables == null ? null : List.copyOf(selectedTables);
        sinkAckedByTable = Collections.unmodifiableMap(new LinkedHashMap<>(sinkAckedByTable));
    }

    /**
     * A consumer whose selection and table acknowledgements were not recorded: the shape of every record
     * written before either existed, and of a caller that has neither to give.
     */
    public ConsumerOffset(
            String pipelineId,
            Map<String, Long> perTableSeq,
            ChainPosition sinkAcked,
            List<String> snapshotCompletedTables,
            String cdcStartPosition,
            long snapshotEpoch) {
        this(pipelineId, perTableSeq, sinkAcked, snapshotCompletedTables, cdcStartPosition, snapshotEpoch,
                null, null, Map.of());
    }

    /** A cursor with completion state but no snapshot seam recorded yet. */
    public ConsumerOffset(
            String pipelineId,
            Map<String, Long> perTableSeq,
            ChainPosition sinkAcked,
            List<String> snapshotCompletedTables) {
        this(pipelineId, perTableSeq, sinkAcked, snapshotCompletedTables, null, 0L);
    }

    /** A cursor with no table's initial load confirmed yet — the shape a pipeline has before its first ack. */
    public ConsumerOffset(String pipelineId, Map<String, Long> perTableSeq, ChainPosition sinkAcked) {
        this(pipelineId, perTableSeq, sinkAcked, List.of(), null, 0L);
    }

    /** The acked token, or null when the sink has acked nothing yet — what a read resumes from. */
    public String sinkAckedSrcpos() {
        return sinkAcked == null ? null : sinkAcked.token();
    }

    /**
     * Whether this pipeline reads {@code table} through the chain's shared ring. A consumer whose selection
     * was never recorded answers yes for every table: it may read any of them, and treating it as reading
     * none would let the chain move past a change it still owes.
     */
    public boolean selects(String table) {
        return selectedTables == null || selectedTables.contains(table);
    }

    /** The same consumer with {@code position} as its chain-level acknowledgement, everything else kept. */
    public ConsumerOffset withSinkAcked(ChainPosition position) {
        return new ConsumerOffset(pipelineId, perTableSeq, position, snapshotCompletedTables, cdcStartPosition,
                snapshotEpoch, selectedTables, selectedTablesEpoch, sinkAckedByTable);
    }
}
