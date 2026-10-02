package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A faithful in-memory {@link SrsMetaStore} for the data-plane tests, synchronized so a Jet worker's
 * member-side cursor advance is visible to the test thread's read-back: insert-only create, per-facet
 * mutators that reject an unseeded chain, and a read-cursor advance that upserts one consumer's
 * {@code perTableSeq} without clobbering its sink-ack. Enough to exercise the capture run's provision,
 * cdc-start, offset and cursor wiring without a store backend.
 */
final class InMemorySrsMetaStore implements SrsMetaStore {

    private final Map<String, SrsMeta> records = new LinkedHashMap<>();
    /** Per chain, per pipeline: the ring sequence of the last change each table's sink confirmed. */
    private final Map<String, Map<String, Map<String, Long>>> ringDone = new LinkedHashMap<>();
    /** Per chain and pipeline, the writer plan and the progress kept apart beneath its derived cursor. */
    private final Map<String, Map<String, SinkWriters>> sinkWriters = new LinkedHashMap<>();
    private final Map<String, LinkedHashSet<String>> captureTables = new LinkedHashMap<>();
    private final Map<String, List<String>> servingTables = new LinkedHashMap<>();
    private final Map<String, Long> servingEpoch = new LinkedHashMap<>();
    private final Map<String, Map<String, DirectCapture>> directCaptures = new LinkedHashMap<>();

    private static final class DirectCapture {
        private final long epoch;
        private final List<DirectBatch> pending = new ArrayList<>();
        private String anchor;

        private DirectCapture(long epoch) {
            this.epoch = epoch;
        }
    }

    private record DirectBatch(ChainPosition position, Map<String, Long> targets) { }

    private static final class SinkWriters {
        private Map<String, List<String>> expected = Map.of();
        private final Map<String, Map<String, WriterProgress>> progress = new LinkedHashMap<>();
    }

    private static final class WriterProgress {
        private ChainPosition acked;
        private Long ringDone;
        private boolean snapshotComplete;
    }

    @Override
    public synchronized Optional<SrsMeta> read(String miningChainId) {
        return Optional.ofNullable(records.get(miningChainId));
    }

    @Override
    public synchronized void requestCaptureTables(String miningChainId, List<String> tables) {
        require(miningChainId);
        captureTables.computeIfAbsent(miningChainId, ignored -> new LinkedHashSet<>()).addAll(tables);
    }

    @Override
    public synchronized List<String> captureTables(String miningChainId) {
        return List.copyOf(captureTables.getOrDefault(miningChainId, new LinkedHashSet<>()));
    }

    @Override
    public synchronized List<String> captureServingTables(String miningChainId) {
        return servingTables.getOrDefault(miningChainId, List.of());
    }

    @Override
    public synchronized boolean publishCaptureTables(String miningChainId, long epoch, List<String> tables) {
        SrsMeta meta = require(miningChainId);
        if (meta.epoch() != epoch || !tables.containsAll(captureTables(miningChainId))) {
            return false;
        }
        servingTables.put(miningChainId, List.copyOf(new LinkedHashSet<>(tables)));
        servingEpoch.put(miningChainId, epoch);
        return true;
    }

    @Override
    public synchronized void create(String miningChainId, String retention) {
        if (records.containsKey(miningChainId)) {
            throw new IllegalStateException("mining chain already seeded: " + miningChainId);
        }
        records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), List.of(), retention));
    }

    @Override
    public synchronized void rewindSourceReadOffset(String miningChainId, String token) {
        SrsMeta m = require(miningChainId);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), new ChainPosition(null, token), m.consumerOffsets(),
                m.schemaHistory(), m.retention(), m.epoch(), Instant.now(), false));
    }

    @Override
    public synchronized void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
        SrsMeta m = require(miningChainId);
        if (m.sourceRead() != null && m.sourceRead().order() != null
                && position.order().compareTo(m.sourceRead().order()) <= 0) {
            return;
        }
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), position, m.consumerOffsets(),
                m.schemaHistory(), m.retention(), m.epoch(), Instant.now(), false));
    }

    @Override
    public synchronized void advanceCaptureCheckpoint(String miningChainId, ChainPosition position) {
        advanceCaptureCheckpoint(miningChainId, position, captureServingTables(miningChainId));
    }

    @Override
    public synchronized void advanceCaptureCheckpoint(
            String miningChainId, ChainPosition position, List<String> servedTables) {
        SrsMeta before = require(miningChainId);
        if (before.epoch() != position.order().epoch()) {
            return;
        }
        if (!captureTables(miningChainId).isEmpty() && servingEpoch.containsKey(miningChainId)
                && (servingEpoch.get(miningChainId) != before.epoch()
                        || !captureServingTables(miningChainId).containsAll(captureTables(miningChainId))
                        || !servedTables.containsAll(captureTables(miningChainId)))) {
            return;
        }
        advanceSourceReadOffset(miningChainId, position);
        SrsMeta m = require(miningChainId);
        if (position.equals(m.sourceRead())) {
            records.put(miningChainId, new SrsMeta(
                    m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                    m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), true));
        }
    }

    @Override
    public synchronized void beginDirectCapture(String miningChainId, String consumerId, long epoch, String anchor) {
        SrsMeta meta = require(miningChainId);
        if (meta.epoch() != epoch) {
            throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        }
        Map<String, DirectCapture> captures = directCaptures.computeIfAbsent(
                miningChainId, ignored -> new LinkedHashMap<>());
        DirectCapture currentCapture = captures.get(consumerId);
        if (currentCapture != null && currentCapture.epoch == epoch) {
            if (anchor == null || currentCapture.anchor != null) {
                return;
            }
            if (!currentCapture.pending.isEmpty()) {
                throw new TapstateException(IoError.SRS_PROGRESS_UNPROVEN,
                        Map.of("pipeline", SrsConsumerId.pipelineOf(consumerId)), null);
            }
        }
        if (currentCapture == null || currentCapture.epoch != epoch) {
            currentCapture = new DirectCapture(epoch);
            captures.put(consumerId, currentCapture);
        }
        currentCapture.anchor = anchor;
        ConsumerOffset current = meta.consumerOffset(consumerId).orElse(null);
        ChainPosition prior = current == null ? null : current.sinkAcked();
        ChainPosition confirmed = anchor == null ? prior : new ChainPosition(SourceOrder.snapshotRow(epoch), anchor);
        replaceConsumer(miningChainId, new ConsumerOffset(consumerId,
                current == null ? Map.of() : current.perTableSeq(), confirmed, completedOf(current),
                current == null ? null : current.cdcStartPosition(), current == null ? 0L : current.snapshotEpoch(),
                confirmedOf(current), ConsumerProgressKind.DIRECT_SOURCE));
        if (anchor != null) {
            advanceSourceReadOffset(miningChainId, new ChainPosition(SourceOrder.snapshotRow(epoch), anchor));
        }
    }

    @Override
    public synchronized void recordDirectBatch(String miningChainId, String consumerId, ChainPosition position,
            Map<String, Long> targets) {
        SrsMeta meta = require(miningChainId);
        if (position.token() == null) {
            return;
        }
        DirectCapture capture = directCapture(miningChainId, consumerId);
        if (capture == null || capture.epoch != position.order().epoch()
                || meta.epoch() != position.order().epoch()) {
            throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        }
        capture.pending.add(new DirectBatch(position, Map.copyOf(targets)));
        advanceDirectCompletion(miningChainId, consumerId);
    }

    @Override
    public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        SrsMeta m = require(miningChainId);
        // A rewritten record carries no per-table acks, as the real store's replacement carries none.
        forgetRingSeqs(miningChainId, offset.pipelineId());
        forgetSinkWriters(miningChainId, offset.pipelineId());
        forgetDirectCapture(miningChainId, offset.pipelineId());
        List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
        next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
        next.add(offset);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    /**
     * The tables {@code existing} was already recorded as having loaded, or none for a consumer with no
     * record yet.
     *
     * <p>Carried through every per-facet mutator, because the real store's are path-scoped updates that
     * touch one field and leave the rest of the consumer's record alone. Rebuilding the consumer from
     * the facet being written -- which the three-argument constructor does, defaulting this to none --
     * erases the completed tables on the next cursor or ack advance, and it erases them in the one
     * direction nothing notices: the load looks unfinished, so a resume reads it all again and gets the
     * right answer for the wrong reason.
     */
    private static List<String> completedOf(ConsumerOffset existing) {
        return existing == null ? List.of() : existing.snapshotCompletedTables();
    }

    private static Map<String, ChainPosition> confirmedOf(ConsumerOffset existing) {
        return existing == null ? Map.of() : existing.sinkAckedByTable();
    }

    private static ConsumerProgressKind kindOf(ConsumerOffset existing) {
        return existing == null ? ConsumerProgressKind.LEGACY : existing.progressKind();
    }

    @Override
    public synchronized void advanceConsumerReadSeq(
            String miningChainId, String pipelineId, String table, long lastReadSeq) {
        SrsMeta m = require(miningChainId);
        List<ConsumerOffset> next = new ArrayList<>();
        ConsumerOffset existing = null;
        for (ConsumerOffset c : m.consumerOffsets()) {
            if (c.pipelineId().equals(pipelineId)) {
                existing = c;
            } else {
                next.add(c);
            }
        }
        Map<String, Long> perTable = new LinkedHashMap<>(existing == null ? Map.of() : existing.perTableSeq());
        perTable.merge(table, lastReadSeq, Math::max);
        ChainPosition ack = existing == null ? null : existing.sinkAcked();
        next.add(new ConsumerOffset(
                pipelineId,
                perTable,
                ack,
                completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch(), confirmedOf(existing), kindOf(existing)));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    @Override
    public synchronized void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
        SrsMeta m = require(miningChainId);
        List<ConsumerOffset> next = new ArrayList<>();
        ConsumerOffset existing = null;
        for (ConsumerOffset c : m.consumerOffsets()) {
            if (c.pipelineId().equals(pipelineId)) {
                existing = c;
            } else {
                next.add(c);
            }
        }
        Map<String, Long> perTable = existing == null ? Map.of() : existing.perTableSeq();
        next.add(new ConsumerOffset(
                pipelineId,
                perTable,
                position,
                completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch(), confirmedOf(existing), kindOf(existing)));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    @Override
    public synchronized void advanceSinkAcked(
            String miningChainId,
            String pipelineId,
            ChainPosition position,
            WorkloadClaimFence fence) {
        advanceSinkAcked(miningChainId, pipelineId, position);
    }

    @Override
    public synchronized void setCdcStart(
            String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch) {
        SrsMeta m = require(miningChainId);
        List<ConsumerOffset> next = new ArrayList<>();
        ConsumerOffset existing = null;
        for (ConsumerOffset consumer : m.consumerOffsets()) {
            if (consumer.pipelineId().equals(pipelineId)) {
                existing = consumer;
            } else {
                next.add(consumer);
            }
        }
        next.add(new ConsumerOffset(
                pipelineId,
                existing == null ? Map.of() : existing.perTableSeq(),
                existing == null ? null : existing.sinkAcked(),
                completedOf(existing),
                cdcStartPosition,
                snapshotEpoch, confirmedOf(existing), kindOf(existing)));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    @Override
    public synchronized long openEpoch(String miningChainId) {
        SrsMeta m = require(miningChainId);
        long opened = m.epoch() + 1;
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                m.schemaHistory(), m.retention(), opened, m.sourceReadAt(), m.sourceReadDurable()));
        return opened;
    }

    @Override
    public synchronized void appendSchemaVersion(String miningChainId, SchemaVersion version) {
        SrsMeta m = require(miningChainId);
        List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
        next.add(version);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                next, m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    @Override
    public synchronized void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
        SrsMeta m = require(miningChainId);
        // Per pipeline, not per chain: the mark says this pipeline's sink took the table, and the
        // pipelines sharing a chain each write somewhere of their own.
        List<ConsumerOffset> consumers = new ArrayList<>();
        ConsumerOffset mine = null;
        for (ConsumerOffset consumer : m.consumerOffsets()) {
            if (consumer.pipelineId().equals(pipelineId)) {
                mine = consumer;
            } else {
                consumers.add(consumer);
            }
        }
        List<String> completed =
                new ArrayList<>(mine == null ? List.of() : mine.snapshotCompletedTables());
        if (!completed.contains(table)) {
            completed.add(table);
        }
        consumers.add(new ConsumerOffset(pipelineId, mine == null ? Map.of() : mine.perTableSeq(),
                mine == null ? null : mine.sinkAcked(), completed,
                mine == null ? null : mine.cdcStartPosition(),
                mine == null ? 0L : mine.snapshotEpoch(), confirmedOf(mine), kindOf(mine)));
        records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), consumers,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    @Override
    public synchronized List<String> miningChainIdsWithConsumer(String pipelineId) {
        List<String> chains = new ArrayList<>();
        records.forEach((chainId, m) -> {
            if (m.consumerOffsets().stream().anyMatch(c -> SrsConsumerId.belongsTo(c.pipelineId(), pipelineId))) {
                chains.add(chainId);
            }
        });
        return chains;
    }

    @Override
    public synchronized void dropChain(String miningChainId) {
        // Idempotent for the same reason the detach below is: an absent chain already satisfies it.
        records.remove(miningChainId);
        ringDone.remove(miningChainId);
        sinkWriters.remove(miningChainId);
        captureTables.remove(miningChainId);
        servingTables.remove(miningChainId);
        servingEpoch.remove(miningChainId);
        directCaptures.remove(miningChainId);
    }

    @Override
    public synchronized void advanceSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        SrsMeta m = require(miningChainId);
        ConsumerOffset current = m.consumerOffset(pipelineId).orElse(null);
        Map<String, ChainPosition> byTable = new LinkedHashMap<>(confirmedOf(current));
        byTable.merge(table, position, (old, updated) ->
                updated.order().compareTo(old.order()) > 0 ? updated : old);
        ChainPosition aggregate = current == null ? null : current.sinkAcked();
        SinkWriters configured = writers(miningChainId, pipelineId);
        boolean activeDirect = directCapture(miningChainId, pipelineId) != null;
        if (!activeDirect && ((configured == null && byTable.size() == 1
                && (current == null || current.perTableSeq().size() <= 1))
                || (configured != null && configured.expected.size() == 1))) {
            aggregate = position;
        } else if (kindOf(current) != ConsumerProgressKind.DIRECT_SOURCE
                && aggregate != null && aggregate.order().seq() != SourceOrder.SNAPSHOT_SEQ) {
            aggregate = null;
        }
        replaceConsumer(miningChainId, new ConsumerOffset(pipelineId,
                current == null ? Map.of() : current.perTableSeq(), aggregate, completedOf(current),
                current == null ? null : current.cdcStartPosition(), current == null ? 0L : current.snapshotEpoch(),
                byTable, kindOf(current)));
        if (position.order().seq() >= 0) {
            ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                    .computeIfAbsent(pipelineId, pipeline -> new LinkedHashMap<>())
                    .merge(table, position.order().seq(), Math::max);
        }
    }

    @Override
    public synchronized void advanceSinkAcked(
            String miningChainId,
            String pipelineId,
            String table,
            ChainPosition position,
            WorkloadClaimFence fence) {
        advanceSinkAcked(miningChainId, pipelineId, table, position);
    }

    @Override
    public synchronized void configureSinkWriters(
            String miningChainId, String pipelineId, Map<String, List<String>> writerIdsByTable) {
        configureSinkWriters(miningChainId, pipelineId, writerIdsByTable, ConsumerProgressKind.LEGACY);
    }

    @Override
    public synchronized void configureSinkWriters(
            String miningChainId, String pipelineId, Map<String, List<String>> writerIdsByTable,
            ConsumerProgressKind kind) {
        SrsMeta meta = require(miningChainId);
        String owningPipeline = SrsConsumerId.pipelineOf(pipelineId);
        ConsumerOffset legacy = SrsConsumerId.sourceOf(pipelineId).isPresent()
                ? meta.consumerOffset(owningPipeline).orElse(null) : null;
        if (legacy != null && (legacy.sinkAcked() != null || legacy.cdcStartPosition() != null
                || !legacy.snapshotCompletedTables().isEmpty()
                || legacy.perTableSeq().values().stream().anyMatch(sequence -> sequence >= 0))) {
            long writerCount = writerIdsByTable.values().stream().flatMap(List::stream).distinct().count();
            if (writerCount > 1) {
                throw new TapstateException(IoError.SINK_WRITER_PROGRESS_AMBIGUOUS,
                        Map.of("pipeline", owningPipeline), null);
            }
            throw new TapstateException(IoError.SRS_PROGRESS_UNPROVEN, Map.of("pipeline", owningPipeline), null);
        }
        if (meta.consumerOffset(pipelineId).isEmpty()) {
            List<ConsumerOffset> consumers = new ArrayList<>(meta.consumerOffsets());
            consumers.add(new ConsumerOffset(pipelineId, Map.of(), null));
            meta = new SrsMeta(meta.miningChainId(), meta.sourceRead(), consumers,
                    meta.schemaHistory(), meta.retention(), meta.epoch(), meta.sourceReadAt(), meta.sourceReadDurable());
            records.put(miningChainId, meta);
        }
        ConsumerOffset existing = meta.consumerOffset(pipelineId).orElse(null);
        Map<String, Long> completedRing = ringDoneThrough(miningChainId, pipelineId);
        Set<String> completedSnapshots = existing == null
                ? Set.of()
                : Set.copyOf(existing.snapshotCompletedTables());
        LinkedHashSet<String> allWriterIds = new LinkedHashSet<>();
        writerIdsByTable.values().forEach(allWriterIds::addAll);
        SinkWriters priorPlan = writers(miningChainId, pipelineId);
        boolean hasAggregateProgress = (existing != null && existing.sinkAcked() != null)
                || !completedSnapshots.isEmpty();
        if (priorPlan == null && allWriterIds.size() > 1 && hasAggregateProgress) {
            throw new TapstateException(IoError.SINK_WRITER_PROGRESS_AMBIGUOUS,
                    Map.of("pipeline", pipelineId), null);
        }
        replaceConsumer(miningChainId, new ConsumerOffset(pipelineId, existing.perTableSeq(), existing.sinkAcked(),
                existing.snapshotCompletedTables(), existing.cdcStartPosition(), existing.snapshotEpoch(),
                existing.sinkAckedByTable(), kind));
        SinkWriters writers = priorPlan != null
                ? priorPlan
                : sinkWriters.computeIfAbsent(miningChainId, ignored -> new LinkedHashMap<>())
                        .computeIfAbsent(pipelineId, ignored -> new SinkWriters());
        Map<String, List<String>> expected = new LinkedHashMap<>();
        writerIdsByTable.forEach((table, writerIds) -> {
            expected.put(table, List.copyOf(writerIds));
            for (String writerId : writerIds) {
                WriterProgress progress = writerProgress(writers, writerId, table);
                if (progress.acked == null && existing != null) {
                    progress.acked = existing.sinkAckedByTable().get(table);
                    if (progress.acked == null && (writerIdsByTable.size() == 1
                            || (existing.sinkAcked() != null
                                    && existing.sinkAcked().order().seq() == SourceOrder.SNAPSHOT_SEQ))) {
                        progress.acked = existing.sinkAcked();
                    }
                }
                if (progress.ringDone == null) {
                    progress.ringDone = completedRing.get(table);
                }
                if (completedSnapshots.contains(table)) {
                    progress.snapshotComplete = true;
                }
            }
        });
        writers.expected = Map.copyOf(expected);
    }

    @Override
    public synchronized void configureSinkWriters(
            String miningChainId,
            String pipelineId,
            Map<String, List<String>> writerIdsByTable,
            WorkloadClaimFence fence) {
        configureSinkWriters(miningChainId, pipelineId, writerIdsByTable);
    }

    @Override
    public synchronized void configureSinkWriters(
            String miningChainId, String pipelineId, Map<String, List<String>> writerIdsByTable,
            ConsumerProgressKind kind, WorkloadClaimFence fence) {
        configureSinkWriters(miningChainId, pipelineId, writerIdsByTable, kind);
    }

    @Override
    public synchronized void advanceSinkWriterAcked(
            String miningChainId,
            String pipelineId,
            String writerId,
            String table,
            ChainPosition position) {
        require(miningChainId);
        SinkWriters writers = requireSinkWriter(writers(miningChainId, pipelineId), writerId, table);
        WriterProgress progress = writerProgress(writers, writerId, table);
        if (progress.acked == null || position.order().compareTo(progress.acked.order()) > 0) {
            progress.acked = position;
        }
        if (position.order().seq() >= 0) {
            progress.ringDone = progress.ringDone == null
                    ? position.order().seq()
                    : Math.max(progress.ringDone, position.order().seq());
        }
        derivedTableAck(writers, table).ifPresent(derived ->
                advanceSinkAcked(miningChainId, pipelineId, table, derived));
        derivedAck(writers).ifPresent(derived -> {
            ConsumerOffset current = require(miningChainId).consumerOffset(pipelineId).orElse(null);
            boolean comparable = directCapture(miningChainId, pipelineId) == null
                    && (current.progressKind() == ConsumerProgressKind.DIRECT_SOURCE
                            || writers.expected.size() == 1 || derived.order().seq() == SourceOrder.SNAPSHOT_SEQ);
            if (comparable && (current.sinkAcked() == null
                    || derived.order().compareTo(current.sinkAcked().order()) > 0)) {
                advanceSinkAcked(miningChainId, pipelineId, derived);
            }
        });
        derivedRing(writers, table).ifPresent(seq ->
                ringDone.computeIfAbsent(miningChainId, ignored -> new LinkedHashMap<>())
                        .computeIfAbsent(pipelineId, ignored -> new LinkedHashMap<>())
                        .merge(table, seq, Math::max));
        advanceDirectCompletion(miningChainId, pipelineId);
    }

    @Override
    public synchronized void advanceSinkWriterAcked(
            String miningChainId,
            String pipelineId,
            String writerId,
            String table,
            ChainPosition position,
            WorkloadClaimFence fence) {
        advanceSinkWriterAcked(miningChainId, pipelineId, writerId, table, position);
    }

    @Override
    public synchronized void markSinkWriterSnapshotComplete(
            String miningChainId, String pipelineId, String writerId, String table) {
        require(miningChainId);
        SinkWriters writers = requireSinkWriter(writers(miningChainId, pipelineId), writerId, table);
        writerProgress(writers, writerId, table).snapshotComplete = true;
        if (writers.expected.get(table).stream()
                .allMatch(expected -> writerProgress(writers, expected, table).snapshotComplete)) {
            markSnapshotComplete(miningChainId, pipelineId, table);
        }
    }

    @Override
    public synchronized void markSnapshotComplete(
            String miningChainId, String pipelineId, String table, WorkloadClaimFence fence) {
        markSnapshotComplete(miningChainId, pipelineId, table);
    }

    @Override
    public synchronized void markSinkWriterSnapshotComplete(
            String miningChainId,
            String pipelineId,
            String writerId,
            String table,
            WorkloadClaimFence fence) {
        markSinkWriterSnapshotComplete(miningChainId, pipelineId, writerId, table);
    }

    @Override
    public synchronized void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
        require(miningChainId);
        Long done = ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                .computeIfAbsent(pipelineId, pipeline -> new LinkedHashMap<>())
                .putIfAbsent(table, seq);
        if (done == null) {
            advanceConsumerReadSeq(miningChainId, pipelineId, table, seq);
            SinkWriters writers = writers(miningChainId, pipelineId);
            if (writers != null && writers.expected.containsKey(table)) {
                for (String writerId : writers.expected.get(table)) {
                    WriterProgress progress = writerProgress(writers, writerId, table);
                    if (progress.ringDone == null) {
                        progress.ringDone = seq;
                    }
                }
            }
        }
    }

    @Override
    public synchronized Map<String, Long> ringDoneThrough(String miningChainId, String pipelineId) {
        return Map.copyOf(ringDone.getOrDefault(miningChainId, Map.of()).getOrDefault(pipelineId, Map.of()));
    }

    private void forgetRingSeqs(String miningChainId, String pipelineId) {
        Map<String, Map<String, Long>> byPipeline = ringDone.get(miningChainId);
        if (byPipeline != null) {
            byPipeline.remove(pipelineId);
        }
    }

    private void forgetSinkWriters(String miningChainId, String pipelineId) {
        Map<String, SinkWriters> byPipeline = sinkWriters.get(miningChainId);
        if (byPipeline != null) {
            byPipeline.remove(pipelineId);
        }
    }

    private void forgetDirectCapture(String miningChainId, String consumerId) {
        Map<String, DirectCapture> captures = directCaptures.get(miningChainId);
        if (captures != null) {
            captures.remove(consumerId);
        }
    }

    private DirectCapture directCapture(String miningChainId, String consumerId) {
        return directCaptures.getOrDefault(miningChainId, Map.of()).get(consumerId);
    }

    private void advanceDirectCompletion(String miningChainId, String consumerId) {
        DirectCapture capture = directCapture(miningChainId, consumerId);
        SinkWriters writers = writers(miningChainId, consumerId);
        if (capture == null || writers == null || require(miningChainId).epoch() != capture.epoch) {
            return;
        }
        int completed = 0;
        ChainPosition candidate = null;
        for (DirectBatch batch : capture.pending) {
            boolean settled = batch.position().order().epoch() == capture.epoch;
            for (Map.Entry<String, Long> target : batch.targets().entrySet()) {
                ChainPosition confirmed = derivedTableAck(writers, target.getKey()).orElse(null);
                if (confirmed == null || confirmed.order().epoch() != capture.epoch
                        || confirmed.order().seq() < target.getValue()) {
                    settled = false;
                    break;
                }
            }
            if (!settled) {
                break;
            }
            candidate = batch.position();
            completed++;
        }
        if (candidate != null) {
            advanceSinkAcked(miningChainId, consumerId, candidate);
            advanceSourceReadOffset(miningChainId, candidate);
            capture.pending.subList(0, completed).clear();
        }
    }

    @Override
    public synchronized void detachConsumer(String miningChainId, String pipelineId) {
        // Idempotent, unlike the advancing mutators: an absent chain already satisfies what a detach states.
        SrsMeta m = records.get(miningChainId);
        if (m == null) {
            return;
        }
        List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
        boolean oneNode = SrsConsumerId.sourceOf(pipelineId).isPresent();
        next.removeIf(c -> {
            boolean remove = oneNode ? c.pipelineId().equals(pipelineId)
                    : SrsConsumerId.belongsTo(c.pipelineId(), pipelineId);
            if (remove) {
                forgetRingSeqs(miningChainId, c.pipelineId());
                forgetSinkWriters(miningChainId, c.pipelineId());
                forgetDirectCapture(miningChainId, c.pipelineId());
            }
            return remove;
        });
        // Every field but the departing consumer is carried across. The chain generation and every
        // staying consumer's snapshot state remain unchanged.
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    private void replaceConsumer(String miningChainId, ConsumerOffset replacement) {
        SrsMeta meta = require(miningChainId);
        List<ConsumerOffset> consumers = new ArrayList<>(meta.consumerOffsets());
        consumers.removeIf(current -> current.pipelineId().equals(replacement.pipelineId()));
        consumers.add(replacement);
        records.put(miningChainId, new SrsMeta(meta.miningChainId(), meta.sourceRead(), consumers,
                meta.schemaHistory(), meta.retention(), meta.epoch(), meta.sourceReadAt(), meta.sourceReadDurable()));
    }

    private SrsMeta require(String miningChainId) {
        SrsMeta m = records.get(miningChainId);
        if (m == null) {
            throw new IllegalStateException("mining chain not seeded: " + miningChainId);
        }
        return m;
    }

    private SinkWriters writers(String miningChainId, String pipelineId) {
        return sinkWriters.getOrDefault(miningChainId, Map.of()).get(pipelineId);
    }

    private static SinkWriters requireSinkWriter(SinkWriters writers, String writerId, String table) {
        if (writers == null || !writers.expected.getOrDefault(table, List.of()).contains(writerId)) {
            throw new IllegalStateException("sink writer '" + writerId
                    + "' is not configured to receive table '" + table + "'");
        }
        return writers;
    }

    private static WriterProgress writerProgress(SinkWriters writers, String writerId, String table) {
        return writers.progress.computeIfAbsent(writerId, ignored -> new LinkedHashMap<>())
                .computeIfAbsent(table, ignored -> new WriterProgress());
    }

    private static Optional<ChainPosition> derivedAck(SinkWriters writers) {
        ChainPosition lowest = null;
        for (Map.Entry<String, List<String>> entry : writers.expected.entrySet()) {
            for (String writerId : entry.getValue()) {
                ChainPosition acked = writerProgress(writers, writerId, entry.getKey()).acked;
                if (acked == null) {
                    return Optional.empty();
                }
                if (lowest == null || acked.order().compareTo(lowest.order()) < 0) {
                    lowest = acked;
                }
            }
        }
        return Optional.ofNullable(lowest);
    }

    private static Optional<ChainPosition> derivedTableAck(SinkWriters writers, String table) {
        ChainPosition lowest = null;
        for (String writerId : writers.expected.getOrDefault(table, List.of())) {
            ChainPosition acked = writerProgress(writers, writerId, table).acked;
            if (acked == null) {
                return Optional.empty();
            }
            if (lowest == null || acked.order().compareTo(lowest.order()) < 0) {
                lowest = acked;
            }
        }
        return Optional.ofNullable(lowest);
    }

    private static Optional<Long> derivedRing(SinkWriters writers, String table) {
        Long lowest = null;
        for (String writerId : writers.expected.getOrDefault(table, List.of())) {
            Long done = writerProgress(writers, writerId, table).ringDone;
            if (done == null) {
                return Optional.empty();
            }
            lowest = lowest == null ? done : Math.min(lowest, done);
        }
        return Optional.ofNullable(lowest);
    }
}
