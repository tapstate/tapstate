package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterRun;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * A faithful in-memory {@link SrsMetaStore} for the data-plane tests, synchronized so a Jet worker's
 * member-side cursor advance is visible to the test thread's read-back: insert-only create, per-facet
 * mutators that reject an unseeded chain, and a read-cursor advance that upserts one consumer's
 * {@code perTableSeq} without clobbering its sink-ack. Enough to exercise the capture run's provision,
 * cdc-start, offset and cursor wiring without a store backend.
 */
final class InMemorySrsMetaStore implements SrsMetaStore {

    private final Map<String, SrsMeta> records = new LinkedHashMap<>();
    /** Per chain, per consumer: the ring sequence up to which each table has nothing left to receive. */
    private final Map<String, Map<String, Map<String, Long>>> ringDone = new LinkedHashMap<>();
    /**
     * Per chain, per consumer: the current run's writer accounting, without the load generation - that is
     * read off the consumer when the run is answered, as the real store reads both off one document.
     */
    private final Map<String, Map<String, WriterRun>> writerRuns = new LinkedHashMap<>();
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
        ChainPosition initial = anchor == null ? null : new ChainPosition(SourceOrder.snapshotRow(epoch), anchor);
        advanceConsumer(miningChainId, consumerId, current -> with(consumerId, current,
                initial == null ? acked(current) : initial, confirmedOf(current), ConsumerProgressKind.DIRECT_SOURCE));
        if (initial != null) {
            advanceSourceReadOffset(miningChainId, initial);
        }
        settleDirectBatches(miningChainId, consumerId);
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
        settleDirectBatches(miningChainId, consumerId);
    }

    @Override
    public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        SrsMeta m = require(miningChainId);
        // A rewritten record carries no ring places and no writer accounting, as the real store's replacement
        // carries neither.
        forget(miningChainId, offset.pipelineId());
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

    private static ChainPosition acked(ConsumerOffset existing) {
        return existing == null ? null : existing.sinkAcked();
    }

    /** {@code existing} with its acked position, table confirmations and kind replaced, everything else kept. */
    private static ConsumerOffset with(String consumerId, ConsumerOffset existing, ChainPosition acked,
            Map<String, ChainPosition> confirmed, ConsumerProgressKind kind) {
        return new ConsumerOffset(consumerId,
                existing == null ? Map.of() : existing.perTableSeq(), acked, completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch(), confirmed, kind);
    }

    @Override
    public synchronized void advanceConsumerReadSeq(
            String miningChainId, String pipelineId, String table, long lastReadSeq) {
        advanceConsumer(miningChainId, pipelineId, existing -> {
            Map<String, Long> perTable =
                    new LinkedHashMap<>(existing == null ? Map.of() : existing.perTableSeq());
            perTable.merge(table, lastReadSeq, Math::max);
            return new ConsumerOffset(pipelineId, perTable, acked(existing), completedOf(existing),
                    existing == null ? null : existing.cdcStartPosition(),
                    existing == null ? 0L : existing.snapshotEpoch(), confirmedOf(existing), kindOf(existing));
        });
    }

    @Override
    public synchronized void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
        advanceConsumer(miningChainId, pipelineId,
                existing -> with(pipelineId, existing, position, confirmedOf(existing), kindOf(existing)));
    }

    @Override
    public synchronized void setCdcStart(
            String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch) {
        advanceConsumer(miningChainId, pipelineId, existing -> new ConsumerOffset(
                pipelineId,
                existing == null ? Map.of() : existing.perTableSeq(),
                acked(existing),
                completedOf(existing),
                cdcStartPosition,
                snapshotEpoch, confirmedOf(existing), kindOf(existing)));
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
        // Per consumer, not per chain: the mark says this pipeline's sink took the table, and the
        // pipelines sharing a chain each write somewhere of their own.
        advanceConsumer(miningChainId, pipelineId, mine -> {
            List<String> completed = new ArrayList<>(completedOf(mine));
            if (!completed.contains(table)) {
                completed.add(table);
            }
            return new ConsumerOffset(pipelineId, mine == null ? Map.of() : mine.perTableSeq(),
                    acked(mine), completed,
                    mine == null ? null : mine.cdcStartPosition(),
                    mine == null ? 0L : mine.snapshotEpoch(), confirmedOf(mine), kindOf(mine));
        });
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
        writerRuns.remove(miningChainId);
        captureTables.remove(miningChainId);
        servingTables.remove(miningChainId);
        servingEpoch.remove(miningChainId);
        directCaptures.remove(miningChainId);
    }

    @Override
    public synchronized void beginWriterRun(String miningChainId, String pipelineId, String runId,
            Map<String, List<String>> expectedWritersByTable) {
        beginWriterRun(miningChainId, pipelineId, runId, expectedWritersByTable, ConsumerProgressKind.LEGACY);
    }

    /**
     * Replaces the consumer's writer accounting on the chain with {@code runId}'s, creating the consumer when it
     * has none yet, as the real store's upsert of the consumer document does - and refusing, as the real store
     * does, a consumer named for its source node whose pipeline still holds progress under its own name.
     */
    @Override
    public synchronized void beginWriterRun(String miningChainId, String consumerId, String runId,
            Map<String, List<String>> expectedWritersByTable, ConsumerProgressKind kind) {
        SrsMeta meta = require(miningChainId);
        if (SrsConsumerId.sourceOf(consumerId).isPresent()) {
            String pipeline = SrsConsumerId.pipelineOf(consumerId);
            ConsumerOffset legacy = meta.consumerOffset(pipeline).orElse(null);
            WriterRun legacyRun = writerRuns.getOrDefault(miningChainId, Map.of()).get(pipeline);
            if (legacy != null && (legacy.sinkAcked() != null || !legacy.sinkAckedByTable().isEmpty()
                    || !legacy.snapshotCompletedTables().isEmpty()
                    || legacyRun != null && legacyRun.progress().values().stream().anyMatch(w -> !w.isEmpty()))) {
                long writers = expectedWritersByTable.values().stream().flatMap(List::stream).distinct().count();
                throw new TapstateException(writers > 1 ? IoError.SINK_WRITER_PROGRESS_AMBIGUOUS
                        : IoError.SRS_PROGRESS_UNPROVEN, Map.of("pipeline", pipeline), null);
            }
        }
        advanceConsumer(miningChainId, consumerId,
                existing -> with(consumerId, existing, acked(existing), confirmedOf(existing), kind));
        writerRuns.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                .put(consumerId, new WriterRun(runId, expectedWritersByTable, Map.of(), null));
    }

    /** Refuses a run that is not the current one, as the real store's run-filtered update does. */
    @Override
    public synchronized Optional<WriterRun> advanceWriter(String miningChainId, String pipelineId, String runId,
            String writerId, String table, WriterProgress progress) {
        require(miningChainId);
        WriterRun run = writerRuns.getOrDefault(miningChainId, Map.of()).get(pipelineId);
        if (run == null || !run.runId().equals(runId)) {
            return Optional.empty();
        }
        Map<String, Map<String, WriterProgress>> next = new LinkedHashMap<>(run.progress());
        Map<String, WriterProgress> byWriter = new LinkedHashMap<>(run.progressFor(table));
        byWriter.put(writerId, progress);
        next.put(table, byWriter);
        WriterRun advanced = new WriterRun(runId, run.expected(), next, null);
        writerRuns.get(miningChainId).put(pipelineId, advanced);
        return Optional.of(withLoadOf(miningChainId, pipelineId, advanced));
    }

    @Override
    public synchronized Optional<WriterRun> writerRun(String miningChainId, String pipelineId) {
        return Optional.ofNullable(writerRuns.getOrDefault(miningChainId, Map.of()).get(pipelineId))
                .map(run -> withLoadOf(miningChainId, pipelineId, run));
    }

    /** {@code run} with the generation the consumer's load was read under, where it recorded one. */
    private WriterRun withLoadOf(String miningChainId, String pipelineId, WriterRun run) {
        ConsumerOffset consumer = consumerOf(miningChainId, pipelineId);
        Long loadedIn = consumer == null || consumer.cdcStartPosition() == null ? null : consumer.snapshotEpoch();
        return new WriterRun(run.runId(), run.expected(), run.progress(), loadedIn);
    }

    private void raiseRing(String miningChainId, String consumerId, String table, long seq) {
        if (seq >= 0) {
            ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                    .computeIfAbsent(consumerId, consumer -> new LinkedHashMap<>())
                    .merge(table, seq, Math::max);
        }
    }

    private ConsumerOffset consumerOf(String miningChainId, String pipelineId) {
        SrsMeta m = records.get(miningChainId);
        return m == null ? null : m.consumerOffset(pipelineId).orElse(null);
    }

    /** Replaces the consumer with what {@code next} makes of the one there, or of none. */
    private void advanceConsumer(String miningChainId, String pipelineId, UnaryOperator<ConsumerOffset> next) {
        SrsMeta m = require(miningChainId);
        List<ConsumerOffset> consumers = new ArrayList<>();
        ConsumerOffset existing = null;
        for (ConsumerOffset consumer : m.consumerOffsets()) {
            if (consumer.pipelineId().equals(pipelineId)) {
                existing = consumer;
            } else {
                consumers.add(consumer);
            }
        }
        consumers.add(next.apply(existing));
        records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), consumers,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    /**
     * Compared with the table's own last position only, as the real store does: a report landing after a later
     * one of the table moves nothing but the ring, and another table's position is not ranked against it.
     */
    @Override
    public synchronized void advanceSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        require(miningChainId);
        ChainPosition last = confirmedOf(consumerOf(miningChainId, pipelineId)).get(table);
        if (last == null || position.order().compareTo(last.order()) > 0) {
            advanceConsumer(miningChainId, pipelineId, existing -> {
                Map<String, ChainPosition> confirmed = new LinkedHashMap<>(confirmedOf(existing));
                confirmed.put(table, position);
                return with(pipelineId, existing, position, confirmed, kindOf(existing));
            });
        }
        raiseRing(miningChainId, pipelineId, table, position.order().seq());
    }

    @Override
    public synchronized void advanceTableConfirmed(String miningChainId, String consumerId, String table,
            ChainPosition confirmed) {
        require(miningChainId);
        ConsumerOffset existing = consumerOf(miningChainId, consumerId);
        if (existing == null) {
            return;
        }
        ChainPosition last = confirmedOf(existing).get(table);
        if (last == null || confirmed.order().compareTo(last.order()) > 0) {
            Map<String, ChainPosition> next = new LinkedHashMap<>(confirmedOf(existing));
            next.put(table, confirmed);
            advanceConsumer(miningChainId, consumerId,
                    current -> with(consumerId, current, acked(current), next, kindOf(current)));
        }
        raiseRing(miningChainId, consumerId, table, confirmed.order().seq());
    }

    @Override
    public synchronized void raiseSinkAcked(String miningChainId, String consumerId, ChainPosition position) {
        require(miningChainId);
        ConsumerOffset existing = consumerOf(miningChainId, consumerId);
        if (existing == null || existing.sinkAcked() != null && existing.sinkAcked().order() != null
                && position.order().compareTo(existing.sinkAcked().order()) <= 0) {
            return;
        }
        advanceConsumer(miningChainId, consumerId,
                current -> with(consumerId, current, position, confirmedOf(current), kindOf(current)));
    }

    @Override
    public synchronized void settleDirectBatches(String miningChainId, String consumerId) {
        ConsumerOffset consumer = consumerOf(miningChainId, consumerId);
        if (consumer == null || consumer.progressKind() != ConsumerProgressKind.DIRECT_SOURCE) {
            return;
        }
        Map<String, ChainPosition> confirmed = consumer.sinkAckedByTable();
        DirectCapture capture = directCapture(miningChainId, consumerId);
        if (capture == null) {
            WriterRun run = writerRuns.getOrDefault(miningChainId, Map.of()).get(consumerId);
            ChainPosition lowest = null;
            for (String table : run == null ? List.<String>of() : run.expected().keySet()) {
                ChainPosition position = confirmed.get(table);
                if (position == null) {
                    return;
                }
                if (lowest == null || position.order().compareTo(lowest.order()) < 0) {
                    lowest = position;
                }
            }
            if (lowest != null) {
                raiseSinkAcked(miningChainId, consumerId, lowest);
            }
            return;
        }
        if (require(miningChainId).epoch() != capture.epoch) {
            return;
        }
        int completed = 0;
        ChainPosition candidate = null;
        for (DirectBatch batch : capture.pending) {
            boolean settled = batch.position().order().epoch() == capture.epoch;
            for (Map.Entry<String, Long> target : batch.targets().entrySet()) {
                ChainPosition ack = confirmed.get(target.getKey());
                if (ack == null || ack.order().epoch() != capture.epoch || ack.order().seq() < target.getValue()) {
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
    public synchronized void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
        require(miningChainId);
        Long done = ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                .computeIfAbsent(pipelineId, pipeline -> new LinkedHashMap<>())
                .putIfAbsent(table, seq);
        if (done == null) {
            advanceConsumerReadSeq(miningChainId, pipelineId, table, seq);
        }
    }

    @Override
    public synchronized Map<String, Long> ringDoneThrough(String miningChainId, String pipelineId) {
        return Map.copyOf(ringDone.getOrDefault(miningChainId, Map.of()).getOrDefault(pipelineId, Map.of()));
    }

    private void forget(String miningChainId, String consumerId) {
        Map<String, Map<String, Long>> byConsumer = ringDone.get(miningChainId);
        if (byConsumer != null) {
            byConsumer.remove(consumerId);
        }
        Map<String, WriterRun> runs = writerRuns.get(miningChainId);
        if (runs != null) {
            runs.remove(consumerId);
        }
        Map<String, DirectCapture> captures = directCaptures.get(miningChainId);
        if (captures != null) {
            captures.remove(consumerId);
        }
    }

    private DirectCapture directCapture(String miningChainId, String consumerId) {
        return directCaptures.getOrDefault(miningChainId, Map.of()).get(consumerId);
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
                forget(miningChainId, c.pipelineId());
            }
            return remove;
        });
        // Every field but the departing consumer is carried across. The chain generation and every
        // staying consumer's snapshot state remain unchanged.
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch(), m.sourceReadAt(), m.sourceReadDurable()));
    }

    private SrsMeta require(String miningChainId) {
        SrsMeta m = records.get(miningChainId);
        if (m == null) {
            throw new IllegalStateException("mining chain not seeded: " + miningChainId);
        }
        return m;
    }

    // The fenced forms record what the unfenced ones do: this double keeps no claims to prove a fence against,
    // and the store-side fence is witnessed against the real store.

    @Override
    public synchronized boolean beginWriterRun(String miningChainId, String pipelineId, String runId,
            Map<String, List<String>> expectedWritersByTable, WorkloadClaimFence fence) {
        beginWriterRun(miningChainId, pipelineId, runId, expectedWritersByTable);
        return true;
    }

    @Override
    public synchronized boolean beginWriterRun(String miningChainId, String consumerId, String runId,
            Map<String, List<String>> expectedWritersByTable, ConsumerProgressKind kind, WorkloadClaimFence fence) {
        beginWriterRun(miningChainId, consumerId, runId, expectedWritersByTable, kind);
        return true;
    }

    @Override
    public synchronized Optional<WriterRun> advanceWriter(String miningChainId, String pipelineId, String runId,
            String writerId, String table, WriterProgress progress, WorkloadClaimFence fence) {
        return advanceWriter(miningChainId, pipelineId, runId, writerId, table, progress);
    }

    @Override
    public synchronized boolean advanceSinkAcked(String miningChainId, String pipelineId, String table,
            ChainPosition position, WorkloadClaimFence fence) {
        advanceSinkAcked(miningChainId, pipelineId, table, position);
        return true;
    }

    @Override
    public synchronized boolean advanceTableConfirmed(String miningChainId, String consumerId, String table,
            ChainPosition confirmed, WorkloadClaimFence fence) {
        advanceTableConfirmed(miningChainId, consumerId, table, confirmed);
        return true;
    }

    @Override
    public synchronized boolean raiseSinkAcked(String miningChainId, String consumerId, ChainPosition position,
            WorkloadClaimFence fence) {
        raiseSinkAcked(miningChainId, consumerId, position);
        return true;
    }

    @Override
    public synchronized boolean markSnapshotComplete(String miningChainId, String pipelineId, String table,
            WorkloadClaimFence fence) {
        markSnapshotComplete(miningChainId, pipelineId, table);
        return true;
    }
}
