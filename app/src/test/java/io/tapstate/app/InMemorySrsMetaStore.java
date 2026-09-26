package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterRun;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    /**
     * Per chain, per pipeline: the current run's writer accounting, without the load generation - that is
     * read off the consumer when the run is answered, as the real store reads both off one document.
     */
    private final Map<String, Map<String, WriterRun>> writerRuns = new LinkedHashMap<>();

    @Override
    public synchronized Optional<SrsMeta> read(String miningChainId) {
        return Optional.ofNullable(records.get(miningChainId));
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
                m.schemaHistory(), m.retention(), m.epoch(), Instant.now()));
    }

    @Override
    public synchronized void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
        SrsMeta m = require(miningChainId);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), position, m.consumerOffsets(),
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        SrsMeta m = require(miningChainId);
        // A rewritten record carries no per-table acks and no writer accounting, as the real store's
        // replacement carries neither.
        forgetRingSeqs(miningChainId, offset.pipelineId());
        List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
        next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
        next.add(offset);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
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
        perTable.put(table, lastReadSeq);
        ChainPosition ack = existing == null ? null : existing.sinkAcked();
        next.add(new ConsumerOffset(
                pipelineId,
                perTable,
                ack,
                completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch()));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
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
        if (existing != null && existing.sinkAcked() != null
                && position.order().compareTo(existing.sinkAcked().order()) <= 0) {
            // Only ever raised, as the contract says: a confirmation landing after a later one moves nothing.
            return;
        }
        Map<String, Long> perTable = existing == null ? Map.of() : existing.perTableSeq();
        next.add(new ConsumerOffset(
                pipelineId,
                perTable,
                position,
                completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch()));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
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
                snapshotEpoch));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized long openEpoch(String miningChainId) {
        SrsMeta m = require(miningChainId);
        long opened = m.epoch() + 1;
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                m.schemaHistory(), m.retention(), opened));
        return opened;
    }

    @Override
    public synchronized void appendSchemaVersion(String miningChainId, SchemaVersion version) {
        SrsMeta m = require(miningChainId);
        List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
        next.add(version);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                next, m.retention(), m.epoch()));
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
                mine == null ? 0L : mine.snapshotEpoch()));
        records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), consumers,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized List<String> miningChainIdsWithConsumer(String pipelineId) {
        List<String> chains = new ArrayList<>();
        records.forEach((chainId, m) -> {
            if (m.consumerOffsets().stream().anyMatch(c -> c.pipelineId().equals(pipelineId))) {
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
    }

    /**
     * Replaces the pipeline's writer accounting on the chain with {@code runId}'s, creating the consumer when
     * the pipeline has none yet, as the real store's upsert of the consumer document does.
     */
    @Override
    public synchronized void beginWriterRun(String miningChainId, String pipelineId, String runId,
            Map<String, List<String>> expectedWritersByTable) {
        require(miningChainId);
        if (consumerOf(miningChainId, pipelineId) == null) {
            advanceConsumer(miningChainId, pipelineId, existing -> new ConsumerOffset(
                    pipelineId, Map.of(), null, List.of(), null, 0L));
        }
        writerRuns.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                .put(pipelineId, new WriterRun(runId, expectedWritersByTable, Map.of(), null));
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

    /** {@code run} with the generation the pipeline's load was read under, where it recorded one. */
    private WriterRun withLoadOf(String miningChainId, String pipelineId, WriterRun run) {
        ConsumerOffset consumer = consumerOf(miningChainId, pipelineId);
        Long loadedIn = consumer == null || consumer.cdcStartPosition() == null ? null : consumer.snapshotEpoch();
        return new WriterRun(run.runId(), run.expected(), run.progress(), loadedIn);
    }

    @Override
    public synchronized void advanceRingDone(String miningChainId, String pipelineId, String table, long seq) {
        require(miningChainId);
        if (seq >= 0) {
            ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                    .computeIfAbsent(pipelineId, pipeline -> new LinkedHashMap<>())
                    .merge(table, seq, Math::max);
        }
    }

    private ConsumerOffset consumerOf(String miningChainId, String pipelineId) {
        SrsMeta m = records.get(miningChainId);
        return m == null ? null : m.consumerOffsets().stream()
                .filter(consumer -> consumer.pipelineId().equals(pipelineId))
                .findFirst()
                .orElse(null);
    }

    /** Replaces the pipeline's consumer with what {@code next} makes of the one there, or of none. */
    private void advanceConsumer(String miningChainId, String pipelineId,
            java.util.function.UnaryOperator<ConsumerOffset> next) {
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
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized void advanceSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        advanceSinkAcked(miningChainId, pipelineId, position);
        if (position.order().seq() >= 0) {
            ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                    .computeIfAbsent(pipelineId, pipeline -> new LinkedHashMap<>())
                    .merge(table, position.order().seq(), Math::max);
        }
    }

    @Override
    public synchronized void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
        require(miningChainId);
        ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                .computeIfAbsent(pipelineId, pipeline -> new LinkedHashMap<>())
                .putIfAbsent(table, seq);
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
        Map<String, WriterRun> runs = writerRuns.get(miningChainId);
        if (runs != null) {
            runs.remove(pipelineId);
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
        next.removeIf(c -> c.pipelineId().equals(pipelineId));
        forgetRingSeqs(miningChainId, pipelineId);
        // Every field but the departing consumer is carried across. The chain generation and every
        // staying consumer's snapshot state remain unchanged.
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    private SrsMeta require(String miningChainId) {
        SrsMeta m = records.get(miningChainId);
        if (m == null) {
            throw new IllegalStateException("mining chain not seeded: " + miningChainId);
        }
        return m;
    }
}
