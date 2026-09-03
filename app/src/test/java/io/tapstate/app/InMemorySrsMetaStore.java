package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
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

    @Override
    public synchronized Optional<SrsMeta> read(String miningChainId) {
        return Optional.ofNullable(records.get(miningChainId));
    }

    @Override
    public synchronized void create(String miningChainId, String retention) {
        if (records.containsKey(miningChainId)) {
            throw new IllegalStateException("mining chain already seeded: " + miningChainId);
        }
        records.put(miningChainId, new SrsMeta(miningChainId, null, List.of(), null, List.of(), retention));
    }

    @Override
    public synchronized void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
        SrsMeta m = require(miningChainId);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), position, m.consumerOffsets(), m.cdcStartPosition(),
                m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
    }

    @Override
    public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        SrsMeta m = require(miningChainId);
        List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
        next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
        next.add(offset);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next, m.cdcStartPosition(),
                m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
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
        next.add(new ConsumerOffset(pipelineId, perTable, ack));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next, m.cdcStartPosition(),
                m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
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
        next.add(new ConsumerOffset(pipelineId, perTable, position));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next, m.cdcStartPosition(),
                m.schemaHistory(), m.retention(), m.epoch(), m.snapshotEpoch()));
    }

    @Override
    public synchronized void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
        SrsMeta m = require(miningChainId);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(), cdcStartPosition,
                m.schemaHistory(), m.retention(), m.epoch(), snapshotEpoch));
    }

    @Override
    public synchronized long openEpoch(String miningChainId) {
        SrsMeta m = require(miningChainId);
        long opened = m.epoch() + 1;
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(), m.cdcStartPosition(),
                m.schemaHistory(), m.retention(), opened, m.snapshotEpoch()));
        return opened;
    }

    @Override
    public synchronized void appendSchemaVersion(String miningChainId, SchemaVersion version) {
        SrsMeta m = require(miningChainId);
        List<SchemaVersion> next = new ArrayList<>(m.schemaHistory());
        next.add(version);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(), m.cdcStartPosition(),
                next, m.retention(), m.epoch(), m.snapshotEpoch()));
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
                mine == null ? null : mine.sinkAcked(), completed));
        records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), consumers,
                m.cdcStartPosition(), m.schemaHistory(), m.retention(), m.epoch(),
                m.snapshotEpoch()));
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
        // Every field but the consumers is carried across. The six-argument constructor would
        // default the snapshot-complete tables to none and both generations to zero, which is not what
        // a detach does to a chain -- and a generation reset is indistinguishable, later, from a chain
        // that never opened one.
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next, m.cdcStartPosition(),
                m.schemaHistory(), m.retention(), m.epoch(),
                m.snapshotEpoch()));
    }

    private SrsMeta require(String miningChainId) {
        SrsMeta m = records.get(miningChainId);
        if (m == null) {
            throw new IllegalStateException("mining chain not seeded: " + miningChainId);
        }
        return m;
    }
}
