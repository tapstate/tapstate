package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import java.util.LinkedHashSet;

/**
 * A faithful in-memory {@link SrsMetaStore} for the data-plane tests, synchronized so a Jet worker's
 * member-side cursor advance is visible to the test thread's read-back: insert-only create, per-facet
 * mutators that reject an unseeded chain, and a read-cursor advance that upserts one consumer's
 * {@code perTableSeq} without clobbering its sink-ack. Enough to exercise the capture run's provision,
 * cdc-start, offset and cursor wiring without a store backend.
 */
final class InMemorySrsMetaStore implements SrsMetaStore {

    private final Map<String, SrsMeta> records = new LinkedHashMap<>();
    private final Set<String> trustedPhysicalPrefixes = new LinkedHashSet<>();
    private final Map<String, PhysicalSelection> physicalSelections = new LinkedHashMap<>();
    private final Map<String, Set<String>> physicalRequests = new LinkedHashMap<>();
    /** Per chain, per pipeline: the ring sequence of the last change each table's sink confirmed. */
    private final Map<String, Map<String, Map<String, Long>>> ringDone = new LinkedHashMap<>();

    @Override
    public synchronized Optional<SrsMeta> read(String miningChainId) {
        return Optional.ofNullable(records.get(miningChainId));
    }

    @Override
    public synchronized Optional<PhysicalSelection> physicalSelection(String miningChainId) {
        return Optional.ofNullable(physicalSelections.get(miningChainId));
    }

    @Override
    public synchronized boolean publishPhysicalSelection(String miningChainId, PhysicalSelection selection) {
        SrsMeta current = require(miningChainId);
        if (current.epoch() != selection.epoch()
                || !selection.tables().containsAll(requestedPhysicalTables(miningChainId))) {
            return false;
        }
        PhysicalSelection previous = physicalSelections.get(miningChainId);
        if (previous != null && previous.epoch() == selection.epoch()
                && !Set.copyOf(previous.tables()).equals(Set.copyOf(selection.tables()))) {
            return false;
        }
        physicalSelections.put(miningChainId, selection);
        return true;
    }

    @Override
    public synchronized List<String> requestedPhysicalTables(String miningChainId) {
        return physicalRequests.getOrDefault(miningChainId, Set.of()).stream().sorted().toList();
    }

    @Override
    public synchronized boolean requestPhysicalTables(String miningChainId, long epoch, List<String> tables) {
        if (require(miningChainId).epoch() != epoch) {
            return false;
        }
        physicalRequests.computeIfAbsent(miningChainId, ignored -> new LinkedHashSet<>()).addAll(tables);
        return true;
    }

    @Override
    public synchronized boolean replacePhysicalSelection(
            String miningChainId, PhysicalSelection expected, PhysicalSelection replacement) {
        if (require(miningChainId).epoch() != expected.epoch()
                || !Objects.equals(physicalSelections.get(miningChainId), expected)
                || replacement.epoch() != expected.epoch()
                || replacement.revision() != expected.revision() + 1
                || !replacement.tables().containsAll(expected.tables())
                || !replacement.tables().containsAll(requestedPhysicalTables(miningChainId))) {
            return false;
        }
        physicalSelections.put(miningChainId, replacement);
        return true;
    }

    @Override
    public synchronized void clearPhysicalRequests(String miningChainId, long epoch, List<String> tables) {
        if (require(miningChainId).epoch() != epoch) {
            return;
        }
        Set<String> requested = physicalRequests.get(miningChainId);
        if (requested != null) {
            requested.removeAll(tables);
            if (requested.isEmpty()) {
                physicalRequests.remove(miningChainId);
            }
        }
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
        trustedPhysicalPrefixes.add(miningChainId);
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
    public synchronized boolean physicalPrefixTrusted(String miningChainId) {
        return trustedPhysicalPrefixes.contains(miningChainId);
    }

    @Override
    public synchronized boolean establishPhysicalAnchor(String miningChainId, ChainPosition position) {
        SrsMeta current = require(miningChainId);
        if (current.epoch() != position.order().epoch()) {
            return false;
        }
        if (current.sourceRead() != null) {
            return physicalPrefixTrusted(miningChainId);
        }
        if (position.token() == null) {
            return false;
        }
        advanceSourceReadOffset(miningChainId, position);
        trustedPhysicalPrefixes.add(miningChainId);
        return true;
    }

    @Override
    public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        SrsMeta m = require(miningChainId);
        // A rewritten record carries no per-table acks, as the real store's replacement carries none.
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

    private static Map<String, ChainPosition> tableAcksOf(ConsumerOffset existing) {
        return existing == null ? Map.of() : existing.sinkAckedByTable();
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
                existing == null ? 0L : existing.snapshotEpoch(),
                existing == null ? null : existing.selectedTables(),
                existing == null ? null : existing.selectedTablesEpoch(),
                existing == null ? null : existing.cursorWriterToken(), tableAcksOf(existing)));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized void selectConsumerTables(
            String miningChainId, String pipelineId, List<String> tables, long epoch,
            String cursorWriterToken) {
        SrsMeta m = require(miningChainId);
        if (m.epoch() != epoch) {
            throw new IllegalStateException("consumer selection must match the open ring generation");
        }
        ConsumerOffset previous = m.consumerOffset(pipelineId).orElse(null);
        Map<String, Long> retained = new LinkedHashMap<>();
        if (previous != null && Objects.equals(previous.selectedTablesEpoch(), epoch)
                && Objects.equals(previous.cursorWriterToken(), cursorWriterToken)
                && previous.selectedTables() != null) {
            for (String table : tables) {
                if (previous.selectedTables().contains(table)
                        && previous.perTableSeq().containsKey(table)) {
                    retained.put(table, previous.perTableSeq().get(table));
                }
            }
        }
        List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
        next.removeIf(offset -> offset.pipelineId().equals(pipelineId));
        Map<String, ChainPosition> retainedAcks = new LinkedHashMap<>();
        if (previous != null && Objects.equals(previous.selectedTablesEpoch(), epoch)) {
            for (String table : tables) {
                ChainPosition ack = previous.sinkAckedByTable().get(table);
                if (ack != null) {
                    retainedAcks.put(table, ack);
                }
            }
        } else {
            forgetRingSeqs(miningChainId, pipelineId);
        }
        next.add(new ConsumerOffset(pipelineId, retained,
                previous == null ? null : previous.sinkAcked(), completedOf(previous),
                previous == null ? null : previous.cdcStartPosition(),
                previous == null ? 0L : previous.snapshotEpoch(),
                tables, epoch, cursorWriterToken, retainedAcks));
        records.put(miningChainId, new SrsMeta(m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized void advanceConsumerReadSeq(
            String miningChainId, String pipelineId, String table, long epoch,
            String cursorWriterToken, long lastReadSeq) {
        ConsumerOffset current = require(miningChainId).consumerOffset(pipelineId).orElse(null);
        if (current != null && Objects.equals(current.selectedTablesEpoch(), epoch)
                && Objects.equals(current.cursorWriterToken(), cursorWriterToken)
                && current.selectedTables() != null && current.selectedTables().contains(table)) {
            advanceConsumerReadSeq(miningChainId, pipelineId, table, lastReadSeq);
        }
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
                existing == null ? 0L : existing.snapshotEpoch(),
                existing == null ? null : existing.selectedTables(),
                existing == null ? null : existing.selectedTablesEpoch(),
                existing == null ? null : existing.cursorWriterToken(), tableAcksOf(existing)));
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
                snapshotEpoch,
                existing == null ? null : existing.selectedTables(),
                existing == null ? null : existing.selectedTablesEpoch(),
                existing == null ? null : existing.cursorWriterToken(), tableAcksOf(existing)));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized boolean setCdcStartIfCurrent(String miningChainId, String pipelineId,
            String cursorWriterToken, long selectedTablesEpoch,
            String cdcStartPosition, long snapshotEpoch) {
        ConsumerOffset current = require(miningChainId).consumerOffset(pipelineId).orElse(null);
        if (current == null || !Objects.equals(current.cursorWriterToken(), cursorWriterToken)
                || !Objects.equals(current.selectedTablesEpoch(), selectedTablesEpoch)) {
            return false;
        }
        setCdcStart(miningChainId, pipelineId, cdcStartPosition, snapshotEpoch);
        return true;
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
                mine == null ? 0L : mine.snapshotEpoch(),
                mine == null ? null : mine.selectedTables(),
                mine == null ? null : mine.selectedTablesEpoch(),
                mine == null ? null : mine.cursorWriterToken(), tableAcksOf(mine)));
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
        trustedPhysicalPrefixes.remove(miningChainId);
        physicalSelections.remove(miningChainId);
        physicalRequests.remove(miningChainId);
        ringDone.remove(miningChainId);
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
    public synchronized void advanceTableSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        SrsMeta meta = require(miningChainId);
        ConsumerOffset current = meta.consumerOffset(pipelineId).orElse(null);
        if (current != null && current.selectedTablesEpoch() != null
                && current.selectedTablesEpoch() != position.order().epoch()) {
            return;
        }
        if (current != null && current.selectedTables() != null
                && !current.selectedTables().contains(table)) {
            return;
        }
        ChainPosition prior = current == null ? null : current.sinkAckedByTable().get(table);
        if (prior != null && prior.order().compareTo(position.order()) >= 0) {
            return;
        }
        Map<String, ChainPosition> tableAcks = new LinkedHashMap<>(tableAcksOf(current));
        tableAcks.put(table, position);
        List<ConsumerOffset> next = new ArrayList<>(meta.consumerOffsets());
        next.removeIf(offset -> offset.pipelineId().equals(pipelineId));
        next.add(new ConsumerOffset(pipelineId,
                current == null ? Map.of() : current.perTableSeq(),
                current == null ? null : current.sinkAcked(),
                completedOf(current),
                current == null ? null : current.cdcStartPosition(),
                current == null ? 0L : current.snapshotEpoch(),
                current == null ? null : current.selectedTables(),
                current == null ? null : current.selectedTablesEpoch(),
                current == null ? null : current.cursorWriterToken(), tableAcks));
        records.put(miningChainId, new SrsMeta(meta.miningChainId(), meta.sourceRead(), next,
                meta.schemaHistory(), meta.retention(), meta.epoch()));
        if (position.order().seq() >= 0) {
            ringDone.computeIfAbsent(miningChainId, ignored -> new LinkedHashMap<>())
                    .computeIfAbsent(pipelineId, ignored -> new LinkedHashMap<>())
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
