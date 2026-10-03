package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ResumePoint;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
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
    /** Per chain, the highest generation opened on it, shared or direct. */
    private final Map<String, Long> epochsOpened = new LinkedHashMap<>();
    /** Per chain, per pipeline: the ring sequence of the last change each table's sink confirmed. */
    private final Map<String, Map<String, Map<String, Long>>> ringDone = new LinkedHashMap<>();
    /** The chains whose source read offset every table they carry can resume from. */
    private final Set<String> trusted = new HashSet<>();
    /** Per chain, where a restart resumes when that is not the read offset itself. */
    private final Map<String, ResumePoint> resumeFrom = new LinkedHashMap<>();
    /** What each chain's reader subscribed to, as its record would carry it. */
    private final Map<String, PhysicalSelection> selections = new LinkedHashMap<>();
    /** Tables asked for by pipelines arriving on each chain, until a subscription serves them. */
    private final Map<String, java.util.Set<String>> requests = new LinkedHashMap<>();
    /** Per chain and pipeline, the writer plan and the progress kept apart beneath its derived cursor. */
    private final Map<String, Map<String, SinkWriters>> sinkWriters = new LinkedHashMap<>();

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
    public synchronized boolean replacePhysicalSelection(
            String miningChainId, PhysicalSelection current, PhysicalSelection wider) {
        SrsMeta m = require(miningChainId);
        if (m.epoch() != wider.epoch() || !current.equals(selections.get(miningChainId))
                || !wider.tables().containsAll(requests.getOrDefault(miningChainId, java.util.Set.of()))) {
            return false;
        }
        selections.put(miningChainId, wider);
        return true;
    }

    @Override
    public synchronized List<String> requestedPhysicalTables(String miningChainId) {
        return requests.getOrDefault(miningChainId, java.util.Set.of()).stream().sorted().toList();
    }

    @Override
    public synchronized boolean requestPhysicalTables(String miningChainId, long epoch, List<String> tables) {
        if (require(miningChainId).epoch() != epoch) {
            return false;
        }
        requests.computeIfAbsent(miningChainId, chain -> new java.util.TreeSet<>()).addAll(tables);
        return true;
    }

    @Override
    public synchronized void clearPhysicalRequests(String miningChainId, List<String> tables) {
        require(miningChainId);
        java.util.Set<String> asked = requests.get(miningChainId);
        if (asked != null) {
            tables.forEach(asked::remove);
        }
    }

    @Override
    public synchronized Optional<PhysicalSelection> physicalSelection(String miningChainId) {
        return Optional.ofNullable(selections.get(miningChainId));
    }

    @Override
    public synchronized boolean publishPhysicalSelection(String miningChainId, PhysicalSelection selection) {
        SrsMeta m = require(miningChainId);
        if (selection.revision() != 1L) {
            throw new IllegalArgumentException("a generation's first subscription is its revision one");
        }
        PhysicalSelection current = selections.get(miningChainId);
        if (!selection.tables().containsAll(requests.getOrDefault(miningChainId, java.util.Set.of()))) {
            return false;
        }
        if (m.epoch() != selection.epoch() || current != null && current.epoch() >= selection.epoch()) {
            return current != null && current.equals(selection);
        }
        selections.put(miningChainId, selection);
        return true;
    }

    @Override
    public synchronized boolean advancePhysicalSourceReadOffset(
            String miningChainId, long epoch, ChainPosition position) {
        return advancePhysicalSourceReadOffset(miningChainId, epoch, position, true);
    }

    @Override
    public synchronized boolean advancePhysicalSourceReadOffset(
            String miningChainId, long epoch, ChainPosition position, boolean resumable) {
        SrsMeta m = require(miningChainId);
        if (m.epoch() != epoch) {
            return false;
        }
        if (ranksAfter(position, m.sourceRead())) {
            if (!resumable && !resumeFrom.containsKey(miningChainId) && m.sourceReadOffset() != null) {
                // A record from before the resume point was kept apart resumes from its read offset; it keeps it.
                resumeFrom.put(miningChainId, new ResumePoint(m.sourceRead(), m.sourceReadAt()));
            }
            Instant at = Instant.now();
            records.put(miningChainId, new SrsMeta(m.miningChainId(), position, m.consumerOffsets(),
                    m.schemaHistory(), m.retention(), m.epoch(), at));
            if (resumable && position.token() != null) {
                resumeFrom.put(miningChainId, new ResumePoint(position, at));
            }
        }
        return true;
    }

    @Override
    public synchronized Optional<ResumePoint> resumePoint(String miningChainId) {
        ResumePoint resume = resumeFrom.get(miningChainId);
        return resume != null ? Optional.of(resume) : SrsMetaStore.super.resumePoint(miningChainId);
    }

    @Override
    public synchronized boolean physicalPrefixTrusted(String miningChainId) {
        return trusted.contains(miningChainId);
    }

    @Override
    public synchronized boolean establishPhysicalAnchor(String miningChainId, ChainPosition position) {
        SrsMeta m = require(miningChainId);
        long epoch = position.order().epoch();
        if (m.epoch() != epoch) {
            return false;
        }
        ChainPosition stored = m.sourceRead();
        boolean trustedNow = trusted.contains(miningChainId);
        if (stored == null || stored.token() == null
                || trustedNow && (stored.order() == null || stored.order().epoch() < epoch)) {
            Instant at = Instant.now();
            records.put(miningChainId, new SrsMeta(m.miningChainId(), position, m.consumerOffsets(),
                    m.schemaHistory(), m.retention(), m.epoch(), at));
            resumeFrom.put(miningChainId, new ResumePoint(position, at));
            trusted.add(miningChainId);
            return true;
        }
        return trustedNow;
    }

    @Override
    public synchronized boolean trustSourceReadOffset(String miningChainId, long epoch) {
        if (require(miningChainId).epoch() != epoch) {
            return false;
        }
        trusted.add(miningChainId);
        return true;
    }

    @Override
    public synchronized boolean advancePhysicalSinkAcked(
            String miningChainId, String pipelineId, long epoch, ChainPosition position) {
        SrsMeta m = require(miningChainId);
        if (m.epoch() != epoch) {
            return false;
        }
        Optional<ConsumerOffset> existing = m.consumerOffset(pipelineId);
        if (existing.isPresent() && ranksAfter(position, existing.get().sinkAcked())) {
            advanceSinkAcked(miningChainId, pipelineId, position);
        }
        return true;
    }

    /** Whether {@code candidate} would move a position that is absent, unordered or ranked before it. */
    private static boolean ranksAfter(ChainPosition candidate, ChainPosition stored) {
        return stored == null || stored.order() == null || stored.order().compareTo(candidate.order()) < 0;
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
        Instant at = Instant.now();
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), new ChainPosition(null, token), m.consumerOffsets(),
                m.schemaHistory(), m.retention(), m.epoch(), at));
        resumeFrom.put(miningChainId, new ResumePoint(new ChainPosition(null, token), at));
        trusted.add(miningChainId);
    }

    @Override
    public synchronized void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
        advanceSourceReadOffset(miningChainId, position, true);
    }

    @Override
    public synchronized void advanceSourceReadOffset(
            String miningChainId, ChainPosition position, boolean resumable) {
        SrsMeta m = require(miningChainId);
        if (!resumable && !resumeFrom.containsKey(miningChainId) && m.sourceReadOffset() != null) {
            // A record from before the resume point was kept apart resumes from its read offset; it keeps it.
            resumeFrom.put(miningChainId, new ResumePoint(m.sourceRead(), m.sourceReadAt()));
        }
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), position, m.consumerOffsets(),
                m.schemaHistory(), m.retention(), m.epoch()));
        if (resumable && position.token() != null) {
            resumeFrom.put(miningChainId, new ResumePoint(position, null));
        }
    }

    @Override
    public synchronized void advanceDirectSourceReadOffset(
            String miningChainId, ChainPosition position, boolean resumable) {
        advanceSourceReadOffset(miningChainId, position, resumable);
        trusted.add(miningChainId);
    }

    @Override
    public synchronized void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        SrsMeta m = require(miningChainId);
        // A rewritten record carries no per-table acks, as the real store's replacement carries none.
        forgetRingSeqs(miningChainId, offset.pipelineId());
        forgetSinkWriters(miningChainId, offset.pipelineId());
        List<ConsumerOffset> next = new ArrayList<>(m.consumerOffsets());
        next.removeIf(c -> c.pipelineId().equals(offset.pipelineId()));
        next.add(offset);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized void releaseSinkAcknowledgements(String miningChainId, String pipelineId) {
        letGoOfWhatItLanded(miningChainId, pipelineId, false, null);
    }

    @Override
    public synchronized void moveConsumerStart(String miningChainId, String pipelineId, String cdcStartPosition) {
        letGoOfWhatItLanded(miningChainId, pipelineId, true, cdcStartPosition);
    }

    /** Path-scoped, as the real store's is: the acknowledgements go; the writers, ring places and the rest stay. */
    private void letGoOfWhatItLanded(String miningChainId, String pipelineId, boolean moveStart, String start) {
        SrsMeta m = require(miningChainId);
        List<ConsumerOffset> next = new ArrayList<>();
        for (ConsumerOffset c : m.consumerOffsets()) {
            next.add(!c.pipelineId().equals(pipelineId) ? c : new ConsumerOffset(c.pipelineId(), c.perTableSeq(),
                    null, c.snapshotCompletedTables(), moveStart ? start : c.cdcStartPosition(), c.snapshotEpoch(),
                    c.selectedTables(), c.selectedTablesEpoch(), Map.of()));
        }
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

    /**
     * One consumer rebuilt with the facets a mutator names, carrying its selection and table acks across --
     * the same scoping the real store's path updates give, where a write to one facet leaves the rest alone.
     */
    private static ConsumerOffset rebuilt(ConsumerOffset existing, String pipelineId, Map<String, Long> perTable,
            ChainPosition ack, List<String> completed, String cdcStart, long snapshotEpoch) {
        return new ConsumerOffset(pipelineId, perTable, ack, completed, cdcStart, snapshotEpoch,
                existing == null ? null : existing.selectedTables(),
                existing == null ? null : existing.selectedTablesEpoch(),
                existing == null ? Map.of() : existing.sinkAckedByTable());
    }

    @Override
    public synchronized void selectConsumerTables(
            String miningChainId, String pipelineId, List<String> tables, long epoch) {
        SrsMeta m = require(miningChainId);
        List<String> selected = tables.stream().distinct().sorted().toList();
        List<ConsumerOffset> next = new ArrayList<>();
        ConsumerOffset existing = null;
        for (ConsumerOffset c : m.consumerOffsets()) {
            if (c.pipelineId().equals(pipelineId)) {
                existing = c;
            } else {
                next.add(c);
            }
        }
        Map<String, ChainPosition> retained = new LinkedHashMap<>();
        if (existing != null && existing.selectedTablesEpoch() != null && existing.selectedTablesEpoch() == epoch) {
            existing.sinkAckedByTable().forEach((table, position) -> {
                if (selected.contains(table)) {
                    retained.put(table, position);
                }
            });
        }
        if (selected.isEmpty()) {
            // A pipeline that selects nothing has no place in any ring.
            Map<String, Map<String, Long>> rings = ringDone.get(miningChainId);
            if (rings != null) {
                rings.remove(pipelineId);
            }
        }
        next.add(new ConsumerOffset(pipelineId,
                existing == null || selected.isEmpty() ? Map.of() : existing.perTableSeq(),
                existing == null ? null : existing.sinkAcked(),
                completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch(),
                selected, epoch, retained));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
    }

    @Override
    public synchronized void advanceTableSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        ConsumerOffset existing = require(miningChainId).consumerOffset(pipelineId).orElse(null);
        if (!raiseTableAck(miningChainId, pipelineId, table, position)) {
            return;
        }
        if (position.order().seq() >= 0 && positionsItsRing(existing, table, position.order())) {
            ringDone.computeIfAbsent(miningChainId, chain -> new LinkedHashMap<>())
                    .computeIfAbsent(pipelineId, pipeline -> new LinkedHashMap<>())
                    .merge(table, position.order().seq(), Math::max);
        }
    }

    /** Raises what {@code table} has landed for the pipeline to {@code position}; answers whether it moved. */
    private boolean raiseTableAck(String miningChainId, String pipelineId, String table, ChainPosition position) {
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
        ChainPosition prior = existing == null ? null : existing.sinkAckedByTable().get(table);
        if (prior != null && prior.order().compareTo(position.order()) >= 0) {
            return false;
        }
        Map<String, ChainPosition> acks = new LinkedHashMap<>(existing == null ? Map.of() : existing.sinkAckedByTable());
        acks.put(table, position);
        next.add(new ConsumerOffset(pipelineId,
                existing == null ? Map.of() : existing.perTableSeq(),
                existing == null ? null : existing.sinkAcked(),
                completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch(),
                existing == null ? null : existing.selectedTables(),
                existing == null ? null : existing.selectedTablesEpoch(),
                acks));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
        return true;
    }

    /** Only a sequence of the ring a pipeline is positioned in says where a run of it carries on. */
    private static boolean positionsItsRing(ConsumerOffset existing, String table, SourceOrder order) {
        return existing == null || existing.selectedTables() == null
                || existing.selects(table) && existing.selectedTablesEpoch() == order.epoch();
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
        next.add(rebuilt(existing, pipelineId, perTable, ack, completedOf(existing),
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
        Map<String, Long> perTable = existing == null ? Map.of() : existing.perTableSeq();
        next.add(rebuilt(existing, pipelineId, perTable, position, completedOf(existing),
                existing == null ? null : existing.cdcStartPosition(),
                existing == null ? 0L : existing.snapshotEpoch()));
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), next,
                m.schemaHistory(), m.retention(), m.epoch()));
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
        next.add(rebuilt(existing, pipelineId,
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
        long opened = Math.max(m.epoch(), epochsOpened.getOrDefault(miningChainId, 0L)) + 1;
        epochsOpened.put(miningChainId, opened);
        records.put(miningChainId, new SrsMeta(
                m.miningChainId(), m.sourceRead(), m.consumerOffsets(),
                m.schemaHistory(), m.retention(), opened));
        return opened;
    }

    @Override
    public synchronized long highestGenerationOpened(String miningChainId) {
        SrsMeta m = records.get(miningChainId);
        return m == null ? 0L : Math.max(m.epoch(), epochsOpened.getOrDefault(miningChainId, 0L));
    }

    @Override
    public synchronized long openDirectEpoch(String miningChainId) {
        SrsMeta m = require(miningChainId);
        long opened = Math.max(m.epoch(), epochsOpened.getOrDefault(miningChainId, 0L)) + 1;
        epochsOpened.put(miningChainId, opened);
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
        consumers.add(rebuilt(mine, pipelineId, mine == null ? Map.of() : mine.perTableSeq(),
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
        resumeFrom.remove(miningChainId);
        trusted.remove(miningChainId);
        selections.remove(miningChainId);
        sinkWriters.remove(miningChainId);
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
        SrsMeta meta = require(miningChainId);
        if (meta.consumerOffset(pipelineId).isEmpty()) {
            List<ConsumerOffset> consumers = new ArrayList<>(meta.consumerOffsets());
            consumers.add(new ConsumerOffset(pipelineId, Map.of(), null));
            meta = new SrsMeta(meta.miningChainId(), meta.sourceRead(), consumers,
                    meta.schemaHistory(), meta.retention(), meta.epoch());
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
        boolean hasAggregateProgress = (existing != null
                && (existing.sinkAcked() != null || !existing.sinkAckedByTable().isEmpty()))
                || !completedSnapshots.isEmpty();
        if (priorPlan == null && allWriterIds.size() > 1 && hasAggregateProgress) {
            throw new TapstateException(IoError.SINK_WRITER_PROGRESS_AMBIGUOUS,
                    Map.of("pipeline", pipelineId), null);
        }
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
        ConsumerOffset existing = require(miningChainId).consumerOffset(pipelineId).orElse(null);
        if (position.order().seq() >= 0 && positionsItsRing(existing, table, position.order())) {
            progress.ringDone = progress.ringDone == null
                    ? position.order().seq()
                    : Math.max(progress.ringDone, position.order().seq());
        }
        // What the table has landed, never the chain-level position: the chain's reader releases that.
        derivedTableAck(writers, table).ifPresent(derived -> raiseTableAck(miningChainId, pipelineId, table, derived));
        derivedRing(writers, table).ifPresent(seq ->
                ringDone.computeIfAbsent(miningChainId, ignored -> new LinkedHashMap<>())
                        .computeIfAbsent(pipelineId, ignored -> new LinkedHashMap<>())
                        .merge(table, seq, Math::max));
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
        forgetSinkWriters(miningChainId, pipelineId);
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
