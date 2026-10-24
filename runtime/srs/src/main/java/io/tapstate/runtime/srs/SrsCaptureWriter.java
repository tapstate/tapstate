package io.tapstate.runtime.srs;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** One original native capture session: actual models precede its events and complete checkpoints. */
public final class SrsCaptureWriter implements AutoCloseable {
    private final CacheBusinessScope scope;
    private final SrsChainState opened;
    private final WorkloadClaimFence fence;
    private final SrsChainStore chains;
    private final SrsModelStore models;
    private final SrsLogStore log;
    private final HazelcastInstance member;
    private final Runnable guard;
    private final boolean positionEditsRestricted;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final java.util.Set<Subscription> sourceHandles = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final AtomicBoolean started = new AtomicBoolean();
    private final Map<String, SrsModel> published = new LinkedHashMap<>();
    private final Map<String, SrsTableCheckpoint> frontiers = new LinkedHashMap<>();
    private final Map<String, SrsWriteGate> gates = new LinkedHashMap<>();
    private boolean streamPrepared;
    private long revision;
    private long batchOrder;

    /** The caller has already allocated this epoch and published its immutable serving selection. */
    public SrsCaptureWriter(CacheBusinessScope scope, SrsChainState opened, WorkloadClaimFence originalCapture,
                            SrsStores stores, HazelcastInstance member, Runnable requireActive) {
        this(scope, opened, originalCapture, stores, member, requireActive, false);
    }
    public SrsCaptureWriter(CacheBusinessScope scope, SrsChainState opened, WorkloadClaimFence originalCapture,
                            SrsStores stores, HazelcastInstance member, Runnable requireActive, boolean priorCacheHistory) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.opened = Objects.requireNonNull(opened, "opened");
        this.fence = Objects.requireNonNull(originalCapture, "originalCapture");
        this.member = Objects.requireNonNull(member, "member");
        this.guard = Objects.requireNonNull(requireActive, "requireActive");
        this.positionEditsRestricted = priorCacheHistory || opened.pendingFreshFull() != null
                || opened.checkpoint() != null && opened.checkpoint().positionEditsRestricted();
        Objects.requireNonNull(stores, "stores");
        if (opened.epoch() < 1 || opened.servingTables().isEmpty()
                || !opened.servingTables().containsAll(opened.requestedTables())
                || fence.key().type() != WorkloadClaimType.CAPTURE
                || !scope.clusterId().equals(fence.key().clusterId())
                || !opened.captureId().equals(fence.key().resourceId())) throw unproven();
        this.chains = stores.chains().guardedBy(this::requireActive);
        this.models = stores.models().guardedBy(this::requireActive);
        this.log = stores.log();
        this.revision = opened.checkpointRevision();
        this.batchOrder = opened.checkpoint() != null && opened.checkpoint().position().order().epoch() == opened.epoch()
                ? Math.max(0, opened.checkpoint().position().order().seq()) : 0;
        requireActive();
    }

    /** Called with actual selected metadata from each source session, before that session may read rows. */
    public synchronized void prepare(Map<String, SourceTable> actualTables) {
        prepareTables(actualTables, true);
    }

    /** Publishes only tables this snapshot session actually reads; it grants no CDC preparation receipt. */
    public synchronized void prepareSnapshotTables(Map<String, SourceTable> actualTables) {
        prepareTables(actualTables, false);
    }

    private void prepareTables(Map<String, SourceTable> actualTables, boolean completeSelection) {
        requireActive();
        Map<String, SourceTable> actual = new LinkedHashMap<>();
        for (var entry : Map.copyOf(actualTables).entrySet()) {
            var table = entry.getValue();
            if (!entry.getKey().equals(table.name())) throw unavailable(entry.getKey());
            actual.put(entry.getKey(), new SourceTable(table.name(), table.fields(), table.primaryKey(), table.indexes()));
        }
        if (actual.isEmpty() || !opened.servingTables().containsAll(actual.keySet())
                || (completeSelection && !actual.keySet().equals(opened.servingTables()))) throw unproven();
        // Validate the entire retained baseline before publishing any new model. Current discovery cannot
        // describe a different schema's unread source history after a restart.
        String mismatch = null;
        for (var entry : actual.entrySet()) {
            String table = entry.getKey();
            SrsTableCheckpoint prior = opened.checkpoint() == null ? null : opened.checkpoint().tables().get(table);
            var baseline = opened.pendingFreshFull() == null ? prior == null ? null : prior.model() : opened.pendingFreshFull().models().get(table);
            if (baseline != null && !models.require(baseline).table().equals(entry.getValue())) mismatch = table;
            SrsModel held = published.get(table);
            if (held != null && !held.table().equals(entry.getValue())) throw unavailable(table);
            String ring = SrsRingbuffer.ringName(opened.miningChainId(), table);
            var configuration = member.getConfig().findRingbufferConfig(ring);
            if (!configuration.getRingbufferStoreConfig().isEnabled()
                    || !(configuration.getRingbufferStoreConfig().getFactoryImplementation() instanceof SrsLogRingbufferStoreFactory factory)
                    || !factory.backs(log) || configuration.getInMemoryFormat() != InMemoryFormat.OBJECT) {
                throw new TapstateException(CaptureError.SRS_NOT_RECOVERABLE, Map.of("chain", opened.miningChainId()), null);
            }
        }
        if (mismatch != null) {
            if (completeSelection && opened.pendingFreshFull() == null && published.isEmpty() && opened.checkpoint() != null
                    && opened.checkpoint().tables().keySet().equals(actual.keySet()))
                throw new SrsBaselineModelMismatch(opened.miningChainId(), mismatch);
            throw unavailable(mismatch);
        }
        for (var entry : actual.entrySet()) {
            String table = entry.getKey();
            if (published.containsKey(table)) continue;
            String ring = SrsRingbuffer.ringName(opened.miningChainId(), table);
            var key = new SrsModelKey(opened.miningChainId(), table, scope.cacheIncarnation(), opened.epoch(), 0);
            SrsModel saved = models.publishInitial(key, ring, entry.getValue(), fence);
            if (!saved.key().equals(key) || !saved.ringName().equals(ring) || !saved.table().equals(entry.getValue())) throw unavailable(table);
            requireActive();
            published.put(table, saved);
            SrsTableCheckpoint prior = opened.checkpoint() == null ? null : opened.checkpoint().tables().get(table);
            long sequence = prior != null && prior.model().equals(key) ? prior.sequence() : saved.ddlSequence() - 1;
            frontiers.put(table, new SrsTableCheckpoint(key, sequence));
        }
        requireActive();
        if (completeSelection && started.get()) streamPrepared = true;
    }

    /** Starts only once; a snapshot preparation receipt cannot replace this CDC session's own callback. */
    public Subscription start(CapturePort port, CaptureConfig config, CaptureStart start, CaptureHealth health) {
        Objects.requireNonNull(port, "port"); Objects.requireNonNull(config, "config");
        Objects.requireNonNull(start, "start"); Objects.requireNonNull(health, "health");
        requireActive();
        if (!java.util.Set.copyOf(config.streams()).equals(opened.servingTables())) throw unproven();
        if (opened.resumeToken() != null && (!(start instanceof CaptureStart.Resume resume)
                || !opened.resumeToken().equals(resume.position().token()))) throw unproven();
        if (!started.compareAndSet(false, true)) throw unproven();
        try {
            Subscription source = port.cdc(config, start, health.recording(new CaptureStartedListener() {
                @Override public void onStart(SourcePosition position) {
                    try { began(position); } catch (RuntimeException | Error failure) { close(); throw failure; }
                }
                @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                    try { append(events, position); } catch (RuntimeException | Error failure) { close(); throw failure; }
                }
            }));
            sourceHandles.add(source);
            try { requireActive(); }
            catch (RuntimeException | Error failure) {
                try { stopSource(source); } catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
                throw failure;
            }
            return new Subscription() {
                @Override public void acknowledge(SourcePosition durable) { requireActive(); source.acknowledge(durable); }
                @Override public void close() { SrsCaptureWriter.this.close(); stopSource(source); }
            };
        } catch (RuntimeException | Error failure) { close(); throw failure; }
    }

    /** Cleanup never needs business activation; a failed start retains the original source until it closes. */
    public void stopSources() {
        Throwable first = null;
        for (var source : List.copyOf(sourceHandles)) {
            try { stopSource(source); }
            catch (RuntimeException | Error failure) {
                if (first == null) first = failure; else if (first != failure) first.addSuppressed(failure);
            }
        }
        if (first instanceof RuntimeException runtime) throw runtime;
        if (first instanceof Error error) throw error;
    }
    private void stopSource(Subscription source) {
        if (!sourceHandles.contains(source)) return;
        source.close(); sourceHandles.remove(source);
    }

    private synchronized void began(SourcePosition position) {
        ready();
        if (opened.pendingFreshFull() != null) return;
        if (opened.checkpoint() == null || opened.checkpoint().position().order().epoch() != opened.epoch()) {
            checkpoint(new ChainPosition(SourceOrder.snapshotRow(opened.epoch()), position.token()));
        }
    }

    private synchronized void append(List<Envelope> events, Optional<SourcePosition> position) {
        ready();
        Map<String, List<SrsItem>> selected = new LinkedHashMap<>();
        for (int i = 0; i < events.size(); i++) {
            Envelope event = events.get(i);
            SrsModel model = published.get(event.src());
            if (model == null) throw new TapstateException(CaptureError.EVENT_TABLE_NOT_SELECTED, Map.of("table", event.src()), null);
            // The connector's DDL origin is not a complete structural model or an ordered model boundary.
            if (event.op() == Op.DDL || event.op() == Op.READ) throw unavailable(event.src());
            SourcePosition token = i == events.size() - 1 ? position.orElse(null) : null;
            selected.computeIfAbsent(event.src(), ignored -> new ArrayList<>()).add(new SrsItem(token, event.op(), event.ts(),
                    event.before(), event.after(), model.key().modelVersion(), fence, opened.epoch()));
        }
        // Validate every table and model before admitting the first share of this source batch.
        for (var entry : selected.entrySet()) {
            requireActive();
            SrsWriteGate gate = gates.computeIfAbsent(entry.getKey(), table -> new SrsWriteGate(
                    new SrsRingbuffer(member.getRingbuffer(SrsRingbuffer.ringName(opened.miningChainId(), table)))));
            long last = CdcPhase.admitDurable(gate, entry.getKey(), entry.getValue(), this::requireActive);
            frontiers.put(entry.getKey(), new SrsTableCheckpoint(published.get(entry.getKey()).key(), last));
        }
        position.ifPresent(token -> {
            if (batchOrder >= Long.MAX_VALUE - 1) throw unproven();
            long next = batchOrder + 1;
            checkpoint(new ChainPosition(new SourceOrder(opened.epoch(), next), token.token()));
            batchOrder = next;
        });
    }

    private void checkpoint(ChainPosition position) {
        requireActive();
        var expected = new SrsCaptureCheckpoint(position, Map.copyOf(frontiers), positionEditsRestricted);
        SrsChainState saved = chains.checkpoint(opened.miningChainId(), revision, expected, fence).orElseThrow(this::unproven);
        if (!expected.equals(saved.checkpoint()) || saved.checkpointRevision() != revision + 1) throw unproven();
        requireActive();
        revision = saved.checkpointRevision();
    }

    private void ready() {
        requireActive();
        if (!streamPrepared || !published.keySet().equals(opened.servingTables())) throw unproven();
    }
    private void requireActive() {
        if (closed.get()) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null);
        guard.run();
    }
    private TapstateException unproven() {
        return new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", opened.miningChainId()), null);
    }
    private TapstateException unavailable(String table) {
        return new TapstateException(IoError.SRS_MODEL_UNAVAILABLE,
                Map.of("chain", opened.miningChainId(), "table", table, "version", "0"), null);
    }
    @Override public void close() { closed.set(true); }
}
