package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.SnapshotPreparation;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/** One original Pipeline preparation compares same-session metadata and pins exact published models before rows. */
public final class SrsSnapshotPreparation implements AutoCloseable {
    private enum Phase { CREATED, OPEN, CLOSED }
    private final AtomicReference<Phase> phase = new AtomicReference<>(Phase.CREATED);
    private final SrsConsumerRun run;
    private final SrsConsumerStore consumers;
    private final SrsModelStore models;
    private final SrsConsumerProgress original;
    private final Map<String,String> tableRings;
    private final List<String> owed;
    private final SnapshotPreparation request;
    private final Runnable requireActive;
    private final AtomicReference<SrsSnapshotPin> accepted = new AtomicReference<>();
    private final SrsFreshFullPreparation freshFull;
    private final SrsSnapshotPin pendingPin;
    private final java.time.Instant preparedAt;
    private final AtomicReference<SrsFreshFullReceipt> freshReceipt = new AtomicReference<>();

    public SrsSnapshotPreparation(SrsStoreViews views,SrsConsumerRun run,Map<String,String> tableRings,long firstSnapshotEpoch,Runnable requireActive) {
        this(views, run, tableRings, firstSnapshotEpoch, requireActive, null, java.time.Instant.EPOCH);
    }
    public SrsSnapshotPreparation(SrsStoreViews views,SrsConsumerRun run,Map<String,String> tableRings,long firstSnapshotEpoch,Runnable requireActive,
            SrsFreshFullPreparation freshFull, java.time.Instant preparedAt) {
        this.run = Objects.requireNonNull(run,"run"); this.requireActive = Objects.requireNonNull(requireActive,"requireActive");
        this.freshFull = freshFull; this.preparedAt = Objects.requireNonNull(preparedAt, "preparedAt");
        this.tableRings = Collections.unmodifiableMap(new TreeMap<>(tableRings));
        if (this.tableRings.isEmpty() || this.tableRings.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || entry.getValue() == null || entry.getValue().isBlank()))
            throw new IllegalArgumentException("a prepared snapshot requires selected tables and their original rings");
        requireUsable(); var stores = Objects.requireNonNull(views,"views").srs(run.cacheScope(),this::requireUsable);
        this.consumers = stores.consumers().guardedBy(this::requireUsable); this.models = stores.models().guardedBy(this::requireUsable);
        this.original = consumers.progress(run.context()).orElseThrow(this::unavailable); requireUsable();
        if (original.membershipRevision() != run.membershipRevision() || !original.cursor().perTableSeq().keySet().containsAll(this.tableRings.keySet())) throw unavailable();
        this.owed = this.tableRings.keySet().stream().filter(table -> !original.cursor().snapshotCompletedTables().contains(table)).toList();
        var chain = original.snapshot() != null ? null : freshFull != null ? freshFull.originalChain()
                : stores.chains().guardedBy(this::requireUsable).load(run.miningChainId()).orElseThrow(this::unavailable);
        var pending = chain == null ? null : chain.pendingFreshFull();
        if (pending == null) pendingPin = null;
        else {
            var references = new TreeMap<String, SrsModelKey>();
            for (String table : this.tableRings.keySet()) {
                var key = pending.models().get(table); if (key == null) throw unavailable(); references.put(table, key);
            }
            pendingPin = new SrsSnapshotPin(pending.snapshotEpoch(), chain.resumeOverride().token(), references);
        }
        var pin = original.snapshot() == null ? pendingPin : original.snapshot();
        if (freshFull != null && (pin != null || owed.size() != this.tableRings.size()
                || firstSnapshotEpoch != freshFull.originalChain().epoch()
                || !run.miningChainId().equals(freshFull.originalChain().miningChainId()))) throw unavailable();
        this.request = new SnapshotPreparation(pin == null ? firstSnapshotEpoch : pin.snapshotEpoch(),pin == null ? null : new SourcePosition(pin.sourcePosition()));
    }
    public SnapshotPreparation request() { return request; }
    public List<String> tables() { return List.copyOf(tableRings.keySet()); }
    public List<String> stillOwed() { return owed; }
    public SrsConsumerRun run() { return run; }

    /** Bound by the host to the same connector session's before-row callback, outside plugin exception wrapping. */
    public void prepare(SnapshotPreparation actualRequest,SourcePosition sampledSeam,Map<String,SourceTable> actualTables) {
        requireOpen();
        var capturedTables = Map.copyOf(actualTables);
        if (!request.equals(actualRequest) || sampledSeam == null || !new HashSet<>(owed).equals(capturedTables.keySet())) throw unavailable(); var references = new TreeMap<String,SrsModelKey>();
        if (freshFull != null) {
            var receipt = freshFull.publish(request.snapshotEpoch(), sampledSeam.token(), capturedTables, preparedAt);
            requireOpen(); var progress = receipt.progress(); var pin = progress.snapshot(); var pending = receipt.chain().pendingFreshFull();
            if (!progress.context().equals(run.context()) || progress.membershipRevision() != run.membershipRevision()
                    || progress.revision() != original.revision() + 1 || pin == null || pending == null
                    || !pending.origin().targetScope().equals(run.scope()) || !pending.origin().sourceId().equals(run.sourceId())
                    || pin.snapshotEpoch() != request.snapshotEpoch() || !pin.sourcePosition().equals(sampledSeam.token())
                    || !pin.models().keySet().equals(tableRings.keySet()) || !pin.models().equals(pending.models())
                    || !progress.ringDoneThrough().equals(progress.cursor().perTableSeq())
                    || progress.cursor().sinkAcked() != null || !progress.cursor().sinkAckedByTable().isEmpty()
                    || !progress.cursor().snapshotCompletedTables().isEmpty() || progress.writerRun() != null) throw unavailable();
            for (var entry : pin.models().entrySet()) {
                var model = models.require(entry.getValue());
                if (!model.ringName().equals(tableRings.get(entry.getKey())) || !model.table().equals(structural(capturedTables.get(entry.getKey())))) throw unavailable();
            }
            var previous = freshReceipt.compareAndExchange(null, receipt);
            if (previous != null && !previous.equals(receipt)) throw unavailable();
            accept(pin); return;
        }
        if (original.snapshot() != null) references.putAll(original.snapshot().models());
        else if (pendingPin != null) references.putAll(pendingPin.models());
        var missing = new TreeMap<String,String>();
        for (String table : owed) if (!references.containsKey(table)) missing.put(table,tableRings.get(table));
        Map<String,SrsModel> candidates = missing.isEmpty() ? Map.of() : Map.copyOf(models.publishedForNewSnapshot(run.miningChainId(),missing).orElseThrow(this::unavailable));
        for (String table : owed) {
            requireOpen(); var key = references.get(table);
            var model = key == null ? candidates.get(table) : models.require(key);
            SourceTable actual = capturedTables.get(table);
            if (model == null || !model.key().table().equals(table) || !model.key().miningChainId().equals(run.miningChainId()) || !model.key().cacheIncarnation().equals(run.scope().cacheIncarnation())
                    || !model.ringName().equals(tableRings.get(table)) || !model.table().equals(structural(actual))) throw unavailable();
            references.put(table,model.key());
        }
        if (!references.keySet().containsAll(tableRings.keySet())) throw unavailable();
        String anchor = request.replayAnchor() == null ? sampledSeam.token() : request.replayAnchor().token();
        var pin = new SrsSnapshotPin(request.snapshotEpoch(),anchor,references);
        requireOpen();
        if (!consumers.pinSnapshot(run.context(),run.membershipRevision(),original.revision(),pin,run.originalClaim())) throw unavailable();
        requireOpen();
        accept(pin);
    }
    private void accept(SrsSnapshotPin pin) {
        requireOpen(); var held = accepted.compareAndExchange(null,pin);
        if (held != null && !held.equals(pin)) throw unavailable();
    }
    public SrsFreshFullReceipt freshFullReceipt() {
        requireOpen(); var receipt = freshReceipt.get(); if (receipt == null || accepted.get() == null) throw unavailable(); return receipt;
    }
    /** A successful callback receipt, never a later pin discovered by reading the current consumer again. */
    public SrsSnapshotPin accepted() { requireOpen(); var pin = accepted.get(); if (pin == null) throw unavailable(); return pin; }
    void requireReading() { requireOpen(); }
    void beginRead() {
        requireUsable();
        if (!phase.compareAndSet(Phase.CREATED,Phase.OPEN)) throw unavailable();
    }
    @Override public void close() { phase.set(Phase.CLOSED); }
    private void requireUsable() { requireActive.run(); if (phase.get() == Phase.CLOSED) throw unavailable(); }
    private void requireOpen() { requireUsable(); if (phase.get() != Phase.OPEN) throw unavailable(); }
    private static SourceTable structural(SourceTable table) {
        Objects.requireNonNull(table,"actual table"); return new SourceTable(table.name(),table.fields(),table.primaryKey(),table.indexes());
    }
    private TapstateException unavailable() { return new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain",run.miningChainId()),null); }
}
