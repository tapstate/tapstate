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

    public SrsSnapshotPreparation(SrsStoreViews views,SrsConsumerRun run,Map<String,String> tableRings,long firstSnapshotEpoch,Runnable requireActive) {
        this.run = Objects.requireNonNull(run,"run"); this.requireActive = Objects.requireNonNull(requireActive,"requireActive");
        this.tableRings = Collections.unmodifiableMap(new TreeMap<>(tableRings));
        if (this.tableRings.isEmpty() || this.tableRings.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || entry.getValue() == null || entry.getValue().isBlank()))
            throw new IllegalArgumentException("a prepared snapshot requires selected tables and their original rings");
        requireUsable(); var stores = Objects.requireNonNull(views,"views").srs(run.cacheScope(),this::requireUsable);
        this.consumers = stores.consumers().guardedBy(this::requireUsable); this.models = stores.models().guardedBy(this::requireUsable);
        this.original = consumers.progress(run.context()).orElseThrow(this::unavailable); requireUsable();
        if (original.membershipRevision() != run.membershipRevision() || !original.cursor().perTableSeq().keySet().containsAll(this.tableRings.keySet())) throw unavailable();
        this.owed = this.tableRings.keySet().stream().filter(table -> !original.cursor().snapshotCompletedTables().contains(table)).toList();
        var pin = original.snapshot();
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
        if (original.snapshot() != null) references.putAll(original.snapshot().models());
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
        var held = accepted.compareAndExchange(null,pin);
        if (held != null && !held.equals(pin)) throw unavailable();
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
