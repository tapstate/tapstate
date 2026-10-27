package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.sink.WriteResult;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A write session over one sink connector, opened once and held across batches. Each write encodes the
 * row envelopes back to PDK record events and drives the connector's write function; the returned
 * stage completes with the count the target accepted, or exceptionally with a coded write failure.
 *
 * <p>The write intent is enforced here: append mode reforges updates and deletes into inserts (an
 * append-only stream), while upsert mode passes them through keyed. A schema-change event is handled by
 * the ddl policy — {@code fail} rejects the batch with a code, {@code ignore} drops it; applying a
 * schema change to the target is a later slice, so {@code apply} drops it for now.
 *
 * <p>The connector is driven against the resolved target table model — its columns and the primary key
 * an upsert matches on. When no model was resolved the connector is handed a bare table id and left to
 * infer structure and keying itself.
 *
 * <p>Writes run off the caller's thread so the call does not block; how many writes are in flight and
 * the backpressure that bounds them belong to the runtime, not this writer.
 */
final class PdkSinkWriter implements SinkWriter {

    private final PdkConnector connector;
    private final WriteRecordFunction write;
    private final WriteMode mode;
    private final Map<String,String> writeAlternatives;
    private final DdlPolicy ddl;
    private final Map<String, TargetTable> targets;
    private final PdkTargetPreparation preparation;
    private Map<String, TargetIdentityPlan> identities = Map.of();
    private SinkConfig identityConfig;
    private boolean identityPrepared;
    // What closing this writer does to its connector: stops and closes it, or - where the connector is shared
    // by the writers of one sink - lets go of this writer's hold on it.
    private final Runnable letGo;
    private final Map<String, TapTable> tableModels = new LinkedHashMap<>();
    // Blocking connector calls must leave the shared async pool available for engine lifecycle callbacks.
    private volatile Thread writeThread;
    private boolean delivering;
    private final ExecutorService writes = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "tapstate-sink-write");
        thread.setDaemon(true);
        writeThread = thread;
        return thread;
    });
    private final Object lifecycle = new Object();
    private final Set<CompletableFuture<WriteResult>> pending = new HashSet<>();
    private boolean closed;
    private final java.util.concurrent.CountDownLatch closeFinished = new java.util.concurrent.CountDownLatch(1);

    PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl, TargetTable target) {
        this(connector, write, mode, ddl, target == null ? Map.of() : Map.of(target.name(), target));
    }

    PdkSinkWriter(
            PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl,
            Map<String, TargetTable> targets) {
        this(connector, write, mode, ddl, targets, (CreateIndexFunction) null);
    }

    PdkSinkWriter(
            PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl,
            Map<String, TargetTable> targets, CreateIndexFunction createIndex) {
        this(connector, write, mode, ddl, targets, new PdkTargetPreparation(connector.context(),
                new ConnectorFunctions().supportCreateIndex(createIndex), OnFullLoad.APPEND, true, null, null));
    }

    PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, SinkConfig config,
            Map<String, TargetTable> targets, KeyedStateStore stateStore) {
        this(connector, write, config, targets, stateStore, false);
    }

    /** As above, and where {@code preparedAhead}, a writer of tables prepared already, which prepares nothing. */
    PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, SinkConfig config,
            Map<String, TargetTable> targets, KeyedStateStore stateStore, boolean preparedAhead) {
        this(connector, write, config, targets, stateStore, preparedAhead, null);
    }

    /**
     * As above, over a connector other writers share, which closing this one lets go of through {@code letGo}
     * rather than stopping; null for a connector of this writer's own.
     */
    PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, SinkConfig config,
            Map<String, TargetTable> targets, KeyedStateStore stateStore, boolean preparedAhead, Runnable letGo) {
        this(connector, write, config, targets, stateStore, preparedAhead, letGo,
                io.tapstate.core.model.PipelineNode.Purpose.SYNC);
    }

    PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, SinkConfig config,
            Map<String, TargetTable> targets, KeyedStateStore stateStore, boolean preparedAhead, Runnable letGo,
            io.tapstate.core.model.PipelineNode.Purpose purpose) {
        this(connector,write,config,targets,stateStore,preparedAhead,letGo,purpose,null);
    }

    PdkSinkWriter(PdkConnector connector,WriteRecordFunction write,SinkConfig config,
            Map<String,TargetTable> targets,KeyedStateStore stateStore,boolean preparedAhead,Runnable letGo,
            io.tapstate.core.model.PipelineNode.Purpose purpose,PdkSinkPreparations receipts) {
        this(connector,write,config,targets,stateStore,preparedAhead,letGo,purpose,receipts,null);
    }

    PdkSinkWriter(PdkConnector connector,WriteRecordFunction write,SinkConfig config,
            Map<String,TargetTable> targets,KeyedStateStore stateStore,boolean preparedAhead,Runnable letGo,
            io.tapstate.core.model.PipelineNode.Purpose purpose,PdkSinkPreparations receipts,
            Map<String,TargetIdentityPlan> plannedIdentities) {
        this(connector, write, config.writeMode(), config.ddl(), targets,
                new PdkTargetPreparation(connector.connectorId(), connector.context(), connector.functions(),
                        config.onFullLoad(), config.fullLoad(), config.node(), stateStore, preparedAhead, purpose, connector::requireActive,receipts), letGo);
        identityConfig = config;
        identities = PdkTargetIdentityPlanner.assess(connector, config, this.targets);
        identities.values().forEach(TargetIdentityProjection::requireUsable);
        PdkTargetIdentityPlanner.requireSame(config, plannedIdentities, identities);
    }

    private PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl,
            Map<String, TargetTable> targets, PdkTargetPreparation preparation) {
        this(connector, write, mode, ddl, targets, preparation, null);
    }

    private PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl,
            Map<String, TargetTable> targets, PdkTargetPreparation preparation, Runnable letGo) {
        this.letGo = letGo != null ? letGo : () -> {
            connector.stopQuietly();
            connector.close();
        };
        this.connector = connector;
        this.writeAlternatives=connector.writeAlternatives(mode);
        this.write = write;
        this.mode = mode;
        this.ddl = ddl;
        this.targets = targets == null ? Map.of() : Map.copyOf(targets);
        this.preparation = preparation;
    }

    /** Prepare selected tables even when their source snapshot contains no rows. */
    void prepareTargets() throws Throwable {
        connector.underLoader(() -> {
            for (Map.Entry<String, TargetTable> entry : targets.entrySet()) {
                TargetTable target = projectedTarget(entry.getKey(), entry.getValue());
                preparation.prepare(target, tableModel(entry.getKey(), target));
            }
            if (identityConfig != null) {
                PdkTargetIdentityPlanner.verifyPrepared(connector, identityConfig, targets, identities);
                identityPrepared = true;
            }
            return null;
        });
    }

    private TapTable tableModel(String stream, TargetTable target) {
        return tableModels.computeIfAbsent(stream, ignored -> {
            TapTable resolved = TargetTapTable.build(target);
            connector.resolveTargetTypes(resolved);
            return resolved;
        });
    }

    /** The connector this writer drives, and the state scope it was opened under. */
    PdkConnector connector() {
        return connector;
    }

    @Override
    public CompletionStage<WriteResult> write(List<Envelope> records) {
        synchronized (lifecycle) {
            if (closed) {
                throw new IllegalStateException("write called after the sink writer was closed");
            }
            // A cancelled stage releases the caller's batch even while its interrupted callback unwinds.
            List<Envelope> batch = List.copyOf(records);
            CompletableFuture<WriteResult> result = CompletableFuture.supplyAsync(() -> {
                synchronized (lifecycle) {
                    if (closed) throw new java.util.concurrent.CancellationException("the sink writer was closed");
                    delivering = true;
                }
                try { return deliver(batch); }
                finally { delivering = false; }
            }, writes);
            pending.add(result);
            result.whenComplete((accepted, failure) -> {
                synchronized (lifecycle) {
                    pending.remove(result);
                }
            });
            return result;
        }
    }

    private synchronized WriteResult deliver(List<Envelope> records) {
        EncodedBatch batch = encodedRows(records);
        Map<String, List<TapRecordEvent>> rowsByTable = batch.rows();
        if (rowsByTable.isEmpty()) {
            return new WriteResult(0);
        }
        if (mode == WriteMode.APPEND && rowsByTable.size() > 1) {
            // APPEND has no rollback or idempotency contract. Reject before the first connector call so a
            // retry cannot duplicate an earlier table after a later table fails.
            throw writeFailed(connector.connectorId(), new IllegalStateException(
                    "append mode rejects a multi-table batch before any connector write"));
        }
        try {
            return connector.underLoader(() -> {
                if (!batch.keys().isEmpty()) {
                    if (!identityPrepared) prepareTargets();
                    batch.keys().forEach((stream, keys) -> PdkMongoIdentityGuard.verify(connector, identities.get(stream), keys));
                }
                return writeRows(rowsByTable);
            });
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw writeFailed(connector.connectorId(), t);
        }
    }

    private record EncodedBatch(Map<String, List<TapRecordEvent>> rows,
            Map<String, List<Map<String, Object>>> keys) { }

    private EncodedBatch encodedRows(List<Envelope> records) {
        Map<String, List<Map<String, Object>>> keys = new LinkedHashMap<>();
        Map<String, List<TapRecordEvent>> rowsByTable = new LinkedHashMap<>();
        for (Envelope env : records) {
            if (env.op() == Op.DDL) {
                if (ddl == DdlPolicy.FAIL) {
                    throw writeFailed(connector.connectorId(),
                            new IllegalStateException("schema change reached a sink whose ddl policy is fail"));
                }
                // ignore: drop the schema change. apply: applying it to the target is a later slice, so drop it too.
                continue;
            }
            TargetIdentityPlan identity = identities.get(env.src());
            Envelope row = identity == null ? env : TargetIdentityProjection.apply(identity, env);
            collectDocumentKeys(identity, row, keys);
            TapRecordEvent encoded = encode(mode == WriteMode.APPEND ? asInsert(row) : row);
            verifyDocumentKeys(identity, row, encoded);
            rowsByTable.computeIfAbsent(env.src(), ignored -> new ArrayList<>()).add(encoded);
        }
        return new EncodedBatch(rowsByTable, keys);
    }

    private static boolean documentIdentity(TargetIdentityPlan identity) {
        return identity.isAutomatic() && identity.requireDocumentKeys();
    }

    private static void collectDocumentKeys(TargetIdentityPlan identity, Envelope row,
            Map<String, List<Map<String, Object>>> keys) {
        if (identity == null || !documentIdentity(identity)) return;
        List<Map<String, Object>> streamKeys = keys.computeIfAbsent(row.src(), ignored -> new ArrayList<>());
        if (row.before() != null) streamKeys.add(MongoIdentityKeys.key(identity, row.before()));
        if (row.after() != null) streamKeys.add(MongoIdentityKeys.key(identity, row.after()));
    }

    private void verifyDocumentKeys(TargetIdentityPlan identity, Envelope row, TapRecordEvent encoded) {
        if (identity != null && documentIdentity(identity)) MongoIdentityKeys.verifyEncoded(connector.conversions(), identity, row, encoded);
    }

    private WriteResult writeRows(Map<String, List<TapRecordEvent>> rowsByTable) throws Throwable {
        long[] accepted = {0};
        for (Map.Entry<String, List<TapRecordEvent>> entry : rowsByTable.entrySet()) {
            List<TapRecordEvent> rows = entry.getValue();
            TargetTable target = projectedTarget(entry.getKey(), targets.get(entry.getKey()));
            TapTable table = target != null
                    ? tableModel(entry.getKey(), target)
                    : TargetTapTable.bare(rows.get(0).getTableId());
            preparation.prepare(target, table);
            if (target != null) {
                // The connector receives both a table model and record events; its write path may
                // route by the event table id, so both must name the resolved target collection.
                rows.forEach(row -> row.setTableId(target.name()));
            }
            // A connector may report the batch in several flushes, one callback each; accumulate.
            // The frozen PDK API stores alternatives by the current thread name.
            writeAlternatives.forEach(connector.context().getConnectorCapabilities()::alternative);
            connector.requireActive();
            write.writeRecord(connector.context(), rows, table,
                    result -> accepted[0] += accepted(result));
        }
        return new WriteResult(accepted[0]);
    }

    private TargetTable projectedTarget(String stream, TargetTable original) {
        TargetIdentityPlan plan = identities.get(stream);
        return plan == null ? original : plan.target();
    }

    private static long accepted(WriteListResult<TapRecordEvent> result) {
        return result.getInsertedCount() + result.getModifiedCount() + result.getRemovedCount();
    }

    private TapRecordEvent encode(Envelope env) {
        return (TapRecordEvent) TapEventCodec.encode(env, connector.conversions());
    }

    /** Reforge a row envelope into an insert (append mode): the row's payload becomes an inserted row. */
    private static Envelope asInsert(Envelope env) {
        Map<String, Object> row = env.after() != null ? env.after() : env.before();
        return Envelope.insert(env.ts(), env.src(), row, env.schema());
    }

    static TapstateException writeFailed(String connectorId, Throwable cause) {
        if(cause instanceof Error invariant)throw invariant;
        if(cause instanceof PdkSinkPreparations.HostInvariant host)throw host.original;
        return new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", connectorId, "detail", detail(cause)), cause);
    }

    private static String detail(Throwable t) {
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }

    @Override
    public void close() {
        if (Thread.currentThread() == writeThread && delivering) {
            throw new IllegalStateException("a target callback cannot close its active writer");
        }
        List<CompletableFuture<WriteResult>> abandoned;
        boolean first;
        synchronized (lifecycle) {
            first = !closed;
            closed = true;
            abandoned = first ? List.copyOf(pending) : List.of();
        }
        if (!first) {
            // A completion callback has left deliver already; waiting here could cycle with another close.
            if (Thread.currentThread() == writeThread) return;
            awaitClosed();
            return;
        }
        writes.shutdownNow();
        try {
            // A blocked target call can need its connector stopped before it can exit.
            letGo.run();
        } finally {
            try {
                // Jet may swallow close failures. Never leave it while an original callback can still write.
                // Completion callbacks on our worker run after deliver; queued target calls are refused.
                if (Thread.currentThread() != writeThread) awaitWrites();
                abandoned.forEach(result -> result.cancel(false));
            } finally {
                closeFinished.countDown();
            }
        }
    }

    private void awaitWrites() {
        boolean interrupted = false;
        try {
            while (!writes.isTerminated()) {
                try { writes.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS); }
                catch (InterruptedException stopping) { interrupted = true; }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private void awaitClosed() {
        boolean interrupted = false;
        try {
            for (;;) {
                try { closeFinished.await(); return; }
                catch (InterruptedException stopping) { interrupted = true; }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

}
