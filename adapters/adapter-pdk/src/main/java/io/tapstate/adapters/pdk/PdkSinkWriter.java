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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
    private final DdlPolicy ddl;
    private final Map<String, TargetTable> targets;
    private final PdkTargetPreparation preparation;
    private final Map<String, TapTable> tableModels = new LinkedHashMap<>();
    private boolean closed;

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
        this(connector, write, config.writeMode(), config.ddl(), targets,
                new PdkTargetPreparation(connector.context(), connector.functions(), config.onFullLoad(),
                        config.fullLoad(), config.node(), stateStore));
    }

    private PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl,
            Map<String, TargetTable> targets, PdkTargetPreparation preparation) {
        this.connector = connector;
        this.write = write;
        this.mode = mode;
        this.ddl = ddl;
        this.targets = targets == null ? Map.of() : Map.copyOf(targets);
        this.preparation = preparation;
    }

    /** The connector this writer drives, and the state scope it was opened under. */
    PdkConnector connector() {
        return connector;
    }

    @Override
    public CompletionStage<WriteResult> write(List<Envelope> records) {
        return CompletableFuture.supplyAsync(() -> deliver(records));
    }

    private synchronized WriteResult deliver(List<Envelope> records) {
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
            rowsByTable.computeIfAbsent(env.src(), ignored -> new ArrayList<>())
                    .add(encode(mode == WriteMode.APPEND ? asInsert(env) : env));
        }
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
                long[] accepted = {0};
                for (Map.Entry<String, List<TapRecordEvent>> entry : rowsByTable.entrySet()) {
                    List<TapRecordEvent> rows = entry.getValue();
                    TargetTable target = targets.get(entry.getKey());
                    TapTable table = target != null
                            ? tableModels.computeIfAbsent(entry.getKey(), ignored -> {
                                TapTable resolved = TargetTapTable.build(target);
                                connector.resolveTargetTypes(resolved);
                                return resolved;
                            })
                            : TargetTapTable.bare(rows.get(0).getTableId());
                    preparation.prepare(target, table);
                    // A connector may report the batch in several flushes, one callback each; accumulate.
                    write.writeRecord(connector.context(), rows, table,
                            result -> accepted[0] += accepted(result));
                }
                return new WriteResult(accepted[0]);
            });
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw writeFailed(connector.connectorId(), t);
        }
    }

    private static long accepted(WriteListResult<TapRecordEvent> result) {
        return result.getInsertedCount() + result.getModifiedCount() + result.getRemovedCount();
    }

    private TapRecordEvent encode(Envelope env) {
        return (TapRecordEvent) TapEventCodec.encode(env, connector.codecs());
    }

    /** Reforge a row envelope into an insert (append mode): the row's payload becomes an inserted row. */
    private static Envelope asInsert(Envelope env) {
        Map<String, Object> row = env.after() != null ? env.after() : env.before();
        return Envelope.insert(env.ts(), env.src(), row, env.schema());
    }

    static TapstateException writeFailed(String connectorId, Throwable cause) {
        return new TapstateException(ConnectorError.WRITE_FAILED,
                Map.of("connector", connectorId, "detail", detail(cause)), cause);
    }

    private static String detail(Throwable t) {
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        connector.stopQuietly();
        connector.close();
    }
}
