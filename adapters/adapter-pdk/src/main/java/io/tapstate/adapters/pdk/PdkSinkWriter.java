package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapdata.pdk.apis.functions.connector.target.CreateIndexFunction;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.sink.WriteResult;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private final CreateIndexFunction createIndex;
    /** Targets whose indexes have been created in this writer's life, so the request is made once. */
    private final Set<String> indexed = new HashSet<>();
    private boolean closed;

    PdkSinkWriter(PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl, TargetTable target) {
        this(connector, write, mode, ddl, target == null ? Map.of() : Map.of(target.name(), target));
    }

    PdkSinkWriter(
            PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl,
            Map<String, TargetTable> targets) {
        this(connector, write, mode, ddl, targets, null);
    }

    PdkSinkWriter(
            PdkConnector connector, WriteRecordFunction write, WriteMode mode, DdlPolicy ddl,
            Map<String, TargetTable> targets, CreateIndexFunction createIndex) {
        this.connector = connector;
        this.write = write;
        this.mode = mode;
        this.ddl = ddl;
        this.targets = targets == null ? Map.of() : Map.copyOf(targets);
        this.createIndex = createIndex;
    }

    /** The connector this writer drives, and the state scope it was opened under. */
    PdkConnector connector() {
        return connector;
    }

    @Override
    public CompletionStage<WriteResult> write(List<Envelope> records) {
        return CompletableFuture.supplyAsync(() -> deliver(records));
    }

    private WriteResult deliver(List<Envelope> records) {
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
                            ? TargetTapTable.build(target)
                            : TargetTapTable.bare(rows.get(0).getTableId());
                    ensureIndexes(target, table);
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

    /**
     * Creates the target's declared indexes the first time this writer writes to it. The table is created
     * by the same descriptor on the same call path, so the index request rides with the write rather than
     * needing a separate provisioning step - and asking once per writer keeps a per-batch call off the
     * hot path without pretending to know what the store already has.
     *
     * <p>The memo is keyed by the table the index is created on, not by the stream that delivered the
     * rows. A view maps many streams onto one collection, so keyed by stream the same create-index
     * request would go out once per stream and the memo would never dedupe across them.
     */
    private void ensureIndexes(TargetTable target, TapTable table) throws Throwable {
        if (createIndex == null || target == null || indexed.contains(target.name())) {
            return;
        }
        TapCreateIndexEvent event = TargetTapTable.createIndexEvent(target);
        if (event != null) {
            createIndex.createIndex(connector.context(), table, event);
        }
        // Memoized only once creation has returned. Recorded before it, a failed first attempt would be
        // remembered as done, and the collection would live on without the indexes it declared.
        indexed.add(target.name());
    }

    private static long accepted(WriteListResult<TapRecordEvent> result) {
        return result.getInsertedCount() + result.getModifiedCount() + result.getRemovedCount();
    }

    private static TapRecordEvent encode(Envelope env) {
        return (TapRecordEvent) TapEventCodec.encode(env);
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
