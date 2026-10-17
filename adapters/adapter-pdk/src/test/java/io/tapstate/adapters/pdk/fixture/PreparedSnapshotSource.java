package io.tapstate.adapters.pdk.fixture;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Exposes preparation order through host-owned counters shared across the isolated connector loader. */
public final class PreparedSnapshotSource implements TapConnector {
    public static final String STATE = "tapstate.test.prepared-snapshot.state";

    @SuppressWarnings("unchecked")
    private static Map<String, Object> state() { return (Map<String, Object>) System.getProperties().get(STATE); }
    private static AtomicInteger counter(String key) { return (AtomicInteger) state().get(key); }
    @Override public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
        functions.supportTimestampToStreamOffset((context, timestamp) ->
                ((AtomicBoolean) state().get("reportSeam")).get() ? "session-seam" : null);
        functions.supportBatchRead((context, table, offset, size, consumer) -> {
            counter("reads").incrementAndGet();
            if (!((AtomicBoolean) state().get("pinned")).get()) counter("earlyReads").incrementAndGet();
            consumer.accept(List.of(), null);
        });
    }
    @Override public void init(TapConnectionContext context) { counter("inits").incrementAndGet(); }
    @Override public void stop(TapConnectionContext context) { counter("stops").incrementAndGet(); }
    @Override public void discoverSchema(TapConnectionContext context, List<String> tables, int size, Consumer<List<TapTable>> consumer) {
        counter("discoveries").incrementAndGet();
        consumer.accept(List.of(table("t1"), table("t2")));
    }
    private static TapTable table(String name) {
        var table = new TapTable(name);
        table.add(new TapField("session_id", "int").isPrimaryKey(true).primaryKeyPos(1));
        table.add(new TapIndex().name("idx_session").unique(true).indexField(new TapIndexField().name("session_id").fieldAsc(true)));
        return table;
    }
    @Override public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) { return ConnectionOptions.create(); }
    @Override public int tableCount(TapConnectionContext context) { return 2; }
}
