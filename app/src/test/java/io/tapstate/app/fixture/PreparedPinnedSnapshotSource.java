package io.tapstate.app.fixture;

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

/** Test-only connector reports whether the host durably pinned its actual schema before batchRead. */
public final class PreparedPinnedSnapshotSource implements TapConnector {
    public static final String STATE = "tapstate.test.native-pinned-snapshot.state";
    @SuppressWarnings("unchecked")
    private static Map<String,Object> state() { return (Map<String,Object>)System.getProperties().get(STATE); }
    private static AtomicInteger count(String key) { return (AtomicInteger)state().get(key); }
    @Override public void registerCapabilities(ConnectorFunctions functions,TapCodecsRegistry codecs) {
        functions.supportTimestampToStreamOffset((context,time) -> "actual-session-seam");
        functions.supportBatchRead((context,table,offset,size,consumer) -> {
            count("reads").incrementAndGet();
            if (!((AtomicBoolean)state().get("pinned")).get()) count("earlyReads").incrementAndGet();
            consumer.accept(List.of(),null);
        });
    }
    @Override public void init(TapConnectionContext context) { count("inits").incrementAndGet(); }
    @Override public void stop(TapConnectionContext context) { count("stops").incrementAndGet(); }
    @Override public void discoverSchema(TapConnectionContext context,List<String> tables,int size,Consumer<List<TapTable>> consumer) {
        count("discoveries").incrementAndGet(); consumer.accept(List.of(table("t1"),table("t2")));
    }
    private static TapTable table(String name) {
        var table = new TapTable(name); table.add(new TapField("session_id","int").isPrimaryKey(true).primaryKeyPos(1));
        table.add(new TapIndex().name("idx_session").unique(true).indexField(new TapIndexField().name("session_id").fieldAsc(true))); return table;
    }
    @Override public ConnectionOptions connectionTest(TapConnectionContext context,Consumer<TestItem> consumer) { return ConnectionOptions.create(); }
    @Override public int tableCount(TapConnectionContext context) { return 2; }
}
