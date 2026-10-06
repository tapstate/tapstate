package io.tapstate.adapters.pdk;

import java.nio.file.Path;

/** Controlled native initialization, read failure and source-release failure. */
final class OpeningSnapshotJars {
    private OpeningSnapshotJars() { }
    static Path source(Path directory) {
        return SyntheticJar.compileToJar(directory, "synthetic.OpeningSnapshotSource", """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.entity.schema.TapTable;
                import io.tapdata.entity.schema.TapField;
                import io.tapdata.entity.event.TapEvent;
                import io.tapdata.entity.event.dml.TapInsertRecordEvent;
                import java.util.List;
                import java.util.Map;
                import java.util.function.Consumer;
                import java.util.concurrent.CountDownLatch;
                import java.util.concurrent.atomic.AtomicReference;
                public class OpeningSnapshotSource implements TapConnector {
                    private static Map<String, Object> state(TapConnectionContext context) {
                        return (Map<String, Object>) System.getProperties().get(
                                context.getConnectionConfig().getString("fixtureKey"));
                    }
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportBatchRead((context, table, offset, size, consumer) -> {
                            Map<String, Object> state = state(context);
                            consumer.accept(List.<TapEvent>of(TapInsertRecordEvent.create().table("t1")
                                    .referenceTime(1L).after(Map.of("id", 1))), null);
                            if ("readError".equals(state.get("mode"))) { throw (Throwable) state.get("primary"); }
                        });
                    }
                    public void init(TapConnectionContext context) throws Throwable {
                        Map<String, Object> state = state(context);
                        ((AtomicReference<Thread>) state.get("reader")).set(Thread.currentThread());
                        ((CountDownLatch) state.get("entered")).countDown();
                        if ("blockedInit".equals(state.get("mode"))) {
                            CountDownLatch release = (CountDownLatch) state.get("release");
                            while (true) {
                                try { release.await(); break; }
                                catch (InterruptedException ignored) { }
                            }
                        }
                        if ("initError".equals(state.get("mode"))) { throw (Throwable) state.get("primary"); }
                    }
                    public void stop(TapConnectionContext context) throws Throwable {
                        Throwable refusal = ((AtomicReference<Throwable>) state(context).get("stopRefusal")).get();
                        if (refusal != null) { throw refusal; }
                    }
                    public void discoverSchema(TapConnectionContext context, List<String> selected, int size,
                            Consumer<List<TapTable>> consumer) {
                        TapTable table = new TapTable("t1"); table.add(new TapField("id", "int"));
                        consumer.accept(List.of(table));
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
                        return ConnectionOptions.create();
                    }
                    public int tableCount(TapConnectionContext context) { return 1; }
                }
                """);
    }
}
