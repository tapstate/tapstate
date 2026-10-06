package io.tapstate.adapters.pdk;

import java.nio.file.Path;

/** A completed native stream whose stop reports the exact test-owned failure. */
final class ThrowingCdcStopJars {
    private ThrowingCdcStopJars() { }
    static Path source(Path directory) {
        return SyntheticJar.compileToJar(directory, "synthetic.ThrowingCdcStopSource", """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.entity.schema.TapTable;
                import io.tapdata.entity.schema.TapField;
                import java.util.List;
                import java.util.Map;
                import java.util.function.Consumer;
                import java.util.concurrent.CountDownLatch;
                import java.util.concurrent.atomic.AtomicInteger;
                import java.util.concurrent.atomic.AtomicReference;
                public class ThrowingCdcStopSource implements TapConnector {
                    private static Map<String, Object> state(TapConnectionContext context) {
                        return (Map<String, Object>) System.getProperties().get(
                                context.getConnectionConfig().getString("fixtureKey"));
                    }
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportStreamRead((context, tables, offset, size, consumer) -> {
                            Map<String, Object> state = state(context);
                            ((AtomicReference<Thread>) state.get("reader")).set(Thread.currentThread());
                            consumer.streamReadStarted();
                            ((CountDownLatch) state.get("readEntered")).countDown();
                        });
                    }
                    public void init(TapConnectionContext context) { }
                    public void stop(TapConnectionContext context) throws Throwable {
                        Map<String, Object> state = state(context);
                        ((AtomicInteger) state.get("stops")).incrementAndGet();
                        Throwable refusal = ((AtomicReference<Throwable>) state.get("refusal")).get();
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
