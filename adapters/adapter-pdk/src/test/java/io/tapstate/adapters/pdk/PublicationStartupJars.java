package io.tapstate.adapters.pdk;

import java.nio.file.Path;
import java.util.Map;

/** A frozen-contract fixture with an observable publication check/create startup race. */
final class PublicationStartupJars {
    private PublicationStartupJars() { }

    static Path source(Path directory) {
        return SyntheticJar.compileToJar(directory, "synthetic.PublicationStartupSource", """
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
                import java.util.concurrent.TimeUnit;
                import java.util.concurrent.atomic.AtomicBoolean;
                import java.util.concurrent.atomic.AtomicInteger;
                public class PublicationStartupSource implements TapConnector {
                    public static class Offset implements java.io.Serializable {
                        public String mark;
                        public Offset() { }
                        public Offset(String mark) { this.mark = mark; }
                    }
                    private static Map<String, Object> state(TapConnectionContext context) {
                        return (Map<String, Object>) System.getProperties().get(
                                context.getConnectionConfig().getString("fixtureKey"));
                    }
                    private static AtomicInteger counter(Map<String, Object> state, String key) {
                        return (AtomicInteger) state.get(key);
                    }
                    private static void await(Map<String, Object> state, String key) throws InterruptedException {
                        if (!((CountDownLatch) state.get(key)).await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("the fixture did not release " + key);
                        }
                    }
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportTimestampToStreamOffset((context, timestamp) -> {
                            Map<String, Object> state = state(context);
                            counter(state, "samples").incrementAndGet();
                            ((CountDownLatch) state.get("sampleEntered")).countDown();
                            String mode = (String) state.get("mode");
                            if ("blockedOffset".equals(mode)) { await(state, "releaseOffset"); }
                            if ("offsetError".equals(mode)) {
                                throw new java.sql.SQLException((String) state.get("errorMessage"), (String) state.get("sqlState"));
                            }
                            AtomicBoolean published = (AtomicBoolean) state.get("published");
                            if ("race".equals(mode) && !published.get()) {
                                CountDownLatch checked = (CountDownLatch) state.get("absentChecks");
                                checked.countDown();
                                // A second unchecked start can join this window. Serialization means it
                                // does not, so the first attempt leaves the bounded window and creates once.
                                checked.await(1, TimeUnit.SECONDS);
                                if (!published.compareAndSet(false, true)) {
                                    throw new IllegalStateException("create publication for all tables failed. Error message: "
                                            + "ERROR: duplicate key value violates unique constraint pg_publication_pubname_index; "
                                            + "Key (pubname)=(dbz_publication) already exists.");
                                }
                            }
                            return new Offset("seam");
                        });
                        functions.supportBatchRead((context, table, offset, size, consumer) -> {
                            Map<String, Object> state = state(context);
                            int batch = counter(state, "batches").incrementAndGet();
                            consumer.accept(List.<TapEvent>of(TapInsertRecordEvent.create().table("t1")
                                    .referenceTime(1L).after(Map.of("id", 1))), null);
                            ((CountDownLatch) state.get("rowEmitted")).countDown();
                            if ("holdFirstBatch".equals(state.get("mode")) && batch == 1) { await(state, "releaseBatch"); }
                            if ("afterRowError".equals(state.get("mode"))) {
                                throw new java.sql.SQLException((String) state.get("errorMessage"), (String) state.get("sqlState"));
                            }
                        });
                    }
                    public void init(TapConnectionContext context) { }
                    public void stop(TapConnectionContext context) { }
                    public void discoverSchema(TapConnectionContext context, List<String> selected, int size,
                            Consumer<List<TapTable>> consumer) {
                        TapTable table = new TapTable("t1"); table.add(new TapField("id", "int"));
                        consumer.accept(List.of(table));
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
                        consumer.accept(new TestItem("ping", TestItem.RESULT_SUCCESSFULLY));
                        return ConnectionOptions.create();
                    }
                    public int tableCount(TapConnectionContext context) { return 1; }
                }
                """, Map.of(), Map.of());
    }
}
