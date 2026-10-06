package io.tapstate.adapters.pdk;

import java.nio.file.Path;

/** Actual stream and connector-owned callback threads controlled only by their test-owned latches. */
final class UnfinishedCdcJars {
    private UnfinishedCdcJars() { }
    static Path source(Path directory) {
        return SyntheticJar.compileToJar(directory, "synthetic.UnfinishedCdcSource", """
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
                import java.util.concurrent.atomic.AtomicInteger;
                import java.util.concurrent.atomic.AtomicReference;
                public class UnfinishedCdcSource implements TapConnector {
                    private static Map<String, Object> state(TapConnectionContext context) {
                        return (Map<String, Object>) System.getProperties().get(
                                context.getConnectionConfig().getString("fixtureKey"));
                    }
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportFlushOffsetFunction((context, offset) -> {
                            Map<String, Object> state = state(context);
                            ((AtomicReference<Thread>) state.get("flushThread")).set(Thread.currentThread());
                            ((CountDownLatch) state.get("flushEntered")).countDown();
                            CountDownLatch release = (CountDownLatch) state.get("releaseFlush");
                            while (true) {
                                try { release.await(); break; }
                                catch (InterruptedException ignored) {
                                    ((AtomicInteger) state.get("flushInterrupts")).incrementAndGet();
                                }
                            }
                        });
                        functions.supportStreamRead((context, tables, offset, size, consumer) -> {
                            Map<String, Object> state = state(context);
                            ((AtomicReference<Thread>) state.get("reader")).set(Thread.currentThread());
                            consumer.streamReadStarted();
                            ((CountDownLatch) state.get("readerEntered")).countDown();
                            if ("stubbornRead".equals(state.get("mode"))) {
                                CountDownLatch release = (CountDownLatch) state.get("releaseRead");
                                while (true) {
                                    try { release.await(); break; }
                                    catch (InterruptedException ignored) { }
                                }
                            } else {
                                if ("ownedAcknowledgement".equals(state.get("mode"))) {
                                    consumer.accept(List.<TapEvent>of(), 41L);
                                }
                                Thread callback = new Thread(() -> {
                                    try {
                                        if ("ownedAcknowledgement".equals(state.get("mode"))) {
                                            ((CountDownLatch) state.get("allowFlush")).await();
                                        }
                                        consumer.accept(List.<TapEvent>of(TapInsertRecordEvent.create().table("t1")
                                                .referenceTime(1L).after(Map.of("id", 1))), null);
                                    } catch (Throwable failure) {
                                        ((AtomicReference<Throwable>) state.get("callbackFailure")).set(failure);
                                    }
                                }, "fixture-owned-cdc-callback");
                                callback.setDaemon(true);
                                ((AtomicReference<Thread>) state.get("callback")).set(callback);
                                callback.start();
                            }
                        });
                    }
                    public void init(TapConnectionContext context) { }
                    public void stop(TapConnectionContext context) {
                        ((AtomicInteger) state(context).get("stops")).incrementAndGet();
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
