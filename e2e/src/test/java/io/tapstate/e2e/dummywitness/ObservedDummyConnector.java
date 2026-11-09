package io.tapstate.e2e.dummywitness;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;

/** Observes bounded correctness runs while delegating to the unchanged release artifact. */
@TapConnectorClass("observed-dummy-spec.json")
public final class ObservedDummyConnector implements TapConnector {
    private final TapConnector delegate;
    private final Path witness;

    public ObservedDummyConnector() {
        var loader = getClass().getClassLoader();
        try (var path = loader.getResourceAsStream("dummy-witness-path.txt")) {
            delegate = loader.loadClass("io.tapdata.dummy.DummyConnector")
                    .asSubclass(TapConnector.class).getDeclaredConstructor().newInstance();
            witness = Path.of(new String(path.readAllBytes(), StandardCharsets.UTF_8));
        } catch (ReflectiveOperationException | IOException failure) {
            throw new AssertionError("Cannot load the Dummy witness", failure);
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
        delegate.registerCapabilities(functions, codecs);
        var batch = functions.getBatchReadFunction();
        functions.supportBatchRead((context, table, offset, size, consumer) ->
                batch.batchRead(context, table, offset, size, (events, next) -> {
                    events.forEach(event -> observe("SNAPSHOT", "", "", event));
                    consumer.accept(events, next);
                }));
        var stream = functions.getStreamReadFunction();
        functions.supportStreamRead((context, tables, offset, size, downstream) -> {
            append("TAIL_START\t" + (offset == null ? "null" : offset.getClass().getSimpleName()));
            try {
                stream.streamRead(context, tables, offset, size, new StreamReadConsumer() {
                    @Override
                    public void accept(List<TapEvent> events, Object next) {
                        events.forEach(event -> observe("CDC", "", "", event));
                        downstream.accept(events, next);
                    }

                    @Override
                    public void streamReadStarted() {
                        downstream.streamReadStarted();
                    }

                    @Override
                    public void streamReadEnded() {
                        downstream.streamReadEnded();
                    }

                    @Override
                    public void asyncMethodAndNoRetry() {
                        downstream.asyncMethodAndNoRetry();
                    }
                });
            } finally {
                append("TAIL_END");
            }
        });
        var write = functions.getWriteRecordFunction();
        functions.supportWriteRecord((context, events, table, consumer) -> {
            String call = UUID.randomUUID().toString();
            events.forEach(event -> observe("WRITE", table.getName(), call, event));
            write.writeRecord(context, events, table, result -> {
                append(String.join("\t", "ACK", String.valueOf(result.getInsertedCount()),
                        String.valueOf(result.getModifiedCount()), String.valueOf(result.getRemovedCount()),
                        String.valueOf(System.currentTimeMillis()), call));
                consumer.accept(result);
            });
        });
    }

    private void observe(String phase, String table, String call, TapEvent event) {
        switch (event) {
            case TapInsertRecordEvent insert -> row(phase, table, call, insert.getTableId(), "I",
                    null, insert.getAfter(), insert.getReferenceTime());
            case TapUpdateRecordEvent update -> row(phase, table, call, update.getTableId(), "U",
                    update.getBefore(), update.getAfter(), update.getReferenceTime());
            case TapDeleteRecordEvent delete -> row(phase, table, call, delete.getTableId(), "D",
                    delete.getBefore(), null, delete.getReferenceTime());
            default -> throw new AssertionError("Unexpected Dummy event " + event.getClass());
        }
    }

    private void row(String phase, String table, String call, String eventTable, String op,
            Map<String, Object> before, Map<String, Object> after, Long referenceTime) {
        append(String.join("\t", phase, table, eventTable, op, normalized(before),
                normalized(after), String.valueOf(referenceTime), call, String.valueOf(System.currentTimeMillis())));
    }

    private static String normalized(Map<String, Object> row) {
        if (row == null) return "null";
        if (!(row.get("id") instanceof Number)) {
            throw new AssertionError("Expected a numeric Dummy key: " + row);
        }
        return new TreeMap<>(row).toString();
    }

    private void append(String line) {
        // The capture thread can be interrupted on stop. Observe its exit without consuming the interrupt.
        try (var out = new FileOutputStream(witness.toFile(), true)) {
            out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    @Override
    public void init(TapConnectionContext context) throws Throwable {
        delegate.init(context);
    }

    @Override
    public void lightInit(TapConnectionContext context) throws Throwable {
        delegate.lightInit(context);
    }

    @Override
    public void stop(TapConnectionContext context) throws Throwable {
        delegate.stop(context);
    }

    @Override
    public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
            Consumer<List<TapTable>> consumer) throws Throwable {
        delegate.discoverSchema(context, tables, size, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) throws Throwable {
        return delegate.connectionTest(context, consumer);
    }

    @Override
    public int tableCount(TapConnectionContext context) throws Throwable {
        return delegate.tableCount(context);
    }
}
