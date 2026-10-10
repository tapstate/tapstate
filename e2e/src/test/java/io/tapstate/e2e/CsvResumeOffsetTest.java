package io.tapstate.e2e;

import io.tapdata.PDKExCode_10;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkConfigEx;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.e2e.connector.CsvConnector;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CsvResumeOffsetTest {
    private static final String TABLE = "orders";

    @Test void aRegisteredStreamResumesAfterItsActualRetainedOffset(@TempDir Path directory) throws Throwable {
        FileEndpoints files = new FileEndpoints();
        EndpointAddress source = EndpointAddress.uri(directory.toString());
        files.seed(source, TABLE, SeedRows.generated(4));
        TapConnectorContext context = pointedAt(directory);
        CsvConnector connector = new CsvConnector();
        ConnectorFunctions functions = registered(connector, context);
        Object retained = functions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(context, null);
        assertThat(retained).isEqualTo(Map.of(TABLE, 4L));
        files.cdc(source, TABLE, CdcOp.INSERT, 1);
        List<Long> received = new ArrayList<>();

        functions.getStreamReadFunction().streamRead(context, List.of(TABLE), retained, 1000,
                StreamReadConsumer.create((events, position) -> {
                    received.addAll(ids(events));
                    connector.stop(context);
                }));

        assertThat(received).containsExactly(5L);
    }

    @Test void anAcceptedCheckpointResumesANewSubscriptionWithoutReminingItsLastRow(@TempDir Path directory) throws Throwable {
        FileEndpoints files = new FileEndpoints();
        EndpointAddress source = EndpointAddress.uri(directory.toString());
        files.seed(source, TABLE, SeedRows.generated(4));
        TapConnectorContext context = pointedAt(directory);
        CsvConnector first = new CsvConnector();
        ConnectorFunctions firstFunctions = registered(first, context);
        Object retained = firstFunctions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(context, null);
        files.cdc(source, TABLE, CdcOp.INSERT, 1);
        AtomicReference<Object> checkpoint = new AtomicReference<>();
        firstFunctions.getStreamReadFunction().streamRead(context, List.of(TABLE), retained, 1000,
                StreamReadConsumer.create((events, position) -> {
                    assertThat(positionFrames(directory).stream().filter(line -> line.contains("\tDELIVERY\t"))).isEmpty();
                    assertThatThrownBy(((Map<?, ?>) position)::clear).isInstanceOf(UnsupportedOperationException.class);
                    checkpoint.set(position);
                    first.stop(context);
                }));
        assertThat(checkpoint.get()).isEqualTo(Map.of(TABLE, 5L));

        files.cdc(source, TABLE, CdcOp.INSERT, 1);
        CsvConnector second = new CsvConnector();
        ConnectorFunctions secondFunctions = registered(second, context);
        List<Long> resumed = new ArrayList<>();
        AtomicReference<Object> nextCheckpoint = new AtomicReference<>();
        secondFunctions.getStreamReadFunction().streamRead(context, List.of(TABLE), checkpoint.get(), 1000,
                StreamReadConsumer.create((events, position) -> {
                    resumed.addAll(ids(events));
                    nextCheckpoint.set(position);
                    second.stop(context);
                }));

        assertThat(resumed).containsExactly(6L);
        assertThat(nextCheckpoint.get()).isEqualTo(Map.of(TABLE, 6L));
        assertThat(positionFrames(directory).stream().map(line -> line.split("\t", -1)).map(cells -> cells[1] + ":" + cells[4]))
                .containsExactly("START:{orders=4}", "DELIVERY:{orders=5}", "START:{orders=5}", "DELIVERY:{orders=6}");
    }

    @Test void callbackOffsetsCoverOnlyTheTablesAlreadyDelivered(@TempDir Path directory) throws Throwable {
        FileEndpoints files = new FileEndpoints();
        EndpointAddress source = EndpointAddress.uri(directory.toString());
        files.seed(source, TABLE, SeedRows.generated(4));
        files.seed(source, "items", SeedRows.generated(4));
        TapConnectorContext context = pointedAt(directory);
        CsvConnector connector = new CsvConnector();
        ConnectorFunctions functions = registered(connector, context);
        Object retained = functions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(context, null);
        files.cdc(source, TABLE, CdcOp.INSERT, 1);
        files.cdc(source, "items", CdcOp.INSERT, 1);
        List<Object> checkpoints = new ArrayList<>();
        List<String> deliveredTables = new ArrayList<>();
        AtomicInteger deliveries = new AtomicInteger();

        functions.getStreamReadFunction().streamRead(context, List.of(TABLE, "items"), retained, 1000,
                StreamReadConsumer.create((events, position) -> {
                    assertThat(ids(events)).containsExactly(5L);
                    deliveredTables.add(((TapInsertRecordEvent) events.getFirst()).getTableId());
                    checkpoints.add(position);
                    if (deliveries.incrementAndGet() == 2) { connector.stop(context); }
                }));

        assertThat(deliveredTables).containsExactly(TABLE, "items");
        assertThat(checkpoints).containsExactly(Map.of(TABLE, 5L, "items", 4L), Map.of(TABLE, 5L, "items", 5L));
    }

    @Test void anUnknownOrInvalidOffsetRefusesBeforeDeliveringAnyRows(@TempDir Path directory) {
        FileEndpoints files = new FileEndpoints();
        files.seed(EndpointAddress.uri(directory.toString()), TABLE, SeedRows.generated(5));
        for (Object offset : List.of("unknown-position", Map.of(), Map.of(TABLE, 4.5), Map.of(TABLE, -1L))) {
            TapConnectorContext context = pointedAt(directory);
            CsvConnector connector = new CsvConnector();
            ConnectorFunctions functions = registered(connector, context);
            List<Long> received = new ArrayList<>();

            assertThatThrownBy(() -> functions.getStreamReadFunction().streamRead(context, List.of(TABLE), offset, 1000,
                    StreamReadConsumer.create((events, position) -> {
                        received.addAll(ids(events));
                        connector.stop(context);
                    }))).isInstanceOfSatisfying(TapPdkConfigEx.class, failure -> {
                        assertThat(failure.getCode()).isEqualTo(PDKExCode_10.CONFIG_ERROR);
                        assertThat(failure.getPdkId()).isEqualTo(E2eConnectorJar.CONNECTOR_ID);
                    });

            assertThat(received).isEmpty();
        }
        assertThat(Files.exists(directory.resolve("reads"))).isFalse();
    }

    @Test void aNullSdkOffsetReadsFromTheBeginning(@TempDir Path directory) throws Throwable {
        FileEndpoints files = new FileEndpoints();
        files.seed(EndpointAddress.uri(directory.toString()), TABLE, SeedRows.generated(2));
        TapConnectorContext context = pointedAt(directory);
        CsvConnector connector = new CsvConnector();
        ConnectorFunctions functions = registered(connector, context);
        List<Long> received = new ArrayList<>();
        AtomicReference<Object> checkpoint = new AtomicReference<>();

        functions.getStreamReadFunction().streamRead(context, List.of(TABLE), null, 1000,
                StreamReadConsumer.create((events, position) -> {
                    received.addAll(ids(events));
                    checkpoint.set(position);
                    connector.stop(context);
                }));

        assertThat(received).containsExactly(1L, 2L);
        assertThat(checkpoint.get()).isEqualTo(Map.of(TABLE, 2L));
    }

    @Test void theSdkBeginningOfTimeOffsetKeepsTheInitialEarliestRead(@TempDir Path directory) throws Throwable {
        FileEndpoints files = new FileEndpoints();
        files.seed(EndpointAddress.uri(directory.toString()), TABLE, SeedRows.generated(2));
        TapConnectorContext context = pointedAt(directory);
        CsvConnector connector = new CsvConnector();
        ConnectorFunctions functions = registered(connector, context);
        Object beginning = functions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(context, 0L);
        assertThat(beginning).isEqualTo(Map.of(TABLE, 0L));
        List<Long> received = new ArrayList<>();

        functions.getStreamReadFunction().streamRead(context, List.of(TABLE), beginning, 1000,
                StreamReadConsumer.create((events, position) -> {
                    received.addAll(ids(events));
                    connector.stop(context);
                }));

        assertThat(received).containsExactly(1L, 2L);
    }

    private static ConnectorFunctions registered(CsvConnector connector, TapConnectorContext context) {
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        connector.init(context);
        return functions;
    }

    private static TapConnectorContext pointedAt(Path directory) {
        TapConnectorContext context = new TapConnectorContext(null, new DataMap().kv("uri", directory.toString())
                .kv("read_witness", directory.resolve("reads").toString()), null, null);
        context.setStateMap(new Notes());
        return context;
    }

    private static List<Long> ids(List<TapEvent> events) {
        return events.stream().map(event -> (TapInsertRecordEvent) event)
                .map(event -> Long.parseLong(String.valueOf(event.getAfter().get(SeedRows.ID)))).toList();
    }

    private static List<String> positionFrames(Path directory) {
        try { return Files.readAllLines(directory.resolve("reads").resolve("positions-" + ProcessHandle.current().pid() + ".tsv")); }
        catch (IOException failed) { throw new UncheckedIOException(failed); }
    }

    private static final class Notes implements KVMap<Object> {
        private final Map<String, Object> values = new HashMap<>();
        @Override public void init(String name, Class<Object> type) { }
        @Override public Object get(String key) { return values.get(key); }
        @Override public void put(String key, Object value) { values.put(key, value); }
        @Override public Object putIfAbsent(String key, Object value) { return values.putIfAbsent(key, value); }
        @Override public Object remove(String key) { return values.remove(key); }
        @Override public void clear() { values.clear(); }
        @Override public void reset() { values.clear(); }
    }
}
