package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.e2e.csvwitness.ObservedCsvConnector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObservedCsvConnectorFunctionsTest {
    @TempDir Path directory;

    @Test void batchEntryPrecedesFailureAndKeepsEveryInputReference() throws Throwable {
        Path journal = directory.resolve("calls.tsv");
        TapConnectorContext context = new TapConnectorContext(null, null, null, null);
        TapTable table = new TapTable("orders");
        Object offset = Map.of("orders", 2L);
        BiConsumer<List<TapEvent>, Object> consumer = (events, point) -> { };
        AssertionError failure = new AssertionError("delegate refusal");
        AtomicBoolean entered = new AtomicBoolean();
        TapConnector delegate = new Empty() {
            @Override public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                functions.supportBatchRead((receivedContext, receivedTable, receivedOffset, size, receivedConsumer) -> {
                    assertThat(Files.readString(journal)).contains("\tBATCH_READ\torders\t");
                    assertThat(receivedContext).isSameAs(context);
                    assertThat(receivedTable).isSameAs(table);
                    assertThat(receivedOffset).isSameAs(offset);
                    assertThat(receivedConsumer).isSameAs(consumer);
                    assertThat(size).isEqualTo(7);
                    entered.set(true);
                    throw failure;
                });
            }
        };
        ConnectorFunctions functions = wrapped(delegate, journal);
        assertThatThrownBy(() -> functions.getBatchReadFunction().batchRead(context, table, offset, 7, consumer))
                .isSameAs(failure);
        assertThat(entered).isTrue();
    }

    @Test void streamKeepsTheExactOffsetAndOriginalConsumerEvenOnRefusal() throws Throwable {
        Path journal = directory.resolve("stream.tsv");
        TapConnectorContext context = new TapConnectorContext(null, null, null, null);
        List<String> tables = List.of("orders");
        Object offset = Map.of("orders", 2L);
        StreamReadConsumer consumer = StreamReadConsumer.create((events, point) -> { });
        AssertionError failure = new AssertionError("stream refusal");
        TapConnector delegate = new Empty() {
            @Override public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                functions.supportStreamRead((receivedContext, receivedTables, receivedOffset, size, receivedConsumer) -> {
                    assertThat(Files.readString(journal)).contains("\tSTREAM_READ\torders\t");
                    assertThat(receivedContext).isSameAs(context);
                    assertThat(receivedTables).isSameAs(tables);
                    assertThat(receivedOffset).isSameAs(offset);
                    assertThat(receivedConsumer).isSameAs(consumer);
                    assertThat(size).isEqualTo(9);
                    throw failure;
                });
            }
        };
        ConnectorFunctions functions = wrapped(delegate, journal);
        assertThatThrownBy(() -> functions.getStreamReadFunction().streamRead(context, tables, offset, 9, consumer))
                .isSameAs(failure);
    }

    @Test void timestampResultIsTheActualDelegateObject() throws Throwable {
        Path journal = directory.resolve("offset.tsv");
        Object actual = Map.of("orders", 2L);
        TapConnector delegate = new Empty() {
            @Override public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                functions.supportTimestampToStreamOffset((context, start) -> {
                    assertThat(Files.readString(journal)).contains("\tTIMESTAMP_OFFSET\t");
                    assertThat(start).isEqualTo(0L);
                    return actual;
                });
            }
        };
        assertThat(wrapped(delegate, journal).getTimestampToStreamOffsetFunction().timestampToStreamOffset(null, 0L))
                .isSameAs(actual);
    }

    @Test void cleanupEntryDoesNotClearTheCallingThreadsInterrupt() throws Throwable {
        Path journal = directory.resolve("stop.tsv");
        AtomicBoolean preserved = new AtomicBoolean();
        TapConnector delegate = new Empty() {
            @Override public void stop(TapConnectionContext context) {
                preserved.set(Thread.currentThread().isInterrupted());
            }
        };
        Thread.currentThread().interrupt();
        try {
            new ObservedCsvConnector(delegate, journal).stop(null);
            assertThat(preserved).isTrue();
        } finally { Thread.interrupted(); }
        assertThat(Files.readString(journal)).contains("\tSTOP\t");
    }

    private static ConnectorFunctions wrapped(TapConnector delegate, Path journal) {
        ConnectorFunctions functions = new ConnectorFunctions();
        new ObservedCsvConnector(delegate, journal).registerCapabilities(functions, new TapCodecsRegistry());
        return functions;
    }
    private static class Empty implements TapConnector {
        @Override public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) { }
        @Override public void init(TapConnectionContext context) { }
        @Override public void lightInit(TapConnectionContext context) { }
        @Override public void stop(TapConnectionContext context) { }
        @Override public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
                Consumer<List<TapTable>> consumer) { }
        @Override public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
            return new ConnectionOptions();
        }
        @Override public int tableCount(TapConnectionContext context) { return 0; }
    }
}
