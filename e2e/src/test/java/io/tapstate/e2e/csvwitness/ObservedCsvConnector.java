package io.tapstate.e2e.csvwitness;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/** Records entry into the unchanged fixture SDK without changing its arguments, callbacks, or failures. */
@TapConnectorClass("observed-csv-spec.json")
public final class ObservedCsvConnector implements TapConnector {
    private final TapConnector delegate;
    private final Path journal;

    public ObservedCsvConnector() {
        try {
            delegate = getClass().getClassLoader().loadClass("io.tapstate.e2e.connector.CsvConnector")
                    .asSubclass(TapConnector.class).getDeclaredConstructor().newInstance();
            try (InputStream input = getClass().getClassLoader().getResourceAsStream("csv-entry-witness-path.txt")) {
                if (input == null) throw new AssertionError("missing source entry journal path");
                journal = Path.of(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            }
        } catch (ReflectiveOperationException | java.io.IOException unavailable) {
            throw new AssertionError("cannot open the actual fixture SDK entry observer", unavailable);
        }
    }

    public ObservedCsvConnector(TapConnector delegate, Path journal) {
        this.delegate = Objects.requireNonNull(delegate);
        this.journal = Objects.requireNonNull(journal);
    }

    @Override public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
        enter("REGISTER", "-", null);
        delegate.registerCapabilities(functions, codecs);
        var batch = functions.getBatchReadFunction();
        if (batch != null) functions.supportBatchRead((context, table, offset, size, consumer) -> {
            enter("BATCH_READ", table == null ? "-" : table.getId(), offset);
            batch.batchRead(context, table, offset, size, consumer);
        });
        var stream = functions.getStreamReadFunction();
        if (stream != null) functions.supportStreamRead((context, tables, offset, size, consumer) -> {
            enter("STREAM_READ", tables == null ? "-" : String.join(",", tables), offset);
            stream.streamRead(context, tables, offset, size, consumer);
        });
        var timestamp = functions.getTimestampToStreamOffsetFunction();
        if (timestamp != null) functions.supportTimestampToStreamOffset((context, start) -> {
            enter("TIMESTAMP_OFFSET", "-", start);
            return timestamp.timestampToStreamOffset(context, start);
        });
        var count = functions.getCountByPartitionFilterFunction();
        if (count != null) functions.supportCountByPartitionFilterFunction((context, table, filter) -> {
            enter("COUNT", table == null ? "-" : table.getId(), filter);
            return count.countByPartitionFilter(context, table, filter);
        });
        var write = functions.getWriteRecordFunction();
        if (write != null) functions.supportWriteRecord((context, events, table, consumer) -> {
            enter("WRITE", table == null ? "-" : table.getId(), events);
            write.writeRecord(context, events, table, consumer);
        });
        var clear = functions.getClearTableFunction();
        if (clear != null) functions.supportClearTable((context, event) -> {
            enter("CLEAR", event == null ? "-" : event.getTableId(), event);
            clear.clearTable(context, event);
        });
        var names = functions.getGetTableNamesFunction();
        if (names != null) functions.supportGetTableNamesFunction((context, size, consumer) -> {
            enter("TABLE_NAMES", "-", null);
            names.tableNames(context, size, consumer);
        });
        var info = functions.getGetTableInfoFunction();
        if (info != null) functions.supportGetTableInfoFunction((context, table) -> {
            enter("TABLE_INFO", table, null);
            return info.getTableInfo(context, table);
        });
        var command = functions.getExecuteCommandFunction();
        if (command != null) functions.supportExecuteCommandFunction((context, request, consumer) -> {
            enter("COMMAND", "-", request);
            command.execute(context, request, consumer);
        });
    }

    private void enter(String operation, String selected, Object argument) {
        String identity = argument == null ? "absent" : argument.getClass().getName() + ":" + System.identityHashCode(argument);
        String line = ProcessHandle.current().pid() + "\t" + operation + "\t" + selected + "\t" + identity + "\n";
        try {
            synchronized (ObservedCsvConnector.class) {
                try (var output = new FileOutputStream(journal.toFile(), true)) {
                    output.write(line.getBytes(StandardCharsets.UTF_8));
                }
            }
        } catch (java.io.IOException unavailable) {
            throw new AssertionError("cannot append the source delegate entry journal", unavailable);
        }
    }

    @Override public void init(TapConnectionContext context) throws Throwable {
        enter("INIT", "-", context);
        delegate.init(context);
    }
    @Override public void lightInit(TapConnectionContext context) throws Throwable {
        enter("LIGHT_INIT", "-", context);
        delegate.lightInit(context);
    }
    @Override public void stop(TapConnectionContext context) throws Throwable {
        enter("STOP", "-", context);
        delegate.stop(context);
    }
    @Override public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
            Consumer<List<TapTable>> consumer) throws Throwable {
        enter("DISCOVER", tables == null ? "-" : String.join(",", tables), null);
        delegate.discoverSchema(context, tables, size, consumer);
    }
    @Override public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) throws Throwable {
        enter("CONNECTION_TEST", "-", context);
        return delegate.connectionTest(context, consumer);
    }
    @Override public int tableCount(TapConnectionContext context) throws Throwable {
        enter("TABLE_COUNT", "-", context);
        return delegate.tableCount(context);
    }
}
