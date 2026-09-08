package io.tapstate.e2e.connector;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * A connector over a directory of comma-separated files: one file per table, a header naming the
 * columns, a row per line. It reads (snapshot and tail) and writes, so one specification can use it at
 * both ends of a pipeline and watch rows cross the product.
 *
 * <p>It exists because the connectors a release ships are not available to this build, and a
 * specification that cannot move a row through a connector is a specification testing the absence of
 * one. Everything the product does around the connector - registering the artifact, resolving the
 * class, deriving the target model, running the DAG, keying the upsert - is the real thing; only the
 * store at each end is a directory instead of a database.
 *
 * <h2>Two rules this class must not break</h2>
 *
 * <p><b>It may touch nothing but the JDK and {@code io.tapdata.*}.</b> It is loaded from its jar by an
 * isolating loader that delegates only the frozen PDK contract to the host and resolves nothing else,
 * so a reference to any other type - very much including anything under {@code io.tapstate} - resolves
 * against this jar alone and fails to link at load time. That is not a style rule; it is what the
 * loader does.
 *
 * <p><b>It shares no code with the harness driver that reads the same files.</b> The format is the
 * only thing between them. A count taken through this class would agree with it by construction and
 * would keep agreeing while nothing crossed the product at all.
 *
 * <h2>What its tail can and cannot see</h2>
 *
 * <p>No read position is ever handed to a connector - the product passes a null offset to both reads -
 * so this tail has nowhere to resume from and replays the table from the beginning on its first poll.
 * The rows the snapshot already carried are therefore delivered twice. That is not a defect to paper
 * over here: it is the same overlap the product's own snapshot-to-cdc seam leaves, and an idempotent
 * upsert on the discovered key is what absorbs it. A specification that counts rows after both phases
 * is asserting exactly that absorption.
 *
 * <p>The tail sees rows appear; it does not see rows change or vanish. Detecting those in a flat file
 * would mean diffing every row on every poll, and no specification needs it yet.
 */
@TapConnectorClass("e2e-file-spec.json")
public class CsvConnector implements TapConnector {

    /** The setting naming the directory this connector reads and writes: a plain filesystem path. */
    private static final String URI = "uri";

    /**
     * A test affordance: when set truthy on a target connection, every write is rejected. It exists so a
     * specification can drive a sink that fails and assert the product's observable-error path end to end -
     * a dead data-plane job becoming a FAILED state and an error count. A sink that never fails cannot
     * witness that path, and the connectors a release ships are not here to fail on demand.
     */
    private static final String FAIL_WRITES = "fail_writes";

    /**
     * A test affordance on the read side, the mirror of {@link #FAIL_WRITES}: when set truthy on a source
     * connection, the cdc tail starts and then throws. It exists so a specification can drive a source whose
     * change stream dies and assert the product surfaces it as an observable error - a dead cdc tail becoming
     * a FAILED state and an error count - even though the tail runs on its own thread and the job reading the
     * ring it fills keeps running over a ring gone quiet. A tail that never fails cannot witness that path.
     */
    private static final String FAIL_CDC = "fail_cdc";

    private static final String SUFFIX = ".csv";

    /**
     * The one command the read face dispatches. It is pinned on both sides on purpose: the caller sends
     * only this name and this connector answers only this name, so the command channel cannot become a
     * way to reach anything else the connector can do.
     */
    private static final String QUERY_COMMAND = "executeQuery";

    /** Every column is text: a comma-separated file declares no types, so inventing one would be a lie. */
    private static final String COLUMN_TYPE = "string";

    private static final long POLL_MILLIS = 100;

    /** Set by {@code stop}; the tail also honours its thread's interrupt. Both signals arrive on cancel. */
    private volatile boolean stopped;

    public CsvConnector() {
    }

    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
        functions
                .supportBatchRead((context, table, offset, size, consumer) ->
                        consumer.accept(snapshot(context, table.getId()), null))
                .supportStreamRead((context, tables, offset, size, consumer) ->
                        tail(context, tables, consumer))
                // A streaming source states where its stream stands, and this one does the same. A snapshot
                // samples it before reading its first row and hands it on as the seam the tail joins at; a
                // source that names no position leaves that join to guesswork, which a snapshot followed by
                // a tail refuses rather than papers over. Every stream-capable connector the ecosystem ships
                // declares this -- the only ones that do not are its own benchmark fixtures.
                .supportTimestampToStreamOffset((context, startTime) -> highWaterMarks(context))
                .supportWriteRecord((context, events, table, consumer) ->
                        consumer.accept(write(context, events, table)))
                // The three the read face drives. Registering them is what lets a specification exercise
                // the browse chain without a real database at the far end; a connector missing any one of
                // them is refused by name before the read starts.
                .supportGetTableNamesFunction((context, batchSize, consumer) ->
                        consumer.accept(tableNames(context)))
                .supportGetTableInfoFunction(CsvConnector::tableInfo)
                .supportExecuteCommandFunction((context, command, consumer) ->
                        consumer.accept(execute(context, command)));
    }

    @Override
    public void init(TapConnectionContext context) {
        stopped = false;
    }

    @Override
    public void stop(TapConnectionContext context) {
        stopped = true;
    }

    /**
     * The tables and their columns, read from each file's header. The first column is reported as the
     * primary key: a comma-separated file declares no key, and the product derives the upsert key from
     * this model, so a connector that reported none would leave every sink writing it unkeyed.
     */
    @Override
    public void discoverSchema(
            TapConnectionContext context, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
        List<TapTable> discovered = new ArrayList<>();
        for (String name : tables.isEmpty() ? tableNames(context) : tables) {
            List<String> header = header(file(context, name));
            if (header.isEmpty()) {
                continue;
            }
            TapTable table = new TapTable(name);
            for (int column = 0; column < header.size(); column++) {
                TapField field = new TapField(header.get(column), COLUMN_TYPE);
                if (column == 0) {
                    field.primaryKeyPos(1);
                }
                table.add(field);
            }
            discovered.add(table);
        }
        consumer.accept(discovered);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
        boolean reachable = Files.isDirectory(directory(context));
        consumer.accept(new TestItem(
                "directory",
                reachable ? TestItem.RESULT_SUCCESSFULLY : TestItem.RESULT_FAILED,
                reachable ? null : directory(context) + " is not a directory"));
        return ConnectionOptions.create();
    }

    @Override
    public int tableCount(TapConnectionContext context) {
        return tableNames(context).size();
    }

    // ---- reads -----------------------------------------------------------------------------------

    /** Every row the table holds now, insert-shaped: a batch read yields insert-shaped rows only. */
    private List<TapEvent> snapshot(TapConnectionContext context, String table) {
        List<TapEvent> events = new ArrayList<>();
        for (Map<String, Object> row : rows(file(context, table))) {
            events.add(insert(table, row));
        }
        return events;
    }

    /**
     * Polls each table for rows it has not delivered yet and streams them until the run is cancelled.
     * Cancellation arrives as an interrupt, a {@code stop}, or both; either ends the loop, because a
     * tail that outlived its cancel would hold the connector's loader open behind it.
     */
    private void tail(TapConnectionContext context, List<String> tables, StreamReadConsumer consumer) {
        Map<String, Long> delivered = new LinkedHashMap<>();
        consumer.streamReadStarted();
        if (cdcRejected(context)) {
            // The stream started, then dies - the read-side mirror of a rejected write. The product wraps
            // whatever the tail throws and surfaces it as an observable failure; the type here is immaterial.
            throw new IllegalStateException(
                    "the '" + FAIL_CDC + "' setting makes this source's cdc stream fail");
        }
        while (!stopped && !Thread.currentThread().isInterrupted()) {
            for (String table : tables) {
                List<TapEvent> fresh = new ArrayList<>();
                for (Map<String, Object> row : rows(file(context, table))) {
                    long id = idOf(row);
                    if (id > delivered.getOrDefault(table, 0L)) {
                        fresh.add(insert(table, row));
                        delivered.put(table, id);
                    }
                }
                if (!fresh.isEmpty()) {
                    consumer.accept(fresh, null);
                }
            }
            try {
                Thread.sleep(POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        consumer.streamReadEnded();
    }

    /**
     * Where each table's tail stands right now: the highest ordering column value the file currently
     * holds, and zero for a table with no rows yet. Serializable on purpose -- a recorded position is
     * written down and read back by a later run, so it outlives the process that sampled it.
     *
     * <p>This connector does not yet resume from one: its tail re-reads from the beginning and the sink
     * absorbs the overlap, so the value is honest about where the stream stood without being acted on.
     * Making the tail start here is what a resume witness needs, and it belongs with that witness.
     */
    private static LinkedHashMap<String, Long> highWaterMarks(TapConnectionContext context) {
        LinkedHashMap<String, Long> marks = new LinkedHashMap<>();
        for (String table : tableNames(context)) {
            long high = 0;
            for (Map<String, Object> row : rows(file(context, table))) {
                high = Math.max(high, idOf(row));
            }
            marks.put(table, high);
        }
        return marks;
    }

    /**
     * A row's position in the tail's order, taken from its first column. The tail delivers rows whose
     * position it has not reached yet, so the column has to order them; a file whose first column does
     * not is a file this connector cannot tail, and saying so beats delivering an arbitrary subset.
     */
    private static long idOf(Map<String, Object> row) {
        Object first = row.values().iterator().next();
        try {
            return Long.parseLong(String.valueOf(first));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("the first column orders the tail, so it must be a whole number: " + first, e);
        }
    }

    private static TapEvent insert(String table, Map<String, Object> row) {
        return TapInsertRecordEvent.create().table(table).referenceTime(System.currentTimeMillis()).after(row);
    }

    // ---- writes ----------------------------------------------------------------------------------

    /**
     * Applies a batch to the target file, creating it when this is the first write. Rows are keyed on
     * the target model's primary key when it declares one, and appended when it does not - the product
     * resolves that key from the source's discovered model, so a target written without a discovery
     * behind it is the one that grows a duplicate per replayed row.
     */
    private WriteListResult<TapRecordEvent> write(
            TapConnectionContext context, List<TapRecordEvent> events, TapTable target) {
        if (writesRejected(context)) {
            // The product wraps whatever a connector's write throws into a coded write failure, so the type
            // here is immaterial; what matters is that the batch does not complete.
            throw new IllegalStateException(
                    "the '" + FAIL_WRITES + "' setting makes this sink reject every write");
        }
        Path file = file(context, target.getId());
        List<String> key = primaryKeyOf(target);
        Map<String, Map<String, Object>> byKey = new LinkedHashMap<>();
        List<Map<String, Object>> appended = new ArrayList<>();
        for (Map<String, Object> existing : rows(file)) {
            if (key.isEmpty()) {
                appended.add(existing);
            } else {
                byKey.put(keyOf(existing, key), existing);
            }
        }

        long inserted = 0;
        long modified = 0;
        long removed = 0;
        for (TapRecordEvent event : events) {
            Map<String, Object> after = after(event);
            if (event instanceof TapDeleteRecordEvent delete) {
                if (!key.isEmpty() && byKey.remove(keyOf(delete.getBefore(), key)) != null) {
                    removed++;
                }
                continue;
            }
            if (key.isEmpty()) {
                appended.add(after);
                inserted++;
            } else if (byKey.put(keyOf(after, key), after) == null) {
                inserted++;
            } else {
                modified++;
            }
        }

        List<Map<String, Object>> rows = key.isEmpty() ? appended : new ArrayList<>(byKey.values());
        write(file, header(target, rows), rows);
        return new WriteListResult<TapRecordEvent>()
                .insertedCount(inserted)
                .modifiedCount(modified)
                .removedCount(removed);
    }

    /**
     * The target's key columns, or none when it declares no structure at all.
     *
     * <p>The guard is not defensive style: when no source model was discovered, the product hands the sink a
     * table carrying an id and nothing else, meaning "infer the structure and the keying yourself" - and on
     * that table {@code primaryKeys()} throws, because it reads a field map that was never built. Asking the
     * natural question the natural way is what crashes, so the question has to be asked around.
     */
    private static List<String> primaryKeyOf(TapTable target) {
        return target.getNameFieldMap() == null ? List.of() : new ArrayList<>(target.primaryKeys());
    }

    private static Map<String, Object> after(TapRecordEvent event) {
        if (event instanceof TapInsertRecordEvent insert) {
            return insert.getAfter();
        }
        if (event instanceof TapUpdateRecordEvent update) {
            return update.getAfter();
        }
        return Map.of();
    }

    /**
     * The columns the target file carries: the resolved target model's fields when the product supplied
     * one, and otherwise whatever the rows themselves hold. The model is the better answer because it
     * names the columns in the order the source declared them, and a file has to fix an order.
     */
    private static List<String> header(TapTable target, List<Map<String, Object>> rows) {
        if (target.getNameFieldMap() != null && !target.getNameFieldMap().isEmpty()) {
            return new ArrayList<>(target.getNameFieldMap().keySet());
        }
        List<String> columns = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            for (String column : row.keySet()) {
                if (!columns.contains(column)) {
                    columns.add(column);
                }
            }
        }
        return columns;
    }

    private static String keyOf(Map<String, Object> row, List<String> key) {
        StringBuilder value = new StringBuilder();
        for (String column : key) {
            value.append(String.valueOf(row.get(column))).append('\0');
        }
        return value.toString();
    }

    // ---- the read face ---------------------------------------------------------------------------

    /**
     * What one table measures. Counted and measured off the file rather than estimated: a directory is
     * small enough that an exact answer costs nothing, and a specification asserting a total wants the
     * number the collection actually holds.
     */
    private static TableInfo tableInfo(TapConnectionContext context, String table) {
        Path file = file(context, table);
        long rows = rows(file).size();
        long bytes = sizeOf(file);
        return TableInfo.create()
                .numOfRows(rows)
                .storageSize(bytes)
                .avgObjSize(rows == 0 ? 0L : bytes / rows);
    }

    private static long sizeOf(Path file) {
        if (!Files.isRegularFile(file)) {
            return 0L;
        }
        try {
            return Files.size(file);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot measure the table at " + file, e);
        }
    }

    /**
     * One read of the browse face, answered whole.
     *
     * <p>A failure travels back inside the result rather than out of the call, because that is the
     * contract the caller reads: it inspects the result for an error and raises it afterwards. Throwing
     * from here would arrive as a broken connector instead of as a query that failed.
     */
    private static ExecuteResult<List<Map<String, Object>>> execute(
            TapConnectorContext context, TapExecuteCommand command) {
        ExecuteResult<List<Map<String, Object>>> result = new ExecuteResult<>();
        try {
            return result.result(query(context, command));
        } catch (Throwable failure) {
            return result.error(failure);
        }
    }

    /** The rows one read asks for: the named table, ordered if an order was asked for, cut to the bound. */
    private static List<Map<String, Object>> query(TapConnectorContext context, TapExecuteCommand command) {
        if (!QUERY_COMMAND.equals(command.getCommand())) {
            // The face pins the command name rather than forwarding one, so anything else reaching here
            // is the read having been widened into a general dispatch.
            throw new IllegalArgumentException(
                    "this connector answers '" + QUERY_COMMAND + "', not '" + command.getCommand() + "'");
        }
        Map<String, Object> params = command.getParams() == null ? Map.of() : command.getParams();
        requireEveryRow(params.get("filter"));
        List<Map<String, Object>> rows = new ArrayList<>(rows(file(context, table(params))));
        order(rows, params.get("sort"));
        int bound = bound(params);
        return rows.size() <= bound ? rows : new ArrayList<>(rows.subList(0, bound));
    }

    private static String table(Map<String, Object> params) {
        Object collection = params.get("collection");
        if (collection == null || String.valueOf(collection).isBlank()) {
            throw new IllegalArgumentException("a read names the collection it reads");
        }
        return String.valueOf(collection);
    }

    /**
     * Accepts the filter meaning "every row" and refuses every other one, by design.
     *
     * <p>This connector could evaluate the operators over its own rows, and deliberately does not. Every
     * column here is text, so its comparisons would order and match differently from a real backend's
     * typed ones — and a specification that passed on those semantics would be asserting this class's
     * reading of a filter rather than the product's. Refusing keeps that impossible: a specification
     * needing a filter witnessed end to end fails here, loudly, instead of passing on a second truth.
     */
    private static void requireEveryRow(Object filter) {
        if (filter instanceof Map<?, ?> document && document.isEmpty()) {
            return;
        }
        throw new UnsupportedOperationException(
                "this connector answers unfiltered reads only, and was sent the filter " + filter
                        + "; evaluating one here would assert this connector's semantics, not the product's");
    }

    /**
     * Puts the rows in the asked-for order, or leaves them in the one the file has.
     *
     * <p>Lexicographic, because every column this connector reports is text and there is nothing else it
     * could honestly sort on. A specification ordering by a field must therefore seed values that order
     * the same way as strings.
     */
    private static void order(List<Map<String, Object>> rows, Object sort) {
        if (sort == null) {
            return;
        }
        if (!(sort instanceof Map<?, ?> ordering) || ordering.size() != 1) {
            throw new IllegalArgumentException("a read orders by exactly one field, and was sent " + sort);
        }
        Map.Entry<?, ?> only = ordering.entrySet().iterator().next();
        String field = String.valueOf(only.getKey());
        Comparator<Map<String, Object>> byField = Comparator.comparing(row -> text(row.get(field)));
        boolean descending = only.getValue() instanceof Number direction && direction.intValue() < 0;
        rows.sort(descending ? byField.reversed() : byField);
    }

    /** The number of rows the caller will take. Absent is not a reading: an unbounded read is not asked for. */
    private static int bound(Map<String, Object> params) {
        if (!(params.get("limit") instanceof Number limit)) {
            throw new IllegalArgumentException(
                    "a read carries the number of rows it wants, and was sent " + params.get("limit"));
        }
        return Math.max(0, limit.intValue());
    }

    private static String text(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    // ---- the format ------------------------------------------------------------------------------

    private static List<String> header(Path file) {
        List<String> lines = read(file);
        return lines.isEmpty() ? List.of() : columns(lines.get(0));
    }

    /** Every row of the file, each a column-to-value map in header order. Absent file, no rows. */
    private static List<Map<String, Object>> rows(Path file) {
        List<String> lines = read(file);
        if (lines.isEmpty()) {
            return List.of();
        }
        List<String> header = columns(lines.get(0));
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            if (line.isBlank()) {
                continue;
            }
            List<String> values = columns(line);
            Map<String, Object> row = new LinkedHashMap<>();
            for (int column = 0; column < header.size(); column++) {
                row.put(header.get(column), column < values.size() ? values.get(column) : null);
            }
            rows.add(row);
        }
        return rows;
    }

    private static List<String> columns(String line) {
        List<String> columns = new ArrayList<>();
        for (String column : line.split(",", -1)) {
            columns.add(column.trim());
        }
        return columns;
    }

    private static void write(Path file, List<String> header, List<Map<String, Object>> rows) {
        StringBuilder text = new StringBuilder(String.join(",", header)).append('\n');
        for (Map<String, Object> row : rows) {
            List<String> values = new ArrayList<>();
            for (String column : header) {
                Object value = row.get(column);
                values.add(value == null ? "" : String.valueOf(value));
            }
            text.append(String.join(",", values)).append('\n');
        }
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, text.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the table at " + file, e);
        }
    }

    private static List<String> read(Path file) {
        if (!Files.isRegularFile(file)) {
            return List.of();
        }
        try {
            return Files.readAllLines(file);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the table at " + file, e);
        }
    }

    private static List<String> tableNames(TapConnectionContext context) {
        Path directory = directory(context);
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : (Iterable<Path>) files.sorted()::iterator) {
                String name = file.getFileName().toString();
                if (name.endsWith(SUFFIX)) {
                    names.add(name.substring(0, name.length() - SUFFIX.length()));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot list the tables at " + directory, e);
        }
        return names;
    }

    private static Path file(TapConnectionContext context, String table) {
        return directory(context).resolve(table + SUFFIX);
    }

    /** Whether this connection is configured to reject every write. Off unless a target opts in. */
    private static boolean writesRejected(TapConnectionContext context) {
        Object flag = context.getConnectionConfig() == null
                ? null
                : context.getConnectionConfig().getObject(FAIL_WRITES);
        return flag != null && Boolean.parseBoolean(String.valueOf(flag));
    }

    /** Whether this connection is configured to fail its cdc stream. Off unless a source opts in. */
    private static boolean cdcRejected(TapConnectionContext context) {
        Object flag = context.getConnectionConfig() == null
                ? null
                : context.getConnectionConfig().getObject(FAIL_CDC);
        return flag != null && Boolean.parseBoolean(String.valueOf(flag));
    }

    private static Path directory(TapConnectionContext context) {
        Object uri = context.getConnectionConfig() == null ? null : context.getConnectionConfig().getObject(URI);
        if (uri == null || String.valueOf(uri).isBlank()) {
            throw new IllegalArgumentException("this connector reads a directory named by the '" + URI + "' setting");
        }
        return Path.of(String.valueOf(uri));
    }
}
