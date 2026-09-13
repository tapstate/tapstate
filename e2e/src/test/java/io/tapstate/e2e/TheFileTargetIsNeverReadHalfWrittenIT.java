package io.tapstate.e2e;

import io.tapstate.e2e.connector.CsvConnector;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A target this harness hands out is readable at every instant, not only between batches.
 *
 * <p>The sink reached a table by opening its file for writing, which is an order of operations rather
 * than an atomic step: the file was emptied when it was opened and filled again when the batch was
 * written, so for the length of the write the table was observably empty while every row it held was
 * still there. The product applies a batch by rewriting the whole target, so a run that had already
 * carried its rows kept producing that window - and a reader landing in one read a count the run never
 * wrote.
 *
 * <p>That is the gap the published-example sweep was sampling around: a run that had settled on four
 * rows could still be read as zero afterwards, and that reading is indistinguishable from the silence
 * the example exists to catch - a view that materialized nothing looks exactly like a view caught
 * mid-write. Widening the read, which is what a bounded poll does, works around the window instead of
 * removing it: the row count is the only signal that separates "materialized" from "quietly did nothing",
 * and a signal a reader has to sample around is a signal that intermittently lies. The window is closed
 * at the write instead, which is what this case holds in place, and the sweep reads its target once
 * again.
 *
 * <p>So the case here is the window itself rather than a race against it. The table is materialized
 * first, the sink is then driven to apply the same rows again and again the way a re-materializing view
 * does, and the harness's own reader - the same driver the sweep reads a settled count through - is
 * asked what the table holds while that is going on. Every answer must be the count that is in the file:
 * the reader is never entitled to see the write, only the table.
 *
 * <p>Each writer applies a bounded number of replacements while the reader checks the settled count.
 * Reverting either writer to an in-place write must make its corresponding case fail; that mutation
 * is checked separately for the connector and the harness driver.
 */
class TheFileTargetIsNeverReadHalfWrittenIT {

    /** The table the example this came from settles on, read the way its target is read. */
    private static final String TABLE = "order_state";

    /** The rows that example settles on. The number is the reading it asserts, so it is asserted here. */
    private static final long ROWS = 4;

    /**
     * How many times the sink is driven to apply the same rows. The reader reads until the last of them
     * has been applied, so it cannot finish around the sink: the sink is still working through its passes
     * while the reader reads, and a window entered once per pass is a window a reading can land in.
     */
    private static final int PASSES = 300;

    @TempDir
    private Path target;

    private final FileEndpoints files = new FileEndpoints();

    @Test
    void aReadWhileTheSinkKeepsWritingTheSameRowsStillSeesEveryRow() throws Throwable {
        WriteRecordFunction sink = theProductsWritePath();
        TapConnectorContext connection = pointedAt(target);
        TapTable table = new TapTable(TABLE)
                .add(new TapField(SeedRows.ID, "int").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField(SeedRows.SEQ, "int"));
        table.refreshPrimaryKeys();
        List<TapRecordEvent> batch = theSettledRows();

        // The table first: everything below reads a target that already holds its rows, so a reader that
        // finds it empty is never reading it before the first write.
        apply(sink, connection, batch, table);
        assertThat(count()).isEqualTo(ROWS);

        assertStableCountDuring(() -> apply(sink, connection, batch, table));
    }

    @Test
    void aReadWhileTheDriverKeepsSeedingTheSameRowsStillSeesEveryRow() throws Throwable {
        EndpointAddress address = EndpointAddress.uri(target.toString());
        List<Map<String, Object>> rows = SeedRows.generated(ROWS);
        files.seed(address, TABLE, rows);
        assertThat(count()).isEqualTo(ROWS);

        assertStableCountDuring(() -> files.seed(address, TABLE, rows));
    }

    @Test
    void theDriverCanReplaceATableWhoseFileNameFitsTheFilesystemLimit() throws Exception {
        String name = "t".repeat(251);
        Path file = target.resolve(name + ".csv");
        // Establish that the table name itself is accepted by this filesystem.
        Files.writeString(file, "id,seq\n1,1\n");
        EndpointAddress address = EndpointAddress.uri(target.toString());

        files.seed(address, name, SeedRows.generated(ROWS));

        assertThat(files.count(address, name)).isEqualTo(ROWS);
        try (var entries = Files.list(target)) {
            assertThat(entries).containsExactly(file);
        }
    }

    @Test
    void theSinkCanReplaceATableWhoseFileNameFitsTheFilesystemLimit() throws Throwable {
        String name = "t".repeat(251);
        Path file = target.resolve(name + ".csv");
        Files.writeString(file, "id,seq\n1,1\n");
        TapTable table = new TapTable(name)
                .add(new TapField(SeedRows.ID, "int").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField(SeedRows.SEQ, "int"));
        table.refreshPrimaryKeys();

        apply(theProductsWritePath(), pointedAt(target), theSettledRows(), table);

        assertThat(files.count(EndpointAddress.uri(target.toString()), name)).isEqualTo(ROWS);
        try (var entries = Files.list(target)) {
            assertThat(entries).containsExactly(file);
        }
    }

    private void assertStableCountDuring(Replacement replacement) throws Throwable {
        AtomicInteger applied = new AtomicInteger();
        AtomicReference<Throwable> writerFailed = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                for (int pass = 0; pass < PASSES; pass++) {
                    replacement.run();
                    applied.incrementAndGet();
                }
            } catch (Throwable failed) {
                writerFailed.set(failed);
            }
        }, "reapplying-the-same-rows");
        writer.start();

        int readings = 0;
        try {
            do {
                readings++;
                long rows = count();
                assertThat(rows)
                        .as("the table at %s holds %s rows and the writer is applying those same rows again; "
                                + "reading %s found %s of them, so the target is being written where it can "
                                + "be read rather than replaced", target, ROWS, readings, rows)
                        .isEqualTo(ROWS);
            } while (applied.get() < PASSES && writerFailed.get() == null);
        } finally {
            writer.join();
        }
        assertThat(writerFailed.get())
                .as("the writer rejected rows it had already written")
                .isNull();
    }

    @FunctionalInterface
    private interface Replacement {
        void run() throws Throwable;
    }

    /** The harness's own reading of a target, which is what the sweep's closing assertion reads through. */
    private long count() {
        return files.count(EndpointAddress.uri(target.toString()), TABLE);
    }

    /**
     * The write path the product's batches reach the store through: the connector's registered write
     * function, registered here the way the product asks for it rather than through a pipeline, so the
     * case is about the write and the read and nothing else.
     *
     * <p>The registry is the live one the product hands over rather than nothing at all: this connector
     * registers no codec today, so either would do, and the day it registers one a null here would fail
     * the case inside registration, where the reason is not the write path it is about.
     */
    private static WriteRecordFunction theProductsWritePath() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new CsvConnector().registerCapabilities(functions, new TapCodecsRegistry());
        return functions.getWriteRecordFunction();
    }

    /** The connection a target is addressed by: this run's directory, named the way a resource names it. */
    private static TapConnectorContext pointedAt(Path directory) {
        return new TapConnectorContext(null, new DataMap().kv("uri", directory.toString()), null, null);
    }

    /** The rows the example settles on, in the shape the connector writes them from. */
    private static List<TapRecordEvent> theSettledRows() {
        List<TapRecordEvent> events = new ArrayList<>();
        for (Map<String, Object> row : SeedRows.generated(ROWS)) {
            Map<String, Object> after = new LinkedHashMap<>(row);
            events.add(TapInsertRecordEvent.create().table(TABLE).after(after));
        }
        return events;
    }

    private static void apply(
            WriteRecordFunction sink,
            TapConnectorContext connection,
            List<TapRecordEvent> batch,
            TapTable table)
            throws Throwable {
        sink.writeRecord(connection, batch, table, result -> { });
    }
}
