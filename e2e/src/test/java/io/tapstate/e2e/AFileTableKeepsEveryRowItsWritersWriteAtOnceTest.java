package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapstate.e2e.connector.CsvConnector;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A table this harness hands out keeps every row written to it by several writers at once, the way a database
 * target does.
 *
 * <p>A sink runs several writers by default, each with a connector of its own, spread over every member: a
 * table's rows reach it from all of them, while the rows before them are still being written. A write here reads
 * the table, applies its batch and puts the whole table back, so two writes of one table at once each put back a
 * table missing the other's rows - and a run that carried every row reads as having lost some, which is the one
 * reading the published examples exist to catch.
 */
class AFileTableKeepsEveryRowItsWritersWriteAtOnceTest {

    private static final String TABLE = "orders";
    private static final int WRITERS = 8;
    private static final int BATCHES_PER_WRITER = 25;
    private static final int ROWS_PER_BATCH = 4;

    @Test
    void everyRowOfEveryWriterIsInTheTable(@TempDir Path directory) throws Exception {
        TapConnectorContext connection =
                new TapConnectorContext(null, new DataMap().kv("uri", directory.toString()), null, null);
        TapTable table = new TapTable(TABLE)
                .add(new TapField(SeedRows.ID, "int").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField(SeedRows.SEQ, "int"));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
        try {
            List<Future<?>> writers = new ArrayList<>();
            for (int writer = 0; writer < WRITERS; writer++) {
                // A connector of its own, as each writer of a sink opens one.
                WriteRecordFunction sink = writePathOfANewConnector();
                int first = writer * BATCHES_PER_WRITER * ROWS_PER_BATCH;
                writers.add(pool.submit(() -> {
                    start.await();
                    for (int batch = 0; batch < BATCHES_PER_WRITER; batch++) {
                        try {
                            sink.writeRecord(connection, rows(first + batch * ROWS_PER_BATCH), table, result -> { });
                        } catch (Throwable failed) {
                            throw new AssertionError("a writer's batch was refused", failed);
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> writer : writers) {
                writer.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(new FileEndpoints().count(EndpointAddress.uri(directory.toString()), TABLE))
                .as("every writer's rows are in the table, whichever wrote last")
                .isEqualTo((long) WRITERS * BATCHES_PER_WRITER * ROWS_PER_BATCH);
    }

    private static WriteRecordFunction writePathOfANewConnector() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new CsvConnector().registerCapabilities(functions, new TapCodecsRegistry());
        return functions.getWriteRecordFunction();
    }

    private static List<TapRecordEvent> rows(int firstId) {
        List<TapRecordEvent> events = new ArrayList<>();
        for (int id = firstId; id < firstId + ROWS_PER_BATCH; id++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put(SeedRows.ID, id);
            row.put(SeedRows.SEQ, id);
            events.add(TapInsertRecordEvent.create().table(TABLE).after(row));
        }
        return events;
    }
}
