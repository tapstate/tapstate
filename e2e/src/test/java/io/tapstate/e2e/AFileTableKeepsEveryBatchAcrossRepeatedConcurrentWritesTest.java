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
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** A closing writer must not forget the lock held by the next writer of the same table. */
class AFileTableKeepsEveryBatchAcrossRepeatedConcurrentWritesTest {

    private static final String TABLE = "orders";

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void everyBatchSurvivesRepeatedConcurrentWrites(@TempDir Path directory) throws Exception {
        // Only the child opens JDK internals, to schedule real channel closes without changing the registry.
        Path output = directory.resolve("writer-process.log");
        Process child = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-cp", System.getProperty("java.class.path"),
                AFileTableKeepsEveryBatchAcrossRepeatedConcurrentWritesTest.class.getName(),
                directory.toString())
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        try {
            assertThat(child.waitFor(30, TimeUnit.SECONDS)).as("both writes finish").isTrue();
            assertThat(child.exitValue()).as("writer process: %s", Files.readString(output)).isZero();
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }

        assertThat(new FileEndpoints().count(EndpointAddress.uri(directory.toString()), TABLE))
                .as("every row of both completed four-row batches is in the table")
                .isEqualTo(8L);
    }

    /** Runs in one JVM, like the original concurrent-writer case. */
    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        Path lock = directory.resolve(CsvConnector.lockNameOf(TABLE + ".csv"));
        TapConnectorContext connection =
                new TapConnectorContext(null, new DataMap().kv("uri", directory.toString()), null, null);
        TapTable table = new TapTable(TABLE)
                .add(new TapField(SeedRows.ID, "int").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField(SeedRows.SEQ, "int"));
        CountDownLatch firstRead = new CountDownLatch(1);
        CountDownLatch publishFirst = new CountDownLatch(1);
        try (FileChannel older = FileChannel.open(lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileChannel newer = FileChannel.open(lock, StandardOpenOption.WRITE);
                var writers = Executors.newFixedThreadPool(2)) {
            // These are the released channels that per-batch writers close after dropping their locks.
            older.tryLock().release();
            newer.tryLock().release();
            Object oldList = lockListOf(older);
            FutureTask<Void> closeOlder = new FutureTask<>(() -> {
                older.close();
                return null;
            });
            Thread closer = new Thread(closeOlder, "closing-earlier-writer");
            Future<?> first;
            try {
                synchronized (oldList) {
                    closer.start();
                    awaitCloseWaitingOn(closer, oldList);
                    // Removes the empty old list while the earlier close still refers to that list.
                    newer.close();
                    first = writers.submit(() ->
                            write(connection, table, rows(0, new HeldValue(firstRead, publishFirst))));
                    assertThat(firstRead.await(10, TimeUnit.SECONDS)).as("first apply has read the table").isTrue();
                }
                // The earlier close now removes the newer lock's registry entry using its old empty list.
                closeOlder.get(10, TimeUnit.SECONDS);
                var second = writers.submit(() -> write(connection, table, rows(4, 4)));
                try {
                    second.get(1, TimeUnit.SECONDS);
                } catch (TimeoutException stillSerialized) {
                    // Correct exclusion lets the second apply proceed only after the first publishes.
                } finally {
                    publishFirst.countDown();
                }
                first.get(10, TimeUnit.SECONDS);
                second.get(10, TimeUnit.SECONDS);
            } finally {
                publishFirst.countDown();
            }
        }
    }

    /** Reads the registry's list without adding, removing or replacing any entry. */
    private static Object lockListOf(FileChannel channel) throws Exception {
        Field tableField = channel.getClass().getDeclaredField("fileLockTable");
        tableField.setAccessible(true);
        Object table = tableField.get(channel);
        Field keyField = table.getClass().getDeclaredField("fileKey");
        keyField.setAccessible(true);
        Field mapField = table.getClass().getDeclaredField("lockMap");
        mapField.setAccessible(true);
        return ((Map<?, ?>) mapField.get(null)).get(keyField.get(table));
    }

    private static void awaitCloseWaitingOn(Thread closer, Object list) {
        var threads = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            var state = threads.getThreadInfo(closer.threadId());
            if (state != null && state.getThreadState() == Thread.State.BLOCKED
                    && state.getLockInfo().getIdentityHashCode() == System.identityHashCode(list)
                    && state.getLockOwnerId() == Thread.currentThread().threadId()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        throw new AssertionError("the earlier close did not reach the old lock list");
    }

    private static void write(TapConnectorContext connection, TapTable table, List<TapRecordEvent> rows) {
        ConnectorFunctions functions = new ConnectorFunctions();
        new CsvConnector().registerCapabilities(functions, new TapCodecsRegistry());
        WriteRecordFunction sink = functions.getWriteRecordFunction();
        try {
            sink.writeRecord(connection, rows, table, result -> { });
        } catch (Throwable failure) {
            throw new AssertionError("a writer's batch was refused", failure);
        }
    }

    private static List<TapRecordEvent> rows(int first, Object firstValue) {
        List<TapRecordEvent> rows = new ArrayList<>();
        for (int id = first; id < first + 4; id++) {
            rows.add(TapInsertRecordEvent.create().table(TABLE).after(
                    Map.of(SeedRows.ID, id, SeedRows.SEQ, id == first ? firstValue : id)));
        }
        return rows;
    }

    /** Stops the first apply after its read, before it publishes, without changing the row's value. */
    private record HeldValue(CountDownLatch read, CountDownLatch publish) {
        @Override
        public String toString() {
            read.countDown();
            try {
                if (!publish.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("the first writer was not released");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
            return "0";
        }
    }
}
