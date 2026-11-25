package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapstate.adapters.pdk.ConnectorClassLoader;
import io.tapstate.e2e.connector.CsvConnector;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** A stalled batch must not hide another writer's contention deadline. */
class CsvWriterLockTimeoutTest {

    @Test
    void nativeContentionDoesNotRestartAnExpiredDeadline(@TempDir Path directory) throws Exception {
        var acquire = CsvConnector.class.getDeclaredMethod("acquire", FileChannel.class, Path.class, long.class);
        acquire.setAccessible(true);
        Path table = directory.resolve("orders.csv");
        Path lock = directory.resolve(CsvConnector.lockNameOf(table.getFileName().toString()));
        try (var writer = Executors.newSingleThreadExecutor()) {
            try (FileChannel owner = FileChannel.open(lock, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    FileLock held = owner.lock();
                    FileChannel waiting = FileChannel.open(lock, StandardOpenOption.WRITE)) {
                long deadline = System.nanoTime() - 1;
                var attempt = writer.submit(() -> {
                    try {
                        ((FileLock) acquire.invoke(null, waiting, table, deadline)).release();
                        return null;
                    } catch (InvocationTargetException failure) {
                        return failure.getCause();
                    }
                });
                assertThat(attempt.get(1, TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class)
                        .hasMessage("the table at " + table + " was held by another write for over 60s");
            }
        }
    }

    @Test
    @Timeout(value = 80, unit = TimeUnit.SECONDS)
    void anotherLoaderTimesOutWhileTheFirstWriterStillHoldsTheTable(@TempDir Path directory) throws Exception {
        Path data = Files.createDirectory(directory.resolve("data"));
        Path alias = Files.createSymbolicLink(directory.resolve("alias"), data);
        Path jar = E2eConnectorJar.buildInto(directory);
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var firstLoader = ConnectorClassLoader.open(List.of(jar));
                var secondLoader = ConnectorClassLoader.open(List.of(jar));
                var writers = Executors.newFixedThreadPool(2)) {
            WriteRecordFunction firstSink = sink(firstLoader);
            WriteRecordFunction secondSink = sink(secondLoader);
            var first = writers.submit(() -> write(firstSink, data, 0, new HeldValue(held, release)));
            try {
                assertThat(held.await(10, TimeUnit.SECONDS)).as("first writer holds the table").isTrue();
                CountDownLatch started = new CountDownLatch(1);
                var second = writers.submit(() -> {
                    started.countDown();
                    return write(secondSink, alias, 1, 1);
                });
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                long startedAt = System.nanoTime();
                Throwable failure = second.get(62, TimeUnit.SECONDS);
                assertThat(failure).isInstanceOf(IllegalStateException.class)
                        .hasMessage("the table at " + alias.resolve("orders.csv")
                                + " was held by another write for over 60s");
                assertThat(System.nanoTime() - startedAt).isGreaterThan(TimeUnit.SECONDS.toNanos(59));
                assertThat(first.isDone()).as("the first writer is still held when contention is reported").isFalse();
            } finally {
                release.countDown();
            }
            assertThat(first.get(10, TimeUnit.SECONDS)).isNull();
            assertThat(write(secondSink, alias, 2, 2)).as("the exclusion is released after publication").isNull();
        }
        assertThat(new FileEndpoints().count(EndpointAddress.uri(data.toString()), "orders")).isEqualTo(2L);
    }

    private static WriteRecordFunction sink(ConnectorClassLoader loader) throws Exception {
        var connector = loader.loadConnectorClass(CsvConnector.class.getName()).getConstructor().newInstance();
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        return functions.getWriteRecordFunction();
    }

    private static Throwable write(WriteRecordFunction sink, Path directory, int id, Object value) {
        TapConnectorContext context =
                new TapConnectorContext(null, new DataMap().kv("uri", directory.toString()), null, null);
        TapTable table = new TapTable("orders")
                .add(new TapField("id", "int").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("seq", "int"));
        try {
            sink.writeRecord(context, List.of(TapInsertRecordEvent.create().table("orders")
                    .after(Map.of("id", id, "seq", value))), table, result -> { });
            return null;
        } catch (Throwable failure) {
            return failure;
        }
    }

    private record HeldValue(CountDownLatch held, CountDownLatch release) {
        @Override
        public String toString() {
            held.countDown();
            try {
                if (!release.await(75, TimeUnit.SECONDS)) {
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
