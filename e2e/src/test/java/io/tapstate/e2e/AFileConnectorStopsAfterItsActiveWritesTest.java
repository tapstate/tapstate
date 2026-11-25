package io.tapstate.e2e;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.e2e.connector.CsvConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AFileConnectorStopsAfterItsActiveWritesTest {
    @ParameterizedTest
    @ValueSource(strings = {"before", "after", "write-release"})
    void stoppingReleasesAHeldCallbackWithoutAnInterrupt(String phase, @TempDir Path directory) throws Exception {
        CsvConnector connector = new CsvConnector();
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        Path signals = Files.createDirectory(directory.resolve("signals"));
        Holds holds = new Holds(signals);
        if (phase.equals("before")) holds.before("orders", 1);
        if (phase.equals("after")) holds.after("orders", 1);
        String setting = phase.equals("write-release") ? "hold_writes" : "hold";
        TapConnectorContext context = new TapConnectorContext(null,
                new DataMap().kv("uri", directory.toString()).kv(setting, signals.toString()), null, null);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var writing = pool.submit(() -> write(functions, context, "orders"));
            Await.until("the callback to reach its " + phase + " hold", Duration.ofSeconds(5), () ->
                    switch (phase) {
                        case "before" -> holds.holderBefore("orders", 1).isPresent();
                        case "after" -> holds.holderAfter("orders", 1).isPresent();
                        default -> Files.exists(signals.resolve("waiting"));
                    }, () -> "hold=" + phase);
            pool.submit(() -> connector.stop(context)).get(2, TimeUnit.SECONDS);
            if (phase.equals("write-release")) {
                assertThatThrownBy(() -> writing.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("stopped");
            } else {
                writing.get(5, TimeUnit.SECONDS);
            }
        } finally {
            holds.releaseBefore("orders", 1);
            holds.releaseAfter("orders", 1);
            Files.writeString(signals.resolve("release"), "");
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void stoppingWaitsForAnActiveWriteAndRefusesLaterWrites(@TempDir Path directory) throws Exception {
        CsvConnector connector = new CsvConnector();
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        TapConnectorContext context = context(directory);
        AtomicReference<Thread> writerThread = new AtomicReference<>();
        CountDownLatch stoppingEntered = new CountDownLatch(1);
        var pool = Executors.newFixedThreadPool(2);
        Path lockPath = directory.resolve(CsvConnector.lockNameOf("orders.csv"));
        try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock held = channel.lock()) {
            var writing = pool.submit(() -> {
                writerThread.set(Thread.currentThread());
                write(functions, context, "orders");
            });
            Await.until("the owned write to reach the held file lock", Duration.ofSeconds(5),
                    () -> waitsForLock(writerThread.get()), () -> "writer=" + writerThread.get());
            var stopping = pool.submit(() -> {
                stoppingEntered.countDown();
                connector.stop(context);
            });
            try {
                assertThat(stoppingEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> stopping.get(200, TimeUnit.MILLISECONDS))
                        .as("stop must wait for the callback that still owns the target")
                        .isInstanceOf(TimeoutException.class);
            } finally {
                held.release();
                writing.get(5, TimeUnit.SECONDS);
                stopping.get(5, TimeUnit.SECONDS);
            }
            String settled = Files.readString(directory.resolve("orders.csv"));
            assertThatThrownBy(() -> write(functions, context, "after_stop"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("stopped");
            assertThat(Files.exists(directory.resolve("after_stop.csv"))).isFalse();
            assertThat(Files.readString(directory.resolve("orders.csv"))).isEqualTo(settled);
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void anotherTargetCanWriteWhileOneTargetWaitsForItsLock(@TempDir Path directory) throws Exception {
        CsvConnector connector = new CsvConnector();
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        TapConnectorContext context = context(directory);
        AtomicReference<Thread> writerThread = new AtomicReference<>();
        var pool = Executors.newFixedThreadPool(2);
        Path lockPath = directory.resolve(CsvConnector.lockNameOf("orders.csv"));
        try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock held = channel.lock()) {
            var writing = pool.submit(() -> {
                writerThread.set(Thread.currentThread());
                write(functions, context, "orders");
            });
            try {
                Await.until("the owned write to reach the held file lock", Duration.ofSeconds(5),
                        () -> waitsForLock(writerThread.get()), () -> "writer=" + writerThread.get());
                pool.submit(() -> write(functions, context, "items")).get(5, TimeUnit.SECONDS);
                assertThat(new FileEndpoints().count(EndpointAddress.uri(directory.toString()), "items")).isEqualTo(1);
            } finally {
                held.release();
                writing.get(5, TimeUnit.SECONDS);
            }
            connector.stop(context);
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static TapConnectorContext context(Path directory) {
        return new TapConnectorContext(null, new DataMap().kv("uri", directory.toString()), null, null);
    }

    private static boolean waitsForLock(Thread thread) {
        return thread != null && Arrays.stream(thread.getStackTrace()).anyMatch(frame ->
                frame.getClassName().equals(CsvConnector.class.getName()) && frame.getMethodName().equals("acquire"));
    }

    private static void write(ConnectorFunctions functions, TapConnectorContext context, String table) {
        TapTable model = new TapTable(table).add(new TapField("id", "int").isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("seq", "int"));
        try {
            functions.getWriteRecordFunction().writeRecord(context,
                    List.of(TapInsertRecordEvent.create().table(table).after(Map.of("id", 1, "seq", 1))), model, result -> { });
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Throwable failure) {
            throw new AssertionError(failure);
        }
    }
}
