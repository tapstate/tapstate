package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.sink.WriteResult;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PdkSinkWriterConcurrencyTest {

    @Test
    void blockingTargetWritesLeaveSharedAsyncWorkAvailable(@TempDir Path directory) throws Throwable {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (PdkSinkWriter writer = writer(directory, (context, events, table, result) -> {
            entered.countDown();
            release.await();
            result.accept(new WriteListResult<>(1L, 0L, 0L));
        })) {
            try {
                // Enough outstanding writes to exhaust a shared pool, including writes waiting for the
                // same connector. Engine lifecycle callbacks still need that pool while a target waits.
                for (int i = 0; i < Math.max(2, ForkJoinPool.getCommonPoolParallelism()); i++) {
                    writer.write(batch(i));
                }
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(CompletableFuture.supplyAsync(() -> "lifecycle callback")
                        .get(2, TimeUnit.SECONDS)).isEqualTo("lifecycle callback");
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void closeInterruptsTheActiveWriteAndSettlesQueuedBatches(@TempDir Path directory) throws Throwable {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger cleanups = new AtomicInteger();
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try (PdkSinkWriter writer = writer(directory, (context, events, table, result) -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
                // A client operation may require its resource to close as well as an interrupt.
                boolean cancelled = false;
                while (release.getCount() != 0) {
                    try {
                        release.await();
                    } catch (InterruptedException stopping) {
                        interrupted.countDown();
                        cancelled = true;
                    }
                }
                if (cancelled) {
                    throw new InterruptedException("the held target call was cancelled");
                }
            } finally {
                exited.countDown();
            }
            result.accept(new WriteListResult<>(1L, 0L, 0L));
        }, () -> {
            cleanups.incrementAndGet();
            try {
                if (!interrupted.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("the write was not interrupted before connector cleanup");
                }
            } catch (InterruptedException stopping) {
                Thread.currentThread().interrupt();
                throw new AssertionError("connector cleanup was interrupted", stopping);
            }
            release.countDown();
        })) {
            try {
                CompletableFuture<WriteResult> active = writer.write(batch(1)).toCompletableFuture();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                CompletableFuture<WriteResult> queued = writer.write(batch(2)).toCompletableFuture();
                active.whenComplete((accepted, failure) -> {
                    try {
                        if (!exited.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("the active callback did not exit before its completion handler");
                        }
                    } catch (InterruptedException cancelled) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("the completion handler was interrupted", cancelled);
                    }
                });
                closer.submit(writer::close).get(2, TimeUnit.SECONDS);
                assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(active).isCompletedExceptionally();
                assertThat(queued).isCancelled();
                assertThat(calls).hasValue(1);
                writer.close();
                assertThat(cleanups).hasValue(1);
            } finally {
                release.countDown();
                closer.shutdownNow();
            }
        }
    }

    private static PdkSinkWriter writer(Path directory, WriteRecordFunction write) throws Throwable {
        return writer(directory, write, null);
    }

    private static PdkSinkWriter writer(Path directory, WriteRecordFunction write, Runnable cleanup) throws Throwable {
        Path jar = Synthetic.countingSink(directory);
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.CountingSink", "2.0.8", null);
        PdkConnector connector = PdkConnector.open("demo", ref, Map.of());
        TargetTable target = new TargetTable("t1", List.of(new TargetField("id", "int", true)));
        if (cleanup == null) {
            return new PdkSinkWriter(connector, write, WriteMode.UPSERT, DdlPolicy.FAIL, target);
        }
        SinkConfig config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, target);
        return new PdkSinkWriter(connector, write, config, Map.of("t1", target), null, false, () -> {
            cleanup.run();
            connector.stopQuietly();
            connector.close();
        });
    }

    private static List<Envelope> batch(int id) {
        return List.of(Envelope.insert(id, "t1", Map.of("id", id), null));
    }
}
