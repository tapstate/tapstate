package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.sink.WriteResult;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PdkSinkWriterCancellationTest {

    @Test
    void blockingWritesLeaveSharedAsyncWorkAvailable(@TempDir Path directory) throws Throwable {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (PdkSinkWriter writer = writer(directory, (context, events, table, result) -> {
            entered.countDown();
            release.await();
            result.accept(new WriteListResult<>(1L, 0L, 0L));
        }, null)) {
            try {
                // Include calls queued behind the same writer, which otherwise occupy shared workers too.
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeInterruptsActiveWritesAndSettlesQueuedBatchesDespiteCleanupFailure(
            boolean cleanupFails, @TempDir Path directory) throws Throwable {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger cleanups = new AtomicInteger();
        IllegalStateException cleanupFailure = new IllegalStateException("fixture cleanup failed");
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try (PdkSinkWriter writer = writer(directory, (context, events, table, result) -> {
            calls.incrementAndGet();
            entered.countDown();
            try {
                // A blocked client may need its resource released as well as an interrupt.
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
                    throw new InterruptedException("the target call was cancelled");
                }
            } finally {
                exited.countDown();
            }
            result.accept(new WriteListResult<>(1L, 0L, 0L));
        }, () -> {
            cleanups.incrementAndGet();
            await(interrupted, "the write was not interrupted before connector cleanup");
            release.countDown();
            if (cleanupFails) {
                throw cleanupFailure;
            }
        })) {
            try {
                CompletableFuture<WriteResult> active = writer.write(batch(1)).toCompletableFuture();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                CompletableFuture<WriteResult> queued = writer.write(batch(2)).toCompletableFuture();
                active.whenComplete((accepted, failure) ->
                        await(exited, "the active callback did not exit before its completion handler"));
                var closing = closer.submit(writer::close);
                if (cleanupFails) {
                    assertThatThrownBy(() -> closing.get(2, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class).hasCause(cleanupFailure);
                } else {
                    closing.get(2, TimeUnit.SECONDS);
                }
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

    @Test void closeDoesNotReportRetirementWhileAnInterruptedTargetCallStillRuns(@TempDir Path directory) throws Throwable {
        var entered=new CountDownLatch(1);var interrupted=new CountDownLatch(1);var release=new CountDownLatch(1);var exited=new CountDownLatch(1);
        var closer=Executors.newFixedThreadPool(2);
        PdkSinkWriter writer=writer(directory,(context,events,table,result) -> {
            entered.countDown();
            try {
                while(release.getCount()!=0)try {release.await();}catch(InterruptedException stopping){interrupted.countDown();}
                result.accept(new WriteListResult<>(1L,0L,0L));
            } finally {exited.countDown();}
        },() -> {});
        try {
            writer.write(batch(1));assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();
            var closing=closer.submit(writer::close);assertThat(interrupted.await(5,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> closing.get(200,TimeUnit.MILLISECONDS)).isInstanceOf(java.util.concurrent.TimeoutException.class);
            var retry=closer.submit(writer::close);
            assertThatThrownBy(() -> retry.get(200,TimeUnit.MILLISECONDS)).isInstanceOf(java.util.concurrent.TimeoutException.class);
            release.countDown();closing.get(5,TimeUnit.SECONDS);retry.get(5,TimeUnit.SECONDS);assertThat(exited.getCount()).isZero();
        } finally {release.countDown();writer.close();closer.shutdownNow();}
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void aCompletionCallbackCanCloseItsWriterWithoutWaitingForItself(boolean concurrentClose,@TempDir Path directory) throws Throwable {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var interrupted=new CountDownLatch(1);
        var closer=Executors.newSingleThreadExecutor(task -> {var thread=new Thread(task,"fixture-close");thread.setDaemon(true);return thread;});
        var writer=writer(directory,(context,events,table,result) -> {
            entered.countDown();
            while(release.getCount()!=0)try {release.await();}catch(InterruptedException stopping){interrupted.countDown();}
            result.accept(new WriteListResult<>(1L,0L,0L));
        },() -> {});
        try {
            var stage=writer.write(batch(1));assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();
            var closed=stage.handle((result,failure) -> {writer.close();return null;}).toCompletableFuture();
            var external=concurrentClose?closer.submit(writer::close):null;
            if(concurrentClose)assertThat(interrupted.await(5,TimeUnit.SECONDS)).isTrue();
            release.countDown();closed.get(2,TimeUnit.SECONDS);if(external!=null)external.get(2,TimeUnit.SECONDS);writer.close();
        } finally {release.countDown();closer.shutdownNow();}
    }

    private static void await(CountDownLatch signal, String failure) {
        try {
            if (!signal.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError(failure);
            }
        } catch (InterruptedException cancelled) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure, cancelled);
        }
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
            try {
                cleanup.run();
            } finally {
                connector.stopQuietly();
                connector.close();
            }
        });
    }

    private static List<Envelope> batch(int id) {
        return List.of(Envelope.insert(id, "t1", Map.of("id", id), null));
    }
}
