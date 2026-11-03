package io.tapstate.app;

import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeValidationScanTimingTest {
    @ParameterizedTest @ValueSource(booleans = {false, true})
    void onlyTheEnteredWorkerIsTimedOnceIncludingItsFailure(boolean fail) throws Exception {
        try (var executor = Executors.newSingleThreadExecutor()) {
            var queue = new CountDownLatch(1); var occupied = new CountDownLatch(1);
            executor.submit(() -> { occupied.countDown(); await(queue); });
            assertThat(occupied.await(5, TimeUnit.SECONDS)).isTrue();
            var expected = new IllegalStateException("original-worker");
            var done = new CountDownLatch(1); var entered = new AtomicBoolean(); var failure = new AtomicReference<Throwable>();
            var work = mock(NativeValidationFullJob.ScanWork.class);
            doAnswer(invocation -> {
                if (entered.compareAndSet(false, true)) {
                    Consumer<Runnable> measured = invocation.getArgument(0);
                    executor.submit(() -> {
                        try { measured.accept(() -> { if (fail) throw expected; }); }
                        catch (Throwable failed) { failure.set(failed); }
                        finally { done.countDown(); }
                    });
                }
                return null;
            }).when(work).launch(any());
            when(work.finished()).thenAnswer(ignored -> done.getCount() == 0);
            doAnswer(ignored -> { if (failure.get() instanceof Error fatal) throw fatal; if (failure.get() instanceof RuntimeException failed) throw failed; return null; }).when(work).throwFailure();
            var processor = new NativeValidationFullJob.FullScanProcessor(work);
            processor.init(new TestOutbox(1), new TestProcessorContext());
            try {
                for (int poll = 0; poll < 32; poll++) assertThat(processor.complete()).isFalse();
                var timer = processor.timing();
                assertThat(timer.value().count()).isZero();
                queue.countDown(); assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
                if (fail) assertThat(catchThrowable(processor::complete)).isSameAs(expected);
                else for (int poll = 0; poll < 32; poll++) assertThat(processor.complete()).isTrue();
                assertThat(timer.value().count()).isEqualTo(1);
            } finally { queue.countDown(); processor.close(); }
            verify(work).cancel();
        }
    }

    @Test void rejectedSubmissionAndCloseBeforeInitInventNoWorkUnit() {
        {
            var work = mock(NativeValidationFullJob.ScanWork.class);
            var rejected = new RejectedExecutionException("not-entered");
            doThrow(rejected).when(work).launch(any());
            var processor = new NativeValidationFullJob.FullScanProcessor(work);
            assertThat(catchThrowable(processor::complete)).isSameAs(rejected);
            processor.close(); assertThat(processor.timing().value().count()).isZero(); verify(work).cancel();
        }
    }

    @Test void anEnteredCancelledWorkerEndsItsUnitOnlyWhenItsBodyExits() throws Exception {
        var release = new CountDownLatch(1); var started = new CountDownLatch(1); var exited = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var work = mock(NativeValidationFullJob.ScanWork.class); var once = new AtomicBoolean(); var cancelled = new AtomicBoolean(); var failed = new AtomicReference<Throwable>();
            doAnswer(ignored -> { cancelled.set(true); return null; }).when(work).cancel();
            doAnswer(invocation -> { if (once.compareAndSet(false, true)) {
                Consumer<Runnable> measured = invocation.getArgument(0);
                executor.submit(() -> { try { measured.accept(() -> { started.countDown(); await(release); assertThat(cancelled).isTrue(); }); } catch (Throwable failure) { failed.set(failure); } finally { exited.countDown(); } });
            } return null; }).when(work).launch(any());
            var processor = new NativeValidationFullJob.FullScanProcessor(work);
            processor.init(new TestOutbox(1), new TestProcessorContext()); processor.complete();
            var timer = processor.timing();
            try {
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue(); processor.close();
                assertThat(timer.value().count()).isZero(); assertThat(exited.getCount()).isEqualTo(1);
                release.countDown(); assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(timer.value().count()).isEqualTo(1); assertThat(failed.get()).isNull();
            } finally { release.countDown(); }
        }
    }

    @Test void timingPublicationFailureKeepsTheOriginalFatalFailureAndRunsTheCallerFinalizer() throws Exception {
        for (boolean businessFailure : java.util.List.of(false, true)) {
            var timer = mock(io.tapstate.runtime.engine.StageTimer.class);
            var publication = new IllegalStateException("metric-publication");
            var original = new AssertionError("original-fatal"); var finalized = new AtomicBoolean();
            when(timer.begin()).thenReturn(7L); doThrow(publication).when(timer).end(7L);
            try (var timers = mockStatic(io.tapstate.runtime.engine.StageTimer.class)) {
                timers.when(() -> io.tapstate.runtime.engine.StageTimer.of(eq(io.tapstate.core.lifecycle.Stage.VALIDATION),
                        any(com.hazelcast.jet.core.Processor.Context.class), eq(io.tapstate.runtime.engine.StageTimer.Publication.WORKER))).thenReturn(timer);
                var work = mock(NativeValidationFullJob.ScanWork.class);
                doAnswer(invocation -> {
                    Consumer<Runnable> measured = invocation.getArgument(0);
                    try { measured.accept(() -> {if (businessFailure) throw original;}); }
                    finally { finalized.set(true); }
                    return null;
                }).when(work).launch(any());
                var processor = new NativeValidationFullJob.FullScanProcessor(work);
                processor.init(new TestOutbox(1), new TestProcessorContext());
                assertThat(catchThrowable(processor::complete)).isSameAs(businessFailure ? original : publication);
                assertThat(finalized).isTrue(); verify(timer).end(7L);
                if (businessFailure) assertThat(original.getSuppressed()).contains(publication);
                processor.close();
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("fixture barrier timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
