package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A slow lifecycle operation cannot consume the scheduler or an unbounded number of worker slots. */
class LifecycleWorkDispatcherTest {

    @Test
    void aBlockedStartLeavesAnotherPipelineAbleToFinish() throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        try (LifecycleWorkDispatcher dispatcher = new LifecycleWorkDispatcher(2, 1)) {
            assertThat(dispatcher.offer("slow", desired("slow", PipelineState.RUNNING), () -> {
                slowEntered.countDown();
                awaitLatch(releaseSlow);
                return done();
            })).isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(slowEntered.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(dispatcher.offer("fast", desired("fast", PipelineState.RUNNING),
                    LifecycleWorkDispatcherTest::done))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(awaitResult(dispatcher, "fast").result()).isEqualTo(done());
            assertThat(releaseSlow.getCount()).isEqualTo(1L);

            releaseSlow.countDown();
            assertThat(awaitResult(dispatcher, "slow").result()).isEqualTo(done());
        } finally {
            releaseSlow.countDown();
        }
    }

    @Test
    void saturationRetainsNoFutureForARefusedPipelineAndRetriesAfterCapacityReturns() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        try (LifecycleWorkDispatcher dispatcher = new LifecycleWorkDispatcher(1, 1)) {
            dispatcher.offer("first", desired("first", PipelineState.RUNNING), () -> {
                firstEntered.countDown();
                awaitLatch(releaseFirst);
                return done();
            });
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(dispatcher.offer("second", desired("second", PipelineState.RUNNING),
                    LifecycleWorkDispatcherTest::done))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(dispatcher.offer("third", desired("third", PipelineState.RUNNING),
                    LifecycleWorkDispatcherTest::done))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.CAPACITY);
            assertThat(dispatcher.activeCount()).isEqualTo(2);

            releaseFirst.countDown();
            assertThat(awaitResult(dispatcher, "first").result()).isEqualTo(done());
            assertThat(awaitResult(dispatcher, "second").result()).isEqualTo(done());
            assertThat(dispatcher.offer("third", desired("third", PipelineState.RUNNING),
                    LifecycleWorkDispatcherTest::done))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(awaitResult(dispatcher, "third").result()).isEqualTo(done());
        } finally {
            releaseFirst.countDown();
        }
    }

    @Test
    void aStopReplacesAQueuedStartWithoutExecutingIt() throws Exception {
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger obsoleteStarts = new AtomicInteger();
        AtomicInteger stops = new AtomicInteger();
        try (LifecycleWorkDispatcher dispatcher = new LifecycleWorkDispatcher(1, 1)) {
            dispatcher.offer("first", desired("first", PipelineState.RUNNING), () -> {
                firstEntered.countDown();
                awaitLatch(releaseFirst);
                return done();
            });
            assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
            dispatcher.offer("second", desired("second", PipelineState.RUNNING), () -> {
                obsoleteStarts.incrementAndGet();
                return done();
            });

            assertThat(dispatcher.offer("second", desired("second", PipelineState.STOPPED), () -> {
                stops.incrementAndGet();
                return done();
            })).isEqualTo(LifecycleWorkDispatcher.Submission.COALESCED);
            assertThat(awaitResult(dispatcher, "second").superseded()).isTrue();
            assertThat(dispatcher.offer("second", desired("second", PipelineState.STOPPED), () -> {
                stops.incrementAndGet();
                return done();
            })).isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);

            releaseFirst.countDown();
            awaitResult(dispatcher, "first");
            assertThat(awaitResult(dispatcher, "second").result()).isEqualTo(done());
            assertThat(obsoleteStarts).hasValue(0);
            assertThat(stops).hasValue(1);
        } finally {
            releaseFirst.countDown();
        }
    }

    private static DesiredState desired(String pipelineId, PipelineState target) {
        return new DesiredState(pipelineId, target, "revision-1");
    }

    private static ConvergeResult done() {
        return new ConvergeResult(ConvergeStatus.NOTHING_TO_DO, Optional.empty(), Optional.empty());
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("blocked lifecycle work was not released");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("lifecycle work was interrupted", interrupted);
        }
    }

    private static LifecycleWorkDispatcher.Outcome awaitResult(
            LifecycleWorkDispatcher dispatcher, String pipelineId) throws InterruptedException {
        long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < until) {
            LifecycleWorkDispatcher.Outcome outcome = dispatcher.take(pipelineId);
            if (outcome != null) {
                return outcome;
            }
            TimeUnit.MILLISECONDS.sleep(5);
        }
        throw new AssertionError("no completed lifecycle work for " + pipelineId);
    }
}
