package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Resource comparisons use complete external readings and the largest sampled memory values. */
class BenchmarkResourceSamplerTest {

    @Test
    void supplied_resource_samples_cannot_supply_an_owned_return_connection() {
        try (var sampler = BenchmarkResourceSampler.from(() -> reading(1, 1, 1, 1), Duration.ofHours(1))) {
            assertThatThrownBy(sampler::writeReturnReader).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("actual owned resource connection");
        }
    }

    @Test
    void computesCounterDeltasAndPeakMemoryAcrossTheWholeWindow() {
        BenchmarkResourceSampler.Summary summary = BenchmarkResourceSampler.summarize(List.of(
                reading(100, 20, 1_000, 2_000),
                reading(125, 21, 1_500, 2_500),
                reading(170, 23, 1_200, 2_200)));

        assertThat(summary.cpuNanos()).isEqualTo(70);
        assertThat(summary.gcCollectionMillis()).isEqualTo(3);
        assertThat(summary.peakHeapBytes()).isEqualTo(1_500);
        assertThat(summary.peakRssBytes()).isEqualTo(2_500);
        assertThat(summary.sampleCount()).isEqualTo(3);
    }

    @Test
    void anUnavailableSampleOrBackwardCounterRefusesTheWindow() {
        BenchmarkProcessProbe.Snapshot missing = new BenchmarkProcessProbe.Snapshot(
                OptionalLong.of(120), OptionalLong.empty(), OptionalLong.of(2_000), OptionalLong.of(21));
        assertThatThrownBy(() -> BenchmarkResourceSampler.summarize(List.of(
                reading(100, 20, 1_000, 2_000), missing)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("incomplete sample");
        assertThatThrownBy(() -> BenchmarkResourceSampler.summarize(List.of(
                reading(100, 20, 1_000, 2_000), reading(90, 19, 1_200, 2_200))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("counter moved backward");
    }

    @Test
    void startAndFinishSampleBothEndsWithoutWaitingForThePeriodicTick() {
        AtomicInteger calls = new AtomicInteger();
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(
                () -> calls.getAndIncrement() == 0
                        ? reading(100, 20, 1_000, 2_000)
                        : reading(150, 21, 1_100, 2_100), Duration.ofHours(1))) {
            sampler.start();
            BenchmarkResourceSampler.Summary result = sampler.finish();
            assertThat(result.cpuNanos()).isEqualTo(50);
            assertThat(result.peakRssBytes()).isEqualTo(2_100);
            assertThat(result.sampleCount()).isEqualTo(2);
            assertThat(calls).hasValue(2);
            assertThat(sampler.checkpointEvidence()).isEmpty();
        }
    }

    @Test void a_checkpoint_is_an_immutable_prefix_and_later_finish_keeps_the_original_baseline_and_tail() {
        AtomicInteger reads = new AtomicInteger(); AtomicLong clock = new AtomicLong();
        try (var sampler = BenchmarkResourceSampler.from(() -> switch (reads.incrementAndGet()) {
            case 1 -> reading(100, 20, 1000, 2000);
            case 2 -> reading(150, 21, 1500, 2500);
            default -> reading(300, 24, 1200, 4000);
        }, Duration.ofHours(1), clock::incrementAndGet)) {
            sampler.start(); var checkpoint = sampler.checkpoint();
            assertThat(checkpoint.attemptIndex()).isEqualTo(2);
            assertThat(checkpoint.startedAtNanos()).isEqualTo(3); assertThat(checkpoint.completedAtNanos()).isEqualTo(4);
            assertThat(checkpoint.summary().cpuNanos()).isEqualTo(50);
            assertThat(checkpoint.summary().gcCollectionMillis()).isEqualTo(1);
            assertThat(checkpoint.summary().sampleCount()).isEqualTo(2);
            var actual = checkpoint.summary().sampling().orElseThrow().retainedAttempts().getLast();
            assertThat(actual.index()).isEqualTo(checkpoint.attemptIndex());
            assertThat(actual.startedAtNanos()).isEqualTo(checkpoint.startedAtNanos());
            assertThat(actual.completedAtNanos()).isEqualTo(checkpoint.completedAtNanos());
            assertThat(sampler.checkpointEvidence()).contains(checkpoint);
            var full = sampler.finish();
            assertThat(full.cpuNanos()).isEqualTo(200); assertThat(full.gcCollectionMillis()).isEqualTo(4);
            assertThat(full.peakRssBytes()).isEqualTo(4000); assertThat(full.sampleCount()).isEqualTo(3);
            assertThat(checkpoint.summary().cpuNanos()).isEqualTo(50); assertThat(checkpoint.summary().sampleCount()).isEqualTo(2);
            assertThat(reads).hasValue(3);
        }
    }

    @Test void checkpoint_follows_an_active_periodic_read_on_the_same_serial_worker() throws Exception {
        AtomicInteger reads = new AtomicInteger(), active = new AtomicInteger(), maximumActive = new AtomicInteger();
        CountDownLatch periodicStarted = new CountDownLatch(1), releasePeriodic = new CountDownLatch(1);
        CountDownLatch checkpointQueued = new CountDownLatch(1), releaseLater = new CountDownLatch(1);
        AtomicReference<BenchmarkResourceSampler.Checkpoint> checkpoint = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        try (var sampler = BenchmarkResourceSampler.fromWithCheckpointWaiter(() -> {
            maximumActive.accumulateAndGet(active.incrementAndGet(), Math::max);
            try {
                int index = reads.incrementAndGet();
                if (index == 2) { periodicStarted.countDown(); await(releasePeriodic); }
                if (index >= 4) { await(releaseLater); }
                return reading(100L * index, index, 1000, 2000);
            } finally { active.decrementAndGet(); }
        }, Duration.ofMillis(1), new AtomicLong()::incrementAndGet, (future, timeout, unit) -> {
            checkpointQueued.countDown(); return future.get(timeout, unit);
        })) {
            sampler.start(); await(periodicStarted);
            Thread requester = new Thread(() -> {
                try { checkpoint.set(sampler.checkpoint()); } catch (Throwable failure) { failed.set(failure); }
            }, "owned-resource-checkpoint-control"); requester.setDaemon(true);
            try {
                requester.start(); await(checkpointQueued); assertThat(reads).hasValue(2);
                releasePeriodic.countDown(); requester.join(2000);
                assertThat(requester.isAlive()).isFalse(); assertThat(failed.get()).isNull();
                assertThat(checkpoint.get().attemptIndex()).isEqualTo(3);
                assertThat(checkpoint.get().summary().sampleCount()).isEqualTo(3);
                assertThat(maximumActive).hasValue(1);
                releaseLater.countDown(); var full = sampler.finish();
                assertThat(full.sampleCount()).isGreaterThan(checkpoint.get().summary().sampleCount());
            } finally { releasePeriodic.countDown(); releaseLater.countDown(); requester.join(2000); }
        }
    }

    @Test void checkpoint_timeout_retains_the_actual_pending_read_and_a_later_completion_without_retry() {
        AtomicInteger reads = new AtomicInteger(); CountDownLatch active = new CountDownLatch(1), release = new CountDownLatch(1);
        TimeoutException primary = new TimeoutException("controlled wait timeout");
        try (var sampler = BenchmarkResourceSampler.fromWithCheckpointWaiter(() -> {
            int index = reads.incrementAndGet();
            if (index == 2) { active.countDown(); await(release); }
            return reading(100L * index, index, 1000, 2000);
        }, Duration.ofHours(1), new AtomicLong()::incrementAndGet, (future, timeout, unit) -> {
            assertThat(timeout).isEqualTo(5); assertThat(unit).isEqualTo(TimeUnit.SECONDS); await(active); throw primary;
        })) {
            try {
                sampler.start();
                var failure = org.assertj.core.api.Assertions.catchThrowableOfType(sampler::checkpoint, BenchmarkResourceSampler.SamplingFailure.class);
                assertThat(failure.stage()).isEqualTo(BenchmarkResourceSampler.FailureStage.CHECKPOINT);
                assertThat(failure.reason()).isEqualTo(BenchmarkResourceSampler.FailureReason.CHECKPOINT_TIMEOUT);
                assertThat(failure.getCause()).isSameAs(primary); assertThat(failure.diagnostics().attemptCount()).isEqualTo(1);
                assertThat(failure.pendingRead()).isPresent(); assertThat(failure.pendingRead().orElseThrow().index()).isEqualTo(2);
                assertThat(failure.pendingRead().orElseThrow().checkpointRead()).isTrue();
                assertThat(sampler.checkpointEvidence()).isEmpty(); assertThat(reads).hasValue(2);
                release.countDown(); var full = sampler.finish();
                assertThat(full.sampleCount()).isEqualTo(3); assertThat(reads).hasValue(3);
                assertThat(sampler.checkpointEvidence().orElseThrow().attemptIndex()).isEqualTo(2);
                assertThat(failure.diagnostics().attemptCount()).isEqualTo(1); assertThat(failure.pendingRead()).isPresent();
            } finally { release.countDown(); }
        }
    }

    @Test void failed_checkpoint_keeps_the_completed_error_and_the_identical_primary_cause() {
        AtomicInteger reads = new AtomicInteger(); IllegalStateException primary = new IllegalStateException("controlled read failure");
        try (var sampler = BenchmarkResourceSampler.from(() -> {
            if (reads.incrementAndGet() == 2) { throw primary; }
            return reading(100, 20, 1000, 2000);
        }, Duration.ofHours(1))) {
            sampler.start();
            var failure = org.assertj.core.api.Assertions.catchThrowableOfType(sampler::checkpoint, BenchmarkResourceSampler.SamplingFailure.class);
            assertThat(failure.getCause()).isSameAs(primary);
            assertThat(failure.reason()).isEqualTo(BenchmarkResourceSampler.FailureReason.READ_ERROR);
            assertThat(failure.diagnostics().attemptCount()).isEqualTo(2); assertThat(failure.diagnostics().failureCount()).isEqualTo(1);
            assertThat(failure.diagnostics().retainedAttempts().getLast().outcome()).isEqualTo(BenchmarkResourceSampler.Outcome.ERROR);
            assertThat(failure.pendingRead()).isEmpty(); assertThat(sampler.checkpointEvidence()).isEmpty();
            var terminal = org.assertj.core.api.Assertions.catchThrowableOfType(sampler::finish, BenchmarkResourceSampler.SamplingFailure.class);
            assertThat(terminal.getCause()).isSameAs(primary); assertThat(reads).hasValue(2);
        }
    }

    private static void await(CountDownLatch latch) {
        try { assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }

    private static BenchmarkProcessProbe.Snapshot reading(long cpu, long gc, long heap, long rss) {
        return new BenchmarkProcessProbe.Snapshot(
                OptionalLong.of(cpu), OptionalLong.of(heap), OptionalLong.of(rss), OptionalLong.of(gc));
    }
}
