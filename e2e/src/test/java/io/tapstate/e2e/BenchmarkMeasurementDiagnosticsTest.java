package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** Diagnostic evidence stays explicit and bounded without changing any performance arithmetic. */
class BenchmarkMeasurementDiagnosticsTest {

    @TempDir
    Path directory;

    @Test
    void phaseProjectionKeepsRawBatchesAndTheClockReadUncertainty() {
        Instant utc = Instant.parse("2026-09-29T02:00:00Z");
        BenchmarkForkEnvironment.ClockAnchor anchor = new BenchmarkForkEnvironment.ClockAnchor(utc, 100, 140);
        List<BenchmarkForkEnvironment.BatchResult> batches = List.of(
                new BenchmarkForkEnvironment.BatchResult(0, 150, 180),
                new BenchmarkForkEnvironment.BatchResult(1, 200, 260));
        BenchmarkResourceSampler.AttemptLog attempts = new BenchmarkResourceSampler.AttemptLog();
        attempts.append(145, 148, BenchmarkResourceSampler.Outcome.SUCCESS, null,
                reading(100, 20, 1_000, 2_000));
        attempts.append(280, 290, BenchmarkResourceSampler.Outcome.SUCCESS, null,
                reading(170, 23, 1_500, 2_500));
        BenchmarkResourceSampler.Summary resources = new BenchmarkResourceSampler.Summary(
                70, 3, 1_500, 2_500, 2, Optional.of(attempts.snapshot()));
        RealBenchmarkForkDriver.MeasuredPhase phase = new RealBenchmarkForkDriver.MeasuredPhase(
                "cdc-update", 12_000, 150, 260, 300, 12_000, 12_000, 12_002,
                anchor, batches, resources);

        Map<String, Object> projected = PipelineBenchmarkLiveRunIT.phaseEvidence(phase);
        assertThat(projected).containsEntry("durationNanos", 150L)
                .containsEntry("sourceIssueDurationNanos", 110L)
                .containsEntry("idempotentWriteOverhead", 2L)
                .containsEntry("throughputRecordsPerSecond", 12_000 * 1_000_000_000.0 / 150);
        assertThat(object(projected.get("clockAnchor")))
                .containsEntry("utc", utc.toString()).containsEntry("uncertaintyNanos", 40L)
                .containsEntry("uncertaintyScope", "CLOCK_READ_BRACKET_ONLY");
        List<Map<String, Object>> raw = objects(projected.get("sourceBatches"));
        assertThat(raw).hasSize(2);
        assertThat(raw.getFirst()).containsEntry("index", 0).containsEntry("issuedAtNanos", 150L)
                .containsEntry("completedAtNanos", 180L).containsEntry("issueToCompleteNanos", 30L)
                .containsEntry("issuedAtUtcEarliest", utc.plusNanos(10).toString())
                .containsEntry("issuedAtUtcLatest", utc.plusNanos(50).toString());
        assertThat(raw.getLast()).containsEntry("index", 1).containsEntry("issueToCompleteNanos", 60L);
        assertThat(projected).containsEntry("completedAckAtUtcEarliest", utc.plusNanos(160).toString())
                .containsEntry("completedAckAtUtcLatest", utc.plusNanos(200).toString());
        Map<String, Object> sampling = object(object(projected.get("resources")).get("sampling"));
        assertThat(sampling).containsEntry("attemptCount", 2L).containsEntry("failureCount", 0L)
                .containsEntry("totalDurationNanos", 13L).containsEntry("maxDurationNanos", 10L);
        assertThat(JsonReader.parse(JsonWriter.write(projected))).isInstanceOf(Map.class);
        assertThatThrownBy(() -> new BenchmarkForkEnvironment.ClockAnchor(utc, 140, 100))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("moved backward");
    }

    @Test
    void bothEndSamplesRetainMeasuredReadCostsAndTheOriginalResourceTotals() {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger times = new AtomicInteger();
        long[] clock = {100, 130, 200, 250};
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(
                () -> calls.getAndIncrement() == 0 ? reading(100, 20, 1_000, 2_000)
                        : reading(150, 21, 1_100, 2_100),
                Duration.ofHours(1), () -> clock[times.getAndIncrement()])) {
            sampler.start();
            BenchmarkResourceSampler.Summary summary = sampler.finish();
            assertThat(summary.cpuNanos()).isEqualTo(50);
            assertThat(summary.gcCollectionMillis()).isEqualTo(1);
            assertThat(summary.peakHeapBytes()).isEqualTo(1_100);
            assertThat(summary.peakRssBytes()).isEqualTo(2_100);
            assertThat(summary.sampleCount()).isEqualTo(2);
            BenchmarkResourceSampler.SamplingDiagnostics diagnostics = summary.sampling().orElseThrow();
            assertThat(diagnostics.state()).isEqualTo("COMPLETE");
            assertThat(diagnostics.attemptCount()).isEqualTo(2);
            assertThat(diagnostics.failureCount()).isZero();
            assertThat(diagnostics.totalDurationNanos()).isEqualTo(80);
            assertThat(diagnostics.maxDurationNanos()).isEqualTo(50);
            assertThat(diagnostics.retainedAttempts()).extracting(BenchmarkResourceSampler.Attempt::durationNanos)
                    .containsExactly(30L, 50L);
        }
    }

    @Test
    void boundedAttemptsStillAccountForAnOmittedPeakAndKeepTheLastFailure() {
        BenchmarkResourceSampler.AttemptLog attempts = new BenchmarkResourceSampler.AttemptLog();
        BenchmarkResourceSampler.ResourceTotals totals = new BenchmarkResourceSampler.ResourceTotals();
        for (int sample = 0; sample < 600; sample++) {
            BenchmarkProcessProbe.Snapshot reading = reading(sample * 100L, sample, sample == 300 ? 9_999 : 1_000,
                    sample == 300 ? 19_999 : 2_000);
            totals.add(reading);
            attempts.append(sample * 100L, sample * 100L + 10, BenchmarkResourceSampler.Outcome.SUCCESS, null, reading);
        }
        BenchmarkResourceSampler.Summary summary = totals.summary(Optional.of(attempts.snapshot()));
        assertThat(summary.cpuNanos()).isEqualTo(59_900);
        assertThat(summary.gcCollectionMillis()).isEqualTo(599);
        assertThat(summary.sampleCount()).isEqualTo(600);
        assertThat(summary.peakHeapBytes()).isEqualTo(9_999);
        assertThat(summary.peakRssBytes()).isEqualTo(19_999);
        assertThat(summary.sampling().orElseThrow().retainedAttempts())
                .noneMatch(attempt -> attempt.index() == 301);

        attempts.append(60_000, 60_050, BenchmarkResourceSampler.Outcome.UNAVAILABLE, "INCOMPLETE_SAMPLE", null);
        BenchmarkResourceSampler.SamplingDiagnostics failed = attempts.snapshot();
        assertThat(failed.state()).isEqualTo("FAILED");
        assertThat(failed.attemptCount()).isEqualTo(601);
        assertThat(failed.failureCount()).isEqualTo(1);
        assertThat(failed.totalDurationNanos()).isEqualTo(6_050);
        assertThat(failed.maxDurationNanos()).isEqualTo(50);
        assertThat(failed.retainedAttempts()).hasSize(BenchmarkResourceSampler.MAX_RETAINED_ATTEMPTS);
        assertThat(failed.omittedAttempts()).isEqualTo(345);
        assertThat(failed.retainedAttempts().getFirst().index()).isEqualTo(1);
        assertThat(failed.retainedAttempts().getLast().index()).isEqualTo(601);
        assertThat(failed.retainedAttempts().getLast().outcome()).isEqualTo(BenchmarkResourceSampler.Outcome.UNAVAILABLE);
    }

    @Test
    void unavailableSamplesFailClosedAndPersistExplicitCountsWithoutZeroResources() throws Exception {
        AtomicInteger times = new AtomicInteger();
        long[] clock = {500, 560};
        BenchmarkProcessProbe.Snapshot partial = new BenchmarkProcessProbe.Snapshot(
                OptionalLong.of(100), OptionalLong.empty(), OptionalLong.of(2_000), OptionalLong.of(20));
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(
                () -> partial, Duration.ofHours(1), () -> clock[times.getAndIncrement()])) {
            sampler.start();
            BenchmarkResourceSampler.SamplingFailure failure = catchThrowableOfType(
                    sampler::finish, BenchmarkResourceSampler.SamplingFailure.class);
            assertThat(failure).isNotNull();
            BenchmarkResourceSampler.SamplingDiagnostics diagnostics = failure.diagnostics();
            assertThat(diagnostics.state()).isEqualTo("FAILED");
            assertThat(diagnostics.attemptCount()).isEqualTo(1);
            assertThat(diagnostics.failureCount()).isEqualTo(1);
            assertThat(diagnostics.totalDurationNanos()).isEqualTo(60);
            assertThat(diagnostics.retainedAttempts().getFirst().reading().heapUsedBytes()).isEmpty();
            Path output = directory.resolve("failed-sampling.json");
            BenchmarkLiveReport report = new BenchmarkLiveReport(output);
            report.fail(failure.inPhase("cold-read"));
            Map<String, Object> saved = object(JsonReader.parse(Files.readString(output)));
            assertThat(saved).containsEntry("status", "FAILED").containsEntry("forks", List.of());
            assertThat((String) object(saved.get("failure")).get("message"))
                    .contains("phase=cold-read", "state=FAILED", "attemptCount=1", "failureCount=1")
                    .contains("totalDurationNanos=60", "outcome=UNAVAILABLE", "OptionalLong.empty")
                    .doesNotContain("peakHeapBytes=0", "peakRssBytes=0");
        }
    }

    @Test
    void thrownReadsKeepTheirFailureTypeAndUnknownDiagnosticCostsRemainAbsent() {
        AtomicInteger times = new AtomicInteger();
        long[] clock = {100, 180};
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(() -> {
            throw new IllegalStateException("synthetic read failure");
        }, Duration.ofHours(1), () -> clock[times.getAndIncrement()])) {
            sampler.start();
            BenchmarkResourceSampler.SamplingFailure failure = catchThrowableOfType(
                    sampler::finish, BenchmarkResourceSampler.SamplingFailure.class);
            assertThat(failure).isNotNull();
            assertThat(failure.diagnostics().failureCount()).isEqualTo(1);
            assertThat(failure.diagnostics().retainedAttempts().getFirst().outcome())
                    .isEqualTo(BenchmarkResourceSampler.Outcome.ERROR);
            assertThat(failure.diagnostics().retainedAttempts().getFirst().failureType())
                    .isEqualTo(IllegalStateException.class.getName());
        }
        BenchmarkResourceSampler.Summary knownResources = BenchmarkResourceSampler.summarize(List.of(
                reading(100, 20, 1_000, 2_000), reading(150, 21, 1_100, 2_100)));
        BenchmarkForkEnvironment.ClockAnchor anchor = new BenchmarkForkEnvironment.ClockAnchor(Instant.EPOCH, 100, 120);
        assertThat(object(PipelineBenchmarkLiveRunIT.resourceEvidence(knownResources, anchor).get("sampling")))
                .isEqualTo(Map.of("state", "NOT_RECORDED"));
    }

    @Test
    void summaryAndInvalidLifecycleFailuresPersistWithoutInventedReadFailures() throws Exception {
        List<List<BenchmarkProcessProbe.Snapshot>> invalidWindows = List.of(
                List.of(reading(100, 20, 1_000, 2_000), reading(90, 21, 1_100, 2_100)),
                List.of(reading(100, 20, 0, 2_000), reading(150, 21, 0, 2_100)));
        for (int index = 0; index < invalidWindows.size(); index++) {
            List<BenchmarkProcessProbe.Snapshot> window = invalidWindows.get(index);
            AtomicInteger reads = new AtomicInteger();
            try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(
                    () -> window.get(reads.getAndIncrement()), Duration.ofHours(1))) {
                sampler.start();
                BenchmarkResourceSampler.SamplingFailure failed = catchThrowableOfType(
                        sampler::finish, BenchmarkResourceSampler.SamplingFailure.class);
                assertThat(failed).isNotNull();
                assertThat(failed.stage()).isEqualTo(BenchmarkResourceSampler.FailureStage.SUMMARY);
                assertThat(failed.reason()).isEqualTo(BenchmarkResourceSampler.FailureReason.INVALID_SUMMARY);
                assertThat(failed.diagnostics().state()).isEqualTo("COMPLETE");
                assertThat(failed.diagnostics().attemptCount()).isEqualTo(2);
                assertThat(failed.diagnostics().failureCount()).isZero();
                assertThat(failed.diagnostics().retainedAttempts()).allSatisfy(attempt ->
                        assertThat(attempt.outcome()).isEqualTo(BenchmarkResourceSampler.Outcome.SUCCESS));
                assertSavedFailure("invalid-summary-" + index + ".json", failed,
                        "stage=SUMMARY", "reason=INVALID_SUMMARY", "attemptCount=2", "failureCount=0");
            }
        }
        try (BenchmarkResourceSampler neverStarted = BenchmarkResourceSampler.from(
                () -> reading(100, 20, 1_000, 2_000), Duration.ofHours(1))) {
            BenchmarkResourceSampler.SamplingFailure failed = catchThrowableOfType(
                    neverStarted::finish, BenchmarkResourceSampler.SamplingFailure.class);
            assertThat(failed).isNotNull();
            assertThat(failed.stage()).isEqualTo(BenchmarkResourceSampler.FailureStage.STATE);
            assertThat(failed.reason()).isEqualTo(BenchmarkResourceSampler.FailureReason.NO_OPEN_WINDOW);
            assertSavedFailure("invalid-lifecycle.json", failed,
                    "stage=STATE", "reason=NO_OPEN_WINDOW", "state=NOT_STARTED", "attemptCount=0", "failureCount=0");
        }
    }

    @Test
    void timeoutSnapshotsDoNotWaitForTheBlockedFinalReadMonitor() throws Exception {
        BlockedRead source = new BlockedRead();
        BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(source, Duration.ofHours(1),
                System::nanoTime, (worker, timeout, unit) -> {
                    assertThat(timeout).isEqualTo(5);
                    assertThat(unit).isEqualTo(TimeUnit.SECONDS);
                    assertThat(worker.isShutdown()).isTrue();
                    source.awaitEntered();
                    return false;
                });
        CompletableFuture<FinishResult> result = new CompletableFuture<>();
        Thread finisher = null;
        try {
            sampler.start();
            finisher = finishOnOwnedThread(sampler, result);
            BenchmarkResourceSampler.SamplingFailure failed = result.get(2, TimeUnit.SECONDS).failure();
            assertThat(failed).isNotNull();
            assertThat(failed.stage()).isEqualTo(BenchmarkResourceSampler.FailureStage.SHUTDOWN);
            assertThat(failed.reason()).isEqualTo(BenchmarkResourceSampler.FailureReason.SHUTDOWN_TIMEOUT);
            assertThat(failed.diagnostics().attemptCount()).isEqualTo(1);
            assertThat(failed.diagnostics().failureCount()).isZero();
            assertThat(failed.pendingRead()).isPresent();
            assertThat(failed.pendingRead().orElseThrow().index()).isEqualTo(2);
            assertThat(failed.pendingRead().orElseThrow().finalRead()).isTrue();
            assertSavedFailure("shutdown-timeout.json", failed,
                    "stage=SHUTDOWN", "reason=SHUTDOWN_TIMEOUT", "attemptCount=1", "failureCount=0", "finalRead=true");
            assertThat(failed.diagnostics().retainedAttempts()).hasSize(1);
        } finally {
            source.releaseAndAwait();
            sampler.close();
            if (finisher != null) {
                assertThat(finisher.join(Duration.ofSeconds(2))).isTrue();
            }
        }
    }

    @Test
    void finalReadBookkeepingFailureRetainsItsStageAndLastPublishedEvidence() throws Exception {
        AtomicInteger times = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        long[] clock = {100, 120, 200};
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(
                () -> reads.getAndIncrement() == 0 ? reading(100, 20, 1_000, 2_000)
                        : reading(150, 21, 1_100, 2_100), Duration.ofHours(1), () -> {
                    int index = times.getAndIncrement();
                    if (index == clock.length) {
                        throw new IllegalStateException("owned final clock unavailable");
                    }
                    return clock[index];
                })) {
            sampler.start();
            BenchmarkResourceSampler.SamplingFailure failed = catchThrowableOfType(
                    sampler::finish, BenchmarkResourceSampler.SamplingFailure.class);
            assertThat(failed).isNotNull();
            assertThat(failed.stage()).isEqualTo(BenchmarkResourceSampler.FailureStage.FINAL_SAMPLE);
            assertThat(failed.reason()).isEqualTo(BenchmarkResourceSampler.FailureReason.FINAL_SAMPLE_ERROR);
            assertThat(reads.get()).isEqualTo(2);
            assertThat(failed.diagnostics().attemptCount()).isEqualTo(1);
            assertThat(failed.diagnostics().failureCount()).isZero();
            assertThat(failed.pendingRead().orElseThrow().finalRead()).isTrue();
            assertSavedFailure("final-bookkeeping.json", failed,
                    "stage=FINAL_SAMPLE", "reason=FINAL_SAMPLE_ERROR", "attemptCount=1", "failureCount=0");
        }
    }

    @Test
    void interruptedShutdownRetainsTheFlagAndSafeSnapshotOfAnActivePeriodicRead() throws Exception {
        BlockedRead source = new BlockedRead();
        BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(source, Duration.ofMillis(1),
                System::nanoTime, (worker, timeout, unit) -> {
                    assertThat(timeout).isEqualTo(5);
                    assertThat(unit).isEqualTo(TimeUnit.SECONDS);
                    throw new InterruptedException("owned termination wait interrupted");
                });
        CompletableFuture<FinishResult> result = new CompletableFuture<>();
        Thread finisher = null;
        try {
            sampler.start();
            source.awaitEntered();
            finisher = finishOnOwnedThread(sampler, result);
            FinishResult finished = result.get(2, TimeUnit.SECONDS);
            assertThat(finished.interrupted()).isTrue();
            BenchmarkResourceSampler.SamplingFailure failed = finished.failure();
            assertThat(failed).isNotNull();
            assertThat(failed.reason()).isEqualTo(BenchmarkResourceSampler.FailureReason.SHUTDOWN_INTERRUPTED);
            assertThat(failed.diagnostics().attemptCount()).isEqualTo(1);
            assertThat(failed.diagnostics().failureCount()).isZero();
            assertThat(failed.pendingRead().orElseThrow().finalRead()).isFalse();
            assertSavedFailure("shutdown-interrupted.json", failed,
                    "stage=SHUTDOWN", "reason=SHUTDOWN_INTERRUPTED", "attemptCount=1", "failureCount=0");
        } finally {
            source.releaseAndAwait();
            sampler.close();
            if (finisher != null) {
                assertThat(finisher.join(Duration.ofSeconds(2))).isTrue();
            }
        }
    }

    private void assertSavedFailure(String filename, BenchmarkResourceSampler.SamplingFailure failure,
                                    String... details) throws Exception {
        Path output = directory.resolve(filename);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.fail(failure.inPhase("cdc-update"));
        Map<String, Object> saved = object(JsonReader.parse(Files.readString(output)));
        assertThat(saved).containsEntry("status", "FAILED").containsEntry("forks", List.of());
        assertThat((String) object(saved.get("failure")).get("message"))
                .contains("phase=cdc-update").contains(details);
    }

    private record FinishResult(BenchmarkResourceSampler.SamplingFailure failure, boolean interrupted) { }

    private static Thread finishOnOwnedThread(BenchmarkResourceSampler sampler, CompletableFuture<FinishResult> result) {
        return Thread.ofVirtual().name("measurement-diagnostic-finish").start(() -> {
            try {
                BenchmarkResourceSampler.SamplingFailure failed = catchThrowableOfType(
                        sampler::finish, BenchmarkResourceSampler.SamplingFailure.class);
                result.complete(new FinishResult(failed, Thread.currentThread().isInterrupted()));
            } catch (Throwable problem) {
                result.completeExceptionally(problem);
            }
        });
    }

    /** The owned read deliberately ignores cancellation until released so monitor safety is discriminated. */
    private static final class BlockedRead implements java.util.function.Supplier<BenchmarkProcessProbe.Snapshot> {
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch returned = new CountDownLatch(1);

        @Override
        public BenchmarkProcessProbe.Snapshot get() {
            if (calls.getAndIncrement() == 0) {
                return reading(100, 20, 1_000, 2_000);
            }
            entered.countDown();
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        release.await();
                        return reading(150, 21, 1_100, 2_100);
                    } catch (InterruptedException retryOwnedRead) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                returned.countDown();
            }
        }

        void awaitEntered() throws InterruptedException {
            assertThat(entered.await(2, TimeUnit.SECONDS)).as("owned read entered").isTrue();
        }

        void releaseAndAwait() throws InterruptedException {
            release.countDown();
            if (entered.getCount() == 0) {
                assertThat(returned.await(2, TimeUnit.SECONDS)).as("owned read released").isTrue();
            }
        }
    }

    private static BenchmarkProcessProbe.Snapshot reading(long cpu, long gc, long heap, long rss) {
        return new BenchmarkProcessProbe.Snapshot(OptionalLong.of(cpu), OptionalLong.of(heap),
                OptionalLong.of(rss), OptionalLong.of(gc));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> objects(Object value) {
        return (List<Map<String, Object>>) value;
    }
}
