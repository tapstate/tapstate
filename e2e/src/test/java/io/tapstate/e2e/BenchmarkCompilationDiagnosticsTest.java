package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class BenchmarkCompilationDiagnosticsTest {
    @Test
    void constructionAndEvidenceDoNotPerformARead() {
        AtomicInteger calls = new AtomicInteger();
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> {
            calls.incrementAndGet();
            return success(0);
        }, () -> { throw new AssertionError("no clock read before record"); }, 2);

        assertThat(diagnostics.evidence().attemptCount()).isZero();
        assertThat(diagnostics.wireEvidence()).containsEntry("state", "NOT_RECORDED");
        assertThat(calls).hasValue(0);
    }

    @Test
    void independentlyBracketsRealCounterValuesAndPublishesImmutableSnapshots() {
        AtomicInteger reads = new AtomicInteger();
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> success(reads.getAndIncrement() * 50),
                clock(10, 30, 100, 105), 4);
        var first = diagnostics.record();
        var firstSnapshot = diagnostics.evidence();
        var second = diagnostics.record();

        assertThat(first.startedAtNanos()).isEqualTo(10);
        assertThat(first.completedAtNanos()).isEqualTo(30);
        assertThat(first.reading().totalCompilationMillis()).hasValue(0);
        assertThat(second.startedAtNanos()).isEqualTo(100);
        assertThat(second.completedAtNanos()).isEqualTo(105);
        assertThat(second.reading().totalCompilationMillis()).hasValue(50);
        assertThat(firstSnapshot.attemptCount()).isEqualTo(1);
        assertThat(firstSnapshot.retainedAttempts()).containsExactly(first);
        assertThat(diagnostics.evidence().totalDurationNanos()).isEqualTo(25);
        assertThat(diagnostics.evidence().maxDurationNanos()).isEqualTo(20);
        assertThatThrownBy(() -> firstSnapshot.retainedAttempts().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(JsonWriter.write(diagnostics.wireEvidence()))
                .contains("\"outcome\":\"SUCCESS\"", "\"totalCompilationMillis\":0")
                .doesNotContain("OptionalLong", "CompilationReading");
    }

    @Test
    void unsupportedOrInvalidCountersRemainUnknownAndAreOmittedFromTheWireValue() {
        var readings = List.of(
                unknown(false, BenchmarkProcessProbe.CompilationUnknownReason.MONITORING_UNSUPPORTED),
                unknown(true, BenchmarkProcessProbe.CompilationUnknownReason.INVALID_COUNTER));
        AtomicInteger next = new AtomicInteger();
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> readings.get(next.getAndIncrement()),
                clock(1, 2, 3, 4), 2);
        diagnostics.record();
        diagnostics.record();

        assertThat(diagnostics.evidence().unknownCount()).isEqualTo(2);
        assertThat(diagnostics.evidence().errorCount()).isZero();
        assertThat(diagnostics.evidence().retainedAttempts()).allSatisfy(attempt -> {
            assertThat(attempt.outcome()).isEqualTo(BenchmarkCompilationDiagnostics.Outcome.UNKNOWN);
            assertThat(attempt.reading().totalCompilationMillis()).isEmpty();
        });
        assertThat(JsonWriter.write(diagnostics.wireEvidence()))
                .contains("\"state\":\"UNKNOWN\"", "MONITORING_UNSUPPORTED", "INVALID_COUNTER")
                .doesNotContain("totalCompilationMillis");
    }

    @Test
    void ordinaryReaderFailuresRemainErrorFactsWithoutInventingAReading() {
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> {
            throw new IllegalStateException("external compilation read failed");
        }, clock(4, 9), 2);
        var attempt = diagnostics.record();

        assertThat(attempt.outcome()).isEqualTo(BenchmarkCompilationDiagnostics.Outcome.ERROR);
        assertThat(attempt.failureType()).isEqualTo(IllegalStateException.class.getName());
        assertThat(attempt.reading()).isNull();
        assertThat(diagnostics.evidence().errorCount()).isEqualTo(1);
        assertThat(diagnostics.evidence().totalDurationNanos()).isEqualTo(5);
        assertThat(JsonWriter.write(diagnostics.wireEvidence())).doesNotContain("totalCompilationMillis");
    }

    @Test
    void programmerErrorsStillEscapeAfterTheirActualReadBracketIsRetained() {
        AssertionError invariant = new AssertionError("broken reader invariant");
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> { throw invariant; }, clock(4, 9), 2);

        assertThatThrownBy(diagnostics::record).isSameAs(invariant);
        assertThat(diagnostics.evidence().errorCount()).isEqualTo(1);
        assertThat(diagnostics.evidence().pending()).isEmpty();
        assertThat(diagnostics.evidence().retainedAttempts().getFirst().failureType())
                .isEqualTo(AssertionError.class.getName());
    }

    @Test
    void nullReadingsAndCounterRollbackCannotBecomeSuccessfulZeroes() {
        AtomicInteger next = new AtomicInteger();
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> switch (next.getAndIncrement()) {
            case 0 -> success(100);
            case 1 -> success(99);
            default -> null;
        }, clock(1, 2, 3, 4, 5, 6), 4);
        diagnostics.record();
        var rollback = diagnostics.record();
        var missing = diagnostics.record();

        assertThat(rollback.outcome()).isEqualTo(BenchmarkCompilationDiagnostics.Outcome.UNKNOWN);
        assertThat(rollback.failureType()).isEqualTo("COUNTER_DECREASED");
        assertThat(rollback.reading().totalCompilationMillis()).hasValue(99);
        assertThat(missing.outcome()).isEqualTo(BenchmarkCompilationDiagnostics.Outcome.ERROR);
        assertThat(missing.failureType()).isEqualTo("NULL_READING");
        assertThat(missing.reading()).isNull();
        assertThat(diagnostics.evidence().unknownCount()).isEqualTo(1);
        assertThat(diagnostics.evidence().errorCount()).isEqualTo(1);
        assertThat(diagnostics.wireEvidence()).containsEntry("state", "UNKNOWN");
    }

    @Test
    void retainedFirstAndLastReadsPreserveExactOmissionAndDurationTotals() {
        for (int retention : List.of(2, 3, 4, 5)) {
            AtomicLong time = new AtomicLong();
            var diagnostics = BenchmarkCompilationDiagnostics.from(() -> success(0),
                    () -> time.getAndAdd(10), retention);
            for (int i = 0; i < 7; i++) { diagnostics.record(); }
            var evidence = diagnostics.evidence();
            assertThat(evidence.attemptCount()).isEqualTo(7);
            assertThat(evidence.retainedAttempts()).hasSize(retention);
            assertThat(evidence.retainedAttempts().getFirst().index()).isEqualTo(1);
            assertThat(evidence.retainedAttempts().getLast().index()).isEqualTo(7);
            assertThat(evidence.omittedAttempts()).isEqualTo(7 - retention);
            assertThat(evidence.totalDurationNanos()).isEqualTo(70);
            assertThat(evidence.maxDurationNanos()).isEqualTo(10);
        }
    }

    @Test
    void pendingReadsStayVisibleWithoutWaitingForTheReaderLock() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicLong time = new AtomicLong(100);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("test reader was not released"); }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("test reader interrupted", interrupted);
            }
            return success(1);
        }, time::getAndIncrement, 2);
        Thread worker = Thread.ofPlatform().name("compilation-diagnostic-test-reader")
                .uncaughtExceptionHandler((thread, failure) -> workerFailure.set(failure)).start(diagnostics::record);
        try {
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            var pending = assertTimeoutPreemptively(Duration.ofSeconds(1), diagnostics::evidence);
            assertThat(pending.attemptCount()).isZero();
            assertThat(pending.pending()).contains(new BenchmarkCompilationDiagnostics.PendingRead(1, 100));
            assertThatThrownBy(diagnostics::record).isInstanceOf(IllegalStateException.class);
            var wire = assertTimeoutPreemptively(Duration.ofSeconds(1), diagnostics::wireEvidence);
            assertThat(wire).containsEntry("state", "UNKNOWN");
            Map<?, ?> open = (Map<?, ?>) wire.get("pending");
            assertThat(open.containsKey("startedAtNanos")).isTrue();
            assertThat(open.containsKey("completedAtNanos")).isFalse();
            assertThat(open.containsKey("durationNanos")).isFalse();
        } finally {
            release.countDown();
            worker.join(2_000);
        }
        assertThat(worker.isAlive()).isFalse();
        assertThat(workerFailure.get()).isNull();
        assertThat(diagnostics.evidence().attemptCount()).isEqualTo(1);
        assertThat(diagnostics.evidence().pending()).isEmpty();
    }

    @Test
    void anInvalidCompletionClockCannotPublishAnInventedCompletedRead() {
        var diagnostics = BenchmarkCompilationDiagnostics.from(() -> success(1), clock(10, 9), 2);

        assertThatThrownBy(diagnostics::record).isInstanceOf(IllegalArgumentException.class);
        assertThat(diagnostics.evidence().attemptCount()).isZero();
        assertThat(diagnostics.evidence().pending()).contains(new BenchmarkCompilationDiagnostics.PendingRead(1, 10));
        assertThatThrownBy(diagnostics::record).isInstanceOf(IllegalStateException.class);
        assertThat(JsonWriter.write(diagnostics.wireEvidence())).doesNotContain("completedAtNanos", "durationNanos");
    }

    @Test
    void retentionOutsideItsFiniteRangeIsAProgrammerError() {
        for (int retention : List.of(1, 4097)) {
            assertThatThrownBy(() -> BenchmarkCompilationDiagnostics.from(() -> success(0), () -> 1L, retention))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static LongSupplier clock(long... values) {
        AtomicInteger next = new AtomicInteger();
        return () -> values[next.getAndIncrement()];
    }

    private static BenchmarkProcessProbe.CompilationReading success(long counter) {
        return new BenchmarkProcessProbe.CompilationReading(71, "test-compiler", true, OptionalLong.of(counter),
                BenchmarkProcessProbe.CompilationState.SUCCESS, BenchmarkProcessProbe.CompilationUnknownReason.NONE, null);
    }

    private static BenchmarkProcessProbe.CompilationReading unknown(Boolean supported,
            BenchmarkProcessProbe.CompilationUnknownReason reason) {
        return new BenchmarkProcessProbe.CompilationReading(71, "test-compiler", supported, OptionalLong.empty(),
                BenchmarkProcessProbe.CompilationState.UNKNOWN, reason, null);
    }
}
