package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Optional compilation reads preserve the ordinary resource read boundaries and summaries. */
class BenchmarkCompilationResourceIntegrationTest {

    @Test
    void independentlyBracketsCompilationAfterEachUnchangedResourceRead() {
        AtomicLong clock = new AtomicLong(100);
        AtomicInteger resources = new AtomicInteger();
        AtomicInteger compilation = new AtomicInteger();
        BenchmarkCompilationDiagnostics diagnostics = BenchmarkCompilationDiagnostics.from(() -> {
            clock.addAndGet(30);
            return supported(compilation.incrementAndGet());
        }, clock::get, 8);
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.fromWithCompilation(() -> {
            clock.addAndGet(20);
            return resource(resources.incrementAndGet());
        }, Duration.ofHours(1), clock::get, diagnostics)) {
            sampler.start();
            BenchmarkResourceSampler.Summary summary = sampler.finish();
            assertThat(summary.cpuNanos()).isEqualTo(50);
            assertThat(summary.gcCollectionMillis()).isEqualTo(1);
            assertThat(summary.sampleCount()).isEqualTo(2);
            var trace = summary.sampling().orElseThrow();
            assertThat(trace.totalDurationNanos()).isEqualTo(40);
            assertThat(trace.retainedAttempts()).extracting(BenchmarkResourceSampler.Attempt::startedAtNanos)
                    .containsExactly(100L, 150L);
            assertThat(trace.retainedAttempts()).extracting(BenchmarkResourceSampler.Attempt::completedAtNanos)
                    .containsExactly(120L, 170L);
            assertThat(diagnostics.evidence().totalDurationNanos()).isEqualTo(60);
            assertThat(diagnostics.evidence().retainedAttempts())
                    .extracting(BenchmarkCompilationDiagnostics.Attempt::startedAtNanos)
                    .containsExactly(120L, 170L);
            assertThat(diagnostics.evidence().retainedAttempts())
                    .extracting(BenchmarkCompilationDiagnostics.Attempt::completedAtNanos)
                    .containsExactly(150L, 200L);
            assertThat(sampler.compilationEvidence()).isPresent();
        }
    }

    @Test
    void unsupportedCompilationDoesNotInventZeroOrInvalidateCompleteResources() {
        BenchmarkCompilationDiagnostics diagnostics = BenchmarkCompilationDiagnostics.from(() ->
                new BenchmarkProcessProbe.CompilationReading(42, "test-compiler", false,
                        OptionalLong.empty(), BenchmarkProcessProbe.CompilationState.UNKNOWN,
                        BenchmarkProcessProbe.CompilationUnknownReason.MONITORING_UNSUPPORTED, null),
                System::nanoTime, 8);
        AtomicInteger resources = new AtomicInteger();
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.fromWithCompilation(
                () -> resource(resources.incrementAndGet()), Duration.ofHours(1), System::nanoTime, diagnostics)) {
            sampler.start();
            assertThat(sampler.finish().sampleCount()).isEqualTo(2);
            assertThat(diagnostics.evidence().unknownCount()).isEqualTo(2);
            assertThat(diagnostics.evidence().retainedAttempts()).allSatisfy(attempt ->
                    assertThat(attempt.reading().totalCompilationMillis()).isEmpty());
            assertThat(sampler.compilationEvidence().orElseThrow()).containsEntry("state", "UNKNOWN");
        }
    }

    @Test
    void ordinarySamplerHasNoCompilationEvidence() {
        AtomicInteger calls = new AtomicInteger();
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.from(
                () -> resource(calls.incrementAndGet()), Duration.ofHours(1))) {
            sampler.start();
            assertThat(sampler.finish().sampleCount()).isEqualTo(2);
            assertThat(calls).hasValue(2);
            assertThat(sampler.compilationEvidence()).isEmpty();
        }
    }

    @Test
    void failedResourceReadDoesNotStartAnOptionalCompilationRpc() {
        AtomicInteger compilationCalls = new AtomicInteger();
        BenchmarkCompilationDiagnostics diagnostics = BenchmarkCompilationDiagnostics.from(() -> {
            compilationCalls.incrementAndGet();
            return supported(1);
        }, System::nanoTime, 8);
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.fromWithCompilation(() ->
                new BenchmarkProcessProbe.Snapshot(OptionalLong.empty(), OptionalLong.empty(),
                        OptionalLong.empty(), OptionalLong.empty()),
                Duration.ofHours(1), System::nanoTime, diagnostics)) {
            sampler.start();
            assertThat(compilationCalls).hasValue(0);
            assertThatThrownBy(sampler::finish).isInstanceOf(BenchmarkResourceSampler.SamplingFailure.class)
                    .hasMessageContaining("READ_UNAVAILABLE");
            assertThat(diagnostics.evidence().attemptCount()).isZero();
        }
    }

    @Test
    void periodicCompilerInvariantCannotDisappearWhenTheFinalReadWouldSucceed() throws Exception {
        AtomicInteger compilationCalls = new AtomicInteger();
        CountDownLatch periodicRead = new CountDownLatch(1);
        AssertionError invariant = new AssertionError("periodic compiler invariant");
        BenchmarkCompilationDiagnostics diagnostics = BenchmarkCompilationDiagnostics.from(() -> {
            int call = compilationCalls.incrementAndGet();
            if (call == 2) {
                periodicRead.countDown();
                throw invariant;
            }
            return supported(call);
        }, System::nanoTime, 8);
        AtomicInteger resources = new AtomicInteger();
        try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.fromWithCompilation(
                () -> resource(resources.incrementAndGet()), Duration.ofMillis(10), System::nanoTime, diagnostics)) {
            sampler.start();
            assertThat(periodicRead.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(catchThrowable(sampler::finish)).isSameAs(invariant);
            assertThat(diagnostics.evidence().errorCount()).isEqualTo(1);
        }
    }

    private static BenchmarkProcessProbe.Snapshot resource(int index) {
        return new BenchmarkProcessProbe.Snapshot(OptionalLong.of(100 + (index - 1) * 50L),
                OptionalLong.of(1_000), OptionalLong.of(2_000), OptionalLong.of(index));
    }

    private static BenchmarkProcessProbe.CompilationReading supported(long millis) {
        return new BenchmarkProcessProbe.CompilationReading(42, "test-compiler", true,
                OptionalLong.of(millis), BenchmarkProcessProbe.CompilationState.SUCCESS,
                BenchmarkProcessProbe.CompilationUnknownReason.NONE, null);
    }
}
