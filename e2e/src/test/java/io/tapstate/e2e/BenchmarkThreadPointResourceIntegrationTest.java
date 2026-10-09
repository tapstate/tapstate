package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Optional thread points preserve resource boundaries and cannot hide their own programmer failures. */
class BenchmarkThreadPointResourceIntegrationTest {
    @Test
    void anOrdinaryResourceSamplerHasNoThreadPointEvidence() {
        AtomicInteger count = new AtomicInteger();
        try (var sampler = BenchmarkResourceSampler.from(() -> resource(count.incrementAndGet()), Duration.ofHours(1))) {
            sampler.start();
            assertThat(sampler.finish().sampleCount()).isEqualTo(2);
            assertThat(sampler.threadPointEvidence()).isEmpty();
        }
    }

    @Test
    void aFailedResourceReadDoesNotStartOptionalThreadPreparation() {
        var reader = new BenchmarkThreadPointDiagnosticsTest.FakeReader();
        var diagnostics = BenchmarkThreadPointDiagnostics.prepare(reader, System::nanoTime);
        try (var sampler = BenchmarkResourceSampler.fromWithThreadPoints(() -> {
            throw new IllegalStateException("resource unavailable");
        }, Duration.ofHours(1), System::nanoTime, diagnostics)) {
            sampler.start();
            assertThatThrownBy(sampler::finish).isInstanceOf(BenchmarkResourceSampler.SamplingFailure.class);
            assertThat(reader.calls).isEmpty();
        }
    }

    @Test
    void optionalRpcTimeDoesNotEnterTheOriginalResourceReadDuration() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger count = new AtomicInteger();
        CountDownLatch pointsSeen = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        var reader = new BenchmarkThreadPointDiagnosticsTest.FakeReader() {
            @Override public BenchmarkThreadPointDiagnostics.Identity identity() {
                clock.addAndGet(30);
                return super.identity();
            }
            @Override public long[] cpuNanos(long[] ids) {
                clock.addAndGet(40);
                long[] values = super.cpuNanos(ids);
                pointsSeen.countDown();
                return values;
            }
        };
        var diagnostics = BenchmarkThreadPointDiagnostics.prepare(reader, clock::get);
        try (var sampler = BenchmarkResourceSampler.fromWithThreadPoints(() -> {
            int next = count.incrementAndGet();
            if (next == 34) { await(release); }
            clock.addAndGet(20);
            return resource(next);
        }, Duration.ofMillis(1), clock::get, diagnostics)) {
            sampler.start();
            try {
                assertThat(pointsSeen.await(5, TimeUnit.SECONDS)).isTrue();
            } finally { release.countDown(); }
            var summary = sampler.finish();
            assertThat(summary.sampling().orElseThrow().retainedAttempts())
                    .extracting(BenchmarkResourceSampler.Attempt::durationNanos).containsOnly(20L);
            assertThat(summary.cpuNanos()).isEqualTo((summary.sampleCount() - 1) * 50L);
            assertThat(reader.cpuCalls).isBetween(2, 4);
            assertThat(sampler.threadPointEvidence()).isPresent();
        }
    }

    @Test
    void aPeriodicDiagnosticErrorEscapesFinishAsTheOriginalObject() throws Exception {
        AssertionError invariant = new AssertionError("periodic owned thread invariant");
        CountDownLatch readerEntered = new CountDownLatch(1);
        var reader = new BenchmarkThreadPointDiagnosticsTest.FakeReader() {
            @Override public long[] cpuNanos(long[] ids) {
                readerEntered.countDown();
                throw invariant;
            }
        };
        AtomicInteger count = new AtomicInteger();
        var diagnostics = BenchmarkThreadPointDiagnostics.prepare(reader, System::nanoTime);
        try (var sampler = BenchmarkResourceSampler.fromWithThreadPoints(() -> resource(count.incrementAndGet()),
                Duration.ofMillis(1), System::nanoTime, diagnostics)) {
            sampler.start();
            assertThat(readerEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(catchThrowable(sampler::finish)).isSameAs(invariant);
        }
    }

    private static BenchmarkProcessProbe.Snapshot resource(int index) {
        return new BenchmarkProcessProbe.Snapshot(OptionalLong.of(100 + (index - 1) * 50L),
                OptionalLong.of(1_000), OptionalLong.of(2_000), OptionalLong.of(index));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) { throw new AssertionError("test resource was not released"); }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
