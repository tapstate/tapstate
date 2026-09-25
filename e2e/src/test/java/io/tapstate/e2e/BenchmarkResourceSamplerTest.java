package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Resource comparisons use complete external readings and the largest sampled memory values. */
class BenchmarkResourceSamplerTest {

    @Test
    void computesCounterDeltasAndPeakMemoryAcrossTheWholeWindow() {
        BenchmarkResourceSampler.Summary summary = BenchmarkResourceSampler.summarize(List.of(
                reading(100, 20, 1_000, 2_000),
                reading(125, 21, 1_500, 2_500),
                reading(170, 23, 1_200, 2_200)));

        assertThat(summary.cpuNanos()).isEqualTo(70);
        assertThat(summary.gcPauseMillis()).isEqualTo(3);
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
        }
    }

    private static BenchmarkProcessProbe.Snapshot reading(long cpu, long gc, long heap, long rss) {
        return new BenchmarkProcessProbe.Snapshot(
                OptionalLong.of(cpu), OptionalLong.of(heap), OptionalLong.of(rss), OptionalLong.of(gc));
    }
}
