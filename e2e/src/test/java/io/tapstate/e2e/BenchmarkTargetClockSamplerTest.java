package io.tapstate.e2e;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTargetClockSamplerTest {
    @Test void ownedSamplingStopsWithoutErasingItsClockReadFailure() {
        var calls = new AtomicInteger();
        var closed = new AtomicInteger();
        var sampler = new BenchmarkTargetClockSampler(() -> {
            calls.incrementAndGet(); throw new IllegalStateException("owned clock unavailable");
        }, closed::incrementAndGet);
        sampler.close(); sampler.close();
        assertThat(calls).hasValue(1);
        assertThat(closed).hasValue(1);
        assertThatThrownBy(sampler::evidence).isInstanceOf(AssertionError.class).hasRootCauseMessage("owned clock unavailable");
    }

    @Test void fixedCadencePreservesActualReadBracketsAndItsCommandCount() throws Exception {
        var calls = new AtomicInteger();
        var closed = new AtomicInteger();
        var third = new CountDownLatch(1);
        long anchor = System.nanoTime();
        var sampler = new BenchmarkTargetClockSampler(() -> {
            long began = System.nanoTime(), elapsed = (began - anchor) / 1_000_000L;
            if (calls.incrementAndGet() >= 3) { third.countDown(); }
            return new BenchmarkTargetClock.Reading("owned:27017", "one", 1_000 + elapsed,
                    began, System.nanoTime(), 1_000 + elapsed, 1_000 + elapsed);
        }, closed::incrementAndGet);
        try {
            assertThatThrownBy(sampler::evidence).isInstanceOf(IllegalStateException.class);
            assertThat(third.await(2, TimeUnit.SECONDS)).isTrue();
        } finally { sampler.close(); }
        assertThat(sampler.evidence()).containsEntry("helloCommands", calls.get()).containsEntry("setupPingCommands", 1);
        assertThat(closed).hasValue(1);
    }
}
