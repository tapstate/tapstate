package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkUnreadSamplerTest {
    @Test
    void closeStopsOnlyItsOwnedReadsAndRetainsUnknownInsteadOfInventingZero() throws Exception {
        var calls = new AtomicInteger();
        var secondRead = new CountDownLatch(1);
        var sampler = new BenchmarkUnreadSampler(() -> {
            if (calls.incrementAndGet() == 2) { secondRead.countDown(); }
            return List.of(Map.of("state", "UNKNOWN"));
        });
        try {
            assertThatThrownBy(sampler::samples).isInstanceOf(IllegalStateException.class);
            assertThat(secondRead.await(2, TimeUnit.SECONDS)).isTrue();
        } finally { sampler.close(); }
        sampler.close();
        assertThat(sampler.samples()).hasSizeGreaterThanOrEqualTo(2).allSatisfy(sample ->
                assertThat(sample).containsEntry("state", "UNKNOWN").doesNotContainKey("unreadRecords"));
        int ended = calls.get();
        assertThat(sampler.samples()).hasSize(ended);
    }

    @Test
    void aFailedPointReadRemainsAVisibleFailureAfterShutdown() {
        var calls = new AtomicInteger();
        var sampler = new BenchmarkUnreadSampler(() -> {
            calls.incrementAndGet();
            throw new IllegalStateException("owned diagnostic read failed");
        });
        sampler.close();
        assertThat(calls).hasValue(1);
        assertThatThrownBy(sampler::samples).isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("owned diagnostic read failed");
    }
}
