package io.tapstate.e2e;

import io.tapstate.core.lifecycle.PipelineState;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AMemberKilledMidWriteLeavesNothingHeldTest {
    @Test void dataDeliveryCanPrecedeTheRecoveredRunningObservation() {
        var reads = new AtomicInteger();
        AMemberKilledMidWriteLeavesNothingHeldIT.assertRecoveredRunning(System.nanoTime() + Duration.ofSeconds(2).toNanos(),
                () -> reads.getAndIncrement() == 0 ? Optional.empty() : Optional.of(PipelineState.RUNNING));
        assertThat(reads).hasValue(2);
    }

    @Test void aRunningResponseArrivingAfterTheOriginalDeadlineCannotPass() {
        var now = new AtomicLong();
        var reads = new AtomicInteger();
        assertThatThrownBy(() -> AMemberKilledMidWriteLeavesNothingHeldIT.assertRecoveredRunning(4,
                () -> { reads.incrementAndGet(); now.set(5); return Optional.of(PipelineState.RUNNING); }, now::get))
                .isInstanceOf(AssertionError.class);
        assertThat(reads).hasValue(1);
    }
}
