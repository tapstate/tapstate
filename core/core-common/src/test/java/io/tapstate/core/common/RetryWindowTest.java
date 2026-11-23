package io.tapstate.core.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RetryWindowTest {
    @Test
    void aChangedWindowStillStartsWithTheOriginalFailure() {
        var time = new AtomicLong();
        var window = new RetryWindow(time::get);
        var policy = new RetryPolicy(true, Duration.ofSeconds(2), Duration.ofSeconds(8));
        window.failed();
        assertThat(window.delay(policy)).contains(Duration.ofSeconds(2));
        time.set(Duration.ofSeconds(3).toNanos());
        assertThat(window.delay(policy)).contains(Duration.ZERO);
        window.failed();
        time.set(Duration.ofSeconds(7).toNanos());
        window.failed();
        assertThat(window.delay(policy)).isEmpty();
        assertThat(window.delay(new RetryPolicy(true, policy.interval(), Duration.ofSeconds(12)))).contains(policy.interval());
        assertThat(window.delay(policy.disabled())).isEmpty();
        window.recovered();
        window.failed();
        assertThat(window.delay(policy)).contains(policy.interval());
    }

    @Test
    void aWrappedOperationRunsPreparationAndReturnsTheRetryResult() throws Throwable {
        var calls = new AtomicInteger();
        var preparations = new AtomicInteger();
        var policy = new RetryPolicy(true, Duration.ofMillis(1), Duration.ofSeconds(3));
        var result = RetryWindow.execute(() -> policy,
                failure -> Optional.of(preparations::incrementAndGet), () -> {}, () -> {
                    if (calls.incrementAndGet() == 1) throw new IOException("transient");
                    return "done";
                });
        assertThat(result).isEqualTo("done");
        assertThat(calls).hasValue(2);
        assertThat(preparations).hasValue(1);
    }

    @Test
    void disabledWrapperPreservesTheCauseAndNeverPreparesAnotherAttempt() {
        var failure = new IOException("source failed");
        var policy = new RetryPolicy(false, Duration.ofMillis(1), Duration.ofSeconds(3));
        assertThatThrownBy(() -> RetryWindow.execute(() -> policy,
                cause -> { throw new AssertionError("retry classifier invoked"); }, () -> {}, () -> { throw failure; }))
                .isSameAs(failure);
    }
}
