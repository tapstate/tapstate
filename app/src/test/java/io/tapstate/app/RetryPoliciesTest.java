package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RetrySettingsStore;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RetryPoliciesTest {
    @Test
    void aTemporaryConfigFailurePreservesTheLastPublishedPolicy() {
        var time = new AtomicLong();
        var reads = new AtomicInteger();
        var store = new RetrySettingsStore() {
            @Override public Settings read() {
                return switch (reads.incrementAndGet()) {
                    case 1 -> new Settings(1, DEFAULT, Map.of());
                    case 2 -> throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "temporary"), null);
                    default -> new Settings(2, DEFAULT.disabled(), Map.of());
                };
            }

            @Override public Optional<Settings> replace(long revision, RetryPolicy policy,
                    Map<String, OperationOverride> operations) {
                throw new UnsupportedOperationException();
            }
        };
        var policies = new RetryPolicies(store, time::get);
        assertThat(policies.policy(RetrySettingsStore.STREAM_READ).enabled()).isTrue();
        time.addAndGet(Duration.ofSeconds(10).toNanos());
        assertThat(policies.policy(RetrySettingsStore.STREAM_READ).enabled()).isTrue();
        time.addAndGet(Duration.ofSeconds(10).toNanos());
        assertThat(policies.policy(RetrySettingsStore.STREAM_READ).enabled()).isFalse();
    }
}
