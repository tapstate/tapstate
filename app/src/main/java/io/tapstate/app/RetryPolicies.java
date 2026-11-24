package io.tapstate.app;

import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.RetrySettingsStore;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Refreshes the cluster's policy once per short interval while a retry needs it. */
final class RetryPolicies {
    private static final Logger LOG = LoggerFactory.getLogger(RetryPolicies.class);
    private static final long REFRESH_NANOS = Duration.ofSeconds(5).toNanos();
    private final RetrySettingsStore store;
    private RetrySettingsStore.Settings current;
    private final LongSupplier clock;
    private long refreshAt;

    RetryPolicies(RetrySettingsStore store) {
        this(store, System::nanoTime);
    }

    RetryPolicies(RetrySettingsStore store, LongSupplier clock) {
        this.store = store;
        this.clock = clock;
        refreshAt = clock.getAsLong();
    }

    synchronized RetryPolicy policy(String operation) {
        long now = clock.getAsLong();
        if (now - refreshAt >= 0) {
            refreshAt = now + REFRESH_NANOS;
            try {
                current = store.read();
            } catch (TapstateException unavailable) {
                LOG.warn("Retry settings are unavailable [{}]", unavailable.code().code());
                if (unavailable.code() != io.tapstate.spi.store.IoError.STORE_UNAVAILABLE) current = null;
            }
        }
        return current == null ? RetrySettingsStore.DEFAULT.disabled() : current.policy(operation);
    }
}
