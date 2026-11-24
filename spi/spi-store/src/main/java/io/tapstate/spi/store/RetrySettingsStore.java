package io.tapstate.spi.store;

import io.tapstate.core.common.RetryPolicy;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** The cluster-owned retry policy. Revision zero is the unconfigured cluster. */
public interface RetrySettingsStore {
    String SNAPSHOT_READ = "snapshot-read";
    String STREAM_READ = "stream-read";
    RetryPolicy DEFAULT = new RetryPolicy(true, Duration.ofSeconds(60), Duration.ofHours(8));

    record OperationOverride(Boolean enabled, Duration interval, Duration maxDuration) {
        public RetryPolicy resolve(RetryPolicy defaults) {
            return new RetryPolicy(defaults.enabled() && !Boolean.FALSE.equals(enabled),
                    interval == null ? defaults.interval() : interval,
                    maxDuration == null ? defaults.maxDuration() : maxDuration);
        }
    }

    record Settings(long revision, RetryPolicy defaults, Map<String, OperationOverride> operations) {
        public Settings {
            if (revision < 0) throw new IllegalArgumentException("Retry settings revision must be nonnegative");
            Objects.requireNonNull(defaults, "default retry policy");
            operations = Map.copyOf(operations);
            operations.forEach((operation, override) -> {
                if (!operation.equals(SNAPSHOT_READ) && !operation.equals(STREAM_READ)) {
                    throw new IllegalArgumentException("Unknown retry operation " + operation);
                }
                override.resolve(defaults);
            });
        }

        public RetryPolicy policy(String operation) {
            var override = operations.get(operation);
            return override == null ? defaults : override.resolve(defaults);
        }

        public static Settings unconfigured() {
            return new Settings(0, DEFAULT, Map.of());
        }
    }

    Settings read();

    Optional<Settings> replace(long expectedRevision, RetryPolicy defaults, Map<String, OperationOverride> operations);
}
