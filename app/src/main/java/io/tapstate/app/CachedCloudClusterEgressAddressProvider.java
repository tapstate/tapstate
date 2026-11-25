package io.tapstate.app;

import io.tapstate.control.core.CloudClusterEgressAddress;
import io.tapstate.control.core.CloudClusterEgressAddressProvider;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Bounds Cloud address lookups while retaining a recent value during short provider outages. */
final class CachedCloudClusterEgressAddressProvider implements CloudClusterEgressAddressProvider {

    static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);
    static final Duration FAILURE_RETRY_INTERVAL = Duration.ofSeconds(10);
    static final Duration MAX_STALE_AGE = Duration.ofMinutes(5);

    private final CloudClusterEgressAddressProvider provider;
    private final Clock clock;
    private final Duration refreshInterval;
    private final Duration failureRetryInterval;
    private final Duration maxStaleAge;
    private CachedAddress cached;
    private RefreshAttempt lastAttempt;

    CachedCloudClusterEgressAddressProvider(CloudClusterEgressAddressProvider provider, Clock clock) {
        this(provider, clock, REFRESH_INTERVAL, FAILURE_RETRY_INTERVAL, MAX_STALE_AGE);
    }

    CachedCloudClusterEgressAddressProvider(CloudClusterEgressAddressProvider provider, Clock clock,
            Duration refreshInterval, Duration failureRetryInterval, Duration maxStaleAge) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.refreshInterval = positive(refreshInterval, "refreshInterval");
        this.failureRetryInterval = positive(failureRetryInterval, "failureRetryInterval");
        this.maxStaleAge = positive(maxStaleAge, "maxStaleAge");
    }

    @Override
    public synchronized Optional<CloudClusterEgressAddress> currentAddress(String organizationId, String clusterId) {
        Instant now = clock.instant();
        if (matches(cached, organizationId, clusterId)
                && ageIsWithin(cached.fetchedAt(), now, refreshInterval)) {
            return Optional.of(cached.address());
        }
        if (lastAttempt != null && lastAttempt.matches(organizationId, clusterId)
                && ageIsWithin(lastAttempt.at(), now, failureRetryInterval)) {
            return staleAddress(organizationId, clusterId, now);
        }

        Optional<CloudClusterEgressAddress> refreshed = provider.currentAddress(organizationId, clusterId);
        Instant refreshedAt = clock.instant();
        lastAttempt = new RefreshAttempt(organizationId, clusterId, refreshedAt);
        if (refreshed.isPresent() && matches(refreshed.get(), organizationId, clusterId)) {
            cached = new CachedAddress(refreshed.get(), refreshedAt);
            return refreshed;
        }
        return staleAddress(organizationId, clusterId, refreshedAt);
    }

    private Optional<CloudClusterEgressAddress> staleAddress(String organizationId, String clusterId, Instant now) {
        return matches(cached, organizationId, clusterId)
                && ageIsWithin(cached.fetchedAt(), now, maxStaleAge)
                ? Optional.of(cached.address()) : Optional.empty();
    }

    private static boolean matches(CachedAddress cached, String organizationId, String clusterId) {
        return cached != null && matches(cached.address(), organizationId, clusterId);
    }

    private static boolean matches(CloudClusterEgressAddress address, String organizationId, String clusterId) {
        return address != null && Objects.equals(address.organizationId(), organizationId)
                && Objects.equals(address.clusterId(), clusterId);
    }

    private static boolean ageIsWithin(Instant since, Instant now, Duration limit) {
        return !now.isBefore(since) && Duration.between(since, now).compareTo(limit) < 0;
    }

    private static Duration positive(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return duration;
    }

    private record CachedAddress(CloudClusterEgressAddress address, Instant fetchedAt) {
    }

    private record RefreshAttempt(String organizationId, String clusterId, Instant at) {
        boolean matches(String expectedOrganizationId, String expectedClusterId) {
            return Objects.equals(organizationId, expectedOrganizationId)
                    && Objects.equals(clusterId, expectedClusterId);
        }
    }
}
