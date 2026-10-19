package io.tapstate.app;

import io.tapstate.control.core.CloudClusterEgressAddress;
import io.tapstate.control.core.CloudClusterEgressAddressProvider;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CachedCloudClusterEgressAddressProviderTest {

    private static final String ORGANIZATION = "organization-one";
    private static final String CLUSTER = "cluster-one";
    private static final Instant NOW = Instant.parse("2026-10-19T08:00:00Z");

    @Test
    void cachesRefreshesAndServesOnlyBoundedStaleAddressesDuringProviderFailures() {
        MutableClock clock = new MutableClock(NOW);
        AtomicInteger providerCalls = new AtomicInteger();
        AtomicBoolean available = new AtomicBoolean(true);
        AtomicReference<CloudClusterEgressAddress> current = new AtomicReference<>(
                address("203.0.113.42", "DYNAMIC"));
        CloudClusterEgressAddressProvider provider = (organizationId, clusterId) -> {
            providerCalls.incrementAndGet();
            return available.get() ? Optional.of(current.get()) : Optional.empty();
        };
        CachedCloudClusterEgressAddressProvider cached = new CachedCloudClusterEgressAddressProvider(
                provider, clock, Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(60));

        assertThat(cached.currentAddress(ORGANIZATION, CLUSTER)).contains(address("203.0.113.42", "DYNAMIC"));
        clock.advance(Duration.ofSeconds(29));
        assertThat(cached.currentAddress(ORGANIZATION, CLUSTER)).contains(address("203.0.113.42", "DYNAMIC"));
        assertThat(providerCalls).hasValue(1);

        current.set(address("203.0.113.43", "STABLE"));
        clock.advance(Duration.ofSeconds(1));
        assertThat(cached.currentAddress(ORGANIZATION, CLUSTER)).contains(address("203.0.113.43", "STABLE"));
        assertThat(providerCalls).hasValue(2);

        available.set(false);
        clock.advance(Duration.ofSeconds(30));
        assertThat(cached.currentAddress(ORGANIZATION, CLUSTER)).contains(address("203.0.113.43", "STABLE"));
        clock.advance(Duration.ofSeconds(9));
        assertThat(cached.currentAddress(ORGANIZATION, CLUSTER)).contains(address("203.0.113.43", "STABLE"));
        assertThat(providerCalls).hasValue(3);
        clock.advance(Duration.ofSeconds(1));
        assertThat(cached.currentAddress(ORGANIZATION, CLUSTER)).contains(address("203.0.113.43", "STABLE"));
        assertThat(providerCalls).hasValue(4);

        clock.advance(Duration.ofSeconds(60));
        assertThat(cached.currentAddress(ORGANIZATION, CLUSTER)).isEmpty();
        assertThat(providerCalls).hasValue(5);
    }

    private static CloudClusterEgressAddress address(String ip, String status) {
        return new CloudClusterEgressAddress(ORGANIZATION, CLUSTER, ip, status);
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }
    }
}
