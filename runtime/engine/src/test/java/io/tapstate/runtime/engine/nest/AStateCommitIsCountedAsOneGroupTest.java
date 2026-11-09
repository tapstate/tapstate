package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.StateCommitReading;
import io.tapstate.spi.store.StoreRoundTrips;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The unit the state commit metrics count in is one durable store write: however many keys it carried, it is
 * one group, the round trips the store reported while making it are that group's, and a write that failed is
 * no group at all. What is counted reaches the published reading whole, or - with any part missing - not at
 * all.
 */
class AStateCommitIsCountedAsOneGroupTest {

    private static final String NS = "nest.p.doc.$root";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("state-commit-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void oneWriteIsOneGroupCarryingItsKeysAndTheRoundTripsItMade() {
        List<long[]> groups = new ArrayList<>();

        NestStateIo.commit((keys, trips, nanos) -> groups.add(new long[] {keys, trips, nanos}), 40, () -> {
            StoreRoundTrips.observed();
            StoreRoundTrips.observed();
        });

        assertThat(groups).singleElement().satisfies(group -> {
            assertThat(group[0]).isEqualTo(40L);
            assertThat(group[1]).isEqualTo(2L);
            assertThat(group[2]).isNotNegative();
        });
    }

    @Test
    void aRoundTripOutsideACommitIsNobodysAndAFailedWriteIsNoGroup() {
        StoreRoundTrips.observed();
        List<long[]> groups = new ArrayList<>();

        assertThatThrownBy(() -> NestStateIo.commit((keys, trips, nanos) -> groups.add(new long[] {keys}), 3,
                () -> {
                    StoreRoundTrips.observed();
                    throw new IllegalStateException("store refused");
                })).isInstanceOf(IllegalStateException.class);
        NestStateIo.commit((keys, trips, nanos) -> groups.add(new long[] {keys, trips}), 1, () -> { });

        assertThat(groups).singleElement().satisfies(group -> assertThat(group).containsExactly(1L, 0L));
    }

    @Test
    void countedGroupsFallIntoTheRegisteredBuckets() {
        NestStateStats stats = NestStateStats.of(member);
        assertThat(stats.commits(NS)).as("nothing committed is absent, not zero").isNull();

        stats.committed(NS, 1, 1, 2_000_000L);
        stats.committed(NS, 600, 3, 40_000_000L);

        NestStateStats.Commits commits = stats.commits(NS);
        assertThat(commits.groups()).isEqualTo(2L);
        assertThat(commits.keySum()).isEqualTo(601L);
        assertThat(commits.roundTripSum()).isEqualTo(4L);
        assertThat(commits.durationNanos()).isEqualTo(42_000_000L);
        assertThat(commits.keyBuckets()).hasSize(HistogramBounds.STATE_COMMIT_KEYS.buckets());
        assertThat(sum(commits.keyBuckets())).isEqualTo(2L);
        assertThat(commits.keyBuckets()[0]).as("one key is at or under the first bound").isEqualTo(1L);
        assertThat(commits.keyBuckets()[commits.keyBuckets().length - 1])
                .as("600 keys is past 512 and under 1000, so not the overflow bucket").isZero();
    }

    @Test
    void theReadingCarriesTheCommitsOnlyWhenEveryPartArrived() {
        NestStateStats stats = NestStateStats.of(member);
        stats.committed(NS, 4, 1, 1_000_000L);
        Map<String, Long> kinds = kinds(stats.commits(NS), 7L);

        NestStateReading reading = NestStateMetricNames.readingFrom(kinds, OptionalLong.empty());
        StateCommitReading commits = reading.commits();
        assertThat(commits.groups()).isOne();
        assertThat(commits.evictions()).isEqualTo(7L);
        assertThat(commits.keys().sum()).isEqualTo(4.0);
        assertThat(commits.duration().sum()).isEqualTo(0.001);
        assertThat(commits.countingSince()).isBefore(Instant.now().plusSeconds(1));

        kinds.remove(NestStateMetricNames.COMMIT_NANOS_BUCKET + 3);
        assertThat(NestStateMetricNames.readingFrom(kinds, OptionalLong.empty()).commits()).isNull();
    }

    /** The kinds a gauge publishes for these commits, spelled as the engine reads them back. */
    private static Map<String, Long> kinds(NestStateStats.Commits commits, long evictions) {
        Map<String, Long> kinds = new HashMap<>();
        kinds.put(NestStateMetricNames.ENTRIES, 1L);
        kinds.put(NestStateMetricNames.ACCESSES, 1L);
        kinds.put(NestStateMetricNames.BACKFILLS, 0L);
        kinds.put(NestStateMetricNames.BACKFILL_MILLIS, 0L);
        kinds.put(NestStateMetricNames.COMMIT_GROUPS, commits.groups());
        kinds.put(NestStateMetricNames.COMMIT_KEYS_SUM, commits.keySum());
        kinds.put(NestStateMetricNames.COMMIT_ROUND_TRIPS_SUM, commits.roundTripSum());
        kinds.put(NestStateMetricNames.COMMIT_NANOS_SUM, commits.durationNanos());
        kinds.put(NestStateMetricNames.COMMIT_SINCE, commits.countingSinceMillis());
        kinds.put(NestStateMetricNames.EVICTIONS, evictions);
        put(kinds, NestStateMetricNames.COMMIT_KEYS_BUCKET, commits.keyBuckets());
        put(kinds, NestStateMetricNames.COMMIT_ROUND_TRIPS_BUCKET, commits.roundTripBuckets());
        put(kinds, NestStateMetricNames.COMMIT_NANOS_BUCKET, commits.durationBuckets());
        return kinds;
    }

    private static void put(Map<String, Long> kinds, String prefix, long[] buckets) {
        for (int i = 0; i < buckets.length; i++) {
            kinds.put(prefix + i, buckets[i]);
        }
    }

    private static long sum(long[] values) {
        long total = 0;
        for (long value : values) {
            total += value;
        }
        return total;
    }
}
