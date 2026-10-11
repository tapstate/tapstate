package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest;
import io.tapstate.spi.capture.BoundedSnapshotQueryResult;
import io.tapstate.spi.capture.FieldSchema;
import io.tapstate.spi.capture.TableSchema;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class PreviewSampleCacheTest {

    @Test
    void boundsClusterCacheUnderSequentialAndConcurrentWrites() throws Exception {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("preview-cache-test-" + UUID.randomUUID());
        properties.setMemberPort(0);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            PreviewSampleCache cache = new PreviewSampleCache(member);
            BoundedSnapshotQueryRequest request = request();
            Instant sampledAt = Instant.now();

            for (int index = 0; index < PreviewSampleCache.MAX_ENTRIES + 2; index++) {
                cache.put("principal", "pipeline", "sample-" + index, "connector-v1", request,
                        result(index, sampledAt));
            }

            assertThat(member.<String, PreviewSampleCache.Entry>getMap(PreviewSampleCache.MAP_NAME).size())
                    .isEqualTo(PreviewSampleCache.MAX_ENTRIES);
            assertThat(member.<String, Long>getMap(PreviewSampleCache.INDEX_MAP_NAME).size())
                    .isEqualTo(PreviewSampleCache.MAX_ENTRIES);

            cache.put("principal", "pipeline", "sample-current", "connector-v1", request,
                    result(99, sampledAt));
            assertThat(member.<String, PreviewSampleCache.Entry>getMap(PreviewSampleCache.MAP_NAME).size())
                    .isEqualTo(PreviewSampleCache.MAX_ENTRIES);
            cache.put("principal", "pipeline", "sample-current", "connector-v1", request,
                    result(100, sampledAt));
            assertThat(member.<String, PreviewSampleCache.Entry>getMap(PreviewSampleCache.MAP_NAME).size())
                    .isEqualTo(PreviewSampleCache.MAX_ENTRIES);
            assertThat(cache.get("principal", "pipeline", "sample-current", "connector-v1", request).rows())
                    .extracting(Envelope::after)
                    .containsExactly(Map.of("id", 100));

            CountDownLatch start = new CountDownLatch(1);
            try (var writers = Executors.newFixedThreadPool(4)) {
                List<Future<Object>> writes = java.util.stream.IntStream.range(0, 32)
                        .mapToObj(index -> writers.submit(() -> {
                            start.await();
                            cache.put("principal", "pipeline", "parallel-" + index, "connector-v1", request,
                                    result(index, sampledAt));
                            return null;
                        }))
                        .toList();
                start.countDown();
                for (Future<?> write : writes) {
                    write.get();
                }
            }
            assertThat(member.<String, PreviewSampleCache.Entry>getMap(PreviewSampleCache.MAP_NAME).size())
                    .isEqualTo(PreviewSampleCache.MAX_ENTRIES);
            assertThat(member.<String, Long>getMap(PreviewSampleCache.INDEX_MAP_NAME).size())
                    .isEqualTo(PreviewSampleCache.MAX_ENTRIES);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void skipsUnrepeatableOrNonSnapshotRowsAndPrunesOrphanedIndexes() {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("preview-cache-prune-" + UUID.randomUUID());
        properties.setMemberPort(0);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            PreviewSampleCache cache = new PreviewSampleCache(member);
            BoundedSnapshotQueryRequest request = request();
            Instant sampledAt = Instant.now();
            BoundedSnapshotQueryResult unstable = new BoundedSnapshotQueryResult(
                    List.of(Envelope.read(1, "source.orders", Map.of("id", 1), Map.of())),
                    true, false, false, 1, sampledAt);
            cache.put("principal", "pipeline", "unstable", "connector-v1", request, unstable);
            BoundedSnapshotQueryResult changeEvent = new BoundedSnapshotQueryResult(
                    List.of(Envelope.delete(1, "source.orders", Map.of("id", 2), Map.of())),
                    true, false, true, 1, sampledAt);
            cache.put("principal", "pipeline", "change-event", "connector-v1", request, changeEvent);
            assertThat(cache.get("principal", "pipeline", "unstable", "connector-v1", request)).isNull();
            assertThat(cache.get("principal", "pipeline", "change-event", "connector-v1", request)).isNull();

            member.<String, Long>getMap(PreviewSampleCache.INDEX_MAP_NAME)
                    .put("orphan", System.currentTimeMillis());
            cache.put("principal", "pipeline", "stable", "connector-v1", request, result(3, sampledAt));
            assertThat(member.<String, Long>getMap(PreviewSampleCache.INDEX_MAP_NAME).containsKey("orphan"))
                    .isFalse();
            assertThat(cache.get("missing", "pipeline", "stable", "connector-v1", request)).isNull();
        } finally {
            member.shutdown();
        }
    }

    private static BoundedSnapshotQueryRequest request() {
        return new BoundedSnapshotQueryRequest("source", "csv", Map.of(),
                new TableSchema("orders", List.of(new FieldSchema("id", "integer"))),
                List.of("id"), new BoundedSnapshotQueryRequest.AllRows(), List.of("id"), 1,
                Instant.now().plusSeconds(30));
    }

    private static BoundedSnapshotQueryResult result(int id, Instant sampledAt) {
        return new BoundedSnapshotQueryResult(
                List.of(Envelope.read(sampledAt.toEpochMilli(), "source.orders", Map.of("id", id), Map.of())),
                true, false, true, 1, sampledAt);
    }
}
