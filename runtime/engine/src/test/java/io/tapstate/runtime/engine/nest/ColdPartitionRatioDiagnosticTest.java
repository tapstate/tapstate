package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.NestStateWindow;
import io.tapstate.spi.store.KeyedStateStore;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/** Exercises actual partition loaders; no product counters are assigned by the fixture. */
class ColdPartitionRatioDiagnosticTest {
    @Test
    void oneBatchAcrossColdPartitionsMustNotPublishMoreThanOneHundredPercent() throws Exception {
        Config config = new Config();
        config.setClusterName("issue-904-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", "2");
        config.setProperty("hazelcast.io.input.thread.count", "1");
        config.setProperty("hazelcast.io.output.thread.count", "1");
        config.setProperty("hazelcast.event.thread.count", "1");
        config.getNetworkConfig().setPort(0);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        Backend backend = new Backend();
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            NestStateMapStoreFactory.bindTo(member, backend);
            List<Double> coldShares = new ArrayList<>();
            for (int repeat = 1; repeat <= 3; repeat++) {
                for (int partitions : List.of(1, 4)) {
                    String namespace = "nest.issue904.r" + repeat + ".p" + partitions;
                    List<Object> keys = keysIn(member, partitions, 8 / partitions);
                    for (Object key : keys) backend.seed(namespace, key, "value-" + key);
                    assertThat(member.getMap(namespace).getLocalMapStats().getOwnedEntryCount()).isZero();
                    Capture capture = new Capture();
                    MapNestStore<Object> store = new MapNestStore<>(member.getMap(namespace),
                            NestStateStats.of(member), capture);
                    int callsBefore = backend.calls(namespace);
                    Map<Object, Object> found = store.loadAll(keys);
                    assertLoadedValues(found, keys);
                    NestStateWindow cold = NestStateWindow.fromStart(capture.reading());
                    assertThat(cold.accesses()).isOne();
                    assertThat(cold.keyAccesses()).hasValue(8);
                    assertThat(cold.coldKeys()).hasValue(8);
                    assertThat(cold.backfills()).isEqualTo(partitions);
                    assertThat(backend.calls(namespace) - callsBefore).isBetween(1, partitions);
                    assertThat(backend.loadedKeys(namespace)).containsExactlyInAnyOrderElementsOf(
                            keys.stream().map(NestStateKeys::nameOf).toList());
                    assertThat(NestStateWindow.fromStart(capture.reading()).servedFromCold())
                            .isEqualTo(cold.servedFromCold());
                    emit(repeat, "cold", partitions, 8, cold, backend.calls(namespace) - callsBefore);
                    coldShares.add(cold.servedFromCold().orElseThrow());

                    NestStateReading before = capture.reading();
                    callsBefore = backend.calls(namespace);
                    assertThat(store.loadAll(keys)).isEqualTo(found);
                    NestStateWindow hot = NestStateWindow.between(before, capture.reading());
                    assertThat(hot.accesses()).isOne();
                    assertThat(hot.backfills()).isZero();
                    assertThat(hot.keyAccesses()).hasValue(8);
                    assertThat(hot.coldKeys()).hasValue(0);
                    assertThat(backend.calls(namespace)).isEqualTo(callsBefore);
                    assertThat(hot.servedFromCold()).hasValue(0.0);
                    emit(repeat, "hot", partitions, 0, hot, 0);
                }
                String namespace = "nest.issue904.r" + repeat + ".mixed";
                List<Object> keys = keysIn(member, 4, 2);
                for (Object key : keys) backend.seed(namespace, key, "value-" + key);
                Capture capture = new Capture();
                MapNestStore<Object> store = new MapNestStore<>(member.getMap(namespace),
                        NestStateStats.of(member), capture);
                assertThat(store.loadAll(keys.subList(0, 4))).hasSize(4);
                NestStateReading before = capture.reading();
                int callsBefore = backend.calls(namespace);
                assertThat(store.loadAll(keys)).hasSize(8);
                NestStateWindow mixed = NestStateWindow.between(before, capture.reading());
                assertThat(mixed.accesses()).isOne();
                assertThat(mixed.backfills()).isEqualTo(2);
                assertThat(mixed.keyAccesses()).hasValue(8);
                assertThat(mixed.coldKeys()).hasValue(4);
                assertThat(mixed.servedFromCold()).hasValue(0.5);
                assertThat(backend.calls(namespace) - callsBefore).isBetween(1, 2);
                emit(repeat, "half-hot", 4, 4, mixed, backend.calls(namespace) - callsBefore);
            }
            // The controls above establish complete successful loads and exact callback/key counts.
            // An exact share also rejects a denominator that hides cold work behind batch size.
            assertThat(coldShares).containsOnly(1.0);
        } finally {
            member.shutdown();
        }
    }

    private static void assertLoadedValues(Map<Object, Object> found, List<Object> keys) {
        assertThat(found).hasSize(8);
        keys.forEach(key -> assertThat(found).containsEntry(key, "value-" + key));
    }

    private static void emit(int repeat, String phase, int partitions, int coldKeys,
            NestStateWindow window, int backendCalls) {
        System.out.printf("ISSUE904 repeat=%d phase=%s partitions=%d requestedKeys=8 coldKeys=%d "
                        + "accesses=%d backfills=%d backendCalls=%d share=%.0f%% entries=%d%n",
                repeat, phase, partitions, coldKeys, window.accesses(), window.backfills(), backendCalls,
                window.servedFromCold().orElseThrow() * 100, window.entries());
    }

    @Test
    void emptyDuplicateAndAbsentReadsAndCarriedWritesKeepTheirUnits() throws Exception {
        Config config = new Config();
        config.setClusterName("key-units-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.getNetworkConfig().setPort(0);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            String namespace = "nest.issue904.units";
            NestStateMapStoreFactory.bindTo(member, new Backend());
            NestStateStats stats = NestStateStats.of(member);
            MapNestStore<Object> store = new MapNestStore<>(member.getMap(namespace), stats, new Capture());
            store.loadAll(List.of());
            assertThat(stats.knows(namespace)).isFalse();
            assertThat(store.loadAll(List.of("absent-a", "absent-a", "absent-b"))).isEmpty();
            assertThat(stats.counted(namespace).accesses()).isOne();
            assertThat(stats.counted(namespace).keyAccesses()).isEqualTo(2);
            assertThat(stats.counted(namespace).coldKeys()).isEqualTo(2);
            store.loadAll(List.of("absent-a", "absent-b"));
            assertThat(stats.counted(namespace).keyAccesses()).isEqualTo(4);
            assertThat(stats.counted(namespace).coldKeys()).isEqualTo(4);
            store.save("first-carried-write", "value");
            assertThat(stats.counted(namespace).keyAccesses()).isEqualTo(5);
            assertThat(stats.counted(namespace).coldKeys()).isEqualTo(5);
            store.save("known-absent", "value", true);
            assertThat(stats.counted(namespace).keyAccesses()).isEqualTo(6);
            assertThat(stats.counted(namespace).coldKeys()).isEqualTo(5);
            assertThat(store.load("first-carried-write")).isEqualTo("value");
            assertThat(stats.counted(namespace).keyAccesses()).isEqualTo(7);
            assertThat(stats.counted(namespace).coldKeys()).isEqualTo(5);
        } finally {
            member.shutdown();
        }
    }

    private static List<Object> keysIn(HazelcastInstance member, int partitions, int perPartition) {
        Map<Integer, List<Object>> grouped = new LinkedHashMap<>();
        for (int n = 0; grouped.size() < partitions
                || grouped.values().stream().anyMatch(keys -> keys.size() < perPartition); n++) {
            Object key = "key-" + n;
            int partition = member.getPartitionService().getPartition(key).getPartitionId();
            if (grouped.containsKey(partition) || grouped.size() < partitions) {
                List<Object> bucket = grouped.computeIfAbsent(partition, ignored -> new ArrayList<>());
                if (bucket.size() < perPartition) bucket.add(key);
            }
        }
        System.out.println("ISSUE904 partitionKeys=" + grouped);
        return grouped.values().stream().flatMap(Collection::stream).toList();
    }

    private static final class Capture implements NestStateGauge {
        private final Map<String, Long> kinds = new LinkedHashMap<>();
        @Override public void reading(String namespace, long entries, long accesses, long backfills,
                long millis, long pending) {
            kinds.putAll(Map.of(NestStateMetricNames.ENTRIES, entries, NestStateMetricNames.ACCESSES, accesses,
                    NestStateMetricNames.BACKFILLS, backfills, NestStateMetricNames.BACKFILL_MILLIS, millis,
                    NestStateMetricNames.PENDING_HIGH_WATER, pending));
        }
        @Override public void keyReading(String namespace, long keyAccesses, long coldKeys) {
            kinds.put(NestStateMetricNames.KEY_ACCESSES, keyAccesses);
            kinds.put(NestStateMetricNames.COLD_KEYS, coldKeys);
        }
        NestStateReading reading() {
            return NestStateMetricNames.readingFrom(kinds, OptionalLong.empty());
        }
    }

    private static final class Backend implements KeyedStateStore {
        private final Map<String, Map<String, byte[]>> values = new ConcurrentHashMap<>();
        private final Map<String, List<List<String>>> loads = new ConcurrentHashMap<>();
        void seed(String namespace, Object key, Object value) throws Exception {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) { out.writeObject(value); }
            save(namespace, NestStateKeys.nameOf(key), bytes.toByteArray());
        }
        synchronized int calls(String namespace) { return loads.getOrDefault(namespace, List.of()).size(); }
        synchronized List<String> loadedKeys(String namespace) {
            return loads.getOrDefault(namespace, List.of()).stream().flatMap(Collection::stream).toList();
        }
        @Override public Optional<byte[]> load(String namespace, String key) {
            return Optional.ofNullable(values.getOrDefault(namespace, Map.of()).get(key));
        }
        @Override public synchronized Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            loads.computeIfAbsent(namespace, ignored -> new ArrayList<>()).add(List.copyOf(keys));
            Map<String, byte[]> found = new LinkedHashMap<>();
            keys.forEach(key -> {
                byte[] bytes = values.getOrDefault(namespace, Map.of()).get(key);
                if (bytes != null) found.put(key, bytes);
            });
            return found;
        }
        @Override public void save(String namespace, String key, byte[] bytes) {
            values.computeIfAbsent(namespace, ignored -> new ConcurrentHashMap<>()).put(key, bytes);
        }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] bytes) {
            return Optional.ofNullable(values.computeIfAbsent(namespace, ignored -> new ConcurrentHashMap<>())
                    .putIfAbsent(key, bytes));
        }
        @Override public void delete(String namespace, String key) {
            values.getOrDefault(namespace, Map.of()).remove(key);
        }
        @Override public void dropNamespace(String namespace) { values.remove(namespace); }
        @Override public long count(String namespace) { return values.getOrDefault(namespace, Map.of()).size(); }
    }
}
