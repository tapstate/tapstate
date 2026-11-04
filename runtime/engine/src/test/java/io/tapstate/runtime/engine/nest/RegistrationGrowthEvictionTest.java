package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RegistrationGrowthEvictionTest {
    @Test void aFullPartitionNeverTurnsStoredReferenceMembersIntoAnAbsentSet() {
        Config config = new Config().setClusterName("registration-eviction-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
        config.addMapConfig(NestSettings.defaults().withEntriesHeldInMemory(271).backedStateMaps());
        var member = Hazelcast.newHazelcastInstance(config);
        try {
            NestStateMapStoreFactory.bindTo(member, new Cold());
            String namespace = NestMaps.NAMESPACE_PREFIX + "registration-eviction";
            var map = member.<Object, Set<Object>>getMap(namespace);
            var store = new MapNestStore<Set<Object>>(map);
            Map<Object, Set<Object>> first = new LinkedHashMap<>(), second = new LinkedHashMap<>(), expected = new LinkedHashMap<>();
            for (int bucket = 0; bucket < 64; bucket++) {
                Object key = NestLookup.bucketKey(List.of("one-customer"), bucket);
                Object old = List.of("old", bucket), added = List.of("new", bucket);
                first.put(key, Set.of(old)); second.put(key, Set.of(added)); expected.put(key, Set.of(old, added));
            }
            store.addAll(first); map.evictAll();
            assertThat(map.getLocalMapStats().getOwnedEntryCount()).isZero();
            store.addAll(second); map.evictAll();
            assertThat(store.loadAll(expected.keySet())).isEqualTo(expected);
            // Replaying a completed union must not add duplicates or discard either generation.
            store.addAll(second); map.evictAll();
            assertThat(store.loadAll(expected.keySet())).isEqualTo(expected);
        } finally { member.shutdown(); }
    }

    private static final class Cold implements KeyedStateStore {
        private final Map<String, Map<String, byte[]>> data = new ConcurrentHashMap<>();
        private Map<String, byte[]> values(String namespace) { return data.computeIfAbsent(namespace, ignored -> new ConcurrentHashMap<>()); }
        public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(values(namespace).get(key)); }
        public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            Map<String, byte[]> result = new LinkedHashMap<>();
            for (String key : keys) if (values(namespace).containsKey(key)) result.put(key, values(namespace).get(key));
            return result;
        }
        public void save(String namespace, String key, byte[] value) { values(namespace).put(key, value); }
        public void saveAll(String namespace, Map<String, byte[]> states) { values(namespace).putAll(states); }
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) { return Optional.ofNullable(values(namespace).putIfAbsent(key, value)); }
        public void delete(String namespace, String key) { values(namespace).remove(key); }
        public void dropNamespace(String namespace) { data.remove(namespace); }
        public long count(String namespace) { return values(namespace).size(); }
    }
}
