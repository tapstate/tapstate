package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.NestReleaseStore;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static org.assertj.core.api.Assertions.assertThat;

class NestStateMapStoreReleaseTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void resolverReplacementKeepsItsRecoveryMarkerUntilTheHandoffIsEmpty(boolean batch) {
        var cold = new ReleaseStore();
        var context = new ConcurrentHashMap<String, Object>();
        var member = (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
                new Class<?>[]{HazelcastInstance.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getUserContext")) return context;
                    throw new AssertionError("unexpected operation: " + method.getName());
                });
        NestStateMapStoreFactory.bindTo(member, cold);
        var map = new NestStateMapStore("nest.release");
        map.init(member, new Properties(), "nest.release");
        var key = List.of("pending");
        var state = new ResolverState();
        state.retainRelease(at(1), Map.of(), List.of(), 1);
        replace(map, key, state, batch);
        assertThat(((ResolverState) map.load(key)).releases()).hasSize(1);
        var pending = cold.pendingReleases("nest.release", null);
        assertThat(pending).hasSize(1);
        assertThat(NestStateMapStore.releaseKey(pending.getFirst())).isEqualTo(key);
        replace(map, key, new ResolverState(), batch);
        assertThat(((ResolverState) map.load(key)).releases()).isEmpty();
        assertThat(cold.pendingReleases("nest.release", null)).isEmpty();
        assertThat(cold.releaseWrites).isEqualTo(2);
    }

    private static void replace(NestStateMapStore map, Object key, ResolverState state, boolean batch) {
        if (batch) map.storeAll(Map.of(key, state, "ordinary", "opaque-state"));
        else map.store(key, state);
    }

    private static final class ReleaseStore implements NestReleaseStore {
        private final Map<String, byte[]> rows = new LinkedHashMap<>();
        private final Map<String, byte[]> markers = new LinkedHashMap<>();
        private int releaseWrites;
        @Override public void saveWithRelease(String namespace, String key, byte[] state, byte[] routingKey) {
            releaseWrites++;
            rows.put(key, state);
            if (routingKey == null) markers.remove(key);
            else markers.put(key, routingKey);
        }
        @Override public List<PendingKey> pendingReleases(String namespace, String afterKey) {
            return markers.entrySet().stream().map(entry -> new PendingKey(entry.getKey(), entry.getValue())).toList();
        }
        @Override public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(rows.get(key)); }
        @Override public void save(String namespace, String key, byte[] state) { rows.put(key, state); markers.remove(key); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            return Optional.ofNullable(rows.putIfAbsent(key, state));
        }
        @Override public void delete(String namespace, String key) { rows.remove(key); markers.remove(key); }
        @Override public void dropNamespace(String namespace) { rows.clear(); markers.clear(); }
        @Override public long count(String namespace) { return rows.size(); }
    }
}
