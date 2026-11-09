package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A pipeline switches carriers on restart without a rebuild: what the map carrier stored, the state session
 * reads, and the other way round, because both store a key under the same name and a value as the same bytes.
 */
class CarrierSwitchEncodingTest {

    private static final String NAMESPACE = "nest.p.step.$root";
    private static final Object KEY = List.of("order", 7);

    private final Map<String, byte[]> held = new ConcurrentHashMap<>();
    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("carrier-switch-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        member = Hazelcast.newHazelcastInstance(config);
        NestStateMapStoreFactory.bindTo(member, new Held());
    }

    @AfterEach
    void stopMember() {
        if (member != null) member.shutdown();
    }

    @Test
    void whatTheMapCarrierStoredTheSessionReads() {
        mapCarrier().store(KEY, document("Ada"));
        NestStore<Map<String, Object>> state = NestStateSession.on(member, "assembler/0").view(NAMESPACE, 1 << 20);
        assertThat(state.load(KEY)).isEqualTo(document("Ada"));
    }

    @Test
    void whatTheSessionCommittedTheMapCarrierReads() {
        NestStore<Map<String, Object>> state = NestStateSession.on(member, "assembler/0").view(NAMESPACE, 1 << 20);
        state.save(KEY, document("Grace"));
        state.commit();
        assertThat(mapCarrier().load(KEY)).isEqualTo(document("Grace"));
    }

    @Test
    void bothCarriersStoreTheSameKeyNameAndTheSameBytes() {
        mapCarrier().store(KEY, document("Ada"));
        Map<String, byte[]> byMap = Map.copyOf(held);
        held.clear();
        NestStore<Map<String, Object>> state = NestStateSession.on(member, "assembler/0").view(NAMESPACE, 1 << 20);
        state.save(KEY, document("Ada"));
        state.commit();
        assertThat(held.keySet()).isEqualTo(byMap.keySet());
        held.forEach((name, bytes) -> assertThat(bytes).isEqualTo(byMap.get(name)));
    }

    private NestStateMapStore mapCarrier() {
        NestStateMapStore store = new NestStateMapStore(NAMESPACE);
        store.init(member, new Properties(), NAMESPACE);
        return store;
    }

    private static Map<String, Object> document(String name) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("order_id", 7);
        document.put("customer", Map.of("name", name));
        return document;
    }

    /** One namespace's store, in this test's memory. */
    private final class Held implements KeyedStateStore {
        @Override public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(held.get(namespace + "/" + key)); }
        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            Map<String, byte[]> found = new LinkedHashMap<>();
            for (String key : keys) if (held.containsKey(namespace + "/" + key)) found.put(key, held.get(namespace + "/" + key));
            return found;
        }
        @Override public void save(String namespace, String key, byte[] state) { held.put(namespace + "/" + key, state); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            return Optional.ofNullable(held.putIfAbsent(namespace + "/" + key, state));
        }
        @Override public void delete(String namespace, String key) { held.remove(namespace + "/" + key); }
        @Override public void dropNamespace(String namespace) { held.keySet().removeIf(key -> key.startsWith(namespace + "/")); }
        @Override public long count(String namespace) { return held.keySet().stream().filter(key -> key.startsWith(namespace + "/")).count(); }
    }
}
