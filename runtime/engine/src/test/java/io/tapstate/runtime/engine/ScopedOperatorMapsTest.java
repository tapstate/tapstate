package io.tapstate.runtime.engine;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.join.JoinMaps;
import io.tapstate.runtime.engine.join.JoinStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;

/** Actual named factories reload stable cold bytes into execution-specific hot maps. */
class ScopedOperatorMapsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","orders",3);
    private static OperatorPlacement placement(PipelineIntentScope scope,OperatorPlacement.Resource kind) { return new OperatorPlacement(scope,"cache",kind,"step","root","state"); }
    private static OperatorMapBinding binding(OperatorPlacement placement,int attempt) { return new OperatorMapBinding(placement,"attempt-"+attempt,String.format("%064x",attempt)); }
    private static Object key(OperatorPlacement.Resource kind) { return kind == OperatorPlacement.Resource.NEST_STATE ? List.of("key") : "key"; }
    private static Properties properties() { var properties = new Properties(); properties.setProperty(OperatorMapBindings.SCOPED_PROPERTY,"true"); return properties; }
    @SuppressWarnings("unchecked") private static MapStore<Object,Object> callbacks(HazelcastInstance member,OperatorMapBinding binding) {
        MapLoader<Object,Object> loader = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE
                ? new NestStateMapStoreFactory().newMapStore(binding.mapName(),properties())
                : new JoinStateMapStoreFactory().newMapStore(binding.mapName(),properties());
        ((MapLoaderLifecycleSupport)loader).init(member,properties(),binding.mapName()); return (MapStore<Object,Object>)loader;
    }
    private static MapConfig configuration(OperatorMapBinding binding) {
        MapConfig config = binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE
                ? NestSettings.defaults().backedStateMaps(binding.configurationPattern())
                : JoinMaps.backedStateMaps(binding.configurationPattern(),64);
        config.getMapStoreConfig().getProperties().putAll(properties()); return config;
    }
    private static Config config(List<OperatorMapBinding> bindings) {
        var config = new Config().setClusterName("operator-"+UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled","false"); config.setProperty("hazelcast.shutdownhook.enabled","false");
        config.setProperty("hazelcast.logging.type","none"); config.setProperty("hazelcast.operation.thread.count","2");
        config.setProperty("hazelcast.operation.generic.thread.count","2"); config.setProperty("hazelcast.io.input.thread.count","1");
        config.setProperty("hazelcast.io.output.thread.count","1");
        var network = config.getNetworkConfig(); network.setPort(0).setPortAutoIncrement(false);
        network.getInterfaces().setEnabled(true).addInterface("127.0.0.1"); var join = network.getJoin();
        join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
        bindings.forEach(binding -> config.addMapConfig(configuration(binding))); return config;
    }

    @Test void everyTypedMapReloadsColdStateWithoutReusingAnotherExecutionOrAddingConfigurations() {
        var first = Arrays.stream(OperatorPlacement.Resource.values()).map(kind -> binding(placement(SCOPE,kind),1)).toList();
        var next = first.stream().map(original -> binding(original.placement(),2)).toList();
        var current = new AtomicReference<Object>("attempt-1"); var scope = new AtomicReference<>(SCOPE); var cold = new Cold();
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config(first));
        try (var registry = OperatorMapBindings.bindTo(member,(original,active) -> cold.guarded(original,current,scope,active))) {
            registry.install(first); Map<OperatorPlacement.Resource,MapStore<Object,Object>> old = new EnumMap<>(OperatorPlacement.Resource.class);
            for (var original : first) {
                Object key = key(original.placement().resource()); member.getMap(original.mapName()).put(key,"before");
                old.put(original.placement().resource(),callbacks(member,original));
            }
            int configurations = member.getConfig().getMapConfigs().size(); current.set("attempt-2"); registry.install(next);
            for (int index = 0; index < next.size(); index++) {
                var original = first.get(index); var replacement = next.get(index); Object key = key(replacement.placement().resource());
                var hot = member.getMap(replacement.mapName()); assertThat(hot.get(key)).isEqualTo("before"); hot.put(key,"after");
                MapStore<Object,Object> late = old.get(replacement.placement().resource());
                fenced(() -> late.store(key,"late")); fenced(() -> late.storeAll(Map.of(key,"late")));
                fenced(() -> late.delete(key)); fenced(() -> late.deleteAll(List.of(key)));
                assertThat(hot.get(key)).isEqualTo("after"); hot.evict(key); assertThat(hot.get(key)).isEqualTo("after");
                assertThat(registry.release(original)).isFalse();
                member.getMap(original.mapName()).destroy(); hot.destroy(); assertThat(registry.release(replacement)).isTrue();
                fenced(() -> registry.install(List.of(replacement)));
            }
            assertThat(member.getConfig().getMapConfigs()).hasSize(configurations);
            assertThat(cold.rows.keySet()).extracting(Key::namespace).containsOnlyElementsOf(first.stream().map(binding -> binding.placement().mapName()).toList());
        } finally { member.shutdown(); }
    }

    @Test void aNewStableGenerationBeginsSeparatelyAndAnOldProxyCannotWriteItsColdOrHotValues() {
        var original = binding(placement(SCOPE,OperatorPlacement.Resource.NEST_STATE),1);
        var nextScope = new PipelineIntentScope("cluster",1,"cache","orders",4); var next = binding(placement(nextScope,OperatorPlacement.Resource.NEST_STATE),1);
        var current = new AtomicReference<Object>("attempt-1"); var scope = new AtomicReference<>(SCOPE); var cold = new Cold();
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config(List.of(original,next)));
        try (var registry = OperatorMapBindings.bindTo(member,(binding,active) -> cold.guarded(binding,current,scope,active))) {
            registry.install(List.of(original)); var oldMap = member.getMap(original.mapName()); Object key = List.of("key"); oldMap.put(key,"old");
            scope.set(nextScope); registry.install(List.of(next)); var newMap = member.getMap(next.mapName());
            assertThat(newMap.get(key)).isNull(); newMap.put(key,"new");
            assertThatThrownBy(() -> oldMap.put(key,"late")).hasStackTraceContaining("io.workload-claim-fenced");
            assertThat(newMap.get(key)).isEqualTo("new"); newMap.evict(key); assertThat(newMap.get(key)).isEqualTo("new");
            oldMap.destroy(); assertThat(registry.release(original)).isTrue();
            assertThat(newMap.get(key)).isEqualTo("new");
        } finally { member.shutdown(); }
    }

    private static void fenced(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED)); }
    private record Key(String namespace,String key) {}
    private static final class Cold {
        final Map<Key,byte[]> rows = new ConcurrentHashMap<>();
        KeyedStateStore guarded(OperatorMapBinding original,AtomicReference<Object> identity,AtomicReference<PipelineIntentScope> scope,Runnable active) {
            return new KeyedStateStore() {
                private void check() { active.run(); if (!identity.get().equals(original.executionIdentity()) || !scope.get().equals(original.placement().scope()))
                    throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
                @Override public Optional<byte[]> load(String ns,String key) { check(); return Optional.ofNullable(rows.get(new Key(ns,key))).map(byte[]::clone); }
                @Override public void save(String ns,String key,byte[] value) { check(); rows.put(new Key(ns,key),value.clone()); }
                @Override public Optional<byte[]> saveIfAbsent(String ns,String key,byte[] value) { check(); return Optional.ofNullable(rows.putIfAbsent(new Key(ns,key),value.clone())); }
                @Override public void delete(String ns,String key) { check(); rows.remove(new Key(ns,key)); }
                @Override public void dropNamespace(String ns) { check(); rows.keySet().removeIf(key -> key.namespace().equals(ns)); }
                @Override public long count(String ns) { check(); return rows.keySet().stream().filter(key -> key.namespace().equals(ns)).count(); }
            };
        }
    }
}
