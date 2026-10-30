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

    @Test void aLateMemberInitializesMigratedMapsWithoutWaitingForItsOriginalDescriptorOrTouchingColdState() throws Exception {
        var original = binding(placement(SCOPE,OperatorPlacement.Resource.NEST_STATE),1); var next = binding(original.placement(),2);
        var cold = new Cold(); var current = new AtomicReference<Object>("attempt-1"); var scope = new AtomicReference<>(SCOPE);
        var firstConfig = config(List.of(original)); firstConfig.setProperty("hazelcast.partition.count","17");
        firstConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance first = Hazelcast.newHazelcastInstance(firstConfig); HazelcastInstance second = null;
        var firstRegistry = OperatorMapBindings.bindTo(first,(binding,active) -> cold.guarded(binding,current,scope,active));
        OperatorMapBindings secondRegistry = null;
        try {
            firstRegistry.install(List.of(original)); var map = first.getMap(original.mapName());
            var keys = java.util.stream.IntStream.range(0,64).mapToObj(index -> List.of("key-"+index)).toList();
            keys.forEach(key -> map.put(key,"before"));
            var secondConfig = config(List.of(original)).setClusterName(firstConfig.getClusterName());
            secondConfig.setProperty("hazelcast.partition.count","17");
            secondConfig.setProperty("hazelcast.logging.type","jdk");
            secondConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                    .addMember("127.0.0.1:"+first.getCluster().getLocalMember().getAddress().getPort());
            var opens = new java.util.concurrent.atomic.AtomicInteger();
            secondRegistry = OperatorMapBindings.prepare(secondConfig,(binding,active) -> {
                opens.incrementAndGet(); return cold.guarded(binding,current,scope,active);
            });
            long started = System.nanoTime(); second = Hazelcast.newHazelcastInstance(secondConfig);
            assertThat(java.time.Duration.ofNanos(System.nanoTime()-started)).isLessThan(java.time.Duration.ofSeconds(20));
            var joined = second; var registry = secondRegistry;
            await(() -> registry.awaitingMapNames().contains(original.mapName()));
            assertThat(opens.get()).isZero();
            // This pending descriptor came from migration, before any local getMap or explicit factory call.
            var late = callbacks(second,original); fenced(() -> late.load(keys.getFirst())); assertThat(opens.get()).isZero();
            secondRegistry.attachExisting(List.of(original)); assertThat(opens.get()).isOne();
            await(() -> keys.stream().anyMatch(key -> joined.getCluster().getLocalMember().equals(joined.getPartitionService().getPartition(key).getOwner())));
            var key = keys.stream().filter(candidate -> joined.getCluster().getLocalMember().equals(joined.getPartitionService().getPartition(candidate).getOwner())).findFirst().orElseThrow();
            var localMap = second.getMap(original.mapName()); assertThat(localMap.get(key)).isEqualTo("before");
            localMap.evict(key); assertThat(localMap.get(key)).isEqualTo("before"); localMap.put(key,"after");
            current.set("attempt-2"); firstRegistry.install(List.of(next)); secondRegistry.install(List.of(next));
            fenced(() -> late.store(key,"late")); fenced(() -> late.deleteAll(List.of(key)));
            fenced(() -> registry.attachExisting(List.of(original)));
            var newMap = second.getMap(next.mapName()); assertThat(newMap.get(key)).isEqualTo("after");
            newMap.evict(key); assertThat(newMap.get(key)).isEqualTo("after");
            map.destroy(); newMap.destroy();
        } finally {
            if (secondRegistry != null) secondRegistry.close(); firstRegistry.close();
            if (second != null) second.shutdown(); first.shutdown();
        }
    }

    @Test void typedNestAndJoinStoresUseTheOriginalManifestForEveryActualMapAndColdReload() throws Exception {
        var positions = new ArrayList<OperatorPlacement>();
        positions.add(OperatorMapCoordinates.nest(SCOPE,"cache","nested",List.of(),OperatorMapCoordinates.NestMap.STATE,null));
        positions.add(OperatorMapCoordinates.nest(SCOPE,"cache","nested",List.of("$root"),OperatorMapCoordinates.NestMap.STATE,null));
        positions.add(OperatorMapCoordinates.nest(SCOPE,"cache","nested",List.of(),OperatorMapCoordinates.NestMap.PARKING,null));
        positions.add(OperatorMapCoordinates.nest(SCOPE,"cache","nested",List.of("ref"),OperatorMapCoordinates.NestMap.LOOKUP,"dimension"));
        positions.add(OperatorMapCoordinates.nest(SCOPE,"cache","nested",List.of("ref"),OperatorMapCoordinates.NestMap.REFERENCES,"dimension"));
        for (var kind : OperatorPlacement.Resource.values()) if (kind != OperatorPlacement.Resource.NEST_STATE)
            positions.add(OperatorMapCoordinates.join(SCOPE,"cache","joined",kind,"dimension"));
        var bindings = positions.stream().map(place -> binding(place,1)).toList(); var manifest = new OperatorRunManifest(SCOPE.pipelineId(),"attempt-1",bindings);
        var cold = new Cold(); var current = new AtomicReference<Object>("attempt-1"); var scope = new AtomicReference<>(SCOPE);
        var member = Hazelcast.newHazelcastInstance(config(bindings));
        try (var registry = OperatorMapBindings.bindTo(member,(binding,active) -> cold.guarded(binding,current,scope,active))) {
            registry.install(bindings);
            var traveling = io.tapstate.runtime.engine.nest.NestBinding.onMap(manifest).forStep(SCOPE.pipelineId(),"nested");
            var bytes = new java.io.ByteArrayOutputStream();
            try (var out = new java.io.ObjectOutputStream(bytes)) { out.writeObject(traveling); }
            io.tapstate.runtime.engine.nest.NestBinding.NestStores restored;
            try (var in = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) { restored = (io.tapstate.runtime.engine.nest.NestBinding.NestStores)in.readObject(); }
            var stores = restored.bind(member);
            var root = new io.tapstate.runtime.engine.nest.NestVertex(List.of(),"logical-root","legacy-root",List.of("id"),List.of(),List.of());
            var embedded = new io.tapstate.runtime.engine.nest.NestVertex(List.of("$root"),"logical-embed","legacy-embed",List.of("id"),List.of("parent"),List.of());
            var lookup = new io.tapstate.runtime.engine.nest.NestLookup(List.of("ref"),"dimension","logical-lookup","legacy-lookup",List.of("id"),
                    "orders",List.of("dimension_id"),List.of("id"),List.of(),0);
            Object key = List.of("key");
            stores.forAssembler(root).save(key,new io.tapstate.runtime.engine.nest.RootAssembly());
            stores.forResolver(embedded).save(key,new io.tapstate.runtime.engine.nest.ResolverState());
            assertThat(stores.forParking(root).load(key)).isNull();
            stores.forLookup(lookup).save(key,Map.of("id",1)); stores.forReferences(lookup).save(key,Set.of("fact"));
            for (var binding : bindings) if (binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE) member.getMap(binding.mapName()).evictAll();
            assertThat(stores.forAssembler(root).load(key)).isInstanceOf(io.tapstate.runtime.engine.nest.RootAssembly.class);
            assertThat(stores.forResolver(embedded).load(key)).isInstanceOf(io.tapstate.runtime.engine.nest.ResolverState.class);
            assertThat(stores.forLookup(lookup).load(key)).containsEntry("id",1); assertThat(stores.forReferences(lookup).load(key)).containsExactly("fact");
            var joins = io.tapstate.runtime.engine.join.JoinStoresBinding.onTheCluster(manifest).bind(member,SCOPE.pipelineId(),"joined");
            joins.putFact("f",Map.of("id",1)); joins.putDimensionRow("dimension","d",Map.of("id",2));
            joins.indexAdd("dimension","d","f"); joins.putBatchesTakenIn("writer",3);
            for (var binding : bindings) if (binding.placement().resource() != OperatorPlacement.Resource.NEST_STATE) member.getMap(binding.mapName()).evictAll();
            assertThat(joins.fact("f")).containsEntry("id",1); assertThat(joins.dimensionRow("dimension","d")).containsEntry("id",2);
            assertThat(joins.indexPage("dimension","d",0)).containsExactly("f"); assertThat(joins.batchesTakenIn("writer")).isEqualTo(3);
            assertThat(member.getDistributedObjects().stream().map(com.hazelcast.core.DistributedObject::getName).toList())
                    .containsAll(bindings.stream().map(OperatorMapBinding::mapName).toList()).doesNotContain("legacy-root","legacy-embed","legacy-lookup");
            assertThatThrownBy(() -> io.tapstate.runtime.engine.nest.NestBinding.onMap(manifest).bind(member).forAssembler(root)).isInstanceOf(TapstateException.class);
            for (var binding : bindings) member.getMap(binding.mapName()).destroy();
        } finally { member.shutdown(); }
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime()+java.time.Duration.ofSeconds(15).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new AssertionError("the member did not finish the expected migration");
            Thread.sleep(20);
        }
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
