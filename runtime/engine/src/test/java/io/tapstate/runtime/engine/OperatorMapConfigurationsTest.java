package io.tapstate.runtime.engine;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestMemoryBudget;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

class OperatorMapConfigurationsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","p",2);
    private static final List<OperatorMapCapacity> CAPACITY = List.of(
            new OperatorMapCapacity(new OperatorPlacement(SCOPE,"extra-cache",OperatorPlacement.Resource.NEST_STATE,"nested","[]","state"),512),
            new OperatorMapCapacity(new OperatorPlacement(SCOPE,"cache",OperatorPlacement.Resource.JOIN_FACTS,"joined","root","facts"),64));
    private static OperatorRunManifest manifest(int attempt) {
        return new OperatorRunManifest("p","attempt-"+attempt,CAPACITY.stream().map(c -> new OperatorMapBinding(c.placement(),"attempt-"+attempt,String.format("%064x",attempt))).toList());
    }

    @Test void capacityMustMatchTheCompleteOriginalManifestBeforeAnyConfigurationOrProviderIsUsed() {
        var f = new Fixture(); var original = manifest(1);
        fenced(() -> OperatorMapConfigurations.install(f.member,original,List.of(CAPACITY.getFirst())));
        fenced(() -> OperatorMapConfigurations.install(f.member,original,List.of(CAPACITY.getFirst(),CAPACITY.getFirst())));
        var foreign = new OperatorPlacement(new PipelineIntentScope("cluster",1,"other","p",2),"cache",OperatorPlacement.Resource.JOIN_FACTS,"joined","root","facts");
        fenced(() -> OperatorMapConfigurations.install(f.member,original,List.of(CAPACITY.getFirst(),new OperatorMapCapacity(foreign,64))));
        assertThat(f.config.getMapConfigs()).isEmpty(); assertThat(f.opens.get()).isZero();
    }

    @Test void stableConfigurationsAreCompleteBeforeProviderCallbacksAndRepeatWithoutAttemptConfigs() {
        var f = new Fixture(); var original = manifest(1);
        f.context.put(NestMemoryBudget.SPLIT_BRAIN_PROTECTION_CONTEXT_KEY,"quorum");
        f.check = () -> { for (var capacity : CAPACITY) assertThat(f.config.getMapConfigs()).containsEntry(
                OperatorMapBinding.configurationPattern(capacity.placement()),capacity.configuration("quorum")); };
        OperatorMapConfigurations.install(f.member,original,CAPACITY);
        OperatorMapConfigurations.install(f.member,original,CAPACITY);
        assertThat(f.opens.get()).isEqualTo(4); assertThat(f.config.getMapConfigs()).hasSize(2);
        OperatorMapConfigurations.install(f.member,manifest(2),CAPACITY);
        assertThat(f.config.getMapConfigs()).hasSize(2);
        assertThat(f.config.getMapConfigs().keySet()).allMatch(name -> name.endsWith(".run.*"));
        fenced(() -> f.registry.join(original.bindings().get(1).mapName()));
        assertThat(f.registry.join(manifest(2).bindings().get(1).mapName())).isNotNull();
    }

    @Test void conflictingConfigurationRejectsTheWholeCandidateBeforeAddingOtherPatternsOrBindings() {
        var f = new Fixture(); var conflict = new OperatorMapCapacity(CAPACITY.getLast().placement(),128).configuration(null);
        f.config.addMapConfig(conflict);
        fenced(() -> OperatorMapConfigurations.install(f.member,manifest(1),CAPACITY));
        assertThat(f.config.getMapConfigs()).containsOnlyKeys(conflict.getName()); assertThat(f.opens.get()).isZero();
        fenced(() -> f.registry.nest(manifest(1).bindings().getFirst().mapName(),"extra-cache"));
    }

    @Test void anExistingUnconfiguredMapOrAnOlderHotAttemptNeverReceivesNewAuthority() {
        var f = new Fixture(); var original = manifest(1); f.live.add(mapNamed(original.bindings().getFirst().mapName()));
        fenced(() -> OperatorMapConfigurations.install(f.member,original,CAPACITY)); assertThat(f.opens.get()).isZero();
        f.live.clear(); OperatorMapConfigurations.install(f.member,original,CAPACITY);
        f.live.add(mapNamed(original.bindings().getFirst().mapName()));
        fenced(() -> OperatorMapConfigurations.install(f.member,manifest(2),CAPACITY));
        assertThat(f.registry.nest(original.bindings().getFirst().mapName(),"extra-cache")).isNotNull();
    }

    @Test void twoActualMembersInstallTheSameStablePatternsAndColdReloadOnlyAfterOriginalBindings() {
        var firstConfig = config("operator-prepare-"+UUID.randomUUID());
        firstConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        var first = Hazelcast.newHazelcastInstance(firstConfig); HazelcastInstance second = null;
        OperatorMapBindings firstRegistry = null, secondRegistry = null; var cold = new Bytes();
        try {
            var secondConfig = config(firstConfig.getClusterName());
            secondConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                    .addMember("127.0.0.1:"+first.getCluster().getLocalMember().getAddress().getPort());
            second = Hazelcast.newHazelcastInstance(secondConfig);
            firstRegistry = OperatorMapBindings.bindTo(first,(binding,active) -> cold);
            secondRegistry = OperatorMapBindings.bindTo(second,(binding,active) -> cold);
            var original = manifest(1);
            OperatorMapConfigurations.install(first,original,CAPACITY); OperatorMapConfigurations.install(second,original,CAPACITY);
            for (var binding : original.bindings()) {
                IMap<Object,Object> map = first.getMap(binding.mapName());
                map.set(key(binding),"value"); map.evictAll(); assertThat(map.get(key(binding))).isEqualTo("value");
            }
            OperatorMapConfigurations.install(first,original,CAPACITY);
            var joined = second;
            fenced(() -> OperatorMapConfigurations.install(joined,manifest(2),CAPACITY));
            original.bindings().forEach(binding -> first.getMap(binding.mapName()).destroy());
            for (var binding : original.bindings()) { assertThat(firstRegistry.release(binding)).isTrue(); assertThat(secondRegistry.release(binding)).isTrue(); }
            OperatorMapConfigurations.install(first,manifest(2),CAPACITY); OperatorMapConfigurations.install(second,manifest(2),CAPACITY);
            for (var binding : manifest(2).bindings()) {
                assertThat(second.getMap(binding.mapName()).get(key(binding))).isEqualTo("value");
                assertThat(second.getConfig().getMapConfigs()).containsKey(binding.configurationPattern()).doesNotContainKey(binding.mapName());
            }
        } finally {
            if (second != null) second.shutdown(); first.shutdown();
            if (secondRegistry != null) secondRegistry.close(); if (firstRegistry != null) firstRegistry.close();
        }
    }

    @Test void anOlderHotMapCreatedDuringProviderCallbacksPreventsPublication() {
        var f = new Fixture(); f.check = () -> f.live.add(mapNamed(manifest(1).bindings().getFirst().mapName()));
        fenced(() -> OperatorMapConfigurations.install(f.member,manifest(2),CAPACITY));
        fenced(() -> f.registry.nest(manifest(2).bindings().getFirst().mapName(),"extra-cache"));
    }
    @Test void configurationMutatedDuringProviderCallbacksCannotShareTheExpectedConfigurationObject() {
        var f = new Fixture(); String pattern = manifest(1).bindings().getFirst().configurationPattern();
        f.check = () -> f.config.getMapConfigs().get(pattern).getMapStoreConfig().getProperties().clear();
        fenced(() -> OperatorMapConfigurations.install(f.member,manifest(1),CAPACITY));
        fenced(() -> f.registry.nest(manifest(1).bindings().getFirst().mapName(),"extra-cache"));
    }
    @Test void protectionChangedDuringProviderCallbacksPreventsPublication() {
        var f = new Fixture(); f.check = () -> f.context.put(NestMemoryBudget.SPLIT_BRAIN_PROTECTION_CONTEXT_KEY,"different-quorum");
        fenced(() -> OperatorMapConfigurations.install(f.member,manifest(1),CAPACITY));
    }
    @Test void anExactHotConfigurationCannotShadowTheStableTypedConfiguration() {
        var f = new Fixture(); f.config.addMapConfig(new com.hazelcast.config.MapConfig(manifest(1).bindings().getFirst().mapName()));
        fenced(() -> OperatorMapConfigurations.install(f.member,manifest(1),CAPACITY)); assertThat(f.opens.get()).isZero();
    }
    @Test void aMoreSpecificPatternCannotShadowTheStableTypedConfiguration() {
        var f = new Fixture(); String hot = manifest(1).bindings().getFirst().mapName();
        f.config.addMapConfig(new com.hazelcast.config.MapConfig(hot.substring(0,hot.length()-4)+"*"));
        fenced(() -> OperatorMapConfigurations.install(f.member,manifest(1),CAPACITY)); assertThat(f.opens.get()).isZero();
    }

    @Test void concurrentConflictingCapacitiesHaveOneWinnerAndCannotReplayAStaleMissingConfiguration() throws Exception {
        var f = new Fixture(); var paused = new java.util.concurrent.CountDownLatch(1); var resume = new java.util.concurrent.CountDownLatch(1);
        var completedB = new java.util.concurrent.CountDownLatch(1); var firstCalls = new AtomicInteger();
        var errorA = new java.util.concurrent.atomic.AtomicReference<Throwable>(); var errorB = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var original = manifest(1); var larger = List.of(new OperatorMapCapacity(CAPACITY.getFirst().placement(),1024),CAPACITY.getLast());
        var a = new Thread(() -> {
            try { OperatorMapConfigurations.install(f.member,original,CAPACITY); }
            catch (Throwable failed) { errorA.set(failed); }
        },"operator-capacity-a");
        f.onConfig = () -> {
            if (Thread.currentThread() == a && firstCalls.incrementAndGet() == 2) {
                paused.countDown();
                try { if (!resume.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("configuration barrier expired"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
        };
        var b = new Thread(() -> {
            try { OperatorMapConfigurations.install(f.member,original,larger); f.live.add(mapNamed(original.bindings().getFirst().mapName())); }
            catch (Throwable failed) { errorB.set(failed); }
            finally { completedB.countDown(); }
        },"operator-capacity-b");
        try {
            a.start(); assertThat(paused.await(5,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); b.start();
            long deadline = System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (b.isAlive() && b.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline)
                if (completedB.await(10,java.util.concurrent.TimeUnit.MILLISECONDS)) break;
        } finally { resume.countDown(); a.join(5000); if (b.getState() != Thread.State.NEW) b.join(5000); }
        assertThat(a.isAlive()).isFalse(); assertThat(b.isAlive()).isFalse();
        var failures = java.util.stream.Stream.of(errorA.get(),errorB.get()).filter(Objects::nonNull).toList();
        assertThat(failures).hasSize(1).allSatisfy(failed -> assertThat(failed).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED)));
        int expected = errorA.get() == null ? 512 : 1024;
        assertThat(f.config.getMapConfigs().get(original.bindings().getFirst().configurationPattern()).getEvictionConfig().getSize()).isEqualTo(expected);
    }

    private static Object key(OperatorMapBinding binding) {
        return binding.placement().resource() == OperatorPlacement.Resource.NEST_STATE ? List.of("key") : "key";
    }
    private static Config config(String name) {
        var config = new Config().setClusterName(name); config.setProperty("hazelcast.partition.count","17");
        config.setProperty("hazelcast.phone.home.enabled","false"); config.setProperty("hazelcast.shutdownhook.enabled","false");
        config.setProperty("hazelcast.logging.type","none"); config.setProperty("hazelcast.operation.thread.count","2");
        config.setProperty("hazelcast.operation.generic.thread.count","2"); config.setProperty("hazelcast.io.input.thread.count","1"); config.setProperty("hazelcast.io.output.thread.count","1");
        var network = config.getNetworkConfig(); network.setPort(0).setPortAutoIncrement(false); network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = network.getJoin(); join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
        return config;
    }
    private static IMap<?,?> mapNamed(String name) {
        return (IMap<?,?>)Proxy.newProxyInstance(IMap.class.getClassLoader(),new Class<?>[]{IMap.class},(proxy,method,args) -> {
            if (method.getName().equals("getName")) return name; throw new AssertionError(method.getName());
        });
    }
    private static void fenced(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,f -> assertThat(f.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
    }
    private static final class Fixture {
        final Config config = new Config(); final Map<String,Object> context = new ConcurrentHashMap<>();
        final List<com.hazelcast.core.DistributedObject> live = new ArrayList<>(); final AtomicInteger opens = new AtomicInteger();
        Runnable check = () -> {}; Runnable onConfig = () -> {}; final HazelcastInstance member; final OperatorMapBindings registry;
        Fixture() {
            member = (HazelcastInstance)Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),new Class<?>[]{HazelcastInstance.class},(proxy,method,args) -> switch(method.getName()) {
                case "getUserContext" -> context; case "getConfig" -> { onConfig.run(); yield config; } case "getDistributedObjects" -> live;
                default -> throw new AssertionError(method.getName());
            });
            registry = OperatorMapBindings.bindTo(member,(binding,active) -> { check.run(); active.run(); opens.incrementAndGet(); return new Bytes(); });
        }
    }
    private static final class Bytes implements KeyedStateStore {
        final Map<String,byte[]> values = new ConcurrentHashMap<>();
        @Override public Optional<byte[]> load(String namespace,String key) { return Optional.ofNullable(values.get(namespace+"/"+key)).map(byte[]::clone); }
        @Override public void save(String namespace,String key,byte[] value) { values.put(namespace+"/"+key,value.clone()); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace,String key,byte[] value) { return Optional.ofNullable(values.putIfAbsent(namespace+"/"+key,value.clone())); }
        @Override public void delete(String namespace,String key) { values.remove(namespace+"/"+key); }
        @Override public void dropNamespace(String namespace) { values.keySet().removeIf(key -> key.startsWith(namespace+"/")); }
        @Override public long count(String namespace) { return values.keySet().stream().filter(key -> key.startsWith(namespace+"/")).count(); }
    }
}
