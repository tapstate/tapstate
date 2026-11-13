package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import io.tapstate.core.lifecycle.NestKeyAccess;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NestColdOwnerPublicationTest {
    @Test void remoteColdLoadsRefreshAnIdleOwnersPublishedCounters() {
        var context = new ConcurrentHashMap<String, Object>();
        HazelcastInstance owner = (HazelcastInstance) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{HazelcastInstance.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getUserContext")) return context;
                    throw new AssertionError(method.getName());
                });
        NestStateStats stats = NestStateStats.of(owner);
        LocalMapStats local = (LocalMapStats) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{LocalMapStats.class}, (proxy, method, args) -> 0L);
        @SuppressWarnings("unchecked") IMap<Object, String> map = (IMap<Object, String>) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{IMap.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getName" -> "namespace";
                    case "getLocalMapStats" -> local;
                    case "get" -> null;
                    default -> throw new AssertionError(method.getName());
                });
        var published = new AtomicReference<NestKeyAccess>();
        NestStateGauge gauge = new NestStateGauge() {
            @Override public void reading(String ns, long e, long a, long b, long m, long p) { }
            @Override public void keyAccess(String ns, NestKeyAccess value) { published.set(value); }
        };
        new MapNestStore<>(map, stats, gauge).load("local");
        assertThat(published.get().coldLoads()).isZero();
        // The remote caller does not execute another owner-side MapNestStore operation.
        stats.coldKeys("namespace", 100);
        assertThat(published.get().coldLoads()).isEqualTo(100);
        assertThat(published.get().reads()).isEqualTo(1);

        var replacement = new AtomicReference<NestKeyAccess>();
        stats.publishKeys("namespace", replacement::set);
        stats.coldKeys("namespace", 1);
        assertThat(published.get().coldLoads()).isEqualTo(100);
        assertThat(replacement.get().coldLoads()).isEqualTo(101);
        stats.forget("namespace");
        stats.coldKeys("namespace", 1);
        assertThat(replacement.get().coldLoads()).isEqualTo(101);
    }
}
