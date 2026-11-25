package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.OperatorMapBinding;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.runtime.engine.OperatorMapCoordinates;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.PipelineIntentScope;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NestQueuedWriteFencingTest {
    @Test void aWriteQueuedInsideOneLoaderCannotReachTheSuccessor() throws Exception {
        var context = new ConcurrentHashMap<String, Object>();
        var member = (HazelcastInstance) Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),
                new Class<?>[]{HazelcastInstance.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getUserContext" -> context;
                    case "getDistributedObjects" -> List.of();
                    default -> throw new AssertionError("unexpected operation: " + method.getName());
                });
        var scope = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
        var placement = OperatorMapCoordinates.nest(scope, "cache", "step", List.of(),
                OperatorMapCoordinates.NestMap.LOOKUP, "customer");
        var original = new OperatorMapBinding(placement, "original", "a".repeat(64));
        var successor = new OperatorMapBinding(placement, "successor", "b".repeat(64));
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var backendCalls = new AtomicInteger();
        var oldRows = new ConcurrentHashMap<String, byte[]>();
        var newRows = new ConcurrentHashMap<String, byte[]>();
        try (var bindings = OperatorMapBindings.bindTo(member, (binding, active) -> new ColdStore(
                binding.equals(original) ? oldRows : newRows, () -> {
                    active.run();
                    if (binding.equals(original)) {
                        backendCalls.incrementAndGet();
                        entered.countDown();
                        await(release);
                        active.run();
                    }
                }))) {
            bindings.install(List.of(original));
            var old = loader(member, original);
            var first = refused(() -> old.store("first", "late"));
            var second = refused(() -> old.storeAll(Map.of("second", "late", "third", "late")));
            var firstThread = new Thread(first, "first-scoped-nest-write");
            var secondThread = new Thread(second, "queued-scoped-nest-write");
            try {
                firstThread.start();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                secondThread.start();
                NestStateIoTest.awaitAllBlockedInsideIo(List.of(secondThread));
                assertThat(backendCalls).hasValue(1);
                assertThat(bindings.release(original)).isTrue();
                bindings.install(List.of(successor));
                var current = loader(member, successor);
                current.store("first", "successor");
                release.countDown();
                first.get(5, TimeUnit.SECONDS);
                second.get(5, TimeUnit.SECONDS);
                assertThat(backendCalls).hasValue(1);
                assertThat(oldRows).isEmpty();
                assertThat(newRows).hasSize(1);
                assertThat(current.load("first")).isEqualTo("successor");
            } finally {
                release.countDown();
                firstThread.join(5000);
                secondThread.join(5000);
            }
        }
    }

    private static FutureTask<Void> refused(org.assertj.core.api.ThrowableAssert.ThrowingCallable write) {
        return new FutureTask<>(() -> {
            assertThatThrownBy(write).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            return null;
        });
    }

    private static NestStateMapStore loader(HazelcastInstance member, OperatorMapBinding binding) {
        var properties = new Properties();
        properties.setProperty(OperatorMapBindings.SCOPED_PROPERTY, "true");
        var result = (NestStateMapStore) new NestStateMapStoreFactory().newMapStore(binding.mapName(), properties);
        result.init(member, properties, binding.mapName());
        return result;
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("original write was not released");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private record ColdStore(Map<String, byte[]> rows, Runnable guard) implements KeyedStateStore {
        @Override public Optional<byte[]> load(String namespace, String key) { guard.run(); return Optional.ofNullable(rows.get(key)); }
        @Override public void save(String namespace, String key, byte[] state) { guard.run(); rows.put(key, state); }
        @Override public void saveAll(String namespace, Map<String, byte[]> states) { guard.run(); rows.putAll(states); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            guard.run(); return Optional.ofNullable(rows.putIfAbsent(key, state));
        }
        @Override public void delete(String namespace, String key) { guard.run(); rows.remove(key); }
        @Override public void dropNamespace(String namespace) { guard.run(); rows.clear(); }
        @Override public long count(String namespace) { guard.run(); return rows.size(); }
    }
}
