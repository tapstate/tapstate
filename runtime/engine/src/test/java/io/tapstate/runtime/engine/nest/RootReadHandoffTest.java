package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.*;

import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.*;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import org.junit.jupiter.api.Test;

class RootReadHandoffTest {
    @Test void expiredCollectorHandsExistingAdmissionWaitersOneSharedResult() throws Exception { scenario(null, false, false); }
    @Test void preInterruptedCollectorDefersAndRestoresTheInterruptAcrossHandoff() throws Exception { scenario(null, true, false); }
    @Test void handoffSharesTheOriginalFailureAndAllowsRetry() throws Exception { scenario(new IllegalStateException("read failure"), false, false); }
    @Test void handoffSharesTheOriginalFatalErrorAndAllowsRetry() throws Exception { scenario(new AssertionError("fatal read failure"), false, false); }
    @Test void retirementWhileAdmissionWaitsStillFencesTheHandoffBeforeBackendAccess() throws Exception { scenario(null, false, true); }

    private static void scenario(Throwable failure, boolean preInterrupted, boolean retire) throws Exception {
        Store backend = new Store(); backend.failure = failure;
        var context = new ConcurrentHashMap<String, Object>();
        var member = (com.hazelcast.core.HazelcastInstance) Proxy.newProxyInstance(
                com.hazelcast.core.HazelcastInstance.class.getClassLoader(),
                new Class<?>[] {com.hazelcast.core.HazelcastInstance.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getUserContext" -> context;
                    case "getDistributedObjects" -> List.of();
                    default -> throw new AssertionError(method.getName());
                });
        var scope = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
        var placement = OperatorMapCoordinates.nest(scope, "cache", "nest", List.of(), OperatorMapCoordinates.NestMap.STATE, null);
        var binding = new OperatorMapBinding(placement, "attempt", "1".repeat(64));
        var registry = OperatorMapBindings.bindTo(member, (original, active) -> backend); registry.install(List.of(binding));
        Clock clock = new Clock();
        NestStateIo rootIo = new NestStateIo(registry.nest(binding.mapName(), "cache"), binding.mapName(), 0,
                TimeUnit.MILLISECONDS.toNanos(1), () -> registry.isRootNestState(binding.mapName()), clock);
        List<Thread> threads = new ArrayList<>(); List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        List<AtomicBoolean> interruptedAtExit = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                int index = i; var restored = new AtomicBoolean(); interruptedAtExit.add(restored);
                var task = new FutureTask<Map<String, byte[]>>(() -> {
                    if (index == 0 && preInterrupted) Thread.currentThread().interrupt();
                    try { return rootIo.loadAll(List.of("key-" + index)); }
                    finally { restored.set(Thread.currentThread().isInterrupted()); }
                });
                tasks.add(task); var thread = new Thread(task, "handoff-" + i); threads.add(thread); thread.start();
                if (i == 0) assertThat(clock.entered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            assertThat(threads.subList(1, threads.size())).allSatisfy(thread -> assertThat(Arrays.stream(thread.getStackTrace())
                    .anyMatch(frame -> frame.getClassName().startsWith("java.util.concurrent.locks.ReentrantLock")
                            && frame.getMethodName().equals("lock"))).isTrue());
            if (retire) registry.close();
            clock.release.countDown();
            if (!retire) {
                assertThat(backend.entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(backend.batches).hasSize(1);
                assertThat(backend.batches.getFirst()).containsExactlyInAnyOrder("key-0", "key-1", "key-2", "key-3");
                assertThat(backend.interruptedAtRead).containsExactly(false);
                assertThat(tasks).noneMatch(FutureTask::isDone);
                backend.release.countDown();
            }
            Throwable shared = null;
            for (int i = 0; i < tasks.size(); i++) {
                if (failure == null && !retire) assertThat(tasks.get(i).get(5, TimeUnit.SECONDS)).containsOnlyKeys("key-" + i);
                else {
                    try { tasks.get(i).get(5, TimeUnit.SECONDS); fail("the original failure must reach every waiter"); }
                    catch (ExecutionException failed) {
                        if (retire) assertThat(failed.getCause()).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                coded -> assertThat(coded.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                        else assertThat(failed.getCause()).isSameAs(failure);
                        if (shared == null) shared = failed.getCause(); else assertThat(failed.getCause()).isSameAs(shared);
                    }
                }
            }
            for (int i = 0; i < tasks.size(); i++) assertThat(interruptedAtExit.get(i).get()).isEqualTo(i == 0 && preInterrupted);
            if (retire) assertThat(backend.batches).isEmpty();
            else {
                backend.failure = null;
                assertThat(rootIo.loadAll(List.of("retry"))).containsOnlyKeys("retry");
                assertThat(clock.calls.get()).isEqualTo(2);
            }
        } finally {
            clock.release.countDown(); backend.release.countDown(); registry.close();
            for (Thread thread : threads) thread.join(5_000);
            assertThat(threads).noneMatch(Thread::isAlive);
        }
    }

    @Test void anExpiredCollectorWithNoAdmissionWaiterDoesNotWaitForAnotherArrival() {
        Store store = new Store(); store.release.countDown(); AtomicInteger ticks = new AtomicInteger();
        var io = new NestStateIo(store, "root", 0, 1_000_000L, () -> true,
                () -> { ticks.incrementAndGet(); return System.nanoTime() + TimeUnit.SECONDS.toNanos(1); });
        assertThat(io.loadAll(List.of("alone"))).containsOnlyKeys("alone");
        assertThat(ticks.get()).isEqualTo(1); assertThat(store.batches).containsExactly(List.of("alone"));
    }

    @Test void theReadClockCannotAffectScalarOtherNamespaceLegacyOrWritePaths() {
        Store store = new Store(); store.release.countDown();
        LongSupplier forbidden = () -> { throw new AssertionError("read gather clock is not used on this path"); };
        var root = new NestStateIo(store, "root", 0, 1_000_000L, () -> true, forbidden);
        var other = new NestStateIo(store, "other", 0, 1_000_000L, () -> false, forbidden);
        var legacy = new NestStateIo(store, "legacy", 0, 0, () -> true, forbidden);
        assertThat(root.load("single")).isNotNull(); root.saveAll(Map.of("written", new byte[] {1}));
        assertThat(root.loadAll(List.of())).isEmpty();
        assertThat(other.loadAll(List.of("other"))).containsOnlyKeys("other");
        assertThat(legacy.loadAll(List.of("legacy"))).containsOnlyKeys("legacy");
    }

    private static final class Clock implements LongSupplier {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final AtomicInteger calls = new AtomicInteger();
        @Override public long getAsLong() {
            if (calls.getAndIncrement() == 0) {
                entered.countDown(); boolean interrupted = false;
                for (;;) {
                    try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("clock barrier cap"); break; }
                    catch (InterruptedException ignored) { interrupted = true; }
                }
                if (interrupted) Thread.currentThread().interrupt();
            }
            return System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        }
    }
    private static final class Store implements KeyedStateStore {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final List<List<String>> batches = new CopyOnWriteArrayList<>();
        final List<Boolean> interruptedAtRead = new CopyOnWriteArrayList<>(); volatile Throwable failure;
        @Override public Map<String, byte[]> loadAll(String ns, Collection<String> keys) {
            batches.add(List.copyOf(keys)); interruptedAtRead.add(Thread.currentThread().isInterrupted()); entered.countDown();
            try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("backend barrier cap"); }
            catch (InterruptedException failed) { throw new AssertionError(failed); }
            if (failure instanceof RuntimeException failed) throw failed;
            if (failure instanceof Error failed) throw failed;
            Map<String, byte[]> out = new LinkedHashMap<>(); keys.forEach(key -> out.put(key, new byte[] {1})); return out;
        }
        @Override public Optional<byte[]> load(String ns, String key) { return Optional.ofNullable(loadAll(ns, List.of(key)).get(key)); }
        @Override public void save(String ns, String key, byte[] value) { }
        @Override public void saveAll(String ns, Map<String, byte[]> values) { }
        @Override public Optional<byte[]> saveIfAbsent(String ns, String key, byte[] value) { return Optional.empty(); }
        @Override public void delete(String ns, String key) { }
        @Override public void dropNamespace(String ns) { }
        @Override public long count(String ns) { return 0; }
    }
}
