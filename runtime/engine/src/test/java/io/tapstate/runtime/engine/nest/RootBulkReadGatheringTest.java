package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.*;

import io.tapstate.spi.store.KeyedStateStore;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RootBulkReadGatheringTest {
    @Test void aFreshRootBulkWaveSharesItsFirstBackendResult() throws Exception {
        Store store = new Store(); NestStateIo io = root(store, 1_000);
        List<Thread> threads = new ArrayList<>(); List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                String key = "key-" + i;
                tasks.add(start(() -> io.loadAll(List.of(key, "missing")), threads));
                if (i == 0) NestStateIoTest.awaitAllBlockedInsideIo(threads);
            }
            assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(store.batches.getFirst()).containsExactlyInAnyOrder("key-0", "key-1", "key-2", "missing");
            assertThat(tasks).noneMatch(FutureTask::isDone);
            store.release.countDown();
            for (int i = 0; i < tasks.size(); i++) assertThat(tasks.get(i).get(5, TimeUnit.SECONDS)).containsOnlyKeys("key-" + i);
            assertThat(store.batches).hasSize(1);
        } finally { close(store, threads); }
    }

    @Test void emptyRequestsWritesAndInspectionDoNotInitializeTheReadQueue() {
        Store store = new Store(); store.release.countDown(); AtomicInteger classifications = new AtomicInteger();
        NestStateIo io = new NestStateIo(store, "ns", () -> { classifications.incrementAndGet(); return false; });
        assertThat(io.loadAll(List.of())).isEmpty();
        io.saveAll(Map.of("write", new byte[] {2}));
        assertThat(io.queuedReads()).isEqualTo(new NestStateIo.Pending(0, 0, 0));
        assertThat(classifications.get()).isZero();
        assertThat(io.load("single")).isNotNull();
        assertThat(classifications.get()).isEqualTo(1);
        assertThat(io.load("another-single")).isNotNull();
        assertThat(classifications.get()).isEqualTo(1);
        assertThat(io.loadAll(List.of("other"))).containsOnlyKeys("other");
        assertThat(classifications.get()).isEqualTo(2);
        assertThat(store.batches).containsExactly(List.of("single"), List.of("another-single"), List.of("other"));
    }

    @Test void concurrentFirstScalarAndBulkReadsShareOneQueue() throws Exception {
        Store store = new Store(); var classifyTogether = new CyclicBarrier(2); var classified = new CountDownLatch(2);
        NestStateIo io = new NestStateIo(store, "ns", 0, TimeUnit.SECONDS.toNanos(1), () -> {
            try { classifyTogether.await(5, TimeUnit.SECONDS); }
            catch (Exception failed) { throw new AssertionError(failed); }
            classified.countDown(); return true;
        });
        List<Thread> threads = new ArrayList<>();
        try {
            var scalar = start(() -> io.load("scalar"), threads);
            var bulk = start(() -> io.loadAll(List.of("bulk")), threads);
            assertThat(classified.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            assertThat(threads).allSatisfy(thread -> assertThat(Arrays.stream(thread.getStackTrace())
                    .noneMatch(frame -> frame.getMethodName().equals("gatherReads"))).isTrue());
            assertThat(store.batches).hasSize(1);
            store.release.countDown();
            assertThat(scalar.get(5, TimeUnit.SECONDS)).containsExactly((byte) 1);
            assertThat(bulk.get(5, TimeUnit.SECONDS)).containsOnlyKeys("bulk");
        } finally { close(store, threads); }
    }

    @Test void failedInitialClassificationPublishesNoQueueAndCanBeRetried() {
        Store store = new Store(); store.release.countDown(); AtomicInteger classifications = new AtomicInteger();
        var failure = new IllegalStateException("classification unavailable");
        NestStateIo io = new NestStateIo(store, "ns", () -> {
            if (classifications.incrementAndGet() == 1) throw failure;
            return true;
        });
        assertThatThrownBy(() -> io.load("first")).isSameAs(failure);
        assertThat(io.queuedReads()).isEqualTo(new NestStateIo.Pending(0, 0, 0));
        assertThat(classifications.get()).isEqualTo(1);
        assertThat(store.batches).isEmpty();
        assertThat(io.load("retry")).isNotNull();
        assertThat(io.load("next")).isNotNull();
        assertThat(classifications.get()).isEqualTo(2);
        assertThat(store.batches).containsExactly(List.of("retry"), List.of("next"));
    }

    @Test void aScalarArrivalEndsTheBulkGatherWithoutStartingAnotherWindow() throws Exception {
        Store store = new Store(); NestStateIo io = root(store, 1_000); List<Thread> threads = new ArrayList<>();
        try {
            FutureTask<Map<String, byte[]>> bulk = start(() -> io.loadAll(List.of("bulk")), threads);
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            assertThat(store.entered.getCount()).isEqualTo(1);
            FutureTask<byte[]> scalar = start(() -> io.load("scalar"), threads);
            assertThat(store.entered.await(500, TimeUnit.MILLISECONDS)).isTrue();
            assertThat(store.batches.getFirst()).containsExactlyInAnyOrder("bulk", "scalar");
            assertThat(bulk.isDone()).isFalse(); assertThat(scalar.isDone()).isFalse();
            store.release.countDown(); bulk.get(5, TimeUnit.SECONDS); scalar.get(5, TimeUnit.SECONDS);
        } finally { close(store, threads); }
    }

    @Test void aScalarReadDoesNotOpenTheEnabledBulkWindow() throws Exception {
        Store store = new Store(); NestStateIo io = root(store, 1_000); List<Thread> threads = new ArrayList<>();
        try {
            FutureTask<byte[]> scalar = start(() -> io.load("scalar"), threads);
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            assertThat(io.queuedReads().requests()).isZero();
            assertThat(store.entered.getCount()).isZero();
            assertThat(scalar.isDone()).isFalse();
            store.release.countDown(); assertThat(scalar.get(5, TimeUnit.SECONDS)).containsExactly((byte) 1);
        } finally { close(store, threads); }
    }

    @Test void twoIsolatedBulkReadsStopExtraWaitingAndActualConcurrencyRestoresIt() throws Exception {
        Store store = new Store(); store.release.countDown(); NestStateIo io = root(store, 100);
        io.loadAll(List.of("isolated-0")); io.loadAll(List.of("isolated-1"));
        store.entered = new CountDownLatch(1); store.release = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>(); List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        try {
            tasks.add(start(() -> io.loadAll(List.of("active")), threads));
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            assertThat(io.queuedReads().requests()).isZero();
            assertThat(store.entered.getCount()).isZero();
            tasks.add(start(() -> io.loadAll(List.of("queued-0")), threads));
            tasks.add(start(() -> io.loadAll(List.of("queued-1")), threads));
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            store.release.countDown(); for (var task : tasks) task.get(5, TimeUnit.SECONDS);
            store.entered = new CountDownLatch(1); store.release = new CountDownLatch(1);
            tasks.add(start(() -> io.loadAll(List.of("restored")), threads));
            NestStateIoTest.awaitAllBlockedInsideIo(List.of(threads.getLast()));
            assertThat(io.queuedReads().requests()).isEqualTo(1);
            assertThat(store.entered.getCount()).isEqualTo(1);
            store.release.countDown(); tasks.getLast().get(5, TimeUnit.SECONDS);
        } finally { close(store, threads); }
    }

    @Test void laterReadArrivalsDoNotExtendTheOldestDeadline() throws Exception {
        Store store = new Store(); NestStateIo io = root(store, 100); List<Thread> threads = new ArrayList<>();
        List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < 6; i++) {
                String key = "arrival-" + i;
                tasks.add(start(() -> io.loadAll(List.of(key)), threads));
                if (i == 0) NestStateIoTest.awaitAllBlockedInsideIo(threads);
                Thread.sleep(50);
            }
            assertThat(store.entered.getCount()).isZero();
            assertThat(store.batches.getFirst().size()).isLessThan(6);
            assertThat(tasks).noneMatch(FutureTask::isDone);
            store.release.countDown(); for (var task : tasks) task.get(5, TimeUnit.SECONDS);
        } finally { close(store, threads); }
    }

    @Test void anOversizedReadDoesNotWaitToGather() throws Exception {
        Store store = new Store(); NestStateIo io = root(store, 1_000); List<Thread> threads = new ArrayList<>();
        List<String> keys = java.util.stream.IntStream.range(0, 1_001).mapToObj(i -> "key-" + i).toList();
        try {
            var task = start(() -> io.loadAll(keys), threads);
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            assertThat(io.queuedReads().requests()).isZero();
            assertThat(store.entered.getCount()).isZero();
            store.release.countDown(); assertThat(task.get(5, TimeUnit.SECONDS)).hasSize(1_001);
        } finally { close(store, threads); }
    }

    @Test void gatheredReadFailureReachesAllCallersAndCanBeRetried() throws Exception {
        failure(new IllegalStateException("read failure"));
    }
    @Test void aFatalReadFailureRetainsTheOriginalErrorAndReleasesAllCallers() throws Exception {
        failure(new AssertionError("fatal read failure"));
    }
    private static void failure(Throwable failure) throws Exception {
        Store store = new Store(); store.failure = failure; NestStateIo io = root(store, 1_000);
        List<Thread> threads = new ArrayList<>(); List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                String key = "failed-" + i; tasks.add(start(() -> io.loadAll(List.of(key)), threads));
                if (i == 0) NestStateIoTest.awaitAllBlockedInsideIo(threads);
            }
            assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(store.batches.getFirst()).hasSize(3); store.release.countDown();
            for (var task : tasks) assertThatThrownBy(() -> task.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCause(failure);
            store.failure = null;
            assertThat(io.loadAll(List.of("retry"))).containsOnlyKeys("retry");
        } finally { close(store, threads); }
    }

    @Test void anInterruptedReadLeaderStillAwaitsTheOutcomeAndRestoresItsFlag() throws Exception {
        Store store = new Store(); NestStateIo io = root(store, 1_000); List<Thread> threads = new ArrayList<>();
        try {
            FutureTask<Boolean> first = start(() -> { io.loadAll(List.of("first")); return Thread.currentThread().isInterrupted(); }, threads);
            NestStateIoTest.awaitAllBlockedInsideIo(threads); threads.getFirst().interrupt();
            var second = start(() -> io.loadAll(List.of("second")), threads);
            assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(first.isDone()).isFalse(); assertThat(second.isDone()).isFalse();
            store.release.countDown(); assertThat(first.get(5, TimeUnit.SECONDS)).isTrue(); second.get(5, TimeUnit.SECONDS);
        } finally { close(store, threads); }
    }

    @Test void retiringTheOriginalBindingDuringGatherFencesEveryReadBeforeBackendAccess() throws Exception {
        var context = new ConcurrentHashMap<String, Object>();
        var member = (com.hazelcast.core.HazelcastInstance) java.lang.reflect.Proxy.newProxyInstance(
                com.hazelcast.core.HazelcastInstance.class.getClassLoader(),
                new Class<?>[] {com.hazelcast.core.HazelcastInstance.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "getUserContext" -> context;
                    case "getDistributedObjects" -> List.of();
                    default -> throw new AssertionError(method.getName());
                });
        Store store = new Store();
        var scope = new io.tapstate.spi.store.PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
        var placement = io.tapstate.runtime.engine.OperatorMapCoordinates.nest(scope, "cache", "nest", List.of(),
                io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.STATE, null);
        var binding = new io.tapstate.runtime.engine.OperatorMapBinding(placement, "attempt", "1".repeat(64));
        var registry = io.tapstate.runtime.engine.OperatorMapBindings.bindTo(member, (original, active) -> store);
        registry.install(List.of(binding));
        NestStateIo rootIo = new NestStateIo(registry.nest(binding.mapName(), "cache"), binding.mapName(), 0,
                TimeUnit.SECONDS.toNanos(1), () -> registry.isRootNestState(binding.mapName()));
        List<Thread> threads = new ArrayList<>(); List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                String key = "fenced-" + i; tasks.add(start(() -> rootIo.loadAll(List.of(key)), threads));
                if (i == 0) NestStateIoTest.awaitAllBlockedInsideIo(threads);
            }
            NestStateIoTest.awaitAllBlockedInsideIo(threads);
            assertThat(rootIo.queuedReads().requests()).isEqualTo(3);
            registry.close();
            Throwable shared = null;
            for (var task : tasks) {
                try { task.get(5, TimeUnit.SECONDS); fail("retired reads must be fenced"); }
                catch (ExecutionException failed) {
                    assertThat(failed.getCause()).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            coded -> assertThat(coded.code()).isEqualTo(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED));
                    if (shared == null) shared = failed.getCause(); else assertThat(failed.getCause()).isSameAs(shared);
                }
            }
            assertThat(store.batches).isEmpty();
        } finally { registry.close(); close(store, threads); }
    }

    private static NestStateIo root(Store store, long millis) {
        return new NestStateIo(store, "ns", 0, TimeUnit.MILLISECONDS.toNanos(millis));
    }
    private static <T> FutureTask<T> start(Callable<T> work, List<Thread> threads) {
        FutureTask<T> task = new FutureTask<>(work); Thread thread = new Thread(task, "root-read-test-" + threads.size());
        threads.add(thread); thread.start(); return task;
    }
    private static void close(Store store, List<Thread> threads) throws InterruptedException {
        store.release.countDown(); for (var thread : threads) thread.join(5_000);
        assertThat(threads).noneMatch(Thread::isAlive);
    }
    private static final class Store implements KeyedStateStore {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final List<List<String>> batches = new CopyOnWriteArrayList<>(); volatile Throwable failure;
        @Override public Map<String, byte[]> loadAll(String ns, Collection<String> keys) {
            batches.add(List.copyOf(keys)); entered.countDown();
            try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("read not released"); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
            if (failure instanceof RuntimeException e) throw e;
            if (failure instanceof Error e) throw e;
            Map<String, byte[]> found = new LinkedHashMap<>(); for (String key : keys) if (!key.equals("missing")) found.put(key, new byte[] {1}); return found;
        }
        @Override public Optional<byte[]> load(String ns, String key) { return Optional.ofNullable(loadAll(ns, List.of(key)).get(key)); }
        @Override public void save(String ns, String key, byte[] value) { }
        @Override public void saveAll(String ns, Map<String, byte[]> states) { }
        @Override public Optional<byte[]> saveIfAbsent(String ns, String key, byte[] value) { return Optional.empty(); }
        @Override public void delete(String ns, String key) { }
        @Override public void dropNamespace(String ns) { }
        @Override public long count(String ns) { return 0; }
    }
}
