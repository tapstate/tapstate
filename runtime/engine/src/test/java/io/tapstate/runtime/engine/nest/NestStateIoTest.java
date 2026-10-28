package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.spi.store.KeyedStateStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

class NestStateIoTest {
    @Test
    void partitionReadsWaitingOnOneBackendCallAreSentAsOneFollowingBatch() throws Exception {
        BlockingStore store = new BlockingStore();
        NestStateIo io = new NestStateIo(store, "ns");
        List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) {
                String key = "key-" + i;
                FutureTask<Map<String, byte[]>> task = new FutureTask<>(() -> io.loadAll(List.of(key, "missing")));
                tasks.add(task);
                Thread thread = new Thread(task, "nest-batch-read-" + i);
                threads.add(thread);
                thread.start();
                if (i == 0) assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            awaitAllBlockedInsideIo(threads);
            store.release.countDown();
            for (int i = 0; i < tasks.size(); i++) {
                assertThat(tasks.get(i).get(5, TimeUnit.SECONDS)).containsOnlyKeys("key-" + i);
            }
            assertThat(store.readCalls.get()).as("one active call, then one coalesced batch of waiting partitions")
                    .isEqualTo(2);
        } finally {
            store.release.countDown();
            for (Thread thread : threads) thread.join(5_000);
        }
    }

    @Test
    void writeWaitersReturnOnlyAfterTheirOwnDurableBatchAndEveryValueIsRetained() throws Exception {
        BlockingStore store = new BlockingStore();
        NestStateIo io = new NestStateIo(store, "ns");
        List<FutureTask<Void>> tasks = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) {
                String key = "key-" + i;
                FutureTask<Void> task = new FutureTask<>(() -> { io.saveAll(Map.of(key, new byte[] {1})); return null; });
                tasks.add(task);
                Thread thread = new Thread(task, "nest-batch-write-" + i);
                threads.add(thread);
                thread.start();
                if (i == 0) assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            awaitAllBlockedInsideIo(threads);
            assertThat(tasks).noneMatch(FutureTask::isDone);
            store.release.countDown();
            for (FutureTask<Void> task : tasks) task.get(5, TimeUnit.SECONDS);
            assertThat(store.writeCalls.get()).isEqualTo(2);
            assertThat(store.saved).hasSize(8);
        } finally {
            store.release.countDown();
            for (Thread thread : threads) thread.join(5_000);
        }
    }

    @Test
    void anEmptyRequestDoesNotCallTheBackend() {
        BlockingStore store = new BlockingStore();
        store.release.countDown();
        NestStateIo io = new NestStateIo(store, "ns");
        assertThat(io.loadAll(List.of())).isEmpty();
        io.saveAll(Map.of());
        assertThat(store.readCalls.get()).isZero();
        assertThat(store.writeCalls.get()).isZero();
    }

    @Test
    void backendFailuresReachAllWaitersAndDoNotPoisonTheNextCall() throws Exception {
        assertFailureReachesWaiters(new IllegalStateException("injected backend failure"));
    }

    @Test
    void fatalBackendFailuresAlsoReleaseWaitersAndAreRethrownUnchanged() throws Exception {
        assertFailureReachesWaiters(new AssertionError("injected fatal backend failure"));
    }

    private static void assertFailureReachesWaiters(Throwable failure) throws Exception {
        BlockingStore store = new BlockingStore();
        store.failure = failure;
        NestStateIo io = new NestStateIo(store, "ns");
        List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        try {
            for (int i = 0; i < 4; i++) {
                String key = "key-" + i;
                FutureTask<Map<String, byte[]>> task = new FutureTask<>(() -> io.loadAll(List.of(key)));
                tasks.add(task);
                start(task, threads);
                if (i == 0) assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            awaitAllBlockedInsideIo(threads);
            store.release.countDown();
            for (FutureTask<Map<String, byte[]>> task : tasks) {
                assertThatThrownBy(() -> task.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                        .hasCause(store.failure);
            }
            store.failure = null;
            assertThat(io.loadAll(List.of("recovered"))).containsOnlyKeys("recovered");
        } finally { close(store, threads); }
    }

    @Test
    void competingWritesToOneKeyAreNotCollapsedIntoOneAcknowledgement() throws Exception {
        BlockingStore store = new BlockingStore();
        NestStateIo io = new NestStateIo(store, "ns");
        List<FutureTask<Void>> tasks = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) {
                byte value = (byte) i;
                FutureTask<Void> task = new FutureTask<>(() -> {
                    io.saveAll(Map.of("same", new byte[] {value})); return null;
                });
                tasks.add(task);
                start(task, threads);
                awaitAllBlockedInsideIo(threads);
            }
            store.release.countDown();
            for (FutureTask<Void> task : tasks) task.get(5, TimeUnit.SECONDS);
            assertThat(store.writeCalls.get()).isEqualTo(8);
            assertThat(store.saved.get("same")).containsExactly((byte) 7);
            assertThat(store.writtenValues).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
        } finally { close(store, threads); }
    }

    @Test
    void interruptingAWriteWaiterDoesNotAcknowledgeAnUnfinishedWrite() throws Exception {
        BlockingStore store = new BlockingStore();
        NestStateIo io = new NestStateIo(store, "ns");
        List<Thread> threads = new ArrayList<>();
        FutureTask<Void> first = new FutureTask<>(() -> { io.saveAll(Map.of("first", new byte[] {1})); return null; });
        FutureTask<Boolean> second = new FutureTask<>(() -> {
            io.saveAll(Map.of("second", new byte[] {2}));
            return Thread.currentThread().isInterrupted();
        });
        try {
            start(first, threads);
            assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            start(second, threads);
            awaitAllBlockedInsideIo(threads);
            threads.getLast().interrupt();
            assertThat(second.isDone()).isFalse();
            store.release.countDown();
            first.get(5, TimeUnit.SECONDS);
            assertThat(second.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(store.saved).containsOnlyKeys("first", "second");
        } finally { close(store, threads); }
    }

    @Test
    void thePendingQueueBoundsRequestsAndNamedKeys() throws Exception {
        BlockingStore store = new BlockingStore();
        NestStateIo io = new NestStateIo(store, "ns");
        List<Thread> threads = new ArrayList<>();
        List<FutureTask<Map<String, byte[]>>> tasks = new ArrayList<>();
        try {
            for (int i = 0; i < 66; i++) {
                String key = "key-" + i;
                FutureTask<Map<String, byte[]>> task = new FutureTask<>(() -> io.loadAll(List.of(key)));
                tasks.add(task);
                start(task, threads);
                if (i == 0) assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            awaitAllBlockedInsideIo(threads);
            assertThat(io.queuedReads().requests()).isEqualTo(64);
            assertThat(io.queuedReads().keys()).isEqualTo(64);
            store.release.countDown();
            for (FutureTask<Map<String, byte[]>> task : tasks) assertThat(task.get(5, TimeUnit.SECONDS)).hasSize(1);
        } finally { close(store, threads); }
    }

    @Test
    void pendingWritesAreBoundedByBytesEvenWhenFewKeysAreQueued() throws Exception {
        BlockingStore store = new BlockingStore();
        NestStateIo io = new NestStateIo(store, "ns");
        List<Thread> threads = new ArrayList<>();
        List<FutureTask<Void>> tasks = new ArrayList<>();
        byte[] value = new byte[4 * 1024 * 1024];
        try {
            for (int i = 0; i < 4; i++) {
                String key = "key-" + i;
                FutureTask<Void> task = new FutureTask<>(() -> { io.saveAll(Map.of(key, value)); return null; });
                tasks.add(task);
                start(task, threads);
                if (i == 0) assertThat(store.entered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            awaitAllBlockedInsideIo(threads);
            assertThat(io.queuedWrites().requests()).isEqualTo(1);
            assertThat(io.queuedWrites().bytes()).isLessThanOrEqualTo(8 * 1024 * 1024);
            store.release.countDown();
            for (FutureTask<Void> task : tasks) task.get(5, TimeUnit.SECONDS);
            assertThat(store.saved).hasSize(4);
        } finally { close(store, threads); }
    }

    private static void start(FutureTask<?> task, List<Thread> threads) {
        Thread thread = new Thread(task, "nest-io-test-" + threads.size());
        threads.add(thread);
        thread.start();
    }

    private static void close(BlockingStore store, List<Thread> threads) throws InterruptedException {
        store.release.countDown();
        for (Thread thread : threads) thread.join(5_000);
        assertThat(threads).noneMatch(Thread::isAlive);
    }

    private static void awaitAllBlockedInsideIo(List<Thread> threads) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            boolean blocked = threads.stream().allMatch(thread -> (thread.getState() == Thread.State.WAITING
                    || thread.getState() == Thread.State.TIMED_WAITING)
                    && java.util.Arrays.stream(thread.getStackTrace())
                            .anyMatch(frame -> frame.getClassName().startsWith(NestStateIo.class.getName())));
            if (blocked) return;
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        throw new AssertionError("all callers must have reached the blocked I/O path before release");
    }

    private static final class BlockingStore implements KeyedStateStore {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger readCalls = new AtomicInteger();
        private final AtomicInteger writeCalls = new AtomicInteger();
        private final Map<String, byte[]> saved = new ConcurrentHashMap<>();
        private final List<Integer> writtenValues = new CopyOnWriteArrayList<>();
        private volatile Throwable failure;

        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            readCalls.incrementAndGet();
            block();
            throwFailureIfAny();
            Map<String, byte[]> found = new LinkedHashMap<>();
            for (String key : keys) if (!key.equals("missing")) found.put(key, new byte[] {1});
            return found;
        }
        @Override public void saveAll(String namespace, Map<String, byte[]> states) {
            writeCalls.incrementAndGet();
            block();
            throwFailureIfAny();
            states.values().forEach(value -> writtenValues.add((int) value[0]));
            saved.putAll(states);
        }
        private void throwFailureIfAny() {
            if (failure instanceof RuntimeException raised) throw raised;
            if (failure instanceof Error raised) throw raised;
        }
        private void block() {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("backend was not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        @Override public Optional<byte[]> load(String namespace, String key) {
            return Optional.ofNullable(loadAll(namespace, List.of(key)).get(key));
        }
        @Override public void save(String namespace, String key, byte[] value) { saveAll(namespace, Map.of(key, value)); }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) {
            return Optional.ofNullable(saved.putIfAbsent(key, value));
        }
        @Override public void delete(String namespace, String key) { saved.remove(key); }
        @Override public void dropNamespace(String namespace) { saved.clear(); }
        @Override public long count(String namespace) { return saved.size(); }
    }
}
