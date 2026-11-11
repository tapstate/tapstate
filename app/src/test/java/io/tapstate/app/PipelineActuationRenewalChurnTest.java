package io.tapstate.app;

import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.DeadlineWorkloadClaimStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineActuationRenewalChurnTest {
    private static final int LANES = 4;
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration INTERVAL = Duration.ofSeconds(10);

    @Test void boundedSuccessorChurnCannotStarveAStableOriginalClaim() throws Exception {
        var properties = new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        var membership = new ClusterMembershipGate(properties);membership.observeNodeSession(() -> true);
        membership.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        assertThat(membership.canCommit(Set.of("node-a", "node-b"))).isTrue();
        try (var io = new ChurningIo()) {
            var owner = PipelineActuationOwnership.scoped(new CacheBusinessScope("cluster-a", 1, "cache-a"),
                    new WorkloadOwner("node-a", "boot-a"), membership,
                    new ClusterWorkloadClaims(io.store, membership), TTL, INTERVAL, io.nanos::get);
            var field = PipelineActuationOwnership.class.getDeclaredField("ioAdmission");field.setAccessible(true);
            io.gate = (ReentrantLock) field.get(owner);
            String stable = io.names.getFirst();
            var actors = new ArrayList<Future<?>>();
            Future<?> driver = null;
            boolean expired;
            try {
                for (int lane = 0; lane <= LANES; lane++) {
                    String id = io.names.get(lane);io.lanes.put(id, lane);io.desired.add(id);io.current.set(lane, id);
                    acquireAndAdvance(owner, id);
                }
                var original = io.durable.read(key(stable)).orElseThrow().claim();
                for (int lane = 1; lane <= LANES; lane++) {
                    int assigned = lane;
                    actors.add(io.callers.submit(() -> io.replaceRenewedClaims(owner, assigned)));
                }
                assertThat(io.ready.await(2, TimeUnit.SECONDS)).isTrue();
                io.elapse(INTERVAL);
                driver = io.callers.submit(() -> {
                    try {
                        while (!io.stopping.get()) {
                            owner.renewDue();io.completedPasses.incrementAndGet();
                            if (!io.stopping.get()) { Thread.sleep(1000);io.elapse(Duration.ofSeconds(1)); }
                        }
                    } catch (Throwable failure) { io.recordFailure(failure); }
                });
                long limit = System.nanoTime() + Duration.ofSeconds(90).toNanos();
                expired = false;
                while (io.nanos.get() < Duration.ofSeconds(55).toNanos() && System.nanoTime() < limit) {
                    io.requireHealthyActors();
                    if (!io.durable.read(key(stable)).orElseThrow().leased()) { expired = true;break; }
                    Thread.sleep(10);
                }
                io.requireHealthyActors();
                assertThat(expired || io.nanos.get() >= Duration.ofSeconds(55).toNanos())
                        .as("the controlled store clock made progress").isTrue();
                assertThat(membership.businessEligible()).isTrue();
                assertThat(io.admitted.get()).as("normally acquired and advanced successors").isGreaterThanOrEqualTo(4);
                assertThat(io.maxDesired.get()).isLessThanOrEqualTo(2 * LANES + 1);
                assertThat(io.allowances).allSatisfy(remaining -> assertThat(remaining).isPositive());
                assertThat(WorkloadClaimFence.from(io.durable.read(key(stable)).orElseThrow().claim()))
                        .isEqualTo(WorkloadClaimFence.from(original));
                System.out.println("Renewal churn: stableRenewals=" + io.stableRenewals.get()
                        + ", completedPasses=" + io.completedPasses.get() + ", successors=" + io.admitted.get()
                        + ", storeSeconds=" + Duration.ofNanos(io.nanos.get()).toSeconds() + ", expired=" + expired);
                assertThat(expired).as("bounded successor churn must not expire the stable original claim").isFalse();
                assertThat(io.stableRenewals.get()).isGreaterThan(1);
                assertThat(io.completedPasses.get()).isPositive();
            } finally {
                io.stopping.set(true);owner.stopForShutdown();
                if (driver != null) driver.get(10, TimeUnit.SECONDS);
                for (var actor : actors) actor.get(10, TimeUnit.SECONDS);
                owner.awaitStopped(System.nanoTime() + Duration.ofSeconds(5).toNanos());
            }
        }
    }

    private static WorkloadClaimKey key(String id) {
        return new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, id);
    }

    private static void acquireAndAdvance(PipelineActuationOwnership owner, String id) {
        var scope = new PipelineIntentScope("cluster-a", 1, "cache-a", id, 1);
        var permit = owner.permit(scope);assertThat(permit.granted()).isTrue();
        var execution = owner.beginExecution(scope, WorkloadClaimFence.from(permit.claim()));
        assertThat(execution.allowed()).isTrue();assertThat(execution.fence().executionGeneration()).isPositive();
    }

    /** Advances the independent store clock only after actual, individually bounded IO returns. */
    private static final class ChurningIo implements AutoCloseable {
        private final InMemoryWorkloadClaimStore durable = new InMemoryWorkloadClaimStore();
        private final AtomicLong nanos = new AtomicLong();
        private final java.util.concurrent.ExecutorService workers = Executors.newFixedThreadPool(4);
        private final java.util.concurrent.ExecutorService callers = Executors.newFixedThreadPool(5);
        private final List<Future<?>> tasks = new CopyOnWriteArrayList<>();
        private final List<Long> allowances = new CopyOnWriteArrayList<>();
        private final ThreadLocal<Long> limit = new ThreadLocal<>();
        private final AtomicBoolean stopping = new AtomicBoolean();
        private final AtomicReference<Throwable> problem = new AtomicReference<>();
        private final ConcurrentHashMap<String, Integer> lanes = new ConcurrentHashMap<>();
        private final AtomicReferenceArray<Thread> laneThreads = new AtomicReferenceArray<>(LANES + 1);
        private final AtomicReferenceArray<String> current = new AtomicReferenceArray<>(LANES + 1);
        private final List<BlockingQueue<String>> queues = new ArrayList<>();
        private final Set<String> desired = ConcurrentHashMap.newKeySet();
        private final AtomicInteger sequence = new AtomicInteger(LANES + 1);
        private final AtomicInteger admitted = new AtomicInteger();
        private final AtomicInteger stableRenewals = new AtomicInteger();
        private final AtomicInteger completedPasses = new AtomicInteger();
        private final AtomicInteger maxDesired = new AtomicInteger(LANES + 1);
        private final CountDownLatch ready = new CountDownLatch(LANES);
        private final List<String> names = new ArrayList<>();
        private final WorkloadClaimStore store;
        private ReentrantLock gate;

        private ChurningIo() {
            for (int lane = 0; lane <= LANES; lane++) queues.add(new LinkedBlockingQueue<>());
            for (int i = 0; i < 256; i++) {
                var name = new StringBuilder();
                for (int bit = 0; bit < 8; bit++) name.append((i & (1 << bit)) == 0 ? "Aa" : "BB");
                names.add(name.toString());
            }
            assertThat(names.stream().map(String::hashCode).distinct().count()).isEqualTo(1);
            store = (WorkloadClaimStore) Proxy.newProxyInstance(WorkloadClaimStore.class.getClassLoader(),
                    new Class<?>[]{DeadlineWorkloadClaimStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("withDeadline")) {
                            return bounded((long) args[0], args.length == 3 ? (Runnable) args[1] : () -> {},
                                    (Supplier<?>) args[args.length - 1]);
                        }
                        if (method.getName().equals("awaitQuiescence")) { await((long) args[0]);return null; }
                        if (method.getName().equals("acquirePipeline")) {
                            var scope = (PipelineIntentScope) args[0];
                            return durable.acquire(key(scope.pipelineId()), (WorkloadOwner) args[1], (long) args[2], (Duration) args[3]);
                        }
                        if (method.getName().equals("renew")) return renew((WorkloadClaim) args[0], (Duration) args[1]);
                        try { return method.invoke(durable, args); }
                        catch (InvocationTargetException failed) { throw failed.getCause(); }
                    });
        }

        private Object bounded(long deadline, Runnable guard, Supplier<?> action) throws Throwable {
            var task = workers.submit(() -> {
                limit.set(deadline);
                try { guard.run();return action.get(); }
                finally { limit.remove(); }
            });
            tasks.add(task);
            try { return task.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
            catch (java.util.concurrent.TimeoutException expired) { throw StorageRuntime.unavailable(); }
            catch (java.util.concurrent.ExecutionException failed) { throw failed.getCause(); }
        }

        private Object renew(WorkloadClaim original, Duration ttl) throws Exception {
            String id = original.key().resourceId();
            if (id.equals(names.getFirst())) stableRenewals.incrementAndGet();
            Thread.sleep(3500);elapse(Duration.ofMillis(3500));
            assertThat(limit.get()).isNotNull();
            long remaining = limit.get() - System.nanoTime();allowances.add(remaining);
            assertThat(remaining).as("IO stayed inside its original ten-second allowance").isPositive();
            var renewed = durable.renew(original, ttl);
            Integer lane = lanes.get(id);
            if (renewed.isPresent() && lane != null && lane > 0 && !stopping.get()) {
                queues.get(lane).add(id);
                long until = System.nanoTime() + Duration.ofSeconds(1).toNanos();
                while (!stopping.get() && !gate.hasQueuedThread(laneThreads.get(lane)) && System.nanoTime() < until) Thread.sleep(1);
                if (!stopping.get()) assertThat(gate.hasQueuedThread(laneThreads.get(lane))).isTrue();
            }
            return renewed;
        }

        private void replaceRenewedClaims(PipelineActuationOwnership owner, int lane) {
            laneThreads.set(lane, Thread.currentThread());ready.countDown();
            try {
                while (!stopping.get()) {
                    String old = queues.get(lane).poll(100, TimeUnit.MILLISECONDS);
                    if (old == null || !old.equals(current.get(lane))) continue;
                    String next = names.get(sequence.getAndIncrement());lanes.put(next, lane);desired.add(next);
                    maxDesired.accumulateAndGet(desired.size(), Math::max);
                    acquireAndAdvance(owner, next);
                    admitted.incrementAndGet();desired.remove(old);owner.retain(desired);current.set(lane, next);
                }
            } catch (Throwable failure) { recordFailure(failure); }
        }

        private void elapse(Duration elapsed) { durable.elapse(elapsed);nanos.addAndGet(elapsed.toNanos()); }
        private void recordFailure(Throwable failure) { if (!stopping.get()) problem.compareAndSet(null, failure); }
        private void requireHealthyActors() { if (problem.get() != null) throw new AssertionError("A churn actor failed", problem.get()); }

        private void await(long deadline) throws Exception {
            for (var task : tasks) {
                try { task.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                catch (java.util.concurrent.ExecutionException failed) { if (!stopping.get()) throw failed; }
            }
        }

        @Override public void close() throws Exception {
            stopping.set(true);callers.shutdown();workers.shutdown();
            assertThat(callers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
