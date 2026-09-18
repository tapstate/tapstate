package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One member's own answer to "may this run still touch anything outside the cluster". Every member that
 * runs a piece of a job holds one, and every batch of external calls asks it before making them.
 *
 * <p>It is asked locally and answered locally on purpose: the coordination store is read on a bounded
 * schedule in the background, never once per record, and the answer is trusted for a bounded window
 * measured on the monotonic clock from a point taken <em>before</em> the read went out — so the window
 * cannot outlive the answer it was cut from, and moving the node's wall clock cannot widen it.
 *
 * <p>What it compares is the pair of generations the run was submitted with against the pair the store
 * holds now. A refresh that cannot reach the store, a record that has gone, or either generation having
 * moved all read the same way: refused. Refusing is the safe direction — it stops calls going out, and
 * the run that is genuinely current re-reads its own generations on the next refresh and carries on.
 *
 * <p>What this bounds, and what it does not: after ownership actually changes, this member stops within
 * one window of its last successful refresh, while the next owner cannot even acquire the claim until
 * the previous lease has run out — a longer wait by design, which is why the two do not overlap in the
 * budget the cluster is configured with. A call already in flight when the window closes may still land;
 * that is the delivery contract's business, and the durable position it would advance is fenced
 * separately by the store itself.
 */
final class ExecutionAuthorization implements AutoCloseable {

    static final String USER_CONTEXT_KEY = "tapstate.cluster.execution-authorization";

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionAuthorization.class);

    /** What this member last read for one pipeline, and the monotonic point that reading expires at. */
    private record Entry(long claimGeneration, long executionGeneration, long deadlineNanos) {
    }

    private final String clusterId;
    private final WorkloadClaimStore claims;
    private final long windowNanos;
    private final long refreshIntervalNanos;
    private final LongSupplier nanoTime;
    private final boolean fenced;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final Map<String, Long> nextReadNanos = new ConcurrentHashMap<>();
    private final ScheduledExecutorService refresher;

    private ExecutionAuthorization() {
        this.clusterId = "single";
        this.claims = null;
        this.windowNanos = 0;
        this.refreshIntervalNanos = 0;
        this.nanoTime = System::nanoTime;
        this.fenced = false;
        this.refresher = null;
    }

    /** A single-node run has one member and one run of anything, so nothing here is fenced. */
    static ExecutionAuthorization unfenced() {
        return new ExecutionAuthorization();
    }

    ExecutionAuthorization(String clusterId, WorkloadClaimStore claims, Duration window) {
        this(clusterId, claims, window, System::nanoTime);
    }

    ExecutionAuthorization(
            String clusterId, WorkloadClaimStore claims, Duration window, LongSupplier nanoTime) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.claims = Objects.requireNonNull(claims, "claims");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        Objects.requireNonNull(window, "window");
        if (window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("the local authorization window must be positive");
        }
        this.windowNanos = window.toNanos();
        // Twice per window: often enough that the batch path normally finds a live answer and never waits
        // on the store itself, and it doubles as the floor on how often an unreachable store is retried.
        this.refreshIntervalNanos = Math.max(1, windowNanos / 2);
        this.fenced = true;
        this.refresher = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-execution-authorization");
            thread.setDaemon(true);
            return thread;
        });
        long period = Math.max(1, TimeUnit.NANOSECONDS.toMillis(refreshIntervalNanos));
        refresher.scheduleWithFixedDelay(this::refreshAll, period, period, TimeUnit.MILLISECONDS);
    }

    /**
     * The guard bound to {@code member}, or an unfenced one where nothing is bound — a single-node run,
     * or a member brought up without a coordination store, neither of which has a second run to fence
     * against.
     */
    static ExecutionAuthorization of(HazelcastInstance member) {
        Object bound = member.getUserContext().get(USER_CONTEXT_KEY);
        return bound instanceof ExecutionAuthorization authorization ? authorization : unfenced();
    }

    /** The guard bound to this member, resolved where only the local process is in hand. */
    static ExecutionAuthorization local() {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        return instances.size() == 1 ? of(instances.iterator().next()) : unfenced();
    }

    /** Refuses, with a diagnosis, when this member may no longer act for {@code fence}'s run. */
    void require(ExecutionFence fence) {
        if (!authorized(fence)) {
            throw new TapstateException(
                    EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", fence.pipelineId()), null);
        }
    }

    /** Whether {@code fence}'s run is still the current one, as far as this member can prove locally. */
    boolean authorized(ExecutionFence fence) {
        if (!fenced) {
            return true;
        }
        Objects.requireNonNull(fence, "fence");
        long now = nanoTime.getAsLong();
        Entry entry = entries.get(fence.pipelineId());
        if (entry == null || now - entry.deadlineNanos() >= 0) {
            entry = read(fence.pipelineId(), false);
        }
        return entry != null
                && entry.claimGeneration() == fence.claimGeneration()
                && entry.executionGeneration() == fence.executionGeneration();
    }

    private void refreshAll() {
        for (String pipelineId : Set.copyOf(entries.keySet())) {
            try {
                read(pipelineId, true);
            } catch (RuntimeException unreachable) {
                // read() has already dropped the entry, which is what refuses the next batch. Nothing
                // else is owed here: the point of the background pass is that the batch path finds a
                // fresh answer, not that it reports on the store.
                LOG.debug("Could not refresh the execution authorization for {}", pipelineId, unreachable);
            }
        }
    }

    /**
     * Reads one pipeline's current generations, at most once per window even while the store is
     * unreachable — the batch path calls this whenever its answer has expired, and a store that is down
     * must not turn that into a round trip per batch.
     */
    private synchronized Entry read(String pipelineId, boolean evenIfStillLive) {
        long now = nanoTime.getAsLong();
        Entry entry = entries.get(pipelineId);
        if (entry != null && !evenIfStillLive && now - entry.deadlineNanos() < 0) {
            // Refreshed by the background pass, or by another batch, while this call waited for the lock.
            return entry;
        }
        Long nextRead = nextReadNanos.get(pipelineId);
        if (nextRead != null && now - nextRead < 0) {
            return evenIfStillLive ? entry : null;
        }
        // Taken before the request goes out: whatever comes back is no older than this point, so a window
        // measured from here is never longer than the answer was actually good for.
        long startedAt = nanoTime.getAsLong();
        nextReadNanos.put(pipelineId, startedAt + refreshIntervalNanos);
        Optional<WorkloadClaim> current;
        try {
            current = claims.read(
                    new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId));
        } catch (RuntimeException unreachable) {
            entries.remove(pipelineId);
            return null;
        }
        if (current.isEmpty()) {
            entries.remove(pipelineId);
            return null;
        }
        Entry refreshed = new Entry(
                current.get().claimGeneration(), current.get().executionGeneration(),
                startedAt + windowNanos);
        entries.put(pipelineId, refreshed);
        return refreshed;
    }

    @Override
    public void close() {
        if (refresher != null) {
            refresher.shutdownNow();
        }
    }
}
