package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
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
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One member's own answer to "may this run still touch anything outside the cluster". Every member that
 * runs a piece of a job holds one, and every batch of external calls asks it before making them.
 *
 * <p>It is asked locally and answered locally on purpose: the coordination store is read on a bounded
 * schedule in the background, never once per record, and the answer is trusted until a deadline measured
 * on the monotonic clock from a point taken <em>before</em> the read went out — so the deadline cannot
 * outlive the answer it was cut from, and moving the node's wall clock cannot widen it.
 *
 * <p>What it compares is the pair of generations the run was submitted with against the pair the store
 * holds now. A refresh that cannot reach the store, a record that has gone, either generation having
 * moved, or the lease behind them having lapsed all read the same way: refused. Refusing is the safe
 * direction for a run that should stop — it stops calls going out.
 *
 * <p><b>A refusal is not a pause, so being behind is not a safe place to be.</b> It leaves here as an
 * exception, through the sink writer, into the processor, and it ends the job — there is no next refresh
 * for the run it refused, and nothing rebuilds a run whose members are all still present. So a reading
 * this member knows to be older than the run it is being asked about is refreshed on the spot rather
 * than waited out, and the floor that keeps an unreachable store from being asked per batch is not
 * allowed to become a stretch of time in which a live store is not asked at all. Measured 2026-09-20:
 * that stretch ended 14 of 20 unprompted failovers with the pipeline left failed for a person.
 *
 * <p>What this bounds, and what it does not: the deadline is the shorter of this member's refresh window
 * and what the store says is left of the owner's lease, so this member stops no later than the moment the
 * claim becomes available to anyone else — the two never overlap, rather than merely being sized so that
 * they usually do not. That also covers the case where nobody takes over at all: an owner that died
 * leaves a record whose generations go on matching, and only the lease says it is nobody's. A target call
 * already in flight when the deadline passes may still land; that is the delivery contract's business.
 * A durable acknowledgement carries the exact claim behind this answer to the store as a second fence,
 * because unlike a replayable target write its position cannot safely land after this answer expires.
 *
 * <p>What it still assumes is that the two clocks run at comparable rates — a lease handed out in the
 * store's seconds is counted down in this member's. Offsets between them are not assumed, which is the
 * point of asking the store how much is left rather than reading the deadline it holds.
 */
final class ExecutionAuthorization implements AutoCloseable {

    static final String USER_CONTEXT_KEY = "tapstate.cluster.execution-authorization";

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionAuthorization.class);

    /** The exact claim this member last read for one pipeline, and when that reading expires locally. */
    private record Entry(WorkloadClaimFence claim, long deadlineNanos) {
    }

    private final String clusterId;
    private final WorkloadClaimStore claims;
    private final Duration window;
    private final long windowNanos;
    private final long refreshIntervalNanos;
    private final LongSupplier nanoTime;
    private final Supplier<NodeSessionLease.Proof> sessionProof;
    private final boolean fenced;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final Map<String, Long> nextReadNanos = new ConcurrentHashMap<>();
    private final ScheduledExecutorService refresher;
    private volatile boolean closed;

    private ExecutionAuthorization() {
        this.clusterId = "single";
        this.claims = null;
        this.window = Duration.ZERO;
        this.windowNanos = 0;
        this.refreshIntervalNanos = 0;
        this.nanoTime = System::nanoTime;
        this.sessionProof = null;
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
        this(clusterId, claims, window, nanoTime, null);
    }

    ExecutionAuthorization(String clusterId, WorkloadClaimStore claims, Duration window, NodeSessionLease session) {
        this(clusterId, claims, window, System::nanoTime, session::proof);
    }

    ExecutionAuthorization(String clusterId, WorkloadClaimStore claims, Duration window, LongSupplier nanoTime,
            Supplier<NodeSessionLease.Proof> sessionProof) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.claims = Objects.requireNonNull(claims, "claims");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.sessionProof = sessionProof;
        Objects.requireNonNull(window, "window");
        if (window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("the local authorization window must be positive");
        }
        this.window = window;
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

    /**
     * The guard bound to this member, resolved where only the local process is in hand — the sink writer
     * a job carries is opened without a member handle, exactly as the connector it opens is.
     *
     * <p>Anything other than one member in this process is a refusal rather than an unfenced guard:
     * "which member is this" has no answer then, and answering "allow everything" would be a fence that
     * quietly is not one. The sink connector resolves itself the same way and refuses the same way.
     */
    static ExecutionAuthorization local() {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        if (instances.size() != 1) {
            throw new IllegalStateException(
                    "expected exactly one local Hazelcast member on the sink member, found "
                            + instances.size());
        }
        return of(instances.iterator().next());
    }

    /**
     * The exact live claim behind this member's answer, or {@code null} on an unfenced single-node member.
     * Refuses with a diagnosis when the member may no longer act for {@code fence}'s run.
     */
    WorkloadClaimFence require(ExecutionFence fence) {
        if (!fenced) {
            return null;
        }
        Entry authorized = authorizedEntry(fence);
        if (authorized == null) {
            throw new TapstateException(
                    EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", fence.pipelineId()), null);
        }
        return authorized.claim();
    }

    /** Whether {@code fence}'s run is still the current one, as far as this member can prove locally. */
    boolean authorized(ExecutionFence fence) {
        if (!fenced) {
            return true;
        }
        return authorizedEntry(fence) != null;
    }

    /** The current matching entry, refreshing it under the same rules as the boolean guard. */
    private Entry authorizedEntry(ExecutionFence fence) {
        Objects.requireNonNull(fence, "fence");
        if (closed) {
            return null;
        }
        long now = nanoTime.getAsLong();
        NodeSessionLease.Proof node = sessionProof == null ? null : sessionProof.get();
        if (sessionProof != null && (node == null || !node.live() || now - node.deadlineNanos() >= 0
                || node.claim().profileGeneration() != fence.profileGeneration())) {
            return null;
        }
        Entry entry = entries.get(fence.pipelineId());
        if (entry != null && precedes(entry, fence)) {
            // This reading was taken before the run asking about itself was submitted, so it is not the
            // one that decides it. Only the store hands generations out, and only upwards, so a run
            // carrying more than this member has read is a run submitted since this member last looked —
            // never a superseded one, which is what a reading is held against. So its own deadline is
            // spent rather than waited out: the answer is known to be out of date, and a refusal from it
            // does not pause the run — it reaches the job as an exception, which ends it.
            entry = read(fence.pipelineId(), true);
        } else if (entry == null || now - entry.deadlineNanos() >= 0) {
            entry = read(fence.pipelineId(), false);
        }
        return entry != null
                && entry.claim().claimGeneration() == fence.claimGeneration()
                && entry.claim().executionGeneration() == fence.executionGeneration()
                && entry.claim().profileGeneration() == fence.profileGeneration()
                && nanoTime.getAsLong() - entry.deadlineNanos() < 0
                ? entry
                : null;
    }

    /** Files a sink's known independent failure under the exact run while its claim is still live. */
    void recordSinkWriteFailure(ExecutionFence fence) {
        if (!fenced) {
            return;
        }
        try {
            Optional<WorkloadClaimReading> reading = claims.read(new WorkloadClaimKey(
                    clusterId, WorkloadClaimType.PIPELINE_ACTUATION, fence.pipelineId()));
            if (reading.isEmpty() || !reading.get().leased()) {
                return;
            }
            var claim = reading.get().claim();
            if (claim.claimGeneration() == fence.claimGeneration()
                    && claim.executionGeneration() == fence.executionGeneration()
                    && claim.profileGeneration() == fence.profileGeneration()) {
                claims.recordExecutionFailure(claim, false);
            }
        } catch (RuntimeException unreachable) {
            // A coordination-store error must not replace the sink's own failure.
            LOG.debug("Could not record the sink failure for {}", fence.pipelineId(), unreachable);
        }
    }

    /**
     * Whether {@code entry} was read before the run {@code fence} names was submitted, ordered the way
     * the store advances the pair: taking a claim over takes the next claim generation and carries the
     * execution generation across, and submitting a run under a claim takes that claim's next execution
     * generation.
     */
    private static boolean precedes(Entry entry, ExecutionFence fence) {
        return entry.claim().claimGeneration() < fence.claimGeneration()
                || (entry.claim().claimGeneration() == fence.claimGeneration()
                        && entry.claim().executionGeneration() < fence.executionGeneration());
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
     * Reads one pipeline's current generations, at most once per window while the store cannot be
     * reached — the batch path calls this whenever its answer has expired, and a store that is down must
     * not turn that into a round trip per batch.
     *
     * <p>That floor is held against an unreachable store and nothing else. Every other outcome below is
     * the store answering, and the two answers that refuse — no record, and a lease with nothing left of
     * it — are answers whose whole point is that somebody is about to take the claim over. Holding them
     * for a refresh interval is how a member comes to refuse the very run its own takeover submitted,
     * with no reading in hand to say so and no reading allowed until the floor runs out.
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
        // Taken before the request goes out: whatever comes back is no older than this point, so a deadline
        // measured from here is never longer than the answer was actually good for.
        long startedAt = nanoTime.getAsLong();
        Optional<WorkloadClaimReading> current;
        try {
            current = claims.read(
                    new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId));
        } catch (RuntimeException unreachable) {
            nextReadNanos.put(pipelineId, startedAt + refreshIntervalNanos);
            entries.remove(pipelineId);
            return null;
        }
        nextReadNanos.remove(pipelineId);
        if (current.isEmpty()) {
            entries.remove(pipelineId);
            return null;
        }
        // The shorter of the two bounds, because each covers what the other cannot. The window bounds how
        // stale this reading may be; the lease bounds how long the owner it names is still the owner. Two
        // generations that still match say nothing about the second: the record of a member that died
        // keeps matching until somebody else takes it over, and nobody may be in a hurry to.
        Duration leaseRemaining = current.get().leaseRemaining();
        long held = leaseRemaining.compareTo(window) < 0 ? leaseRemaining.toNanos() : windowNanos;
        if (sessionProof != null) {
            NodeSessionLease.Proof node = sessionProof.get();
            if (node == null || !node.live()
                    || node.claim().profileGeneration() != current.get().claim().profileGeneration()) {
                entries.remove(pipelineId);
                return null;
            }
            held = Math.min(held, node.deadlineNanos() - startedAt);
        }
        if (held <= 0) {
            // Nobody owns this run any more, whoever may own it next. Dropping the entry is what refuses
            // the batch: a reading that was already out of date when it arrived is not a reading.
            entries.remove(pipelineId);
            return null;
        }
        Entry refreshed = new Entry(WorkloadClaimFence.from(current.get().claim()), startedAt + held);
        entries.put(pipelineId, refreshed);
        return refreshed;
    }

    @Override
    public void close() {
        closed = true;
        entries.clear();
        if (refresher != null) {
            refresher.shutdownNow();
        }
    }
}
