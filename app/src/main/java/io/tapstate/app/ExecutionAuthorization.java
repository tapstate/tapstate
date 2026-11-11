package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.DeadlineWorkloadClaimStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.LeasedStateStore;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * <p>Scoped executions and captures keep a separate background refresh for each claim. The data path
 * uses the last matching proof until its original deadline. It waits for background publication if a
 * proof is missing, and stops on confirmed loss or when that wait expires.
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
    private record Entry(WorkloadClaimFence claim, long deadlineNanos, long leaseDeadlineNanos) {
    }

    private record ReadKey(String pipelineId, PipelineIntentScope scope, WorkloadClaimType type) {
        static ReadKey of(ExecutionFence fence) {
            return new ReadKey(fence.pipelineId(), fence.scope(), WorkloadClaimType.PIPELINE_ACTUATION);
        }
    }

    private final String clusterId;
    private final CacheBusinessScope businessScope;
    private final WorkloadClaimStore claims;
    private final Duration window;
    private final long windowNanos;
    private final long refreshIntervalNanos;
    private final LongSupplier nanoTime;
    private final boolean fenced;
    private final BooleanSupplier nodeActive;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<ReadKey, Entry> entries = new ConcurrentHashMap<>();
    private final Map<ReadKey, Long> nextReadNanos = new ConcurrentHashMap<>();
    /** State stores whose lease was proved under one pipeline's reading; held weakly, never kept alive from here. */
    private final Map<ReadKey, List<WeakReference<LeasedStateStore>>> leases = new ConcurrentHashMap<>();
    private final ScheduledExecutorService refresher;
    private final ThreadPoolExecutor proofReaders;
    private final Map<ReadKey, ScopedProof> scopedProofs = new ConcurrentHashMap<>();

    private record ProofResult(Entry entry, boolean denied) {}

    private static final class ScopedProof {
        // A single volatile publication carries the immutable result and its original fence.
        @SuppressWarnings("java:S3077")
        volatile ProofResult result = new ProofResult(null, false);
        volatile String readFailure;
        volatile long lastUsed = System.nanoTime();
        volatile boolean scopeProved;
        final AtomicBoolean storeReading = new AtomicBoolean();
        long nextRead;
        long revision;
        boolean reading;
        boolean lastReadSucceeded;
        boolean superseded;
    }

    private ExecutionAuthorization(boolean denied) {
        this.nodeActive = () -> !denied;
        this.clusterId = "single";
        this.businessScope = null;
        this.claims = null;
        this.window = Duration.ZERO;
        this.windowNanos = 0;
        this.refreshIntervalNanos = 0;
        this.nanoTime = System::nanoTime;
        this.fenced = denied;
        this.refresher = null;
        this.proofReaders = null;
    }

    /** An explicit fixture guard for a port with no coordinated store. */
    static ExecutionAuthorization unfenced() {
        return new ExecutionAuthorization(false);
    }

    ExecutionAuthorization(String clusterId, WorkloadClaimStore claims, Duration window) {
        this(clusterId, claims, window, System::nanoTime);
    }

    ExecutionAuthorization(String clusterId, WorkloadClaimStore claims, Duration window, LongSupplier nanoTime) {
        this(clusterId, claims, window, nanoTime, () -> true);
    }

    ExecutionAuthorization(String clusterId, WorkloadClaimStore claims, Duration window, LongSupplier nanoTime, BooleanSupplier nodeActive) {
        this(clusterId, null, claims, window, nanoTime, nodeActive);
    }

    static ExecutionAuthorization scoped(CacheBusinessScope scope, WorkloadClaimStore claims, Duration window,
            LongSupplier nanoTime, BooleanSupplier nodeActive) {
        Objects.requireNonNull(scope, "scope");
        return new ExecutionAuthorization(scope.clusterId(), scope, claims, window, nanoTime, nodeActive);
    }

    private ExecutionAuthorization(String clusterId, CacheBusinessScope businessScope, WorkloadClaimStore claims,
            Duration window, LongSupplier nanoTime, BooleanSupplier nodeActive) {
        this.businessScope = businessScope;
        this.nodeActive = Objects.requireNonNull(nodeActive, "nodeActive");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.claims = Objects.requireNonNull(claims, "claims");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
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
        proofReaders = businessScope == null ? null : new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(256), runnable -> {
                    var thread = new Thread(runnable, "tapstate-scoped-proof-read");
                    thread.setDaemon(true);
                    return thread;
                });
        if (proofReaders == null) {
            refresher.scheduleWithFixedDelay(this::refreshAll, period, period, TimeUnit.MILLISECONDS);
        } else {
            long tick = Math.max(1, Math.min(1000, period / 2));
            refresher.scheduleWithFixedDelay(this::refreshScopedProofs, tick, tick, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The guard bound to {@code member}; a missing binding refuses instead of granting unprotected work.
     */
    static ExecutionAuthorization of(HazelcastInstance member) {
        Object bound = member.getUserContext().get(USER_CONTEXT_KEY);
        return bound instanceof ExecutionAuthorization authorization ? authorization : new ExecutionAuthorization(true);
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
        if (!fenced && (fence == null || fence.scope() == null)) {
            return null;
        }
        Entry authorized = authorizedEntry(fence);
        if (authorized == null) {
            throw new TapstateException(
                    EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", fence.pipelineId()), null);
        }
        return authorized.claim();
    }

    /**
     * Ties {@code store}'s lease to this member's reading of {@code fence}'s claim: the lease is dropped the
     * moment a refresh cannot reach the store, finds the claim gone, moved to other generations or with no
     * lease left, or a newer scope replaces the reading, and when this guard closes. The store proves a new
     * lease at its next operation, which fails closed while the claim is not this member's.
     */
    void holdLease(ExecutionFence fence, LeasedStateStore store) {
        Objects.requireNonNull(fence); Objects.requireNonNull(store);
        if (!fenced) return;
        sweepLeases();
        leases.compute(ReadKey.of(fence), (key, registered) -> {
            var held = registered == null ? new CopyOnWriteArrayList<WeakReference<LeasedStateStore>>() : registered;
            held.add(new WeakReference<>(store));
            return held;
        });
        if (closed.get()) dropLeases(ReadKey.of(fence));
    }

    private void dropLeases(ReadKey key) {
        var held = leases.get(key);
        if (held == null) return;
        for (var reference : held) {
            var store = reference.get();
            if (store != null) store.dropLease();
        }
        pruneLeases(key);
    }

    /**
     * Sweeps every key, not only one being registered or dropped: a reading that ended keeps no entry to
     * trigger its own cleanup. Runs on every registration and every background refresh.
     */
    private void sweepLeases() {
        for (var key : leases.keySet()) pruneLeases(key);
    }

    /** Forgets collected stores under {@code key}, and the key once none is left, atomically with registration. */
    private void pruneLeases(ReadKey key) {
        leases.computeIfPresent(key, (ignored, registered) -> {
            registered.removeIf(reference -> reference.get() == null);
            return registered.isEmpty() ? null : registered;
        });
    }

    /** A physical source uses the same bounded proof cache, independently of all Pipeline executions. */
    WorkloadClaimFence requireCapture(CacheBusinessScope originalScope,WorkloadClaimFence original) {
        Objects.requireNonNull(originalScope,"originalScope"); Objects.requireNonNull(original,"original");
        if (!fenced || businessScope == null || !businessScope.equals(originalScope)
                || original.key().type() != WorkloadClaimType.CAPTURE || !clusterId.equals(original.key().clusterId())
                || closed.get() || !nodeActive.getAsBoolean()) throw captureRefused();
        var key = new ReadKey(original.key().resourceId(),null,WorkloadClaimType.CAPTURE);
        Entry entry = awaitScopedProof(key, value -> original.equals(value.claim()),
                value -> captureProofPrecedes(value.claim(), original));
        if (entry == null) throw captureRefused();
        return entry.claim();
    }

    private static boolean captureProofPrecedes(WorkloadClaimFence actual, WorkloadClaimFence expected) {
        return actual.claimGeneration() < expected.claimGeneration()
                || actual.claimGeneration() == expected.claimGeneration()
                        && (actual.executionGeneration() < expected.executionGeneration()
                        || actual.executionGeneration() == expected.executionGeneration()
                                && actual.topologyRevision() < expected.topologyRevision());
    }

    /** Rechecks one already-published physical capture claim, without refreshing the store. */
    WorkloadClaimFence requireCaptureLocal(CacheBusinessScope originalScope,WorkloadClaimFence original) {
        var entry=scopedEntry(new ReadKey(original.key().resourceId(),null,WorkloadClaimType.CAPTURE));
        if(!fenced || !Objects.equals(businessScope,originalScope) || closed.get() || !nodeActive.getAsBoolean()
                || entry==null || nanoTime.getAsLong()-entry.deadlineNanos()>=0 || !original.equals(entry.claim()))throw captureRefused();
        return entry.claim();
    }
    private static TapstateException captureRefused() {
        return new TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
    }

    /** Whether {@code fence}'s run is still the current one, as far as this member can prove locally. */
    boolean authorized(ExecutionFence fence) {
        if (!fenced) {
            return fence == null || fence.scope() == null;
        }
        return authorizedEntry(fence) != null;
    }

    /** Rechecks only an already-published proof and its original deadline, without store IO. */
    WorkloadClaimFence requireLocal(ExecutionFence fence) {
        Objects.requireNonNull(fence,"fence");
        Entry entry=businessScope == null ? entries.get(ReadKey.of(fence)) : scopedEntry(ReadKey.of(fence));
        if(!fenced || !accepts(fence) || closed.get() || !nodeActive.getAsBoolean() || entry==null
                || nanoTime.getAsLong()-entry.deadlineNanos()>=0 || !matches(entry.claim(),fence))
            throw new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",fence.pipelineId()),null);
        return entry.claim();
    }

    /** Checks the current local proof for the original fence, without store IO. */
    Runnable localProofGuard(ExecutionFence fence) {
        Runnable require = () -> requireWriterProof(fence);
        require.run();
        return require;
    }

    private void requireWriterProof(ExecutionFence fence) {
        if (businessScope == null) {
            requireLocal(fence);
            return;
        }
        var key = ReadKey.of(fence);
        var proof = scopedProofs.get(key);
        if (proof == null) throw writerRefused(fence);
        proof.lastUsed = System.nanoTime();
        var entry = requireWriterLease(fence, proof);
        if (nanoTime.getAsLong() - entry.deadlineNanos() >= 0)
            throw new WriterProofUnavailable(proof, entry.leaseDeadlineNanos());
    }

    private Entry requireWriterLease(ExecutionFence fence, ScopedProof proof) {
        var result = proof.result;
        var entry = result.entry();
        if (!fenced || !accepts(fence) || closed.get() || !nodeActive.getAsBoolean() || result.denied()
                || entry == null || !matches(entry.claim(), fence)
                || nanoTime.getAsLong() - entry.leaseDeadlineNanos() >= 0) throw writerRefused(fence);
        return entry;
    }

    /** A pre-commit guard may suspend an acknowledgement, but the transaction must exit before waiting. */
    static final class WriterProofUnavailable extends RuntimeException {
        private final ScopedProof proof;
        private final long leaseDeadline;

        private WriterProofUnavailable(ScopedProof proof, long leaseDeadline) {
            this.proof = proof;
            this.leaseDeadline = leaseDeadline;
        }
    }

    void awaitWriterProof(ExecutionFence fence, WriterProofUnavailable unavailable) {
        var key = ReadKey.of(fence);
        var proof = unavailable.proof;
        LOG.info("Waiting for SRS acknowledgement proof for {} claim={} execution={} remainingMs={} lastRead={}",
                fence.pipelineId(), fence.claimGeneration(), fence.executionGeneration(),
                TimeUnit.NANOSECONDS.toMillis(unavailable.leaseDeadline - nanoTime.getAsLong()), proof.readFailure);
        synchronized (proof) {
            while (true) {
                long left = unavailable.leaseDeadline - nanoTime.getAsLong();
                if (left <= 0 || scopedProofs.get(key) != proof) throw writerRefused(fence);
                var entry = requireWriterLease(fence, proof);
                if (nanoTime.getAsLong() - entry.deadlineNanos() < 0) break;
                proof.lastUsed = System.nanoTime();
                requestScopedProof(key, proof, false);
                try {
                    TimeUnit.NANOSECONDS.timedWait(proof, Math.min(left, TimeUnit.SECONDS.toNanos(1)));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw writerRefused(fence);
                }
            }
        }
        LOG.info("Recovered SRS acknowledgement proof for {} claim={} execution={}",
                fence.pipelineId(), fence.claimGeneration(), fence.executionGeneration());
    }

    private static TapstateException writerRefused(ExecutionFence fence) {
        return new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", fence.pipelineId()), null);
    }

    /** The current matching entry, refreshing it under the same rules as the boolean guard. */
    private Entry authorizedEntry(ExecutionFence fence) {
        Objects.requireNonNull(fence, "fence");
        if (!accepts(fence) || closed.get() || !nodeActive.getAsBoolean()) return null;
        ReadKey key = ReadKey.of(fence);
        if (businessScope != null) {
            return awaitScopedProof(key, value -> matches(value.claim(), fence), value -> precedes(value, fence));
        }
        long now = nanoTime.getAsLong();
        Entry entry = entries.get(key);
        if (entry != null && precedes(entry, fence)) {
            // This reading was taken before the run asking about itself was submitted, so it is not the
            // one that decides it. Only the store hands generations out, and only upwards, so a run
            // carrying more than this member has read is a run submitted since this member last looked —
            // never a superseded one, which is what a reading is held against. So its own deadline is
            // spent rather than waited out: the answer is known to be out of date, and a refusal from it
            // does not pause the run — it reaches the job as an exception, which ends it.
            entry = read(key, true);
        } else if (entry == null || now - entry.deadlineNanos() >= 0) {
            entry = read(key, false);
        }
        // IO or scheduling may have spent the proof's entire window. A concurrent refresh may also
        // have replaced or revoked it, so use only the matching proof still published at return time.
        if (closed.get() || !nodeActive.getAsBoolean()) return null;
        long checkedAt = nanoTime.getAsLong();
        Entry published = entries.get(key);
        return !closed.get() && published != null
                && checkedAt - published.deadlineNanos() < 0
                && matches(published.claim(), fence)
                ? published
                : null;
    }

    /** Files a sink's known independent failure under the exact run while its claim is still live. */
    void recordSinkWriteFailure(ExecutionFence fence) {
        if (!fenced || !accepts(fence) || closed.get() || !nodeActive.getAsBoolean()) {
            return;
        }
        try {
            Optional<WorkloadClaimReading> reading = readStore(ReadKey.of(fence));
            if (reading.isEmpty() || !reading.get().leased()) {
                return;
            }
            var claim = reading.get().claim();
            if (matches(WorkloadClaimFence.from(claim), fence)) {
                claims.recordExecutionFailure(claim, false);
            }
        } catch (RuntimeException unreachable) {
            // A coordination-store error must not replace the sink's own failure.
            LOG.debug("Could not record the sink failure for {}", fence.pipelineId(), unreachable);
        }
    }

    private boolean accepts(ExecutionFence fence) {
        PipelineIntentScope scope = fence.scope();
        return businessScope == null ? scope == null
                : scope != null && scope.clusterId().equals(businessScope.clusterId())
                        && scope.bindingVersion() == businessScope.bindingVersion()
                        && scope.cacheIncarnation().equals(businessScope.cacheIncarnation());
    }

    private boolean matches(WorkloadClaimFence claim, ExecutionFence fence) {
        return claim.key().equals(new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION,
                        fence.pipelineId()))
                && claim.claimGeneration() == fence.claimGeneration()
                && claim.executionGeneration() == fence.executionGeneration()
                && (fence.scope() == null || claim.owner().nodeId().equals(fence.ownerNodeId())
                        && claim.owner().bootId().equals(fence.ownerBootId())
                        && claim.topologyRevision() == fence.topologyRevision());
    }

    private Optional<WorkloadClaimReading> readStore(ReadKey key) {
        return key.scope() == null
                ? claims.read(new WorkloadClaimKey(clusterId, key.type(), key.pipelineId()))
                : claims.readPipeline(key.scope());
    }

    private Optional<WorkloadClaimReading> readExecutionProof(ReadKey key, ScopedProof proof, long startedAt) {
        long deadline = System.nanoTime() + windowNanos;
        if (claims instanceof DeadlineWorkloadClaimStore bounded) {
            return bounded.withDeadline(deadline, () -> readScopedStore(key, proof, startedAt, deadline));
        }
        return readScopedStore(key, proof, startedAt, deadline);
    }

    private Optional<WorkloadClaimReading> readScopedStore(ReadKey key, ScopedProof proof, long startedAt, long deadline) {
        // A bounded caller can time out before the underlying Mongo operation has exited.
        if (!proof.storeReading.compareAndSet(false, true)) throw proofUnavailable(key);
        try {
            try {
                return readStore(key);
            } catch (TapstateException failed) {
                if (!proofReadTimedOut(failed) || nanoTime.getAsLong() - startedAt >= windowNanos
                        || System.nanoTime() - deadline >= 0) throw failed;
                return readStore(key);
            }
        } finally {
            proof.storeReading.set(false);
        }
    }

    private static boolean proofReadTimedOut(TapstateException failure) {
        if (failure.code() != io.tapstate.spi.store.IoError.STORE_UNAVAILABLE
                || !(failure.args().get("detail") instanceof String detail)) return false;
        // The Mongo adapter exposes only a sanitized driver type and optional numeric code.
        return switch (detail.split(" ", 2)[0]) {
            case "MongoTimeoutException", "MongoOperationTimeoutException", "MongoExecutionTimeoutException" -> true;
            default -> false;
        };
    }

    private Entry scopedEntry(ReadKey key) {
        var proof = scopedProofs.get(key);
        if (proof == null) return null;
        proof.lastUsed = System.nanoTime();
        return proof.result.entry();
    }

    private Entry awaitScopedProof(ReadKey key, Predicate<Entry> matches, Predicate<Entry> precedes) {
        var proof = scopedProofs.computeIfAbsent(key, ignored -> new ScopedProof());
        proof.lastUsed = System.nanoTime();
        var result = proof.result;
        if (liveMatch(result, matches) && proof.result == result) return result.entry();
        long startedAt = System.nanoTime();
        synchronized (proof) {
            long initialRevision = proof.revision;
            while (!closed.get() && nodeActive.getAsBoolean() && !proof.superseded) {
                result = proof.result;
                var entry = result.entry();
                if (liveMatch(result, matches)) return entry;
                if (entry != null && !precedes.test(entry)
                        && (result.denied() || !matches.test(entry))) return null;
                // A completed read that cannot prove the requested fence is already an answer.
                if (proof.revision != initialRevision) return null;
                if (entry == null && result.denied() && proof.readFailure != null
                        && System.nanoTime() - proof.nextRead < 0) return null;

                long left = 3 * windowNanos - (System.nanoTime() - startedAt);
                if (left <= 0) throw proofUnavailable(key);
                requestScopedProof(key, proof, true);
                try {
                    TimeUnit.NANOSECONDS.timedWait(proof, Math.min(left, TimeUnit.SECONDS.toNanos(1)));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw proofUnavailable(key);
                }
            }
        }
        return null;
    }

    private boolean liveMatch(ProofResult result, Predicate<Entry> matches) {
        var entry = result.entry();
        return !closed.get() && nodeActive.getAsBoolean() && !result.denied() && entry != null
                && nanoTime.getAsLong() - entry.deadlineNanos() < 0 && matches.test(entry);
    }

    private static TapstateException proofUnavailable(ReadKey key) {
        return new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "authorization-proof-unavailable " + key.type().name()), null);
    }

    // Each ScopedProof is private and is the common monitor used by its waiters and publishers.
    @SuppressWarnings("java:S2445")
    private void refreshScopedProofs() {
        sweepLeases();
        long now = System.nanoTime();
        scopedProofs.forEach((key, proof) -> {
            synchronized (proof) {
                if (!proof.reading && !proof.storeReading.get() && now - proof.lastUsed >= 4 * windowNanos) {
                    if (scopedProofs.remove(key, proof)) {
                        proof.superseded = true;
                        dropLeases(key);
                        proof.result = new ProofResult(null, true);
                        proof.notifyAll();
                    }
                } else {
                    requestScopedProof(key, proof, false);
                }
            }
        });
    }

    /** Caller holds only the proof's monitor, never while doing store I/O. */
    private void requestScopedProof(ReadKey key, ScopedProof proof, boolean foreground) {
        boolean refreshNow = foreground && proof.lastReadSucceeded;
        if (closed.get() || proof.superseded || proof.reading || proof.storeReading.get()
                || !refreshNow && proof.nextRead != 0 && System.nanoTime() - proof.nextRead < 0) return;
        proof.reading = true;
        proof.lastReadSucceeded = false;
        try {
            proofReaders.execute(() -> readScopedProof(key, proof));
        } catch (RejectedExecutionException busy) {
            proof.reading = false;
            proof.nextRead = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
        }
    }

    private void readScopedProof(ReadKey key, ScopedProof proof) {
        long startedAt = nanoTime.getAsLong();
        ProofResult result = null;
        boolean scopeRead = false;
        try {
            var reading = readExecutionProof(key, proof, startedAt);
            proof.readFailure = null;
            if (reading.isEmpty()) {
                result = new ProofResult(null, true);
            } else {
                var observed = reading.orElseThrow();
                long lease = observed.leaseRemaining().toNanos();
                long held = Math.min(windowNanos, lease);
                result = new ProofResult(new Entry(WorkloadClaimFence.from(observed.claim()), startedAt + held, startedAt + lease),
                        held <= 0);
            }
            scopeRead = true;
        } catch (RuntimeException unavailable) {
            dropLeases(key);
            proof.readFailure = unavailable instanceof TapstateException coded ? coded.getMessage() : unavailable.getClass().getSimpleName();
            if (definitiveRefusal(unavailable)) {
                result = new ProofResult(null, true);
            } else {
                LOG.debug("Could not refresh {} authorization for {}", key.type(), key.pipelineId(), unavailable);
            }
        } finally {
            publishScopedProof(key, proof, result, scopeRead);
        }
    }

    private static boolean definitiveRefusal(RuntimeException failure) {
        if (!(failure instanceof TapstateException coded) || failure.getCause() instanceof TimeoutException)
            return false;
        return coded.code() == IoError.WORKLOAD_CLAIM_FENCED || coded.code() == IoError.CACHE_BUSINESS_FENCED
                || coded.code() == IoError.PIPELINE_INTENT_FENCED;
    }

    @SuppressWarnings("java:S2445")
    private void publishScopedProof(ReadKey key, ScopedProof proof, ProofResult result, boolean scopeRead) {
        // Publish and retire older scopes in one order. No store calls take either monitor.
        synchronized (scopedProofs) {
            synchronized (proof) {
                proof.reading = false;
                if (!closed.get() && !proof.superseded && scopedProofs.get(key) == proof) {
                    if (result != null) {
                        var previous = proof.result.entry();
                        if (result.denied() || previous != null && (result.entry() == null
                                || !previous.claim().equals(result.entry().claim()))) dropLeases(key);
                        if (key.scope() != null && !proof.scopeProved && newerScopeProved(key)) {
                            proof.superseded = true;
                            proof.result = new ProofResult(null, true);
                        } else {
                            proof.result = result;
                            // A successful scoped read confirms the generation even before it has a claim.
                            if (scopeRead && key.scope() != null && !proof.scopeProved) {
                                proof.scopeProved = true;
                                retireOlderScopes(key);
                            }
                        }
                        proof.revision++;
                    }
                    // A failed refresh retains any still-valid proof without moving its deadline.
                    long delay = result == null ? Math.min(refreshIntervalNanos, TimeUnit.SECONDS.toNanos(1))
                            : refreshIntervalNanos;
                    proof.nextRead = System.nanoTime() + delay;
                    proof.lastReadSucceeded = scopeRead;
                }
                proof.notifyAll();
            }
        }
    }

    private boolean newerScopeProved(ReadKey original) {
        return scopedProofs.entrySet().stream().anyMatch(candidate -> {
            var key = candidate.getKey();
            return key.scope() != null && key.pipelineId().equals(original.pipelineId())
                    && key.scope().stateGeneration() > original.scope().stateGeneration()
                    && candidate.getValue().scopeProved;
        });
    }

    @SuppressWarnings("java:S2445")
    private void retireOlderScopes(ReadKey newer) {
        scopedProofs.forEach((key, proof) -> {
            if (key.scope() == null || !key.pipelineId().equals(newer.pipelineId())
                    || key.scope().stateGeneration() >= newer.scope().stateGeneration()) return;
            synchronized (proof) {
                proof.superseded = true;
                dropLeases(key);
                proof.result = new ProofResult(null, true);
                proof.notifyAll();
            }
        });
    }

    /**
     * Whether {@code entry} was read before the run {@code fence} names was submitted, ordered the way
     * the store advances the pair: taking a claim over takes the next claim generation and carries the
     * execution generation across, and submitting a run under a claim takes that claim's next execution
     * generation. Within the same pair, a newer scoped topology also requires a fresh proof rather than
     * waiting for a cached reading of its predecessor to expire.
     */
    private static boolean precedes(Entry entry, ExecutionFence fence) {
        return entry.claim().claimGeneration() < fence.claimGeneration()
                || (entry.claim().claimGeneration() == fence.claimGeneration()
                        && (entry.claim().executionGeneration() < fence.executionGeneration()
                                || (entry.claim().executionGeneration() == fence.executionGeneration() && fence.scope() != null
                                        && entry.claim().topologyRevision() < fence.topologyRevision())));
    }

    private void refreshAll() {
        for (ReadKey key : Set.copyOf(entries.keySet())) {
            try {
                read(key, true);
            } catch (RuntimeException unreachable) {
                // read() has already dropped the entry, which is what refuses the next batch. Nothing
                // else is owed here: the point of the background pass is that the batch path finds a
                // fresh answer, not that it reports on the store.
                LOG.debug("Could not refresh the execution authorization for {}", key.pipelineId(), unreachable);
            }
        }
        nextReadNanos.forEach((key, deadline) -> {
            if (nanoTime.getAsLong() - deadline >= 0 && !entries.containsKey(key)) {
                nextReadNanos.remove(key, deadline);
            }
        });
        sweepLeases();
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
    private synchronized Entry read(ReadKey key, boolean evenIfStillLive) {
        Entry before = entries.get(key);
        try {
            return readStoreEntry(key, evenIfStillLive);
        } finally {
            Entry after = entries.get(key);
            if (before != null && (after == null || !after.claim().equals(before.claim()))) dropLeases(key);
        }
    }

    private Entry readStoreEntry(ReadKey key, boolean evenIfStillLive) {
        long now = nanoTime.getAsLong();
        Entry entry = entries.get(key);
        if (entry != null && !evenIfStillLive && now - entry.deadlineNanos() < 0) {
            // Refreshed by the background pass, or by another batch, while this call waited for the lock.
            return entry;
        }
        Long nextRead = nextReadNanos.get(key);
        if (nextRead != null && now - nextRead < 0) {
            return evenIfStillLive ? entry : null;
        }
        // Taken before the request goes out: whatever comes back is no older than this point, so a deadline
        // measured from here is never longer than the answer was actually good for.
        long startedAt = nanoTime.getAsLong();
        Optional<WorkloadClaimReading> current;
        try {
            current = readStore(key);
        } catch (RuntimeException unreachable) {
            nextReadNanos.put(key, startedAt + refreshIntervalNanos);
            entries.remove(key);
            return null;
        }
        nextReadNanos.remove(key);
        if (current.isEmpty()) {
            entries.remove(key);
            return null;
        }
        // The shorter of the two bounds, because each covers what the other cannot. The window bounds how
        // stale this reading may be; the lease bounds how long the owner it names is still the owner. Two
        // generations that still match say nothing about the second: the record of a member that died
        // keeps matching until somebody else takes it over, and nobody may be in a hurry to.
        Duration leaseRemaining = current.get().leaseRemaining();
        long held = leaseRemaining.compareTo(window) < 0 ? leaseRemaining.toNanos() : windowNanos;
        if (held <= 0) {
            // Nobody owns this run any more, whoever may own it next. Dropping the entry is what refuses
            // the batch: a reading that was already out of date when it arrived is not a reading.
            entries.remove(key);
            return null;
        }
        Entry refreshed = new Entry(WorkloadClaimFence.from(current.get().claim()), startedAt + held, startedAt + leaseRemaining.toNanos());
        entries.put(key, refreshed);
        return refreshed;
    }

    @Override
    @SuppressWarnings("java:S2445")
    public void close() {
        closed.set(true); entries.clear(); nextReadNanos.clear();
        leases.keySet().forEach(this::dropLeases);
        scopedProofs.values().forEach(proof -> {
            synchronized (proof) { proof.notifyAll(); }
        });
        scopedProofs.clear();
        if (proofReaders != null) proofReaders.shutdownNow();
        if (refresher != null) {
            refresher.shutdownNow();
        }
    }
}
