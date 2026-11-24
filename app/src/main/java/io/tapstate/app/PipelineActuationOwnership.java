package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope.Phase;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimRenewalException;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

/**
 * Decides which member drives one pipeline's convergence, by holding one durable actuation claim per
 * pipeline. Every member reconciles the whole desired set, so without this each of them would drive the
 * same pipeline's lifecycle verbs: the member-local "nothing is carrying this pipeline here" check is
 * true on every member that did not submit the job, so each one starts a second run of it.
 *
 * <p>The claim is the only thing that makes a member the driver. Not the oldest member, not the job
 * coordinator, and not the lifecycle checkpoint epoch: those can land on the same node and none of them
 * is a promise. The checkpoint's compare-and-swap keeps its own job — it fences <em>actual state</em>
 * writes, and a writer that loses that race rebases onto the fresh epoch and retries, which is right for
 * a value two writers may legitimately move. Ownership is not such a value: a holder that cannot prove
 * its claim any more stops driving at once and does not rebase onto whatever the store now says, because
 * rebasing there is precisely how two members would end up driving one pipeline.
 *
 * <p>Between round trips a holder acts on the claim it last proved, for at most one renew interval —
 * shorter than the lease, so the window closes before the lease it was cut from could expire. The
 * interval is measured on the monotonic clock, so moving the node's wall clock cannot widen it.
 *
 * <p>Convergence workers acquire claims, advance them for each run and judge the runs under them;
 * {@link #renewDue()}, on a renewer of its own, keeps every held claim's lease running. Each pipeline has
 * one pass at a time while independent pipelines may progress concurrently. A single start can outlast
 * a lease: renewed only from the pass, every claim the member held ran out behind one slow start and
 * was taken over from a member that was still there. Everything here synchronizes on this object, so a
 * renewal always carries the claim the pass last moved -- the store matches a renewal on the claim's exact
 * generations, and one sent with a claim the pass has just advanced past would read as the claim lost. The
 * same lock keeps a close that starts first from being followed by a durable verdict that calls its dying
 * run independent.
 */
final class PipelineActuationOwnership {
    private static final long RENEW_RETRY_NANOS = TimeUnit.SECONDS.toNanos(1);
    private static final System.Logger LOG=System.getLogger(PipelineActuationOwnership.class.getName());

    /** Whether this member may drive the pipeline, and the claim that says so ({@code null} only in an explicit fixture). */
    record Permit(boolean granted, WorkloadClaim claim) {

        static Permit unfenced() {
            return new Permit(true, null);
        }

        static Permit denied() {
            return new Permit(false, null);
        }
    }

    private final String clusterId;
    private final CacheBusinessScope businessScope;
    private final WorkloadOwner owner;
    private final WorkloadClaimFence nodeSession;
    private final ClusterMembershipGate membership;
    private final ClusterWorkloadClaims claims;
    private final Duration ttl;
    private final long renewIntervalNanos;
    private final LongSupplier nanoTime;
    private final boolean fenced;
    private final Map<String, Held> held = new java.util.concurrent.ConcurrentHashMap<>();
    private final Set<WorkloadClaimKey> dispatchedRenewals = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final Map<String, Control> controls = new HashMap<>();
    private final Map<String, ScopedAcquisition> acquisitions = new HashMap<>();
    private final Map<String, PendingRelease> pendingReleases = new HashMap<>();
    enum StopReleaseStatus { PENDING, CONFIRMED, UNPROVEN }
    static final class StopRelease {
        private volatile StopReleaseStatus status = StopReleaseStatus.PENDING;
        StopReleaseStatus status() { return status; }
    }
    private static final class PendingRelease {
        private final PipelineIntentScope scope;
        private final WorkloadClaim claim;
        private final StopRelease terminal;
        private java.time.Instant deadlineBeforeRelease;
        private boolean inFlight;
        private PendingRelease(PipelineIntentScope scope, WorkloadClaim claim, boolean stopped) {
            this.scope = scope; this.claim = claim; this.terminal = stopped ? new StopRelease() : null;
        }
    }
    private record ScopedAcquisition(Held previous, Held candidate, WorkloadClaim original) {}
    /** Local scheduling order only; neither a lease deadline nor an authority token. */
    private long leaseProofSequence;

    /** A synchronous control command pins the existing holder; it creates no second durable lease. */
    final class Control implements AutoCloseable {
        private final PipelineIntentScope scope;
        private final WorkloadClaimFence claim;
        private boolean tenureEnded;
        private Control(PipelineIntentScope scope, WorkloadClaimFence claim) { this.scope = scope; this.claim = claim; }
        WorkloadClaimFence claim() { return claim; }
        void requireHeld() {
            synchronized (PipelineActuationOwnership.this) {
                if (closing || controls.get(scope.pipelineId()) != this || !membership.businessEligible()) throw ThreeLayerStorePort.refused();
            }
        }
        void requireActive() {
            synchronized (PipelineActuationOwnership.this) {
                if (controls.get(scope.pipelineId()) != this) throw ThreeLayerStorePort.refused();
            }
            if (claim == null ? closing || !membership.businessEligible() : !proveClaim(scope, claim)) throw ThreeLayerStorePort.refused();
            synchronized (PipelineActuationOwnership.this) {
                if (controls.get(scope.pipelineId()) != this) throw ThreeLayerStorePort.refused();
            }
        }
        void released() {
            synchronized (PipelineActuationOwnership.this) {
                requireHeld(); var old = held.get(scope.pipelineId());
                if (old != null && old.scope.equals(scope) && (old.claim == null || WorkloadClaimFence.from(old.claim).equals(claim))) held.remove(scope.pipelineId());
            }
        }
        /** Ends this exact durable tenure while retaining the local exclusion against new passes. */
        void endTenure() {
            admittedCall(()->{endTenureOwned();return null;},()->{throw ThreeLayerStorePort.refused();});
        }
        private void endTenureOwned() {
            synchronized (PipelineActuationOwnership.this) {
                requireHeld();
                if(!fenced || businessScope==null || claim==null || !accepts(scope))throw ThreeLayerStorePort.refused();
                if(tenureEnded)return;
                if(!owner.equals(claim.owner()))throw ThreeLayerStorePort.refused();
                var local=held.get(scope.pipelineId());
                if(local!=null && (!scope.equals(local.scope) || local.claim!=null && !claim.equals(WorkloadClaimFence.from(local.claim))))
                    throw ThreeLayerStorePort.refused();
                var before=claims.readPipeline(scope).orElseThrow(ThreeLayerStorePort::refused);
                if(!claim.equals(WorkloadClaimFence.from(before.claim())))throw ThreeLayerStorePort.refused();
                if(before.leased() && !claims.release(before.claim()))throw ThreeLayerStorePort.refused();
                var after=claims.readPipeline(scope).orElseThrow(ThreeLayerStorePort::refused);
                if(after.leased() || !claim.equals(WorkloadClaimFence.from(after.claim())))throw ThreeLayerStorePort.refused();
                if(local!=null)held.remove(scope.pipelineId(),local);
                tenureEnded=true;
            }
        }
        @Override public void close() {
            synchronized (PipelineActuationOwnership.this) { controls.remove(scope.pipelineId(), this); }
        }
    }
    private final Map<String, Pass> passes = new HashMap<>();
    private final Set<String> waitingControls = java.util.concurrent.ConcurrentHashMap.newKeySet();
    final class Pass implements AutoCloseable {
        private final String pipeline;
        private final Thread thread = Thread.currentThread();
        private Pass(String pipeline) { this.pipeline = pipeline; }

        /** Ends only the settled original tenure. Ambiguous release stays registered for readback. */
        StopRelease relinquishStopped(PipelineIntentScope scope, WorkloadClaimFence expected) {
            if (Thread.currentThread() != thread || !pipeline.equals(scope.pipelineId()) || !admission.enter()) return null;
            try {
                PendingRelease release;
                synchronized (PipelineActuationOwnership.this) {
                    var state = held.get(pipeline);
                    if (closing || !fenced || !accepts(scope) || !membership.businessEligible()
                            || passes.get(pipeline) != this || controls.containsKey(pipeline) || waitingControls.contains(pipeline)
                            || acquisitions.containsKey(pipeline) || pendingReleases.containsKey(pipeline) || state == null
                            || state.executionAdvancing || !scope.equals(state.scope) || state.claim == null
                            || state.runMembers != null && !state.runMembers.equals(Set.of(owner.nodeId()))
                            || !owner.equals(expected.owner()) || !expected.equals(WorkloadClaimFence.from(state.claim))) return null;
                    // A dispatched worker takes this lease by CAS. Never race an in-flight renewal.
                    var lease = state.scopedLease.get();
                    if (lease != null && (lease.inFlight() || lease.lost() || !state.scopedLease.compareAndSet(lease, null))) return null;
                    release = new PendingRelease(scope, state.claim, true);
                    pendingReleases.put(pipeline, release);
                    held.remove(pipeline, state);
                }
                releasePending(release);
                return release.terminal;
            } finally { admission.leave(); }
        }

        @Override public void close() {
            synchronized (PipelineActuationOwnership.this) {
                passes.remove(pipeline, this);
                PipelineActuationOwnership.this.notifyAll();
            }
        }
    }
    synchronized Pass enterPass(PipelineIntentScope scope) {
        if (closing || !accepts(scope) || controls.containsKey(scope.pipelineId()) || passes.containsKey(scope.pipelineId())
                || advancing(scope.pipelineId())
                || waitingControls.contains(scope.pipelineId())) throw ThreeLayerStorePort.refused();
        var pass = new Pass(scope.pipelineId()); passes.put(scope.pipelineId(), pass); return pass;
    }
    Control control(PipelineIntentScope scope) {return admittedCall(()->controlOwned(scope),()->{throw ThreeLayerStorePort.refused();});}
    private synchronized Control controlOwned(PipelineIntentScope scope) {
        if (passes.containsKey(scope.pipelineId())) throw ThreeLayerStorePort.refused();
        var admitted = admittedPermit(scope);
        if (!admitted.granted() || admitted.claim() == null) throw ThreeLayerStorePort.refused();
        var command = new Control(scope, WorkloadClaimFence.from(admitted.claim()));
        controls.put(scope.pipelineId(), command); return command;
    }
    /** Waits only for a local pass; the original durable admission is attempted once after it returns. */
    Control controlAfterPass(PipelineIntentScope scope, boolean unprepared, Duration timeout) {
        long budget = Objects.requireNonNull(timeout, "timeout").toNanos();
        if (budget < 0) throw new IllegalArgumentException("the local pass wait must not be negative");
        String pipeline = scope.pipelineId();
        if(!admission.enter())throw ThreeLayerStorePort.refused();
        boolean reserved=false;
        try {
            admittedCall(()->{synchronized(this){
                if(closing || !accepts(scope) || controls.containsKey(pipeline) || !membership.businessEligible()
                        || !waitingControls.add(pipeline))throw ThreeLayerStorePort.refused();
                return true;
            }},()->{throw ThreeLayerStorePort.refused();});
            reserved=true;long started=System.nanoTime();
            for(;;) {
                var result=admittedCall(()->{synchronized(this){
                    if(closing || !accepts(scope) || !membership.businessEligible())throw ThreeLayerStorePort.refused();
                    var pass=passes.get(pipeline);
                    if(pass!=null) {
                        if(pass.thread==Thread.currentThread() || budget-(System.nanoTime()-started)<=0)
                            throw ThreeLayerStorePort.refused();
                        return null;
                    }
                    return unprepared?unpreparedControl(scope):control(scope);
                }},()->{throw ThreeLayerStorePort.refused();});
                if(result!=null)return result;
                long remaining=Math.min(admission.remainingNanos(),budget-(System.nanoTime()-started));
                if(remaining<=0 || Thread.currentThread().isInterrupted())throw ThreeLayerStorePort.refused();
                java.util.concurrent.locks.LockSupport.parkNanos(Math.min(remaining,java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(25)));
                }
        } finally {
            if(reserved)waitingControls.remove(pipeline);
            admission.leave();
        }
    }
    /** Holds a proven ended tuple locally without acquiring another durable tenure or execution permission. */
    Control retiredControl(PipelineIntentScope scope,WorkloadClaimFence original) {
        return admittedCall(()->retiredControlOwned(scope,original),()->{throw ThreeLayerStorePort.refused();});
    }
    private synchronized Control retiredControlOwned(PipelineIntentScope scope,WorkloadClaimFence original) {
        requireFreeControl(scope,original);
        var local=held.get(scope.pipelineId());
        if(local!=null && (!scope.equals(local.scope) || local.claim!=null && !original.equals(WorkloadClaimFence.from(local.claim))))
            throw ThreeLayerStorePort.refused();
        var observed=claims.readPipeline(scope).orElseThrow(ThreeLayerStorePort::refused);
        if(observed.leased() || !original.equals(WorkloadClaimFence.from(observed.claim())) || !membership.businessEligible())
            throw ThreeLayerStorePort.refused();
        var command=new Control(scope,original);command.tenureEnded=true;
        if(local!=null)held.remove(scope.pipelineId(),local);
        controls.put(scope.pipelineId(),command);return command;
    }
    /** Blocks local acquisition and execution without adopting the transported tuple as a lease. */
    synchronized Control freezeControl(PipelineIntentScope scope,WorkloadClaimFence original) {
        requireFreeControl(scope,original);
        var command=new Control(scope,original);controls.put(scope.pipelineId(),command);return command;
    }
    private void requireFreeControl(PipelineIntentScope scope,WorkloadClaimFence original) {
        String reason;
        if(closing)reason="closing";
        else if(!fenced)reason="unfenced";
        else if(!accepts(scope))reason="scope-mismatch";
        else if(original==null)reason="missing-original-claim";
        else if(!membership.businessEligible())reason="membership-ineligible";
        else if(controls.containsKey(scope.pipelineId()))reason="control-busy";
        else if(acquisitions.containsKey(scope.pipelineId()))reason="acquisition-in-flight";
        else if(advancing(scope.pipelineId()))reason="execution-advancing";
        else if(pendingReleases.containsKey(scope.pipelineId()))reason="claim-release-pending";
        else if(passes.containsKey(scope.pipelineId()))reason="pass-active";
        else if(original.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION)reason="claim-kind";
        else if(!scope.clusterId().equals(original.key().clusterId()))reason="claim-cluster";
        else if(!scope.pipelineId().equals(original.key().resourceId()))reason="claim-resource";
        else return;
        LOG.log(System.Logger.Level.DEBUG,"Native control admission for {0} refused: {1}",scope==null?"<missing>":scope.pipelineId(),reason);
        throw ThreeLayerStorePort.refused();
    }
    synchronized Control unpreparedControl(PipelineIntentScope scope) {
        if (closing || !accepts(scope) || controls.containsKey(scope.pipelineId()) || acquisitions.containsKey(scope.pipelineId())
                || pendingReleases.containsKey(scope.pipelineId()) || passes.containsKey(scope.pipelineId())
                || advancing(scope.pipelineId()) || !membership.businessEligible()) throw ThreeLayerStorePort.refused();
        var command = new Control(scope, null); controls.put(scope.pipelineId(), command); return command;
    }
    private volatile boolean closing;
    private final AdmissionDrain admission = new AdmissionDrain();
    private final java.util.concurrent.locks.ReentrantLock ioAdmission = new java.util.concurrent.locks.ReentrantLock(true);

    /** Fresh proof reads share the shutdown deadline, but never serialize unrelated store reads. */
    private <T>T admittedRead(java.util.function.Supplier<T> action,java.util.function.Supplier<T> refused) {
        if(!admission.enter())return refused.get();
        try {
            if(Thread.currentThread().isInterrupted() || !admission.current())return refused.get();
            return action.get();
        }finally {admission.leave();}
    }

    private <T>T admittedCall(java.util.function.Supplier<T> action,java.util.function.Supplier<T> refused) {
        if(!admission.enter())return refused.get();
        boolean locked=false;
        try {
            long remaining=admission.remainingNanos();
            if(remaining<=0)return refused.get();
            try (var wait = PipelineDiagnostics.phase(Phase.PIPELINE_OWNERSHIP_WAIT)) {
                locked=ioAdmission.tryLock(remaining,java.util.concurrent.TimeUnit.NANOSECONDS);
            }
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();return refused.get();}
            if(!locked || !admission.current())return refused.get();
            return action.get();
        }finally {if(locked)ioAdmission.unlock();admission.leave();}
    }

    private record ScopedLease(WorkloadClaim expected, long deadline, long due, int retries,
                               boolean inFlight, boolean confirm, RenewalOutcome loss) {
        boolean lost() { return loss != null; }
        boolean retry() { return retries > 0; }

        ScopedLease inFlightCopy() {
            return new ScopedLease(expected, deadline, due, retries, true, confirm, null);
        }
    }

    /** One pipeline's local view: the claim this member proved, and when the next round trip is due. */
    private static final class Held {
        private final PipelineIntentScope scope;
        /** Highest candidate already probed; lower retries share its bounded contact interval. */
        private PipelineIntentScope probedScope;
        private long nextScopeProbeNanos;
        // Claims and renewal failures carry immutable snapshots across the ownership and renewal paths.
        @SuppressWarnings("java:S3077")
        private volatile WorkloadClaim claim;
        private volatile boolean executionAdvancing;
        // The renewer never needs the ownership monitor or ordinary IO admission to read or publish this proof.
        private final java.util.concurrent.atomic.AtomicReference<ScopedLease> scopedLease =
                new java.util.concurrent.atomic.AtomicReference<>();
        @SuppressWarnings("java:S3077")
        private volatile RenewalFailure renewalFailure;
        private long leaseDeadlineNanos;

        private Held(PipelineIntentScope scope) {
            this.scope = scope;
        }
        private long nextContactNanos;
        private boolean contacted;
        /** Successful acquire/renew replies are serialized by the ownership IO gate. */
        private long leaseProofOrder;
        /**
         * The members the run this member last submitted was planned over -- the ones in sight when it
         * was submitted -- or null while it has submitted none. Kept apart from the claim, whose own
         * revision moves forward the moment this member re-acquires under a changed cluster -- which is
         * exactly when the difference between what the run was planned over and what is here now becomes
         * the thing worth knowing.
         *
         * <p>Forgotten when this member takes the pipeline back carrying a run somebody else submitted in
         * between: it describes only the run it was taken for.
         */
        private Set<String> runMembers;
        /** The execution generation of the run {@link #runMembers} describes; zero while there is none. */
        private long runExecutionGeneration;
        /**
         * The execution generation the claim carried when a start on this member was refused before it took
         * a run of its own, or {@link #NEVER}. While the claim still carries it, what failed is that start,
         * not the run the generation names.
         */
        private long startRefusedAtGeneration = NEVER;
        /**
         * The last moment any of those members was out of sight while this member looked, or
         * {@link #NEVER}. Remembered rather than recomputed, because a member that leaves and
         * comes back is a member that left: the run it was carrying pieces of died either way, and by
         * the time anybody asks, a comparison against who is in sight now cannot see that it was ever
         * gone.
         *
         * <p>A moment rather than a flag, and it outlives the run it was taken under, because what it
         * has to answer is asked about the runs that come after: a departure goes on ending them for a
         * stretch, and every one of those is planned over members that are all still here. How long a
         * stretch is the asker's to decide, so it is kept raw here.
         */
        private long lostAMemberAtNanos = NEVER;
        /** The first FAILED observation without visible member loss. */
        private long failureObservedAtNanos = NEVER;
        /** The gate publication present when the member-loss detection window first elapsed. */
        private long visibilityRevisionAtDetectionWindow = NEVER;
    }

    /** No observation has been recorded. */
    private static final long NEVER = Long.MIN_VALUE;

    private PipelineActuationOwnership() {
        this.clusterId = "single";
        this.businessScope = null;
        this.owner = null;
        this.membership = null;
        this.claims = null;
        this.ttl = Duration.ZERO;
        this.renewIntervalNanos = 0;
        this.nanoTime = System::nanoTime;
        this.fenced = false;
        this.nodeSession = null;
    }

    /** An explicit fixture owner for a port with no coordinated store. */
    static PipelineActuationOwnership single() {
        return new PipelineActuationOwnership();
    }

    PipelineActuationOwnership(
            String clusterId,
            WorkloadOwner owner,
            ClusterMembershipGate membership,
            ClusterWorkloadClaims claims,
            Duration ttl,
            Duration renewInterval) {
        this(clusterId, owner, membership, claims, ttl, renewInterval, System::nanoTime);
    }

    PipelineActuationOwnership(
            String clusterId,
            WorkloadOwner owner,
            ClusterMembershipGate membership,
            ClusterWorkloadClaims claims,
            Duration ttl,
            Duration renewInterval,
            LongSupplier nanoTime) {
        this(clusterId, null, owner, membership, claims, ttl, renewInterval, nanoTime, null);
    }

    static PipelineActuationOwnership scoped(CacheBusinessScope scope, WorkloadOwner owner,
            ClusterMembershipGate membership, ClusterWorkloadClaims claims, Duration ttl,
            Duration renewInterval, LongSupplier nanoTime) {
        Objects.requireNonNull(scope, "scope");
        return new PipelineActuationOwnership(scope.clusterId(), scope, owner, membership, claims,
                ttl, renewInterval, nanoTime, null);
    }

    static PipelineActuationOwnership nodeBacked(CacheBusinessScope scope, WorkloadClaimFence node,
            ClusterMembershipGate membership, ClusterWorkloadClaims claims, Duration ttl,
            Duration renewInterval, LongSupplier nanoTime) {
        return new PipelineActuationOwnership(scope.clusterId(), scope, node.owner(), membership, claims,
                ttl, renewInterval, nanoTime, node);
    }

    private PipelineActuationOwnership(String clusterId, CacheBusinessScope businessScope,
            WorkloadOwner owner, ClusterMembershipGate membership, ClusterWorkloadClaims claims,
            Duration ttl, Duration renewInterval, LongSupplier nanoTime, WorkloadClaimFence nodeSession) {
        this.businessScope = businessScope;
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.nodeSession = nodeSession;
        this.membership = Objects.requireNonNull(membership, "membership");
        this.claims = Objects.requireNonNull(claims, "claims").draining(admission);
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        Objects.requireNonNull(renewInterval, "renewInterval");
        if (renewInterval.compareTo(ttl) >= 0) {
            throw new IllegalArgumentException("the actuation renew interval must be shorter than its lease");
        }
        this.renewIntervalNanos = renewInterval.toNanos();
        this.fenced = true;
    }

    /** Answers whether this member drives {@code pipelineId} right now, renewing or acquiring when due. */
    Permit permit(String pipelineId) {
        return admittedCall(()->{synchronized(this){return admittedPermit(pipelineId);}},Permit::denied);
    }
    private Permit admittedPermit(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing || businessScope != null) return Permit.denied();
        if (!fenced) return Permit.unfenced();
        Held state = held.computeIfAbsent(pipelineId, id -> new Held(null));
        return permit(pipelineId, state);
    }

    /** A new physical scope must be admitted by the store before it can replace the local holder. */
    Permit permit(PipelineIntentScope scope) { return permit(scope, false); }

    Permit permit(PipelineIntentScope scope, boolean initialStart) {
        Objects.requireNonNull(scope, "scope");
        if (!admission.enter()) return Permit.denied();
        ScopedAcquisition acquisition = null;
        try {
            Held state;
            synchronized (this) {
                if (closing || !admission.current() || !accepts(scope)
                        || controls.containsKey(scope.pipelineId()) || acquisitions.containsKey(scope.pipelineId())
                        || pendingReleases.containsKey(scope.pipelineId()) || advancing(scope.pipelineId())) {
                    return Permit.denied();
                }
                state = held.get(scope.pipelineId());
            }
            synchronized (this) {
                if (closing || !admission.current() || !accepts(scope)
                        || controls.containsKey(scope.pipelineId()) || acquisitions.containsKey(scope.pipelineId())
                        || pendingReleases.containsKey(scope.pipelineId()) || advancing(scope.pipelineId())) {
                    return Permit.denied();
                }
                state = held.computeIfAbsent(scope.pipelineId(), ignored -> new Held(scope));
                long now = nanoTime.getAsLong();
                if (scope.equals(state.scope) && state.claim != null) return permit(scope.pipelineId(), state, false);
                acquisition = reserveAcquisition(scope, state, now);
                if (acquisition == null) return Permit.denied();
            }
            long started = nanoTime.getAsLong();
            var planned = membership.committed();
            if (planned == null) return Permit.denied();
            var grant = requestClaim(scope.pipelineId(), scope, planned.revision(), initialStart);
            if (grant.isEmpty()) return Permit.denied();
            var claim = grant.orElseThrow();
            PendingRelease pending;
            synchronized (this) {
                if (canAdopt(claim, planned)
                        && acquisitions.get(scope.pipelineId()) == acquisition
                        && held.get(scope.pipelineId()) == acquisition.previous()
                        && acquisition.previous().claim == acquisition.original()
                        && !controls.containsKey(scope.pipelineId())) {
                    adoptAcquisition(acquisition.candidate(), claim, started);
                    held.put(scope.pipelineId(), acquisition.candidate());
                    return new Permit(true, claim);
                }
                pending = registerUnadoptedGrant(scope, claim);
            }
            if (pending != null) releasePending(pending);
            return Permit.denied();
        } finally {
            if (acquisition != null) {
                synchronized (this) { acquisitions.remove(scope.pipelineId(), acquisition); }
            }
            admission.leave();
        }
    }

    /** Caller holds the monitor. A placeholder or a holder of another fence cannot account for this grant. */
    private PendingRelease registerUnadoptedGrant(PipelineIntentScope scope, WorkloadClaim claim) {
        if (closing) return null;
        var local = held.get(scope.pipelineId());
        if (local != null && scope.equals(local.scope) && local.claim != null
                && WorkloadClaimFence.from(local.claim).equals(WorkloadClaimFence.from(claim))) return null;
        var pending = new PendingRelease(scope, claim, false);
        pendingReleases.put(scope.pipelineId(), pending);
        return pending;
    }

    /** Retries an ambiguous release on this pipeline's worker, never on a peer's startup path. */
    boolean finishStoppedRelease(String pipeline) {
        PendingRelease pending;
        synchronized (this) { pending = pendingReleases.get(pipeline); }
        if (pending == null || pending.terminal == null) return true;
        if (!admission.enter()) return false;
        try {
            releasePending(pending);
            synchronized (this) { return !pendingReleases.containsKey(pipeline); }
        } finally { admission.leave(); }
    }

    private void releasePending(PendingRelease pending) {
        synchronized (this) {
            if (closing || !admission.current() || !membership.businessEligible() || pending.inFlight
                    || pendingReleases.get(pending.scope.pipelineId()) != pending) return;
            pending.inFlight = true;
        }
        boolean released = false;
        boolean confirmedStop = false;
        try {
            var expected = WorkloadClaimFence.from(pending.claim);
            var stored = claims.readPipelineForRelease(pending.scope);
            if (closing || !admission.current() || !membership.businessEligible()) return;
            boolean same = stored.isPresent() && expected.equals(WorkloadClaimFence.from(stored.orElseThrow().claim()));
            released = !same || !stored.orElseThrow().leased();
            if (released && same && pending.terminal != null && pending.deadlineBeforeRelease != null) {
                // A naturally expired lease keeps its original server deadline.
                confirmedStop = stored.orElseThrow().claim().leaseUntil().isBefore(pending.deadlineBeforeRelease);
            }
            if (!released) {
                if (pending.terminal != null) pending.deadlineBeforeRelease = stored.orElseThrow().claim().leaseUntil();
                if (claims.release(pending.claim)) {
                    if (pending.terminal == null) released = true;
                    else {
                        var after = claims.readPipelineForRelease(pending.scope);
                        boolean original = after.isPresent() && expected.equals(WorkloadClaimFence.from(after.orElseThrow().claim()));
                        released = !original || !after.orElseThrow().leased();
                        confirmedStop = original && released;
                    }
                }
            }
            if (closing || !admission.current() || !membership.businessEligible()) released = false;
        } catch (TapstateException refused) {
            // A definitive refusal of this physical scope cannot authorize a write to its successor.
            released = !closing && admission.current() && membership.businessEligible()
                    && refused.code() == IoError.WORKLOAD_CLAIM_FENCED && refused.getCause() == null;
        } catch (RuntimeException unavailable) {
            // Retain the original tuple for a later retention pass after a transient store failure.
        } finally {
            synchronized (this) {
                pending.inFlight = false;
                if (released) {
                    if (pending.terminal != null) {
                        pending.terminal.status = confirmedStop ? StopReleaseStatus.CONFIRMED : StopReleaseStatus.UNPROVEN;
                        if (confirmedStop) LOG.log(System.Logger.Level.DEBUG,
                                "Confirmed stopped Pipeline lease release {0}, claim={1}, execution={2}",
                                pending.scope.pipelineId(), pending.claim.claimGeneration(), pending.claim.executionGeneration());
                    }
                    pendingReleases.remove(pending.scope.pipelineId(), pending);
                }
            }
        }
    }

    private ScopedAcquisition reserveAcquisition(PipelineIntentScope scope, Held state, long now) {
        if (scope.equals(state.scope)) {
            if (state.contacted && now - state.nextContactNanos < 0) return null;
            state.contacted = true;
            state.nextContactNanos = now + renewIntervalNanos;
        } else {
            if (scope.stateGeneration() < state.scope.stateGeneration()
                    || state.probedScope != null && scope.stateGeneration() <= state.probedScope.stateGeneration()
                        && now - state.nextScopeProbeNanos < 0) return null;
            state.probedScope = scope;
            state.nextScopeProbeNanos = now + renewIntervalNanos;
        }
        var candidate = new Held(scope);
        if (scope.equals(state.scope)) {
            candidate.runMembers = state.runMembers;
            candidate.runExecutionGeneration = state.runExecutionGeneration;
            candidate.startRefusedAtGeneration = state.startRefusedAtGeneration;
            candidate.lostAMemberAtNanos = state.lostAMemberAtNanos;
            candidate.renewalFailure = state.renewalFailure;
        }
        var acquisition = new ScopedAcquisition(state, candidate, state.claim);
        acquisitions.put(scope.pipelineId(), acquisition);
        return acquisition;
    }
    private Permit admittedPermit(PipelineIntentScope scope) {
        Objects.requireNonNull(scope, "scope");
        if (closing || !admission.current() || !accepts(scope) || controls.containsKey(scope.pipelineId())
                || acquisitions.containsKey(scope.pipelineId()) || pendingReleases.containsKey(scope.pipelineId())
                || advancing(scope.pipelineId())) return Permit.denied();
        String pipelineId = scope.pipelineId();
        Held state = held.get(pipelineId);
        if (state == null) {
            state = new Held(scope);
            held.put(pipelineId, state);
        } else if (!scope.equals(state.scope)) {
            if (scope.stateGeneration() < state.scope.stateGeneration()) return Permit.denied();
            long now = nanoTime.getAsLong();
            if (state.probedScope != null && scope.stateGeneration() <= state.probedScope.stateGeneration()
                    && now - state.nextScopeProbeNanos < 0) return Permit.denied();
            if (state.probedScope == null || scope.stateGeneration() > state.probedScope.stateGeneration()) {
                state.probedScope = scope;
            }
            state.nextScopeProbeNanos = now + renewIntervalNanos;
            Held candidate = new Held(scope);
            Permit admitted = acquire(pipelineId, candidate, now);
            if (admitted.granted()) held.put(pipelineId, candidate);
            return admitted;
        }
        return permit(pipelineId, state);
    }

    private boolean accepts(PipelineIntentScope scope) {
        return businessScope != null && scope.clusterId().equals(businessScope.clusterId())
                && scope.bindingVersion() == businessScope.bindingVersion()
                && scope.cacheIncarnation().equals(businessScope.cacheIncarnation());
    }

    private Permit permit(String pipelineId, Held state) {
        return permit(pipelineId, state, true);
    }

    private Permit permit(String pipelineId, Held state, boolean renew) {
        // Every pass, not every round trip: this reads a local reference the membership reconciler
        // publishes into, so it costs nothing, and the shorter the interval between looks the shorter
        // the absence that can pass unseen between two of them.
        observeMembership(state);
        long now = nanoTime.getAsLong();
        if (state.scope != null && state.claim != null) {
            if (nodeSession != null) return membership.businessEligible() || membership.singleControlEligible(state.claim)
                    ? new Permit(true, state.claim) : Permit.denied();
            var lease = state.scopedLease.get();
            if (lease == null || lease.lost() || now - lease.deadline() >= 0 && !lease.inFlight()) {
                if (lease != null) state.renewalFailure = new RenewalFailure(lease.expected(),
                        lease.lost() ? lease.loss() : RenewalOutcome.UNCONFIRMED);
                state.claim = null;
                state.scopedLease.compareAndSet(lease, null);
                state.nextContactNanos = now + renewIntervalNanos;
                return Permit.denied();
            }
            if (lease.retry() || now - lease.deadline() >= 0) return Permit.denied();
            var local = state.claim;
            var refreshed = lease.expected();
            if (local.leaseUntil().equals(refreshed.leaseUntil())) return new Permit(true, local);
            return new Permit(true, new WorkloadClaim(local.key(), local.owner(), local.claimGeneration(),
                    local.executionGeneration(), local.topologyRevision(), refreshed.leaseUntil(),
                    local.contextExecutionGeneration(), local.executionClaimGeneration(), local.executionNodeIds(),
                    local.failureClaimGeneration(), local.failureAfterMemberLoss()));
        }
        boolean due = !state.contacted || now - state.nextContactNanos >= 0;
        if (state.claim != null && !due) {
            return new Permit(true, state.claim);
        }
        if (state.claim != null) {
            return renew(state, now);
        }
        // Not the driver: probe for a claim its holder may have let expire, but no more often than a
        // holder renews. A probe every tick would put one store round trip per pipeline per second on
        // every member that is not driving anything.
        return due ? acquire(pipelineId, state, now) : Permit.denied();
    }

    /**
     * Whether a run may be submitted, the generations that fence it, and the committed topology the claim behind
     * it was held under - none of either in an explicit uncoordinated fixture.
     */
    record Execution(boolean allowed, ExecutionFence fence, Long topologyRevision, WorkloadClaimFence claimFence) {
        Execution {
            boolean hasIdentity = fence != null || topologyRevision != null || claimFence != null;
            if ((!allowed && hasIdentity) || (hasIdentity && (fence == null || topologyRevision == null || claimFence == null)))
                throw new IllegalArgumentException("an execution carries either its complete identity or none");
            if (hasIdentity && (claimFence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                    || !fence.pipelineId().equals(claimFence.key().resourceId()) || fence.claimGeneration() != claimFence.claimGeneration()
                    || fence.executionGeneration() != claimFence.executionGeneration() || topologyRevision.longValue() != claimFence.topologyRevision()))
                throw new IllegalArgumentException("the execution identity must match its original advanced claim");
            if (hasIdentity && fence.scope() != null && (!fence.scope().clusterId().equals(claimFence.key().clusterId())
                    || !fence.ownerNodeId().equals(claimFence.owner().nodeId())
                    || !fence.ownerBootId().equals(claimFence.owner().bootId())
                    || fence.topologyRevision().longValue() != claimFence.topologyRevision()))
                throw new IllegalArgumentException("the scoped execution must retain its original claim owner");
        }

        static Execution unfenced() {
            return new Execution(true, null, null, null);
        }

        static Execution refused() {
            return new Execution(false, null, null, null);
        }
    }

    /**
     * Takes the next execution generation for a run this member is about to submit. Every submission gets
     * one, including a resubmission by the same holder after a member left: ownership did not change, but
     * it is a different run, and the members still carrying pieces of the previous one have to be able to
     * tell. The generation is allocated by the store under the exact live claim, so two members cannot
     * both believe they took it.
     *
     * <p>Refused when this member no longer holds the pipeline, or when the store cannot say that it
     * does. The caller submits nothing in that case: a run that cannot be fenced is a run nothing could
     * later stop from writing.
     */
    /**
     * Notes that a start of {@code pipelineId} on this member was refused before it took a run, by one of the
     * checks a start makes ahead of everything a run opens. Until a run takes a generation of its own, the
     * failure that refusal records is the refusal's, which no member leaving answers for.
     */
    synchronized void startRefusedBeforeItsRun(String pipelineId) {
        Held state = held.get(Objects.requireNonNull(pipelineId, "pipelineId"));
        if (businessScope == null && state != null && state.claim != null) {
            state.startRefusedAtGeneration = state.claim.executionGeneration();
        }
    }

    /** A refusal witnessed by an old pass cannot alter the later held run's failure attribution. */
    synchronized void startRefusedBeforeItsRun(String pipelineId, WorkloadClaimFence expected) {
        Objects.requireNonNull(expected, "expected");
        Held state = held.get(Objects.requireNonNull(pipelineId, "pipelineId"));
        if (businessScope == null && state != null && state.claim != null && expected.equals(WorkloadClaimFence.from(state.claim))) {
            state.startRefusedAtGeneration = state.claim.executionGeneration();
        }
    }

    synchronized void startRefusedBeforeItsRun(PipelineIntentScope scope, WorkloadClaimFence expected) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(expected, "expected");
        Held state = held.get(scope.pipelineId());
        if (accepts(scope) && state != null && !state.executionAdvancing && scope.equals(state.scope) && state.claim != null
                && expected.equals(WorkloadClaimFence.from(state.claim))) {
            state.startRefusedAtGeneration = state.claim.executionGeneration();
        }
    }

    Execution beginExecution(String pipelineId) {return admittedCall(()->beginExecutionOwned(pipelineId),Execution::refused);}
    private synchronized Execution beginExecutionOwned(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing || businessScope != null) {
            return Execution.refused();
        }
        if (!fenced) {
            return Execution.unfenced();
        }
        Held state = held.get(pipelineId);
        if (state == null || state.claim == null) return Execution.refused();
        return beginExecution(pipelineId, WorkloadClaimFence.from(state.claim));
    }

    /** Advances only the immutable claim captured by this pass, never a later claim held under its id. */
    Execution beginExecution(String pipelineId, WorkloadClaimFence expected) {
        return admittedCall(()->beginExecutionOwned(pipelineId,expected),Execution::refused);
    }
    private synchronized Execution beginExecutionOwned(String pipelineId, WorkloadClaimFence expected) {
        Objects.requireNonNull(pipelineId, "pipelineId"); Objects.requireNonNull(expected, "expected");
        return businessScope == null ? beginLegacyExecution(pipelineId, expected) : Execution.refused();
    }

    Execution beginExecution(PipelineIntentScope scope, WorkloadClaimFence expected) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(expected, "expected");
        if (!admission.enter()) return Execution.refused();
        try {
            var reserved = reserveAdvance(scope, expected);
            if (reserved == null) return Execution.refused();
            try {
                return advanceScopedExecution(scope, reserved);
            } finally {
                synchronized (this) {
                    reserved.state().executionAdvancing = false;
                }
            }
        } finally {
            admission.leave();
        }
    }

    private record ScopedAdvance(Held state, WorkloadClaim original, ClusterMembership planned, Set<String> runMembers) {}

    private synchronized ScopedAdvance reserveAdvance(PipelineIntentScope scope, WorkloadClaimFence expected) {
        String id = scope.pipelineId();
        if (closing || !fenced || !accepts(scope) || controls.containsKey(id)
                || acquisitions.containsKey(id) || pendingReleases.containsKey(id)) return null;
        var state = held.get(id);
        if (state == null || !scope.equals(state.scope) || state.executionAdvancing || state.claim == null
                || !expected.equals(WorkloadClaimFence.from(state.claim))) return null;
        if (nodeSession == null) {
            var lease = state.scopedLease.get();
            if (lease == null || lease.retry() || lease.lost() || nanoTime.getAsLong() - lease.deadline() >= 0)
                return null;
        }
        var planned = membership.committed();
        if (!admission.current() || planned == null || !membership.businessEligible()
                || planned.revision() < expected.topologyRevision()
                || nodeSession != null && planned.revision() != expected.topologyRevision()) return null;
        var runMembers = plannedOver(planned);
        state.executionAdvancing = true;
        return new ScopedAdvance(state, state.claim, planned, runMembers);
    }

    private Execution advanceScopedExecution(PipelineIntentScope scope, ScopedAdvance reserved) {
        var original = reserved.original();
        if (original.topologyRevision() != reserved.planned().revision()) {
            original = rebaseForAdvance(scope, reserved);
            if (original == null) return Execution.refused();
        }
        // The durable CAS still uses the exact claim and original topology of this submission.
        var next = advanceClaim(original, reserved.runMembers());
        synchronized (this) {
            var state = reserved.state();
            if (!holdsAdvance(scope, state, original)) return Execution.refused();
            if (next.isEmpty() || !admission.current() || !reserved.planned().equals(membership.committed())
                    || !membership.businessEligible()) {
                state.claim = null;
                next.ifPresent(claim -> registerUnadoptedGrant(scope, claim));
                return Execution.refused();
            }
            state.claim = next.orElseThrow();
            advanceScopedProof(state, state.claim);
            return submittedExecution(scope.pipelineId(), scope, state, reserved.planned(), reserved.runMembers());
        }
    }

    private WorkloadClaim rebaseForAdvance(PipelineIntentScope scope, ScopedAdvance reserved) {
        var original = reserved.original();
        long started = nanoTime.getAsLong();
        var grant = requestClaim(scope.pipelineId(), scope, reserved.planned().revision());
        synchronized (this) {
            var state = reserved.state();
            if (grant.isEmpty()) return null;
            var claim = grant.orElseThrow();
            if (!holdsAdvance(scope, state, original) || !canAdopt(claim, reserved.planned())
                    || !sameClaim(WorkloadClaimFence.from(claim), WorkloadClaimFence.from(original), false)) {
                if (holdsAdvance(scope, state, original)) state.claim = null;
                registerUnadoptedGrant(scope, claim);
                return null;
            }
            recordLeaseProof(state, claim, started);
            return claim;
        }
    }

    private boolean holdsAdvance(PipelineIntentScope scope, Held state, WorkloadClaim original) {
        return !closing && state.executionAdvancing && held.get(scope.pipelineId()) == state && state.claim == original;
    }

    private boolean advancing(String pipelineId) {
        var state = held.get(pipelineId);
        return state != null && state.executionAdvancing;
    }

    private Execution beginLegacyExecution(String pipelineId, WorkloadClaimFence expected) {
        if (closing || !fenced || controls.containsKey(pipelineId)) return Execution.refused();
        var state = held.get(pipelineId);
        if (state == null || state.claim == null || !expected.equals(WorkloadClaimFence.from(state.claim)))
            return Execution.refused();
        var planned = membership.committed();
        var runMembers = planned == null ? Set.<String>of() : plannedOver(planned);
        var advanced = advanceClaim(state.claim, runMembers);
        if (closing || advanced.isEmpty()) {
            state.claim = null;
            return Execution.refused();
        }
        state.claim = advanced.orElseThrow();
        return submittedExecution(pipelineId, null, state, planned, runMembers);
    }

    private Optional<WorkloadClaim> advanceClaim(WorkloadClaim original, Set<String> runMembers) {
        try {
            return claims.advanceExecution(original, original.topologyRevision(), runMembers);
        } catch (RuntimeException unreachable) {
            return Optional.empty();
        }
    }

    private Execution submittedExecution(String pipelineId, PipelineIntentScope scope, Held state,
            ClusterMembership planned, Set<String> runMembers) {
        state.failureObservedAtNanos = NEVER;
        state.visibilityRevisionAtDetectionWindow = NEVER;
        // What this run is planned over. Null rather than empty when nothing is committed -- which the
        // eligibility gate above makes unreachable -- because an empty set would read as "planned over
        // nobody", and nobody can never go missing.
        state.runMembers = planned == null ? null : runMembers;
        state.runExecutionGeneration = state.claim.executionGeneration();
        // The departure is deliberately not forgotten here. This run is planned over members that are
        // all present, so the comparison below will find nothing missing from it -- and the run is being
        // submitted because a member went away, into a cluster that is still settling from it. Clearing
        // the moment here would make the very next death of this run read as the pipeline's own.
        WorkloadClaimFence advancedFence = WorkloadClaimFence.from(state.claim);
        ExecutionFence run = scope == null
                ? new ExecutionFence(pipelineId, state.claim.claimGeneration(), state.claim.executionGeneration())
                : new ExecutionFence(scope, advancedFence);
        return new Execution(true, run, state.claim.topologyRevision(), advancedFence);
    }

    /**
     * Whether a failed run is one a departure answers for, and why not when it is not: a pipeline left
     * failed has to say what kept it there, or it reads exactly like one nobody asked about.
     */
    enum Departure {
        /** A member the run was planned over is gone, or went recently enough to answer for this death. */
        A_MEMBER_LEFT(null),
        /** Another holder submitted the run, and may have gone before it could say why the run ended. */
        INHERITED(null),
        ALONE("a single member cannot lose a member"),
        SHUTTING_DOWN("this member is shutting down"),
        NOT_DRIVING("this member does not hold the pipeline's actuation claim"),
        NO_RUN("no run has been submitted under the pipeline's actuation claim"),
        START_REFUSED("its last start was refused before it took a run, which no member leaving answers for"),
        ITS_OWN_FAILURE("its run's failure was recorded as its own before any member it was planned over left"),
        NOBODY_LEFT("every member its run was planned over is still in sight,"
                + " and none left within the settling stretch");

        private final String refusal;

        Departure(String refusal) {
            this.refusal = refusal;
        }

        /** Whether this answer lets the run be rebuilt. */
        boolean admits() {
            return refusal == null;
        }

        /** Why the run is not rebuilt, or null when it is. */
        String refusal() {
            return refusal;
        }
    }

    /**
     * Whether a member the run this member last submitted for {@code pipelineId} was planned over is
     * gone, or went within {@code settlingNanos} of now - and when the answer is no, which no it is.
     *
     * <p><b>Why a stretch and not an instant.</b> A member going away does not end one run; it ends the
     * run it was carrying pieces of, and then goes on ending the ones submitted to replace it while the
     * cluster settles - a reconnection, a topology that changed again, a fence the new execution moved
     * out from under the old one. Every one of those replacements is planned over members that are all
     * still present, so an instant reading calls them the pipeline's own deaths and leaves it failed for
     * a person over a member that left. The caller chooses the stretch, because what bounds it is the
     * caller's budget and spacing rather than anything ownership knows.
     *
     * <p>While the member is actually still missing the stretch never runs out: every look that finds it
     * absent takes the moment again. What the stretch bounds is only the tail after it is back, or after
     * a run was re-planned without it.
     *
     * <p>This is the product's own answer to "did a member leave", and it is its own rather than the
     * engine's for a measured reason: a run ended by a member leaving and a run ended by a connector
     * giving up reach this process as the same exception class with the same absent cause, differing
     * only in text inside a message. Who is in sight is read off the membership gate, which this
     * cluster keeps for itself -- and it is read there rather than off the committed set because the
     * committed set only ever grows: a member that goes away keeps its place in it, so an absence could
     * never show.
     *
     * <p>It asks who is <em>gone</em>, not whether the membership moved. A member joining moves the
     * committed revision too, and it takes nothing away from a run already planned: treating that as a
     * reason to replace the run would restart a connector defect that happened to die shortly after
     * somebody started a new node.
     *
     * <p>False while nothing is committed and on a single node -- one member cannot lose a member, it
     * can only be the one that went.
     *
     * <p>The claim carries the run's planned members and the first holder that recorded its failure.
     * If a membership view published after the failure-detection window confirms no member was lost,
     * a later handover cannot make that earlier death recoverable. If a member was lost when failure
     * was recorded, that fact survives a takeover even if the member has returned. An inherited run
     * with no recorded failure is admitted: its driver may have gone away before it could record why
     * the run ended. That includes a run somebody else submitted while this member did not hold the
     * pipeline, whatever this member once planned a run of its own over.
     *
     * <p>A start refused before it took a run is judged as that refusal. The claim still names the run
     * before it, and judged by that run - inherited, or planned over a member that has gone - every
     * refused start would read as the same death again and spend a rebuild meant for a departure.
     */
    synchronized Departure departure(String pipelineId, long settlingNanos) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (!fenced) {
            return Departure.ALONE;
        }
        if (closing) {
            return Departure.SHUTTING_DOWN;
        }
        Held state = held.get(pipelineId);
        if (state == null || state.claim == null) {
            return Departure.NOT_DRIVING;
        }
        if (state.claim.executionGeneration() == 0) {
            return Departure.NO_RUN;
        }
        if (state.startRefusedAtGeneration == state.claim.executionGeneration()) {
            return Departure.START_REFUSED;
        }
        // A failure the submitting holder saw before any member loss remains that same failure when
        // the claim changes hands. A later departure cannot turn it into a cluster-caused death.
        if (state.claim.contextExecutionGeneration() == state.claim.executionGeneration()
                && state.claim.failureClaimGeneration() == state.claim.executionClaimGeneration()
                && !state.claim.failureAfterMemberLoss()) {
            return Departure.ITS_OWN_FAILURE;
        }
        if (state.runMembers == null) {
            // Another holder submitted this run and this member has no snapshot of it. Take the moment
            // here so a replacement that fails while the handover settles can spend the remaining budget.
            state.lostAMemberAtNanos = nanoTime.getAsLong();
            return Departure.INHERITED;
        }
        observeMembership(state);
        if (state.lostAMemberAtNanos == NEVER
                || nanoTime.getAsLong() - (state.lostAMemberAtNanos + settlingNanos) >= 0) {
            return Departure.NOBODY_LEFT;
        }
        return Departure.A_MEMBER_LEFT;
    }

    /**
     * Renews every claim this member holds whose renewal is due, whatever the convergence pass is doing.
     * Called on a renewer of its own, so a start that holds the pass for longer than a lease -- an
     * overloaded host, a slow source, many tables -- runs out no claim of this member, its own included.
     * A definitive refusal drops the claim. A timed-out scoped renewal can retry the same fence before
     * the last confirmed lease deadline.
     */
    void renewDue() { renewDueWith(null, 0); }

    void dispatchDueRenewals(Executor dispatcher) { renewDueWith(Objects.requireNonNull(dispatcher), 0); }

    void dispatchDueRenewals(Executor dispatcher, int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("renewal dispatcher capacity must be positive");
        renewDueWith(Objects.requireNonNull(dispatcher), capacity);
    }

    private void renewDueWith(Executor dispatcher, int capacity) {
        if (nodeSession != null) return;
        if (businessScope != null) {
            long now = nanoTime.getAsLong();
            var due = held.values().stream().filter(state -> {
                var lease = state.scopedLease.get();
                return lease != null && !lease.lost() && now - lease.due() >= 0;
            }).sorted(Comparator.comparingLong(state -> {
                var lease = state.scopedLease.get();
                return lease == null ? Long.MAX_VALUE : lease.deadline();
            })).toList();
            for (Held state : due) {
                long dispatch = nanoTime.getAsLong();
                var lease = state.scopedLease.get();
                if (lease == null || lease.inFlight() || dispatchedRenewals.contains(lease.expected().key())) continue;
                var recoveries = lease.retry() ? java.util.Set.<WorkloadClaimKey>of() : earlierRecoveryKeys(lease, dispatch);
                if (capacity == 0 && !recoveries.isEmpty()) return;
                var reserved = new java.util.HashSet<>(dispatchedRenewals);
                reserved.addAll(recoveries);
                if (!lease.retry() && !recoveries.isEmpty() && reserved.size() >= capacity) return;
                if (dispatcher == null ? !renewScoped(state, dispatch, recoveries) : !dispatchRenewal(dispatcher, state, lease, recoveries)) return;
            }
            return;
        }
        // A finite pass lets previously renewed claims compete again even under continuous arrivals.
        List<Held> due = admittedCall(this::dueRenewals, List::of);
        for (Held state : due) {
            // Each independent renewal owns one allowance and yields the fair IO gate afterward.
            long queued = nanoTime.getAsLong();
            admittedCall(() -> { renewIfDue(state, queued);return null; }, () -> null);
        }
    }

    private boolean dispatchRenewal(Executor dispatcher, Held state, ScopedLease lease, java.util.Set<WorkloadClaimKey> recoveries) {
        var key = lease.expected().key();
        if (!dispatchedRenewals.add(key)) return true;
        try {
            dispatcher.execute(() -> {
                try { renewScoped(state, nanoTime.getAsLong(), recoveries); }
                finally { dispatchedRenewals.remove(key); }
            });
            return true;
        } catch (RejectedExecutionException full) {
            dispatchedRenewals.remove(key);
            return false;
        }
    }

    /** A retry with an earlier cutoff may become due while this batch is executing. */
    private java.util.Set<WorkloadClaimKey> earlierRecoveryKeys(ScopedLease ordinary, long now) {
        var keys = new java.util.HashSet<WorkloadClaimKey>();
        for (Held other : held.values()) {
            var recovery = other.scopedLease.get();
            if (recovery != null && recovery.retry() && !recovery.inFlight() && !recovery.lost()
                    && !dispatchedRenewals.contains(recovery.expected().key()) && recovery.deadline() - now > 0 && recovery.deadline() - ordinary.deadline() < 0
                    && recovery.due() - now <= RENEW_RETRY_NANOS && stillHeld(other, recovery)) keys.add(recovery.expected().key());
        }
        return java.util.Set.copyOf(keys);
    }

    private long retryDue(ScopedLease lease, long now) {
        return Math.min(lease.deadline(), now + Math.min(renewIntervalNanos, RENEW_RETRY_NANOS));
    }

    /** Returns whether this scan can dispatch another renewal. */
    private boolean renewScoped(Held state, long now) { return renewScoped(state, now, java.util.Set.of()); }

    private boolean renewScoped(Held state, long now, java.util.Set<WorkloadClaimKey> recoveries) {
        var lease = state.scopedLease.get();
        if (lease == null || lease.lost() || lease.inFlight() || now - lease.due() < 0
                || !stillHeld(state, lease) || !admission.enter()) return true;
        try {
            if (now - lease.deadline() >= 0) {
                state.scopedLease.compareAndSet(lease, new ScopedLease(lease.expected(), lease.deadline(),
                        lease.due(), lease.retries(), false, false, RenewalOutcome.UNCONFIRMED));
                return true;
            }
            var pending = lease.inFlightCopy();
            if (!state.scopedLease.compareAndSet(lease, pending)) return true;
            Optional<WorkloadClaim> renewed = Optional.empty();
            long renewedDeadline = now + ttl.toNanos();
            long renewedDue = now + renewIntervalNanos;
            boolean retry = false;
            boolean failed = false;
            boolean confirm = lease.confirm();
            try {
                long budget = Math.max(1, Math.min(lease.deadline() - now, renewIntervalNanos / 2));
                LOG.log(System.Logger.Level.DEBUG,
                        "Pipeline renewal dispatch {0}, claim={1}, execution={2}, remainingMs={3}, leaseUntil={4}, attempt={5}",
                        lease.expected().key().resourceId(), lease.expected().claimGeneration(),
                        lease.expected().executionGeneration(), TimeUnit.NANOSECONDS.toMillis(lease.deadline() - now),
                        lease.expected().leaseUntil(), lease.retries() + 1);
                if (confirm) {
                    var readback = claims.confirmPipelineRenewal(state.scope, lease.expected(), budget);
                    var exact = WorkloadClaimFence.from(lease.expected());
                    var current = readback.filter(WorkloadClaimReading::leased)
                            .filter(reading -> exact.equals(WorkloadClaimFence.from(reading.claim())));
                    confirm = false;
                    if (current.isPresent()) {
                        var reading = current.orElseThrow();
                        if (reading.claim().leaseUntil().isAfter(lease.expected().leaseUntil())) {
                            long remaining = Math.min(ttl.toNanos(), reading.leaseRemaining().toNanos());
                            renewed = Optional.of(reading.claim());
                            renewedDeadline = now + remaining;
                            renewedDue = now + Math.min(renewIntervalNanos, remaining / 2);
                            LOG.log(System.Logger.Level.DEBUG, "Confirmed earlier Pipeline renewal for {0}", exact.key().resourceId());
                        } else {
                            // A confirmed unchanged lease gets a separate renewal attempt before its original deadline.
                            retry = true;
                        }
                    }
                } else renewed = renewOriginal(lease, budget, recoveries);
            } catch (RuntimeException failure) {
                if (failure instanceof WorkloadClaimRenewalException renewal
                        && renewal.reason() == WorkloadClaimRenewalException.Reason.NOT_STARTED) {
                    long after = nanoTime.getAsLong();
                    state.scopedLease.compareAndSet(pending, new ScopedLease(lease.expected(), lease.deadline(),
                            retryDue(lease, after), lease.retries(), false, lease.confirm(), null));
                    return false;
                }
                failed = true;
                retry = retryableRenewal(failure);
                confirm = confirm || claimIoTimedOut(failure) || failure instanceof WorkloadClaimRenewalException renewal
                        && renewal.reason() == WorkloadClaimRenewalException.Reason.UNCONFIRMED;
                if (!lease.retry()) LOG.log(System.Logger.Level.WARNING,
                        "Pipeline renewal first failure {0}, claim={1}, execution={2}, retryable={3}, result={4}",
                        lease.expected().key().resourceId(), lease.expected().claimGeneration(),
                        lease.expected().executionGeneration(), retry, failure.toString());
            }
            long after = nanoTime.getAsLong();
            ClusterMembership committed = membership.committed();
            boolean eligible = stillHeld(state, lease) && (membership.businessEligible()
                    ? committed != null && committed.clusterId().equals(lease.expected().key().clusterId())
                            && (lease.expected().executionGeneration() == 0
                                ? committed.revision() == lease.expected().topologyRevision()
                                : committed.revision() >= lease.expected().topologyRevision())
                    : membership.singleControlEligible(lease.expected()));
            LOG.log(System.Logger.Level.DEBUG,
                    "Pipeline renewal response {0}, claim={1}, execution={2}, elapsedMs={3}, matched={4}, attempt={5}",
                    lease.expected().key().resourceId(), lease.expected().claimGeneration(),
                    lease.expected().executionGeneration(), TimeUnit.NANOSECONDS.toMillis(after - now), renewed.isPresent(),
                    lease.retries() + 1);
            if (!failed && eligible && renewed.isEmpty() && !lease.retry())
                LOG.log(System.Logger.Level.WARNING, "Pipeline renewal first refusal {0}, claim={1}, execution={2}, no matching lease",
                        lease.expected().key().resourceId(), lease.expected().claimGeneration(), lease.expected().executionGeneration());
            if (eligible && after - lease.deadline() < 0 && after - renewedDeadline < 0 && renewed.isPresent()
                    && WorkloadClaimFence.from(lease.expected()).equals(WorkloadClaimFence.from(renewed.orElseThrow()))) {
                state.scopedLease.compareAndSet(pending, new ScopedLease(renewed.orElseThrow(),
                        renewedDeadline, renewedDue, 0, false, false, null));
            } else {
                boolean retryable = eligible && retry && after - lease.deadline() < 0;
                if (eligible && retry && !retryable) LOG.log(System.Logger.Level.WARNING,
                        "Pipeline renewal reached its last confirmed deadline {0}, claim={1}, execution={2}, attempts={3}, leaseUntil={4}",
                        lease.expected().key().resourceId(), lease.expected().claimGeneration(),
                        lease.expected().executionGeneration(), lease.retries() + 1, lease.expected().leaseUntil());
                long due = retryable ? retryDue(lease, after) : after;
                var outcome = !eligible ? RenewalOutcome.MEMBERSHIP_CHANGED
                        : !failed && renewed.isEmpty() ? RenewalOutcome.REFUSED : RenewalOutcome.UNCONFIRMED;
                state.scopedLease.compareAndSet(pending, new ScopedLease(lease.expected(), lease.deadline(),
                        due, lease.retries() + (retryable ? 1 : 0), false, confirm, retryable ? null : outcome));
            }
        } finally {
            admission.leave();
        }
        return true;
    }

    private Optional<WorkloadClaim> renewOriginal(ScopedLease lease, long budget, java.util.Set<WorkloadClaimKey> recoveries) {
        if (budget <= 0) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), new TimeoutException());
        return membership.businessEligible() ? claims.renewScopedActuation(lease.expected(), ttl, budget, recoveries)
                : claims.renewActuationControl(lease.expected(), ttl, budget, recoveries);
    }

    private enum RenewalOutcome { REFUSED, UNCONFIRMED, MEMBERSHIP_CHANGED }
    private record RenewalFailure(WorkloadClaim expected, RenewalOutcome outcome) {}

    Optional<TapstateException> renewalFailure(PipelineIntentScope scope, WorkloadClaim current) {
        var state = held.get(scope.pipelineId());
        var failure = state == null ? null : state.renewalFailure;
        if (failure == null || failure.outcome() == RenewalOutcome.MEMBERSHIP_CHANGED || !scope.equals(state.scope) || current.executionGeneration() <= 0
                || current.executionGeneration() != failure.expected().executionGeneration()
                || current.claimGeneration() <= failure.expected().claimGeneration()) return Optional.empty();
        var code = failure.outcome() == RenewalOutcome.REFUSED
                ? io.tapstate.runtime.engine.EngineError.EXECUTION_LEASE_LOST
                : io.tapstate.runtime.engine.EngineError.EXECUTION_RENEWAL_FAILED;
        return Optional.of(new TapstateException(code, Map.of("pipeline", scope.pipelineId()), null));
    }

    private static boolean retryableRenewal(RuntimeException failure) {
        return failure instanceof WorkloadClaimRenewalException || claimIoTimedOut(failure) || failure instanceof TapstateException coded
                && coded.code() == IoError.STORE_UNAVAILABLE
                && coded.args().get("detail") instanceof String detail && detail.endsWith(" code=112");
    }

    private boolean stillHeld(Held state, ScopedLease lease) {
        var current = state.claim;
        return !closing && current != null && held.get(current.key().resourceId()) == state
                && WorkloadClaimFence.from(current).equals(WorkloadClaimFence.from(lease.expected()));
    }

    private void advanceScopedProof(Held state, WorkloadClaim next) {
        if (nodeSession != null) return;
        state.scopedLease.updateAndGet(previous -> {
            if (previous == null) return new ScopedLease(next, state.leaseDeadlineNanos,
                    state.nextContactNanos, 0, false, false, null);
            if (WorkloadClaimFence.from(previous.expected()).equals(WorkloadClaimFence.from(next))) return previous;
            long extension = Math.max(0, Duration.between(previous.expected().leaseUntil(), next.leaseUntil()).toNanos());
            return new ScopedLease(next, previous.deadline() + extension, previous.due(), 0, false, false, null);
        });
    }

    private synchronized List<Held> dueRenewals() {
        if (!fenced || closing || !admission.current()) return List.of();
        long now = nanoTime.getAsLong();
        // Earlier successful lease proofs take priority over subsequently renewed tails. This also
        // gives mid-pass arrivals their turn without interpreting the store's absolute deadlines.
        return held.values().stream().filter(state -> due(state, now))
                .sorted(Comparator.comparingLong((Held state) -> state.leaseDeadlineNanos)
                        .thenComparingLong(state -> state.leaseProofOrder)).toList();
    }

    private synchronized void renewIfDue(Held state, long queued) {
        long now = nanoTime.getAsLong();
        if (!closing && admission.current() && due(state, now)
                && held.get(state.claim.key().resourceId()) == state) {
            LOG.log(System.Logger.Level.DEBUG, "Pipeline renewal admitted {0}, waitMs={1}, remainingMs={2}",
                    state.claim.key().resourceId(), TimeUnit.NANOSECONDS.toMillis(now - queued),
                    TimeUnit.NANOSECONDS.toMillis(state.leaseDeadlineNanos - now));
            renew(state, now);
        }
    }

    private static boolean due(Held state, long now) {
        return state.claim != null && state.contacted && now - state.nextContactNanos >= 0;
    }

    /**
     * Whether the run {@code fence} names is still this member's to submit, asked of the store right before
     * submitting it. A start can outlast a lease, and the renewer that keeps the claim alive through it can
     * still lose it -- the store out of reach, the process paused, another member taking the pipeline over.
     * A run submitted over a claim this member no longer holds dies at its first write, and the member that
     * holds the pipeline then reads that death as the pipeline's.
     *
     * <p>Read rather than renewed: what is asked is who holds the run's generations now, and a renewal is
     * also refused for a claim granted under a topology revision a member joining has since moved, which
     * the same owner takes again on its next pass under the same generations. A later scoped claim is a
     * separate proof and cannot be borrowed here. A missing fence is valid only in an explicit uncoordinated fixture.
     */
    boolean proveExecution(ExecutionFence fence) {
        if (!fenced) return fence == null || fence.scope() == null;
        if (fence == null || closing || (businessScope == null ? fence.scope() != null
                : fence.scope() == null || !accepts(fence.scope())
                        || !owner.nodeId().equals(fence.ownerNodeId()) || !owner.bootId().equals(fence.ownerBootId()))) return false;
        WorkloadClaimFence expected = new WorkloadClaimFence(
                new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, fence.pipelineId()),
                owner, fence.claimGeneration(), fence.executionGeneration(),
                fence.scope() == null ? 0 : fence.topologyRevision());
        return proveClaim(fence.scope(), expected, fence.scope() != null);
    }

    /** Proves the exact admitted command, including a first claim whose execution counter is still zero. */
    boolean proveClaim(PipelineIntentScope scope, WorkloadClaimFence expected) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(expected, "expected");
        return proveClaim(scope, expected, true);
    }

    /** Reports against the original claim without holding the serialized actuation IO lock. */
    boolean proveObservationClaim(PipelineIntentScope scope, WorkloadClaimFence expected) {
        if (!admission.enter()) {
            return false;
        }
        try {
            return admission.current() && proveClaimOwned(scope, expected, true, false);
        } finally {
            admission.leave();
        }
    }

    /** Retries only the start's proof read, under the same admission deadline. */
    boolean proveStartClaim(PipelineIntentScope scope, WorkloadClaimFence expected) {
        return PipelineDiagnostics.measure(scope == null ? null : scope.pipelineId(), Phase.PIPELINE_CLAIM_PROOF,
                () -> admittedRead(() -> !processOwnsClaims()
                        ? proveClaimOwned(scope, expected, true, true)
                        : ownsSingleStart(scope, expected), () -> false));
    }

    boolean processOwnsClaims() {
        return nodeSession != null && io.tapstate.spi.store.ProcessNodeSession.owns(nodeSession);
    }

    /** The single OP already adopted this exact fence. Submission and store mutations keep their own fences. */
    private synchronized boolean ownsSingleStart(PipelineIntentScope scope, WorkloadClaimFence expected) {
        if (closing || scope == null || !accepts(scope) || !processOwnsClaims()
                || !membership.businessEligible()) return false;
        var local = held.get(scope.pipelineId());
        return local != null && scope.equals(local.scope) && !local.executionAdvancing && local.claim != null
                && expected.equals(WorkloadClaimFence.from(local.claim)) && membership.singleControlEligible(local.claim);
    }

    /** Captures a conservative original lease outside CONFIG; the returned guard performs no IO. */
    Optional<Runnable> evaluationProof(PipelineIntentScope scope, WorkloadClaimFence expected) {
        return PipelineDiagnostics.measure(scope == null ? null : scope.pipelineId(), Phase.PIPELINE_EVALUATION_PROOF,
                () -> admittedRead(()->evaluationProofOwned(scope,expected),Optional::empty));
    }
    private Optional<Runnable> evaluationProofOwned(PipelineIntentScope scope, WorkloadClaimFence expected) {
        Objects.requireNonNull(scope);Objects.requireNonNull(expected);
        if(!fenced||closing||!accepts(scope)||!expected.owner().equals(owner)
                ||expected.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION||!scope.pipelineId().equals(expected.key().resourceId()))return Optional.empty();
        WorkloadClaim original;
        synchronized(this){var local=held.get(scope.pipelineId());
            if(local==null||local.executionAdvancing||!scope.equals(local.scope)||local.claim==null
                    ||!WorkloadClaimFence.from(local.claim).equals(expected))return Optional.empty();
            original=local.claim;
        }
        long askedAt=nanoTime.getAsLong();
        var reading=membership.businessEligible()?claims.readPipeline(scope):claims.readActuationControl(scope,original);
        if(reading.isEmpty()||!WorkloadClaimFence.from(reading.orElseThrow().claim()).equals(expected))return Optional.empty();
        long budget=Math.min(Math.min(reading.orElseThrow().leaseRemaining().toNanos(),ttl.toNanos()),TimeUnit.SECONDS.toNanos(10));
        Runnable guard=()->{
            synchronized(this){var current=held.get(scope.pipelineId());long elapsed=nanoTime.getAsLong()-askedAt;
                if(closing||elapsed<0||elapsed>=budget||!accepts(scope)
                        ||!(membership.businessEligible()||membership.singleControlEligible(reading.orElseThrow().claim()))
                        ||current==null||current.executionAdvancing||!scope.equals(current.scope)||current.claim==null
                        ||!WorkloadClaimFence.from(current.claim).equals(expected))throw ThreeLayerStorePort.refused();}
        };
        try{guard.run();return Optional.of(guard);}catch(io.tapstate.core.common.TapstateException expired){return Optional.empty();}
    }

    private boolean proveClaim(PipelineIntentScope scope, WorkloadClaimFence expected, boolean compareTopology) {
        return PipelineDiagnostics.measure(expected.key().resourceId(), Phase.PIPELINE_CLAIM_PROOF,
                () -> admittedRead(()->proveClaimOwned(scope,expected,compareTopology,false),()->false));
    }
    private boolean proveClaimOwned(PipelineIntentScope scope, WorkloadClaimFence expected, boolean compareTopology,
            boolean retryTimeout) {
        if (!fenced || closing || (scope != null && !accepts(scope)) || !expected.owner().equals(owner)
                || expected.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !clusterId.equals(expected.key().clusterId())
                || (scope != null && !scope.pipelineId().equals(expected.key().resourceId()))) return false;
        long askedAt = nanoTime.getAsLong();
        Optional<WorkloadClaimReading> reading;
        for (int attempt = 0; ; attempt++) {
            try {
                reading = scope == null ? claims.read(expected.key()) : claims.readPipeline(scope);
                break;
            } catch (RuntimeException unavailable) {
                if (scope != null && !retryTimeout && retryableRenewal(unavailable)) {
                    if (unavailable instanceof TapstateException coded && coded.code() == IoError.STORE_UNAVAILABLE) throw coded;
                    throw new TapstateException(IoError.STORE_UNAVAILABLE,
                            Map.of("detail", "pipeline claim read timed out"), unavailable);
                }
                if (!retryTimeout || !claimIoTimedOut(unavailable)) return false;
                if (attempt != 0 || !admission.current()) {
                    throw new TapstateException(IoError.STORE_UNAVAILABLE,
                            Map.of("detail", "startup claim read timed out"), unavailable);
                }
            }
        }
        if (reading.isEmpty()) return false;
        if (closing || !membership.businessEligible()) return false;
        Duration remaining = reading.get().leaseRemaining();
        long bound = remaining.compareTo(ttl) < 0 ? remaining.toNanos() : ttl.toNanos();
        long checkedAt = nanoTime.getAsLong();
        Held state = held.get(expected.key().resourceId());
        WorkloadClaim local = state == null ? null : state.claim;
        WorkloadClaim claim = reading.get().claim();
        if (closing || bound <= 0 || checkedAt - askedAt >= bound || state == null
                || !Objects.equals(state.scope, scope) || local == null || state.executionAdvancing) return false;
        // Recheck the mutable ownership facts after comparing the store and local claims.
        return sameClaim(WorkloadClaimFence.from(local), expected, compareTopology)
                && sameClaim(WorkloadClaimFence.from(claim), expected, compareTopology)
                && held.get(expected.key().resourceId()) == state && state.claim == local && !state.executionAdvancing
                && !closing && membership.businessEligible();
    }

    private static boolean sameClaim(WorkloadClaimFence actual, WorkloadClaimFence expected, boolean compareTopology) {
        return actual.key().equals(expected.key()) && actual.owner().equals(expected.owner())
                && actual.claimGeneration() == expected.claimGeneration()
                && actual.executionGeneration() == expected.executionGeneration()
                && (!compareTopology || actual.topologyRevision() == expected.topologyRevision());
    }

    /** Whether a local capture still serves a pipeline this member holds; never acquires or renews a claim. */
    boolean isDriving(String pipelineId) {
        return admittedCall(()->isDrivingOwned(pipelineId),()->!closing&&businessScope==null);
    }
    private boolean isDrivingOwned(String pipelineId) {
        if (businessScope != null) return false;
        if (closing) {
            return false;
        }
        if (!fenced) {
            return true;
        }
        synchronized (this) {
            Held state = held.get(pipelineId);
            if (state != null && state.claim != null) {
                return true;
            }
        }
        WorkloadClaimKey key = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
        try {
            return claims.read(key).filter(WorkloadClaimReading::leased)
                    .map(WorkloadClaimReading::claim)
                    .filter(claim -> claim.owner().equals(owner))
                    .isPresent();
        } catch (RuntimeException unavailable) {
            // An unreadable pipeline claim proves no handoff. The capture still renews its own fenced claim.
            return true;
        }
    }

    boolean isDriving(PipelineIntentScope scope) {
        Objects.requireNonNull(scope,"scope");
        return admittedCall(()->isDrivingOwned(scope),()->!closing&&accepts(scope));
    }
    private boolean isDrivingOwned(PipelineIntentScope scope) {
        Objects.requireNonNull(scope, "scope");
        if (closing || !accepts(scope)) return false;
        synchronized (this) {
            Held state = held.get(scope.pipelineId());
            if (state != null && !scope.equals(state.scope)) return false;
            if (state != null && state.claim != null) return true;
        }
        try {
            return claims.readPipeline(scope).filter(WorkloadClaimReading::leased)
                    .map(WorkloadClaimReading::claim).filter(claim -> claim.owner().equals(owner)).isPresent();
        } catch (RuntimeException unavailable) {
            // Retaining a capture does not grant it work; its original capture claim still fences it.
            return true;
        }
    }

    /** One bounded read proof; it grants no mutation or engine operation. */
    final class ReportingExecution {
        private final PipelineIntentScope scope;
        private final WorkloadClaim expected;
        private final long askedAt;
        private final long remainingNanos;
        private ReportingExecution(PipelineIntentScope scope, WorkloadClaim expected, long askedAt, long remainingNanos) {
            this.scope = scope; this.expected = expected; this.askedAt = askedAt; this.remainingNanos = remainingNanos;
        }
        ExecutionFence execution() { return new ExecutionFence(scope, WorkloadClaimFence.from(expected)); }
        boolean current() {
            synchronized (PipelineActuationOwnership.this) {
                long elapsed = nanoTime.getAsLong() - askedAt;
                Held state = held.get(scope.pipelineId());
                return !closing && accepts(scope) && elapsed >= 0 && elapsed < remainingNanos
                        && membership.singleControlEligible(expected) && state != null && !state.executionAdvancing && scope.equals(state.scope)
                        && state.claim != null && WorkloadClaimFence.from(state.claim).equals(WorkloadClaimFence.from(expected))
                        && state.runExecutionGeneration == expected.executionGeneration()
                        && Set.of(owner.nodeId()).equals(state.runMembers);
            }
        }
    }

    Optional<ReportingExecution> reportingExecution(PipelineIntentScope scope) {
        return PipelineDiagnostics.measure(scope == null ? null : scope.pipelineId(), Phase.PIPELINE_REPORTING_PROOF,
                () -> admittedRead(()->reportingExecutionOwned(scope),Optional::empty));
    }
    private Optional<ReportingExecution> reportingExecutionOwned(PipelineIntentScope scope) {
        WorkloadClaim expected;
        synchronized (this) {
            Held state = held.get(scope.pipelineId());
            if (!fenced || businessScope == null || closing || !accepts(scope) || state == null || state.executionAdvancing
                    || !scope.equals(state.scope) || state.claim == null || state.claim.executionGeneration() < 1
                    || state.runExecutionGeneration != state.claim.executionGeneration()
                    || !Set.of(owner.nodeId()).equals(state.runMembers)) return Optional.empty();
            expected = state.claim;
        }
        long askedAt = nanoTime.getAsLong();
        var reading = claims.readActuationControl(scope, expected);
        if (reading.isEmpty()) return Optional.empty();
        Duration remaining = reading.orElseThrow().leaseRemaining();
        long budget = remaining.compareTo(ttl) < 0 ? remaining.toNanos() : ttl.toNanos();
        var proof = new ReportingExecution(scope, expected, askedAt, budget);
        return proof.current() ? Optional.of(proof) : Optional.empty();
    }

    /** The execution generation the claim this member holds for {@code pipelineId} carries, or zero. */
    synchronized long heldExecutionGeneration(String pipelineId) {
        Held state = fenced ? held.get(pipelineId) : null;
        return state == null || state.claim == null ? 0 : state.claim.executionGeneration();
    }

    /** The exact committed Pipeline lease already held on this member. This guard performs no store I/O. */
    boolean hasLocalWriterLease(ExecutionFence fence) {
        var scope = fence.scope();
        if (scope == null || !accepts(scope) || closing || !membership.businessEligible()) return false;
        var state = held.get(scope.pipelineId());
        if (state == null || !scope.equals(state.scope)) return false;
        var lease = state.scopedLease.get();
        if (lease == null || lease.lost() || nanoTime.getAsLong() - lease.deadline() >= 0) return false;
        var claim = lease.expected();
        var current = state.claim;
        return current != null && WorkloadClaimFence.from(current).equals(WorkloadClaimFence.from(claim))
                && claim.claimGeneration() == fence.claimGeneration()
                && claim.executionGeneration() == fence.executionGeneration()
                && claim.topologyRevision() == fence.topologyRevision()
                && claim.owner().nodeId().equals(fence.ownerNodeId())
                && claim.owner().bootId().equals(fence.ownerBootId())
                && !closing && held.get(scope.pipelineId()) == state && state.scopedLease.get() == lease;
    }

    /** A store-side proof for the named run, never substituting a replacement's generations. */
    synchronized WorkloadClaimFence completionFence(ExecutionFence run) {
        if (run == null) {
            return null;
        }
        Held current = held.get(run.pipelineId());
        long topology = current == null || current.claim == null ? 0L : current.claim.topologyRevision();
        return new WorkloadClaimFence(
                new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, run.pipelineId()),
                owner, run.claimGeneration(), run.executionGeneration(), topology);
    }

    /** Records FAILED once member loss is visible or a post-detection view confirms none. */
    void recordFailure(String pipelineId, long settlingNanos, long detectionWindowNanos) {
        admittedCall(()->{recordFailureOwned(pipelineId,settlingNanos,detectionWindowNanos);return null;},()->null);
    }
    private synchronized void recordFailureOwned(String pipelineId, long settlingNanos, long detectionWindowNanos) {
        if (businessScope == null) recordHeldFailure(pipelineId, settlingNanos, detectionWindowNanos);
    }

    void recordFailure(PipelineIntentScope scope,WorkloadClaimFence expected,long settlingNanos,long detectionWindowNanos) {
        admittedCall(()->{recordFailureOwned(scope,expected,settlingNanos,detectionWindowNanos);return null;},()->null);
    }
    private synchronized void recordFailureOwned(PipelineIntentScope scope, WorkloadClaimFence expected,
            long settlingNanos, long detectionWindowNanos) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(expected, "expected");
        Held state = held.get(scope.pipelineId());
        if (accepts(scope) && state != null && !state.executionAdvancing && scope.equals(state.scope) && state.claim != null
                && expected.equals(WorkloadClaimFence.from(state.claim))) {
            recordHeldFailure(scope.pipelineId(), settlingNanos, detectionWindowNanos);
        }
    }

    private void recordHeldFailure(String pipelineId, long settlingNanos, long detectionWindowNanos) {
        if (!fenced || closing) {
            return;
        }
        Held state = held.get(pipelineId);
        if (state == null || state.claim == null || state.claim.executionGeneration() == 0) {
            return;
        }
        // A prior version could advance the execution while leaving newer context fields untouched.
        // Such a claim carries no reliable failure order until this version allocates another run.
        if (state.claim.contextExecutionGeneration() != state.claim.executionGeneration()) {
            return;
        }
        if (state.claim.failureClaimGeneration() != 0) {
            return;
        }
        ClusterMembershipGate.VisibleSnapshot visible = membership.visibleSnapshot();
        observeMembership(state, visible.nodeIds());
        long now = nanoTime.getAsLong();
        boolean memberLoss = state.lostAMemberAtNanos != NEVER
                && now - (state.lostAMemberAtNanos + settlingNanos) < 0;
        // A new holder has no local run snapshot. The execution's durable members are the snapshot
        // it can use if its first sight of FAILED is after a handover.
        memberLoss |= !state.claim.executionNodeIds().isEmpty()
                && !visible.nodeIds().containsAll(state.claim.executionNodeIds());
        if (!memberLoss) {
            if (state.failureObservedAtNanos == NEVER) {
                state.failureObservedAtNanos = now;
                return;
            }
            // A new publication can still carry the old member set until heartbeat failure detection.
            // Wait for that window, then require another publication so the verdict uses a later view.
            if (now - state.failureObservedAtNanos < detectionWindowNanos) {
                return;
            }
            if (state.visibilityRevisionAtDetectionWindow == NEVER) {
                state.visibilityRevisionAtDetectionWindow = visible.revision();
                return;
            }
            if (visible.revision() <= state.visibilityRevisionAtDetectionWindow) {
                return;
            }
        }
        Optional<WorkloadClaim> recorded;
        try {
            recorded = claims.recordExecutionFailure(state.claim, memberLoss);
        } catch (RuntimeException unreachable) {
            recorded = Optional.empty();
        }
        if (recorded.isEmpty()) {
            state.claim = null;
            return;
        }
        state.claim = recorded.get();
        if (state.scope != null) advanceScopedProof(state, state.claim);
    }

    /** A shutting-down member must leave its dying run unclassified for the next holder to recover. */
    @EventListener(ContextClosedEvent.class)
    void stopForShutdown() {
        closing = true;
        admission.stop();
    }
    void awaitStopped(long deadline) {
        admission.await(deadline);
        if(claims!=null)claims.awaitStopped(deadline);
    }

    /**
     * What each committed member is to the run this member last submitted for {@code pipelineId} -- the
     * ones it was planned over, and the ones that joined afterwards and are therefore carrying none of
     * it. Empty when this member is driving no run of that pipeline, or when nothing is committed:
     * those are "this member cannot say", which is not the same answer as "nobody is waiting".
     */
    synchronized Map<String, MemberRunState> runMembership(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Held state = fenced ? held.get(pipelineId) : null;
        ClusterMembership current = fenced ? membership.committed() : null;
        if (state == null || state.runMembers == null || current == null) {
            return Map.of();
        }
        Map<String, MemberRunState> byNode = new LinkedHashMap<>();
        for (String nodeId : current.activeNodeIds()) {
            byNode.put(nodeId, state.runMembers.contains(nodeId)
                    ? MemberRunState.PARTICIPATING
                    : MemberRunState.AWAITING_REBALANCE);
        }
        return Map.copyOf(byNode);
    }

    /**
     * Records an absence while it can still be seen; a member back before anybody asked still left.
     *
     * <p>Taken again on every look that still finds it missing, so a member that has not come back never
     * stops counting as gone however long it stays away. The moment only stops moving once the run's
     * members are all present again - which is where the caller's stretch takes over.
     */
    private void observeMembership(Held state) {
        observeMembership(state, membership.visibleNodeIds());
    }

    private void observeMembership(Held state, Set<String> visibleNodeIds) {
        if (state.runMembers == null) {
            return;
        }
        // Against the members in sight, not the committed set: that set only ever grows, so a member
        // killed under a run this member is still driving never leaves it, and a comparison against it
        // would call every such death the pipeline's own. Only a run somebody else left behind was ever
        // picked up that way, which is why a cluster losing its driver recovered and one losing any
        // other member of the run stayed failed.
        if (!visibleNodeIds.containsAll(state.runMembers)) {
            state.lostAMemberAtNanos = nanoTime.getAsLong();
        }
    }

    /**
     * The members a run submitted now is planned over: the ones in sight. The engine plans a run over
     * the members it can see, and a committed member that is out of sight is not one of them. The
     * committed set stands in only before this member has looked at all.
     */
    private Set<String> plannedOver(ClusterMembership committed) {
        Set<String> inSight = membership.visibleNodeIds();
        return inSight.isEmpty() ? committed.activeNodeIds() : inSight;
    }

    /**
     * Releases the claims for pipelines that are no longer desired, so a deleted pipeline does not keep
     * this member named as its driver until the lease runs out. Releasing expires the lease and leaves the
     * generation where it is, so the next holder still moves forward from it.
     */
    void retain(Collection<String> pipelineIds) {
        if (businessScope == null) {
            admittedCall(() -> { retainOwned(pipelineIds); return null; }, () -> null);
            return;
        }
        // A later tick retries cleanup while a lifecycle command owns the ordinary I/O gate.
        if (!admission.enter()) return;
        try {
            if (!ioAdmission.tryLock()) return;
            try {
                if (admission.current()) {
                    List<PendingRelease> pending;
                    synchronized (this) {
                        pending = pendingReleases.values().stream()
                                .filter(item -> item.terminal == null || !pipelineIds.contains(item.scope.pipelineId())).toList();
                    }
                    pending.forEach(this::releasePending);
                    retainOwned(pipelineIds);
                }
            } finally {
                ioAdmission.unlock();
            }
        } finally {
            admission.leave();
        }
    }
    private synchronized void retainOwned(Collection<String> pipelineIds) {
        if (!fenced) {
            return;
        }
        Iterator<Map.Entry<String, Held>> entries = held.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Held> entry = entries.next();
            if (pipelineIds.contains(entry.getKey()) || controls.containsKey(entry.getKey())
                    || entry.getValue().executionAdvancing) {
                continue;
            }
            WorkloadClaim claim = entry.getValue().claim;
            if (claim != null) {
                claims.release(claim);
            }
            entries.remove();
        }
    }

    /** Legacy unscoped ownership uses the ordinary admission gate. */
    private Permit renew(Held state, long now) {
        WorkloadClaim original = state.claim;
        Optional<WorkloadClaim> renewed;
        try {
            renewed = membership.businessEligible() ? claims.renew(original, ttl)
                    : claims.renewActuationControl(original, ttl);
        } catch (RuntimeException failure) {
            renewed = Optional.empty();
        }
        state.contacted = true;
        state.nextContactNanos = now + renewIntervalNanos;
        ClusterMembership current = membership.committed();
        boolean qualified = membership.businessEligible()
                ? current != null && current.clusterId().equals(original.key().clusterId())
                        && current.revision() == original.topologyRevision()
                : membership.singleControlEligible(original);
        if (closing || held.get(original.key().resourceId()) != state || state.claim != original)
            return Permit.denied();
        if (!qualified || renewed.isEmpty()
                || !WorkloadClaimFence.from(original).equals(WorkloadClaimFence.from(renewed.orElseThrow()))) {
            state.claim = null;
            return Permit.denied();
        }
        recordLeaseProof(state, renewed.orElseThrow(), now);
        return new Permit(true, state.claim);
    }

    private static boolean claimIoTimedOut(RuntimeException failure) {
        if (!(failure instanceof TapstateException coded)) return false;
        if (coded.code() == IoError.WORKLOAD_CLAIM_FENCED) return coded.getCause() instanceof TimeoutException;
        if (coded.code() != IoError.STORE_UNAVAILABLE || !(coded.args().get("detail") instanceof String detail)) return false;
        return switch (detail.split(" ", 2)[0]) {
            case "MongoTimeoutException", "MongoOperationTimeoutException", "MongoExecutionTimeoutException", "MongoSocketReadTimeoutException" -> true;
            default -> false;
        };
    }

    private synchronized void recordLeaseProof(Held state, WorkloadClaim claim, long startedAt) {
        state.claim = claim;
        // Mongo computes the TTL after this call starts. Starting the local window earlier is conservative.
        state.leaseDeadlineNanos = startedAt + TimeUnit.MILLISECONDS.toNanos(ttl.toMillis());
        state.leaseProofOrder = ++leaseProofSequence;
        if (state.scope != null && nodeSession == null) state.scopedLease.set(new ScopedLease(claim, state.leaseDeadlineNanos,
                startedAt + renewIntervalNanos, 0, false, false, null));
    }

    private Permit acquire(String pipelineId, Held state, long now) {
        state.contacted = true;
        state.nextContactNanos = now + renewIntervalNanos;
        var planned = membership.committed();
        if (planned == null) return Permit.denied();
        var grant = requestClaim(pipelineId, state.scope, planned.revision());
        if (grant.isEmpty()) return Permit.denied();
        var claim = grant.orElseThrow();
        if (!canAdopt(claim, planned)) {
            if (state.scope != null) registerUnadoptedGrant(state.scope, claim);
            return Permit.denied();
        }
        adoptAcquisition(state, claim, now);
        return new Permit(true, claim);
    }

    private Optional<WorkloadClaim> requestClaim(String pipelineId, PipelineIntentScope scope, long revision) {
        return requestClaim(pipelineId, scope, revision, false);
    }

    private Optional<WorkloadClaim> requestClaim(String pipelineId, PipelineIntentScope scope, long revision, boolean initialStart) {
        var key = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
        Optional<WorkloadClaimAttempt> attempt;
        try {
            if (scope == null) attempt = claims.acquire(key, owner, revision, ttl);
            else if (nodeSession != null) attempt = claims.acquireNodeBackedPipeline(scope, nodeSession, revision, initialStart);
            else attempt = claims.acquirePipeline(scope, owner, revision, ttl, initialStart);
        } catch (RuntimeException unreachable) {
            return Optional.empty();
        }
        return attempt.filter(WorkloadClaimAttempt::acquired).map(WorkloadClaimAttempt::claim)
                .filter(claim -> claim.key().equals(key) && claim.owner().equals(owner));
    }

    private boolean canAdopt(WorkloadClaim claim, ClusterMembership planned) {
        return !closing && admission.current() && claim.topologyRevision() == planned.revision()
                && planned.equals(membership.committed()) && membership.businessEligible();
    }

    private void adoptAcquisition(Held state, WorkloadClaim claim, long now) {
        state.contacted = true;
        state.nextContactNanos = now + renewIntervalNanos;
        recordLeaseProof(state, claim, now);
        state.failureObservedAtNanos = NEVER;
        state.visibilityRevisionAtDetectionWindow = NEVER;
        if (claim.executionGeneration() != state.runExecutionGeneration) {
            // The run this claim carries is not the one this member last submitted: somebody else drove the
            // pipeline while this member did not hold it, and replaced that run. What this member planned its
            // own run over says nothing about the run it holds now -- a member that was not in sight when it
            // planned would never be missed from it. Without it, this is the inherited run it is.
            state.runMembers = null;
        }
    }
}
