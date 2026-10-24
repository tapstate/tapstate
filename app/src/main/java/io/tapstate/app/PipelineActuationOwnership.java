package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaimFence;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
 * <p>Two threads use the claims. The convergence pass acquires them, advances them for each run and judges
 * the runs under them; {@link #renewDue()}, on a renewer of its own, keeps every held claim's lease running.
 * The pass is one thread over every pipeline this member drives, and a single start can hold it for longer
 * than a lease: renewed only from the pass, every claim the member held ran out behind one slow start and
 * was taken over from a member that was still there. Everything here synchronizes on this object, so a
 * renewal always carries the claim the pass last moved -- the store matches a renewal on the claim's exact
 * generations, and one sent with a claim the pass has just advanced past would read as the claim lost. The
 * same lock keeps a close that starts first from being followed by a durable verdict that calls its dying
 * run independent.
 */
final class PipelineActuationOwnership {

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
    private final ClusterMembershipGate membership;
    private final ClusterWorkloadClaims claims;
    private final Duration ttl;
    private final long renewIntervalNanos;
    private final LongSupplier nanoTime;
    private final boolean fenced;
    private final Map<String, Held> held = new HashMap<>();
    private final Map<String, Control> controls = new HashMap<>();

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
    final class Pass implements AutoCloseable {
        private final String pipeline;
        private Pass(String pipeline) { this.pipeline = pipeline; }
        @Override public void close() {
            synchronized (PipelineActuationOwnership.this) { passes.remove(pipeline, this); }
        }
    }
    synchronized Pass enterPass(PipelineIntentScope scope) {
        if (closing || !accepts(scope) || controls.containsKey(scope.pipelineId()) || passes.containsKey(scope.pipelineId())) throw ThreeLayerStorePort.refused();
        var pass = new Pass(scope.pipelineId()); passes.put(scope.pipelineId(), pass); return pass;
    }
    synchronized Control control(PipelineIntentScope scope) {
        if (passes.containsKey(scope.pipelineId())) throw ThreeLayerStorePort.refused();
        var admitted = permit(scope);
        if (!admitted.granted() || admitted.claim() == null) throw ThreeLayerStorePort.refused();
        var command = new Control(scope, WorkloadClaimFence.from(admitted.claim()));
        controls.put(scope.pipelineId(), command); return command;
    }
    /** Holds a proven ended tuple locally without acquiring another durable tenure or execution permission. */
    synchronized Control retiredControl(PipelineIntentScope scope,WorkloadClaimFence original) {
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
        if(closing || !fenced || !accepts(scope) || original==null || !membership.businessEligible()
                || controls.containsKey(scope.pipelineId()) || passes.containsKey(scope.pipelineId())
                || original.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION
                || !scope.clusterId().equals(original.key().clusterId()) || !scope.pipelineId().equals(original.key().resourceId()))
            throw ThreeLayerStorePort.refused();
    }
    synchronized Control unpreparedControl(PipelineIntentScope scope) {
        if (closing || !accepts(scope) || controls.containsKey(scope.pipelineId()) || passes.containsKey(scope.pipelineId()) || !membership.businessEligible()) throw ThreeLayerStorePort.refused();
        var command = new Control(scope, null); controls.put(scope.pipelineId(), command); return command;
    }
    private volatile boolean closing;

    /** One pipeline's local view: the claim this member proved, and when the next round trip is due. */
    private static final class Held {
        private final PipelineIntentScope scope;
        /** Highest candidate already probed; lower retries share its bounded contact interval. */
        private PipelineIntentScope probedScope;
        private long nextScopeProbeNanos;
        private WorkloadClaim claim;

        private Held(PipelineIntentScope scope) {
            this.scope = scope;
        }
        private long nextContactNanos;
        private boolean contacted;
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
        this(clusterId, null, owner, membership, claims, ttl, renewInterval, nanoTime);
    }

    static PipelineActuationOwnership scoped(CacheBusinessScope scope, WorkloadOwner owner,
            ClusterMembershipGate membership, ClusterWorkloadClaims claims, Duration ttl,
            Duration renewInterval, LongSupplier nanoTime) {
        Objects.requireNonNull(scope, "scope");
        return new PipelineActuationOwnership(scope.clusterId(), scope, owner, membership, claims,
                ttl, renewInterval, nanoTime);
    }

    private PipelineActuationOwnership(String clusterId, CacheBusinessScope businessScope,
            WorkloadOwner owner, ClusterMembershipGate membership, ClusterWorkloadClaims claims,
            Duration ttl, Duration renewInterval, LongSupplier nanoTime) {
        this.businessScope = businessScope;
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.membership = Objects.requireNonNull(membership, "membership");
        this.claims = Objects.requireNonNull(claims, "claims");
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
    synchronized Permit permit(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing || businessScope != null) return Permit.denied();
        if (!fenced) return Permit.unfenced();
        Held state = held.computeIfAbsent(pipelineId, id -> new Held(null));
        return permit(pipelineId, state);
    }

    /** A new physical scope must be admitted by the store before it can replace the local holder. */
    synchronized Permit permit(PipelineIntentScope scope) {
        Objects.requireNonNull(scope, "scope");
        if (closing || !accepts(scope) || controls.containsKey(scope.pipelineId())) return Permit.denied();
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
        // Every pass, not every round trip: this reads a local reference the membership reconciler
        // publishes into, so it costs nothing, and the shorter the interval between looks the shorter
        // the absence that can pass unseen between two of them.
        observeMembership(state);
        long now = nanoTime.getAsLong();
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
        if (accepts(scope) && state != null && scope.equals(state.scope) && state.claim != null
                && expected.equals(WorkloadClaimFence.from(state.claim))) {
            state.startRefusedAtGeneration = state.claim.executionGeneration();
        }
    }

    synchronized Execution beginExecution(String pipelineId) {
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
    synchronized Execution beginExecution(String pipelineId, WorkloadClaimFence expected) {
        Objects.requireNonNull(pipelineId, "pipelineId"); Objects.requireNonNull(expected, "expected");
        return businessScope == null ? beginExecution(pipelineId, null, expected) : Execution.refused();
    }

    synchronized Execution beginExecution(PipelineIntentScope scope, WorkloadClaimFence expected) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(expected, "expected");
        return accepts(scope) ? beginExecution(scope.pipelineId(), scope, expected) : Execution.refused();
    }

    private Execution beginExecution(String pipelineId, PipelineIntentScope scope, WorkloadClaimFence expected) {
        if (closing || !fenced || controls.containsKey(pipelineId)) return Execution.refused();
        Held state = held.get(pipelineId);
        if (state == null || !Objects.equals(scope, state.scope) || state.claim == null
                || !expected.equals(WorkloadClaimFence.from(state.claim))) return Execution.refused();
        ClusterMembership planned = membership.committed();
        Set<String> runMembers = planned == null ? Set.of() : plannedOver(planned);
        Optional<WorkloadClaim> advanced;
        try {
            if (scope != null) {
                if (planned == null || !membership.businessEligible()
                        || planned.revision() < state.claim.topologyRevision()) return Execution.refused();
                if (state.claim.topologyRevision() != planned.revision()) {
                    var original = state.claim;
                    var rebased = claims.acquirePipeline(scope, owner, planned.revision(), ttl);
                    if (rebased.isEmpty() || !rebased.orElseThrow().acquired()) return Execution.refused();
                    var actual = rebased.orElseThrow().claim();
                    if (!actual.key().equals(original.key()) || !actual.owner().equals(original.owner())
                            || actual.claimGeneration() != original.claimGeneration()
                            || actual.executionGeneration() != original.executionGeneration()
                            || actual.topologyRevision() != planned.revision()) {
                        state.claim = null; return Execution.refused();
                    }
                    state.claim = actual;
                }
                if (!planned.equals(membership.committed()) || !membership.businessEligible()) {
                    state.claim = null; return Execution.refused();
                }
            }
            // Scoped running claims retain their original topology until the caller has stopped the old run.
            advanced = claims.advanceExecution(state.claim, state.claim.topologyRevision(), runMembers);
        } catch (RuntimeException unreachable) {
            advanced = Optional.empty();
        }
        if (advanced.isEmpty()) {
            state.claim = null;
            return Execution.refused();
        }
        state.claim = advanced.get();
        if (scope != null && (!planned.equals(membership.committed()) || !membership.businessEligible())) {
            state.claim = null; return Execution.refused();
        }
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
     * A claim the store will not renew is dropped here exactly as the pass would drop it, and the pass
     * takes it from there.
     */
    synchronized void renewDue() {
        if (!fenced || closing) {
            return;
        }
        long now = nanoTime.getAsLong();
        for (Held state : held.values()) {
            if (state.claim != null && state.contacted && now - state.nextContactNanos >= 0) {
                renew(state, now);
            }
        }
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

    private boolean proveClaim(PipelineIntentScope scope, WorkloadClaimFence expected, boolean compareTopology) {
        if (!fenced || closing || (scope != null && !accepts(scope)) || !expected.owner().equals(owner)
                || expected.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !clusterId.equals(expected.key().clusterId())
                || (scope != null && !scope.pipelineId().equals(expected.key().resourceId()))) return false;
        long askedAt = nanoTime.getAsLong();
        Optional<WorkloadClaimReading> reading;
        try {
            reading = scope == null ? claims.read(expected.key()) : claims.readPipeline(scope);
        } catch (RuntimeException unreachable) {
            return false;
        }
        if (reading.isEmpty()) return false;
        synchronized (this) {
            if (closing || !membership.businessEligible()) return false;
            Duration remaining = reading.get().leaseRemaining();
            long bound = remaining.compareTo(ttl) < 0 ? remaining.toNanos() : ttl.toNanos();
            long checkedAt = nanoTime.getAsLong();
            Held state = held.get(expected.key().resourceId());
            WorkloadClaim claim = reading.get().claim();
            return !closing && bound > 0 && checkedAt - askedAt < bound
                    && state != null && Objects.equals(state.scope, scope) && state.claim != null
                    && sameClaim(WorkloadClaimFence.from(state.claim), expected, compareTopology)
                    && sameClaim(WorkloadClaimFence.from(claim), expected, compareTopology);
        }
    }

    private static boolean sameClaim(WorkloadClaimFence actual, WorkloadClaimFence expected, boolean compareTopology) {
        return actual.key().equals(expected.key()) && actual.owner().equals(expected.owner())
                && actual.claimGeneration() == expected.claimGeneration()
                && actual.executionGeneration() == expected.executionGeneration()
                && (!compareTopology || actual.topologyRevision() == expected.topologyRevision());
    }

    /** Whether a local capture still serves a pipeline this member holds; never acquires or renews a claim. */
    boolean isDriving(String pipelineId) {
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
                        && membership.singleControlEligible(expected) && state != null && scope.equals(state.scope)
                        && state.claim != null && WorkloadClaimFence.from(state.claim).equals(WorkloadClaimFence.from(expected))
                        && state.runExecutionGeneration == expected.executionGeneration()
                        && Set.of(owner.nodeId()).equals(state.runMembers);
            }
        }
    }

    Optional<ReportingExecution> reportingExecution(PipelineIntentScope scope) {
        WorkloadClaim expected;
        synchronized (this) {
            Held state = held.get(scope.pipelineId());
            if (!fenced || businessScope == null || closing || !accepts(scope) || state == null
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
    synchronized void recordFailure(String pipelineId, long settlingNanos, long detectionWindowNanos) {
        if (businessScope == null) recordHeldFailure(pipelineId, settlingNanos, detectionWindowNanos);
    }

    synchronized void recordFailure(PipelineIntentScope scope, WorkloadClaimFence expected,
            long settlingNanos, long detectionWindowNanos) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(expected, "expected");
        Held state = held.get(scope.pipelineId());
        if (accepts(scope) && state != null && scope.equals(state.scope) && state.claim != null
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
    }

    /** A shutting-down member must leave its dying run unclassified for the next holder to recover. */
    @EventListener(ContextClosedEvent.class)
    synchronized void stopForShutdown() {
        closing = true;
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
    synchronized void retain(Collection<String> pipelineIds) {
        if (!fenced) {
            return;
        }
        Iterator<Map.Entry<String, Held>> entries = held.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Held> entry = entries.next();
            if (pipelineIds.contains(entry.getKey()) || controls.containsKey(entry.getKey())) {
                continue;
            }
            WorkloadClaim claim = entry.getValue().claim;
            if (claim != null) {
                claims.release(claim);
            }
            entries.remove();
        }
    }

    private Permit renew(Held state, long now) {
        Optional<WorkloadClaim> renewed;
        try {
            renewed = membership.businessEligible()
                    ? state.scope == null ? claims.renew(state.claim, ttl) : claims.renewScopedActuation(state.claim, ttl)
                    : claims.renewActuationControl(state.claim, ttl);
        } catch (RuntimeException unreachable) {
            // The coordination store is the only thing that can say this member still owns the pipeline,
            // and it did not answer. Unproved is the same as lost here: the alternative is to keep driving
            // on a claim that may already belong to someone else.
            renewed = Optional.empty();
        }
        state.contacted = true;
        state.nextContactNanos = now + renewIntervalNanos;
        if (renewed.isEmpty()) {
            state.claim = null;
            return Permit.denied();
        }
        state.claim = renewed.get();
        return new Permit(true, state.claim);
    }

    private Permit acquire(String pipelineId, Held state, long now) {
        state.contacted = true;
        state.nextContactNanos = now + renewIntervalNanos;
        ClusterMembership current = membership.committed();
        if (current == null) {
            return Permit.denied();
        }
        WorkloadClaimKey key =
                new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
        Optional<WorkloadClaimAttempt> attempt;
        try {
            attempt = state.scope == null ? claims.acquire(key, owner, current.revision(), ttl)
                    : claims.acquirePipeline(state.scope, owner, current.revision(), ttl);
        } catch (RuntimeException unreachable) {
            return Permit.denied();
        }
        if (attempt.isEmpty() || !attempt.get().acquired() || !attempt.get().claim().owner().equals(owner)
                || !attempt.get().claim().key().equals(key)) {
            return Permit.denied();
        }
        state.claim = attempt.get().claim();
        state.failureObservedAtNanos = NEVER;
        state.visibilityRevisionAtDetectionWindow = NEVER;
        if (state.claim.executionGeneration() != state.runExecutionGeneration) {
            // The run this claim carries is not the one this member last submitted: somebody else drove the
            // pipeline while this member did not hold it, and replaced that run. What this member planned its
            // own run over says nothing about the run it holds now -- a member that was not in sight when it
            // planned would never be missed from it. Without it, this is the inherited run it is.
            state.runMembers = null;
        }
        return new Permit(true, state.claim);
    }
}
