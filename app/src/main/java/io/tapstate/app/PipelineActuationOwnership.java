package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.SuccessorAdmission;
import io.tapstate.spi.store.WorkloadClaimFence;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
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
 * <p>The scheduler checks permits while lifecycle workers begin runs. Each pipeline's held state is
 * guarded by its own lock. A busy lock makes the scheduler retry that pipeline on its next pass, so a
 * slow allocation cannot hold up checks for another pipeline.
 * <p>Failure recording and shutdown serialize their boundary so closing first prevents a later verdict.
 */
final class PipelineActuationOwnership {

    /** Whether this member may drive a pipeline, has lost it, or must retry after an in-flight advance. */
    record Permit(Decision decision, WorkloadClaim claim) {

        enum Decision {
            GRANTED,
            DENIED,
            RETRY
        }

        Permit {
            Objects.requireNonNull(decision, "decision");
            if (decision != Decision.GRANTED && claim != null) {
                throw new IllegalArgumentException("a non-granted permit cannot carry a claim");
            }
        }

        boolean granted() {
            return decision == Decision.GRANTED;
        }

        boolean retry() {
            return decision == Decision.RETRY;
        }

        static Permit unfenced() {
            return new Permit(Decision.GRANTED, null);
        }

        static Permit granted(WorkloadClaim claim) {
            return new Permit(Decision.GRANTED, Objects.requireNonNull(claim, "claim"));
        }

        static Permit denied() {
            return new Permit(Decision.DENIED, null);
        }

        static Permit busy() {
            return new Permit(Decision.RETRY, null);
        }
    }

    private final String clusterId;
    private final WorkloadOwner owner;
    private final ClusterMembershipGate membership;
    private final ClusterWorkloadClaims claims;
    private final ExecutionGenerationStore generations;
    private final Duration ttl;
    private final long renewIntervalNanos;
    private final LongSupplier nanoTime;
    private final boolean fenced;
    private final Map<String, Held> held = new ConcurrentHashMap<>();
    private volatile boolean closing;

    /** One pipeline's local view: the claim this member proved, and when the next round trip is due. */
    private static final class Held {
        private final ReentrantLock lock = new ReentrantLock();
        private WorkloadClaim claim;
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
        this.owner = null;
        this.membership = null;
        this.claims = null;
        this.generations = null;
        this.ttl = Duration.ZERO;
        this.renewIntervalNanos = 0;
        this.nanoTime = System::nanoTime;
        this.fenced = false;
    }

    /** For convergence tests with a stand-in actuator; submission refuses without a durable store. */
    static PipelineActuationOwnership single() {
        return new PipelineActuationOwnership();
    }

    /** Single-node ownership needs no lease, but every submitted run still needs a durable generation. */
    static PipelineActuationOwnership single(String clusterId, ExecutionGenerationStore generations) {
        return new PipelineActuationOwnership(clusterId, generations);
    }

    private PipelineActuationOwnership(String clusterId, ExecutionGenerationStore generations) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.owner = null;
        this.membership = null;
        this.claims = null;
        this.generations = Objects.requireNonNull(generations, "generations");
        this.ttl = Duration.ZERO;
        this.renewIntervalNanos = 0;
        this.nanoTime = System::nanoTime;
        this.fenced = false;
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
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.membership = Objects.requireNonNull(membership, "membership");
        this.claims = Objects.requireNonNull(claims, "claims");
        this.generations = null;
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
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing) {
            return Permit.denied();
        }
        if (!fenced) {
            return Permit.unfenced();
        }
        Held state = held.computeIfAbsent(pipelineId, id -> new Held());
        if (!state.lock.tryLock()) {
            return Permit.busy();
        }
        try {
            if (held.get(pipelineId) != state) {
                return Permit.denied();
            }
            return permitHeld(pipelineId, state);
        } finally {
            state.lock.unlock();
        }
    }

    private Permit permitHeld(String pipelineId, Held state) {
        // Every pass, not every round trip: this reads a local reference the membership reconciler
        // publishes into, so it costs nothing, and the shorter the interval between looks the shorter
        // the absence that can pass unseen between two of them.
        observeMembership(state);
        long now = nanoTime.getAsLong();
        boolean due = !state.contacted || now - state.nextContactNanos >= 0;
        if (state.claim != null && !due) {
            return Permit.granted(state.claim);
        }
        if (state.claim != null) {
            return renew(state, now);
        }
        // Not the driver: probe for a claim its holder may have let expire, but no more often than a
        // holder renews. A probe every tick would put one store round trip per pipeline per second on
        // every member that is not driving anything.
        return due ? acquire(pipelineId, state, now) : Permit.denied();
    }

    /** Whether a run may be submitted, and its durable generation identity. */
    record Execution(boolean allowed, ExecutionFence fence, Set<String> executionNodeIds,
            Optional<WorkloadClaim> admittedClaim, Long topologyRevision) {

        Execution {
            executionNodeIds = Set.copyOf(executionNodeIds);
            Objects.requireNonNull(admittedClaim, "admittedClaim");
            if (allowed && fence == null) {
                throw new IllegalArgumentException("an allowed execution requires a fence");
            }
            if (admittedClaim.isPresent()) {
                WorkloadClaim claim = admittedClaim.orElseThrow();
                if (!allowed || claim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                        || !claim.key().resourceId().equals(fence.pipelineId())
                        || claim.claimGeneration() != fence.claimGeneration()
                        || claim.executionGeneration() != fence.executionGeneration()
                        || !claim.executionNodeIds().equals(executionNodeIds)
                        || !Objects.equals(topologyRevision, claim.topologyRevision())) {
                    throw new IllegalArgumentException("an admission receipt must match its factual execution fence");
                }
            }
        }

        Execution(boolean allowed, ExecutionFence fence, Set<String> executionNodeIds,
                Optional<WorkloadClaim> admittedClaim) {
            this(allowed, fence, executionNodeIds, admittedClaim,
                    admittedClaim.map(WorkloadClaim::topologyRevision).orElse(null));
        }

        Execution(boolean allowed, ExecutionFence fence, Long topologyRevision) {
            this(allowed, fence, Set.of(), Optional.empty(), topologyRevision);
        }

        Execution(boolean allowed, ExecutionFence fence, Set<String> executionNodeIds) {
            this(allowed, fence, executionNodeIds, Optional.empty());
        }

        Execution(boolean allowed, ExecutionFence fence) { this(allowed, fence, Set.of()); }

        static Execution refused() {
            return new Execution(false, null, Set.of(), Optional.empty(), null);
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
    void startRefusedBeforeItsRun(String pipelineId) {
        Held state = held.get(Objects.requireNonNull(pipelineId, "pipelineId"));
        if (state == null) { return; }
        state.lock.lock();
        try {
            if (!closing && held.get(pipelineId) == state && state.claim != null) {
                state.startRefusedAtGeneration = state.claim.executionGeneration();
            }
        } finally { state.lock.unlock(); }
    }

    Execution beginExecution(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing) {
            return Execution.refused();
        }
        if (!fenced) {
            if (generations == null) {
                throw new IllegalStateException("standalone execution has no durable generation store");
            }
            OptionalLong advanced;
            try {
                advanced = generations.advanceStandalone(clusterId, pipelineId);
            } catch (TapstateException failure) {
                throw codeUnavailableStore(pipelineId, failure);
            }
            if (advanced.isEmpty()) {
                throw generationUnavailable(pipelineId, null);
            }
            long generation = advanced.getAsLong();
            if (generation < 1) {
                throw new IllegalStateException("the execution store returned a nonpositive generation");
            }
            return new Execution(true, new ExecutionFence(pipelineId, 0, generation));
        }
        Held state = held.get(pipelineId);
        if (state == null) {
            return Execution.refused();
        }
        state.lock.lock();
        try {
            if (closing || held.get(pipelineId) != state || state.claim == null) {
                return Execution.refused();
            }
            return beginUnderClaim(pipelineId, state);
        } finally {
            state.lock.unlock();
        }
    }

    String clusterId() { return clusterId; }

    /** Installs the already advanced authority returned by the atomic handoff admission, without advancing again. */
    Execution adoptAdmission(SuccessorAdmission admission) {
        Objects.requireNonNull(admission, "admission");
        var marker = admission.reservation();
        StopAuthority authority = marker.writerAuthority();
        if (authority == null || !clusterId.equals(authority.clusterId())
                || marker.successor() == null
                || authority.executionGeneration() != marker.successor().scope().executionGeneration()) {
            throw new IllegalStateException("atomic admission returned an inconsistent execution authority");
        }
        if (!fenced) {
            if (!authority.standalone() || admission.advancedClaim().isPresent()) {
                throw new IllegalStateException("standalone admission returned a lease");
            }
            return new Execution(true, new ExecutionFence(marker.pipelineId(), 0, authority.executionGeneration()));
        }
        WorkloadClaim advanced = admission.advancedClaim()
                .orElseThrow(() -> new IllegalStateException("cluster admission returned no advanced claim"));
        if (!owner.equals(advanced.owner())
                || !authority.equals(StopAuthority.claimed(WorkloadClaimFence.from(advanced)))
                || advanced.contextExecutionGeneration() != advanced.executionGeneration()
                || advanced.executionNodeIds().isEmpty()) {
            return Execution.refused();
        }
        Held state = held.computeIfAbsent(marker.pipelineId(), id -> new Held());
        state.lock.lock();
        try {
            if (closing || held.get(marker.pipelineId()) != state || !membership.businessEligible()) {
                return Execution.refused();
            }
            ClusterMembership planned = membership.committed();
            if (planned == null || planned.revision() != advanced.topologyRevision()
                    || state.claim != null && (state.claim.claimGeneration() > advanced.claimGeneration()
                            || state.claim.executionGeneration() > advanced.executionGeneration())) {
                return Execution.refused();
            }
            state.claim = advanced;
            state.runMembers = Set.copyOf(advanced.executionNodeIds());
            state.runExecutionGeneration = advanced.executionGeneration();
            state.failureObservedAtNanos = NEVER;
            state.visibilityRevisionAtDetectionWindow = NEVER;
            // Prove the returned lease again before native submission, including time spent preparing the DAG.
            state.contacted = true;
            state.nextContactNanos = nanoTime.getAsLong();
            return new Execution(true, new ExecutionFence(marker.pipelineId(), advanced.claimGeneration(),
                    advanced.executionGeneration()), advanced.executionNodeIds(), Optional.of(advanced));
        } finally {
            state.lock.unlock();
        }
    }

    /** Captures the same factual member snapshot as ordinary admission under its exact current writer. */
    Optional<Set<String>> plannedExecutionMembers(String pipelineId, StopAuthority writer) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing || writer == null || !clusterId.equals(writer.clusterId())) { return Optional.empty(); }
        if (!fenced) {
            return writer.standalone() ? Optional.of(Set.of()) : Optional.empty();
        }
        Held state = held.get(pipelineId);
        if (state == null) { return Optional.empty(); }
        state.lock.lock();
        try {
            if (closing || held.get(pipelineId) != state || state.claim == null || !membership.businessEligible()
                    || !writer.equals(StopAuthority.claimed(WorkloadClaimFence.from(state.claim)))) { return Optional.empty(); }
            ClusterMembership planned = membership.committed();
            if (planned == null || planned.revision() != state.claim.topologyRevision()) { return Optional.empty(); }
            Set<String> members = Set.copyOf(plannedOver(planned));
            return members.isEmpty() ? Optional.empty() : Optional.of(members);
        } finally { state.lock.unlock(); }
    }

    /** Current stop authority is a read; stopping never allocates a new execution or a standalone lease. */
    Optional<StopAuthority> stopAuthority(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (fenced) {
            Permit permit = permit(pipelineId);
            return permit.granted() ? Optional.of(StopAuthority.claimed(WorkloadClaimFence.from(permit.claim())))
                    : Optional.empty();
        }
        if (generations == null) { return Optional.empty(); }
        OptionalLong generation = generations.currentGeneration(clusterId, pipelineId);
        return generation.isPresent() && generation.getAsLong() > 0
                ? Optional.of(StopAuthority.standalone(clusterId, generation.getAsLong())) : Optional.empty();
    }

    private Execution beginUnderClaim(String pipelineId, Held state) {
        ClusterMembership planned = membership.committed();
        Set<String> runMembers = planned == null ? Set.of() : plannedOver(planned);
        Optional<WorkloadClaim> advanced;
        try {
            // At the claim's own topology revision, which is the committed one: a revision change refuses
            // the renew above, so a claim still held is a claim granted under the current topology.
            advanced = claims.advanceExecution(state.claim, state.claim.topologyRevision(), runMembers);
        } catch (TapstateException unreachable) {
            state.claim = null;
            throw codeUnavailableStore(pipelineId, unreachable);
        }
        if (advanced.isEmpty()) {
            state.claim = null;
            return Execution.refused();
        }
        state.claim = advanced.get();
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
        return new Execution(true, new ExecutionFence(
                pipelineId, state.claim.claimGeneration(), state.claim.executionGeneration()), runMembers,
                Optional.of(state.claim));
    }

    private static TapstateException generationUnavailable(String pipelineId, Throwable cause) {
        return new TapstateException(ActuationError.EXECUTION_GENERATION_UNAVAILABLE,
                Map.of("pipeline", pipelineId), cause);
    }

    private static TapstateException codeUnavailableStore(String pipelineId, TapstateException failure) {
        if (failure.code() == IoError.STORE_UNAVAILABLE || failure.code() == IoError.STORE_UNAUTHORIZED) {
            return generationUnavailable(pipelineId, failure);
        }
        return failure;
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
    Departure departure(String pipelineId, long settlingNanos) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (!fenced) { return Departure.ALONE; }
        if (closing) { return Departure.SHUTTING_DOWN; }
        Held state = held.get(pipelineId);
        if (state == null) { return Departure.NOT_DRIVING; }
        state.lock.lock();
        try {
            if (closing) { return Departure.SHUTTING_DOWN; }
            if (held.get(pipelineId) != state || state.claim == null) { return Departure.NOT_DRIVING; }
            if (state.claim.executionGeneration() == 0) { return Departure.NO_RUN; }
            if (state.startRefusedAtGeneration == state.claim.executionGeneration()) {
                return Departure.START_REFUSED;
            }
            // A recorded independent failure survives later membership and ownership changes.
            if (state.claim.contextExecutionGeneration() == state.claim.executionGeneration()
                    && state.claim.failureClaimGeneration() == state.claim.executionClaimGeneration()
                    && !state.claim.failureAfterMemberLoss()) {
                return Departure.ITS_OWN_FAILURE;
            }
            if (state.runMembers == null) {
                state.lostAMemberAtNanos = nanoTime.getAsLong();
                return Departure.INHERITED;
            }
            observeMembership(state);
            if (state.lostAMemberAtNanos == NEVER
                    || nanoTime.getAsLong() - (state.lostAMemberAtNanos + settlingNanos) >= 0) {
                return Departure.NOBODY_LEFT;
            }
            return Departure.A_MEMBER_LEFT;
        } finally { state.lock.unlock(); }
    }

    /**
     * Renews every claim this member holds whose renewal is due, whatever the convergence pass is doing.
     * Called on a renewer of its own, so a start that holds the pass for longer than a lease -- an
     * overloaded host, a slow source, many tables -- runs out no claim of this member, its own included.
     * A claim the store will not renew is dropped here exactly as the pass would drop it, and the pass
     * takes it from there.
     */
    void renewDue() {
        if (!fenced || closing) { return; }
        long now = nanoTime.getAsLong();
        for (Map.Entry<String, Held> entry : held.entrySet()) {
            Held state = entry.getValue();
            if (!state.lock.tryLock()) { continue; }
            try {
                if (!closing && held.get(entry.getKey()) == state && state.claim != null
                        && state.contacted && now - state.nextContactNanos >= 0) {
                    renew(state, now);
                }
            } finally { state.lock.unlock(); }
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
     * the same owner takes again on its next pass under the same generations. True on a single node, where
     * nothing is fenced.
     */
    boolean proveExecution(ExecutionFence fence) {
        if (!fenced || fence == null) {
            return true;
        }
        if (closing) {
            return false;
        }
        WorkloadClaimKey key =
                new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, fence.pipelineId());
        Optional<WorkloadClaimReading> reading;
        try {
            reading = claims.read(key);
        } catch (RuntimeException unreachable) {
            return false;
        }
        // The claim generation alone says whose tenure it is: every change of holder takes the next one, a
        // restarted member included, so a match on it is a match on this member.
        return reading.filter(WorkloadClaimReading::leased)
                .map(WorkloadClaimReading::claim)
                .filter(claim -> claim.claimGeneration() == fence.claimGeneration()
                        && claim.executionGeneration() == fence.executionGeneration())
                .isPresent();
    }

    /** The execution generation the claim this member holds for {@code pipelineId} carries, or zero. */
    long heldExecutionGeneration(String pipelineId) {
        Held state = fenced ? held.get(pipelineId) : null;
        if (state == null) { return 0; }
        state.lock.lock();
        try {
            return held.get(pipelineId) != state || state.claim == null ? 0 : state.claim.executionGeneration();
        } finally { state.lock.unlock(); }
    }

    /** Records FAILED once member loss is visible or a post-detection view confirms none. */
    void recordFailure(String pipelineId, long settlingNanos, long detectionWindowNanos) {
        if (!fenced || closing) {
            return;
        }
        Held state = held.get(pipelineId);
        if (state == null || !state.lock.tryLock()) { return; }
        try {
            if (closing || held.get(pipelineId) != state || state.claim == null || state.claim.executionGeneration() == 0) { return; }
            recordHeldFailure(state, settlingNanos, detectionWindowNanos);
        } finally { state.lock.unlock(); }
    }

    private void recordHeldFailure(Held state, long settlingNanos, long detectionWindowNanos) {
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
    Map<String, MemberRunState> runMembership(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Held state = fenced ? held.get(pipelineId) : null;
        ClusterMembership current = fenced ? membership.committed() : null;
        if (state == null || current == null) {
            return Map.of();
        }
        state.lock.lock();
        try {
            if (held.get(pipelineId) != state || state.runMembers == null) {
                return Map.of();
            }
            return runMembershipOfHeld(state, current);
        } finally {
            state.lock.unlock();
        }
    }

    private static Map<String, MemberRunState> runMembershipOfHeld(Held state, ClusterMembership current) {
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
        if (!fenced) {
            return;
        }
        for (Map.Entry<String, Held> entry : held.entrySet()) {
            if (pipelineIds.contains(entry.getKey())) {
                continue;
            }
            Held state = entry.getValue();
            if (!state.lock.tryLock()) {
                continue;
            }
            try {
                if (held.get(entry.getKey()) != state) {
                    continue;
                }
                WorkloadClaim claim = state.claim;
                if (claim != null) {
                    claims.release(claim);
                }
                held.remove(entry.getKey(), state);
            } finally {
                state.lock.unlock();
            }
        }
    }

    private Permit renew(Held state, long now) {
        Optional<WorkloadClaim> renewed;
        try {
            renewed = claims.renew(state.claim, ttl);
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
        return Permit.granted(state.claim);
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
            attempt = claims.acquire(key, owner, current.revision(), ttl);
        } catch (RuntimeException unreachable) {
            return Permit.denied();
        }
        if (attempt.isEmpty() || !attempt.get().acquired() || !attempt.get().claim().owner().equals(owner)) {
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
        return Permit.granted(state.claim);
    }
}
