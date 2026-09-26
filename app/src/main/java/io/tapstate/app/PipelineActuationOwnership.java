package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
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
 * <p>One scheduler thread drives the claims. Failure recording and shutdown alone synchronize so a
 * close that starts first cannot be followed by a durable verdict that calls its dying run independent.
 */
final class PipelineActuationOwnership {

    /** Whether this member may drive the pipeline, and the claim that says so ({@code null} on a single node). */
    record Permit(boolean granted, WorkloadClaim claim) {

        static Permit unfenced() {
            return new Permit(true, null);
        }

        static Permit denied() {
            return new Permit(false, null);
        }
    }

    private final String clusterId;
    private final WorkloadOwner owner;
    private final ClusterMembershipGate membership;
    private final ClusterWorkloadClaims claims;
    private final Duration ttl;
    private final long renewIntervalNanos;
    private final LongSupplier nanoTime;
    private final boolean fenced;
    private final Map<String, Held> held = new HashMap<>();
    private volatile boolean closing;

    /** One pipeline's local view: the claim this member proved, and when the next round trip is due. */
    private static final class Held {
        private WorkloadClaim claim;
        private long nextContactNanos;
        private boolean contacted;
        /**
         * The members the run this member last submitted was planned over -- the ones in sight when it
         * was submitted -- or null while it has submitted none. Kept apart from the claim, whose own
         * revision moves forward the moment this member re-acquires under a changed cluster -- which is
         * exactly when the difference between what the run was planned over and what is here now becomes
         * the thing worth knowing.
         */
        private Set<String> runMembers;
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
        /** The gate publication present at the first FAILED observation, before a no-loss verdict. */
        private long failureObservedAtVisibilityRevision = NEVER;
    }

    /** No member has been seen missing under this run. Not a time, so no arithmetic is done on it. */
    private static final long NEVER = Long.MIN_VALUE;

    private PipelineActuationOwnership() {
        this.clusterId = "single";
        this.owner = null;
        this.membership = null;
        this.claims = null;
        this.ttl = Duration.ZERO;
        this.renewIntervalNanos = 0;
        this.nanoTime = System::nanoTime;
        this.fenced = false;
    }

    /** A single-node run has one member, so nothing is fenced and every pipeline is this member's to drive. */
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
    Permit permit(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing) {
            return Permit.denied();
        }
        if (!fenced) {
            return Permit.unfenced();
        }
        Held state = held.computeIfAbsent(pipelineId, id -> new Held());
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

    /** Whether a run may be submitted, and the generations that fence it (none on a single node). */
    record Execution(boolean allowed, ExecutionFence fence) {

        static Execution unfenced() {
            return new Execution(true, null);
        }

        static Execution refused() {
            return new Execution(false, null);
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
    Execution beginExecution(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (closing) {
            return Execution.refused();
        }
        if (!fenced) {
            return Execution.unfenced();
        }
        Held state = held.get(pipelineId);
        if (state == null || state.claim == null) {
            return Execution.refused();
        }
        ClusterMembership planned = membership.committed();
        Set<String> runMembers = planned == null ? Set.of() : plannedOver(planned);
        Optional<WorkloadClaim> advanced;
        try {
            // At the claim's own topology revision, which is the committed one: a revision change refuses
            // the renew above, so a claim still held is a claim granted under the current topology.
            advanced = claims.advanceExecution(state.claim, state.claim.topologyRevision(), runMembers);
        } catch (RuntimeException unreachable) {
            advanced = Optional.empty();
        }
        if (advanced.isEmpty()) {
            state.claim = null;
            return Execution.refused();
        }
        state.claim = advanced.get();
        state.failureObservedAtVisibilityRevision = NEVER;
        // What this run is planned over. Null rather than empty when nothing is committed -- which the
        // eligibility gate above makes unreachable -- because an empty set would read as "planned over
        // nobody", and nobody can never go missing.
        state.runMembers = planned == null ? null : runMembers;
        // The departure is deliberately not forgotten here. This run is planned over members that are
        // all present, so the comparison below will find nothing missing from it -- and the run is being
        // submitted because a member went away, into a cluster that is still settling from it. Clearing
        // the moment here would make the very next death of this run read as the pipeline's own.
        return new Execution(true, new ExecutionFence(
                pipelineId, state.claim.claimGeneration(), state.claim.executionGeneration()));
    }

    /**
     * Whether a member the run this member last submitted for {@code pipelineId} was planned over is
     * gone, or went within {@code settlingNanos} of now.
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
     * If a fresh membership view confirms no member was lost after the submitting holder saw FAILED,
     * a later handover cannot make that earlier death recoverable. If a member was lost when failure
     * was recorded, that fact survives a takeover even if the member has returned. An inherited run
     * with no recorded failure is admitted: its driver may have gone away before it could record why
     * the run ended.
     */
    boolean aMemberLeftUnderTheRun(String pipelineId, long settlingNanos) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (!fenced || closing) {
            return false;
        }
        Held state = held.get(pipelineId);
        if (state == null || state.claim == null || state.claim.executionGeneration() == 0) {
            return false;
        }
        // A failure the submitting holder saw before any member loss remains that same failure when
        // the claim changes hands. A later departure cannot turn it into a cluster-caused death.
        if (state.claim.contextExecutionGeneration() == state.claim.executionGeneration()
                && state.claim.failureClaimGeneration() == state.claim.executionClaimGeneration()
                && !state.claim.failureAfterMemberLoss()) {
            return false;
        }
        if (state.runMembers == null) {
            // The submitting holder is gone and this member has no local run snapshot. Take the moment
            // here so a replacement that fails while the handover settles can spend the remaining budget.
            state.lostAMemberAtNanos = nanoTime.getAsLong();
            return true;
        }
        observeMembership(state);
        if (state.lostAMemberAtNanos == NEVER) {
            return false;
        }
        return nanoTime.getAsLong() - (state.lostAMemberAtNanos + settlingNanos) < 0;
    }

    /** Records the first FAILED observation once the gate can classify its membership. */
    synchronized void recordFailure(String pipelineId, long settlingNanos) {
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
            if (state.failureObservedAtVisibilityRevision == NEVER) {
                state.failureObservedAtVisibilityRevision = visible.revision();
            }
            // The view that preceded FAILED may still include a member that has already died.
            // Only a later publication can make an independent-failure verdict durable.
            if (visible.revision() <= state.failureObservedAtVisibilityRevision) {
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
        if (!fenced) {
            return;
        }
        Iterator<Map.Entry<String, Held>> entries = held.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, Held> entry = entries.next();
            if (pipelineIds.contains(entry.getKey())) {
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
            attempt = claims.acquire(key, owner, current.revision(), ttl);
        } catch (RuntimeException unreachable) {
            return Permit.denied();
        }
        if (attempt.isEmpty() || !attempt.get().acquired() || !attempt.get().claim().owner().equals(owner)) {
            return Permit.denied();
        }
        state.claim = attempt.get().claim();
        return new Permit(true, state.claim);
    }
}
