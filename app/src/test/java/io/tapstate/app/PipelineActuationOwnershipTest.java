package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One pipeline is driven by one member. Every member reconciles the whole desired set, so what decides
 * that is the durable actuation claim and nothing else.
 */
class PipelineActuationOwnershipTest {

    private static final Instant T0 = Instant.parse("2026-09-18T00:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    /** The stretch a departure is asked to answer over; what the production caller spends its budget in. */
    private static final long SETTLING = TTL.multipliedBy(ClusterRebuildAdmission.MAX_ATTEMPTS).toNanos();
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadOwner NODE_B = new WorkloadOwner("node-b", "boot-b");
    private static final WorkloadClaimKey ORDERS =
            new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterMembershipGate membership = eligibleGate();
    private final AtomicLong nanos = new AtomicLong();

    @Test
    void bothMembersDriveOnePipelineUntilAnActuationClaimPicksOne() {
        // The baseline, measured rather than assumed: two members, one desired pipeline, no claim between
        // them. Each member's converge side asks whether a job is carrying the pipeline *here*, so the
        // member that did not submit one starts a second run of it.
        InMemoryDesiredStore unclaimedDesired = new InMemoryDesiredStore();
        InMemoryStateStore unclaimedState = new InMemoryStateStore();
        VerbCounts baseline = new VerbCounts();
        MemberActuator baselineA = new MemberActuator(baseline);
        MemberActuator baselineB = new MemberActuator(baseline);
        ConvergenceDriver unfencedA = driver(unclaimedDesired, unclaimedState, baselineA, null);
        ConvergenceDriver unfencedB = driver(unclaimedDesired, unclaimedState, baselineB, null);

        driveTheFourVerbs(unclaimedDesired, unfencedA, unfencedB);

        assertThat(baseline.count("start"))
                .as("without a claim both members put a job behind the same pipeline").isEqualTo(2);
        assertThat(baselineB.carrying())
                .as("and the duplicate run is never stopped: the stop converged on the member that drove it")
                .contains("orders");

        // The same four verbs again, with the actuation claim deciding who drives.
        InMemoryDesiredStore desired = new InMemoryDesiredStore();
        InMemoryStateStore state = new InMemoryStateStore();
        VerbCounts claimed = new VerbCounts();
        MemberActuator memberA = new MemberActuator(claimed);
        MemberActuator memberB = new MemberActuator(claimed);
        ConvergenceDriver nodeA = driver(desired, state, memberA, NODE_A);
        ConvergenceDriver nodeB = driver(desired, state, memberB, NODE_B);

        driveTheFourVerbs(desired, nodeA, nodeB);

        assertThat(claimed.count("start")).as("one member drives the start").isEqualTo(1);
        assertThat(claimed.count("pause")).as("one member drives the pause").isEqualTo(1);
        assertThat(claimed.count("resume")).as("one member drives the resume").isEqualTo(1);
        assertThat(claimed.count("stop")).as("one member drives the stop").isEqualTo(1);
        assertThat(memberB.carrying()).as("the member that does not drive it never starts a run").isEmpty();
        assertThat(memberA.carrying()).as("and the run that was started is the one that was stopped").isEmpty();

        WorkloadClaim held = claims.read(ORDERS).orElseThrow().claim();
        assertThat(held.owner()).isEqualTo(NODE_A);
        assertThat(held.key().resourceId())
                .as("an actuation claim is keyed by the pipeline it drives").isEqualTo("orders");
    }

    @Test
    void aHolderThatCannotProveItsClaimStopsAtOnceAndDoesNotTakeItStraightBack() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();

        // The lease ran out before this member's next renew landed.
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());

        assertThat(nodeA.permit("orders").granted())
                .as("a claim this member can no longer prove stops it driving").isFalse();
        assertThat(nodeA.permit("orders").granted())
                .as("and it does not pick the now-free claim straight back up: rebasing onto the current "
                        + "state is what a fenced lifecycle write does, and doing it here is how two members "
                        + "would drive one pipeline")
                .isFalse();

        PipelineActuationOwnership nodeB = ownership(NODE_B);
        PipelineActuationOwnership.Permit taken = nodeB.permit("orders");
        assertThat(taken.granted()).isTrue();
        assertThat(taken.claim().claimGeneration())
                .as("ownership changed hands, so the claim generation moved on").isEqualTo(2);
    }

    @Test
    void releasingAPipelineNobodyWantsAnyMoreExpiresTheLeaseAndKeepsTheGeneration() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();

        nodeA.retain(List.of());

        WorkloadClaim released = claims.read(ORDERS).orElseThrow().claim();
        assertThat(released.claimGeneration()).as("a release never lowers a generation").isEqualTo(1);
        PipelineActuationOwnership.Permit taken = ownership(NODE_B).permit("orders");
        assertThat(taken.granted()).as("the lease was expired, so the next member may take it").isTrue();
        assertThat(taken.claim().claimGeneration()).isEqualTo(2);
    }

    /** The four lifecycle verbs, each driven once by writing the intent and ticking every member. */
    private static void driveTheFourVerbs(InMemoryDesiredStore desired, ConvergenceDriver... members) {
        List<PipelineState> intents = List.of(
                PipelineState.RUNNING, PipelineState.PAUSED, PipelineState.RUNNING, PipelineState.STOPPED);
        for (PipelineState intent : intents) {
            desired.save(new DesiredState("orders", intent, "rev-1"));
            for (ConvergenceDriver member : members) {
                member.reconcile();
            }
        }
    }

    private ConvergenceDriver driver(
            InMemoryDesiredStore desired, InMemoryStateStore state, MemberActuator actuator,
            WorkloadOwner owner) {
        PipelineConverger converger =
                new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));
        ObservationPublisher publisher = new ObservationPublisher(state, new InMemoryObservationStore());
        if (owner == null) {
            return new ConvergenceDriver(converger, desired, publisher);
        }
        return new ConvergenceDriver(
                converger, desired, publisher, () -> true, ownership(owner));
    }

    /**
     * The run a killed member left behind is picked up by the one that inherits the pipeline.
     *
     * <p>Whether a member went away is read, everywhere else, off what this member remembers about the
     * run it submitted. After a failover there is nobody left holding that memory: the member that
     * submitted the run is the member that died, and the one that inherits the pipeline has never
     * driven it. Answered only that way, the question is false forever and the pipeline stays failed
     * with nothing in any log saying why - which is what a two-process failover found and no
     * same-member case can.
     *
     * <p>Both directions, because only one of them is the defect: a pipeline nobody has ever run is not
     * something to rebuild, and the member that did submit the run still answers from what moved under
     * it rather than from having inherited anything.
     */
    @Test
    void theRunAKilledMemberLeftBehindIsAdmittedForRebuildingByTheOneThatInheritsIt() {
        PipelineActuationOwnership nodeA = ownership(NODE_A);
        assertThat(nodeA.permit("orders").granted()).isTrue();
        assertThat(nodeA.aMemberLeftUnderTheRun("orders", SETTLING))
                .as("a pipeline nobody has run yet is not a run anybody left behind")
                .isFalse();
        assertThat(nodeA.beginExecution("orders").allowed()).isTrue();
        assertThat(nodeA.aMemberLeftUnderTheRun("orders", SETTLING))
                .as("and nothing has moved under the member that submitted it")
                .isFalse();

        // That member is killed: it stops renewing, and the lease it never released runs out.
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());

        PipelineActuationOwnership nodeB = ownership(NODE_B);
        assertThat(nodeB.permit("orders").granted())
                .as("the pipeline changes hands once the dead holder's lease expires")
                .isTrue();
        assertThat(nodeB.aMemberLeftUnderTheRun("orders", SETTLING))
                .as("the member that inherits a claim already carrying an execution is holding a run "
                        + "nobody is driving, and that is the whole of what it needs to know to replace it")
                .isTrue();
    }

    private PipelineActuationOwnership ownership(WorkloadOwner owner) {
        return new PipelineActuationOwnership(
                "cluster-a", owner, membership, new ClusterWorkloadClaims(claims, membership), TTL, RENEW,
                nanos::get);
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    /** How many times each verb was effectively driven, counted across every member of the cluster. */
    private static final class VerbCounts {
        private final Map<String, Integer> counts = new LinkedHashMap<>();

        private synchronized void record(String verb) {
            counts.merge(verb, 1, Integer::sum);
        }

        private synchronized int count(String verb) {
            return counts.getOrDefault(verb, 0);
        }
    }

    /**
     * One member's actuator: it counts into the cluster-wide tally, and carries jobs member-locally —
     * which is the property that makes the duplicate run possible, since "is anything carrying this
     * pipeline" is answered by the member that is asked, not by the cluster.
     */
    private static final class MemberActuator implements LifecycleActuator {

        private final VerbCounts counts;
        private final Set<String> carrying = new HashSet<>();

        private MemberActuator(VerbCounts counts) {
            this.counts = counts;
        }

        private Set<String> carrying() {
            return carrying;
        }

        @Override
        public void start(String pipelineId) {
            counts.record("start");
            carrying.add(pipelineId);
        }

        @Override
        public void pause(String pipelineId) {
            counts.record("pause");
        }

        @Override
        public void resume(String pipelineId) {
            counts.record("resume");
        }

        @Override
        public void stop(String pipelineId, boolean purgeState) {
            counts.record("stop");
            carrying.remove(pipelineId);
        }

        @Override
        public Optional<Throwable> failure(String pipelineId) {
            return Optional.empty();
        }

        @Override
        public boolean isCarryingAJob(String pipelineId) {
            return carrying.contains(pipelineId);
        }
    }
}
