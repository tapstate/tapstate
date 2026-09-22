package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When a failed run may be replaced without anybody asking, driven through the real claim store and a
 * real membership change rather than a stubbed answer -- the question this decides is "did a member
 * this run was planned over go away", and a stub would be deciding it in the test instead of measuring
 * it.
 */
class ClusterRebuildAdmissionTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    private static final Duration BACKOFF = TTL;
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterProperties properties = production();
    private final ClusterMembershipGate membership = new ClusterMembershipGate(properties);
    private final AtomicLong nanos = new AtomicLong();
    private final PipelineActuationOwnership ownership = new PipelineActuationOwnership(
            "cluster-a", NODE_A, membership, new ClusterWorkloadClaims(claims, membership), TTL, RENEW,
            nanos::get);
    private final ClusterRebuildAdmission admission =
            new ClusterRebuildAdmission(ownership, BACKOFF, nanos::get);

    @Test
    void aRunNothingMovedUnderIsNotRebuilt() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);

        assertThat(admission.admits("orders"))
                .as("the cluster is where it was when this run was fenced, so this death is the "
                        + "pipeline's own and stays recorded as one")
                .isFalse();
    }

    @Test
    void aRunAMemberJoinedUnderIsNotRebuilt() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a", "node-b", "node-c");

        assertThat(admission.admits("orders"))
                .as("a member joining takes nothing away from this run -- every member it was planned "
                        + "over is still here -- so this death is the pipeline's own, and replacing the "
                        + "run would restart a connector defect instead of recording it")
                .isFalse();
    }

    @Test
    void aMemberThatLeftAndCameBackBeforeAnybodyLookedStillCountsAsHavingLeft() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);

        committed(8, "node-a");
        ownership.permit("orders");
        committed(9, "node-a", "node-b");

        assertThat(admission.admits("orders"))
                .as("the run died when that member went, and it stays dead now that it is back: asking "
                        + "only who is here now cannot see an absence that is already over")
                .isTrue();
    }

    @Test
    void aRunWhoseClusterChangedUnderItIsRebuiltOnce_thenNotAgainUntilTheBackoffIsOver() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a");

        assertThat(admission.admits("orders")).isTrue();
        assertThat(admission.admits("orders"))
                .as("one rebuild per backoff: the cluster is often still settling, and rebuilding into a "
                        + "half-formed membership is how one handover becomes several")
                .isFalse();

        nanos.addAndGet(BACKOFF.toNanos());

        assertThat(admission.admits("orders")).isTrue();
    }

    @Test
    void aRunThatKeepsFailingIsLeftFailedRatherThanRebuiltForever() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a");

        for (int attempt = 0; attempt < ClusterRebuildAdmission.MAX_ATTEMPTS; attempt++) {
            assertThat(admission.admits("orders")).as("attempt " + attempt).isTrue();
            nanos.addAndGet(BACKOFF.toNanos());
        }

        assertThat(admission.admits("orders"))
                .as("an automatic recovery that never runs out is a restart loop under another name")
                .isFalse();
        nanos.addAndGet(BACKOFF.multipliedBy(10).toNanos());
        assertThat(admission.admits("orders")).isFalse();
    }

    /**
     * The failure this class was reported for: a pipeline left failed for a person over a member that
     * went away, with the budget meant to bring it back never reaching it.
     *
     * <p>A rebuilt run is planned over the members that are here now, so nothing is missing from it -
     * and it is started into a cluster still settling from the departure, which goes on ending runs for
     * seconds afterwards. Read at the instant of that second death, the departure has already stopped
     * being the answer, and nothing replaces the run: the pipeline stays failed, unasked and for good.
     */
    @Test
    void aRebuiltRunThatDiesWhileTheDepartureIsStillSettlingIsRebuiltToo() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a");

        assertThat(admission.admits("orders")).isTrue();

        // What a rebuild does: the holder takes the next execution generation, and the run it submits is
        // planned over the members committed now. The member that went is not one of this run's.
        submitRunUnder(8);
        nanos.addAndGet(BACKOFF.toNanos());

        assertThat(admission.admits("orders"))
                .as("what killed the replacement is what the departure left behind, so refusing here "
                        + "leaves the pipeline failed over a member that left - which is the one death "
                        + "this loop exists to answer, and nothing else will come back for it")
                .isTrue();
    }

    /**
     * And the spacing holds across the pass that saw an intact run, which is the other half of the same
     * report: two attempts were spent 2.2 seconds apart, both inside the window the transient was still
     * open, so the budget was gone before the cluster had stopped moving.
     */
    @Test
    void thePassThatSeesAnIntactRunDoesNotHandTheSpacingBack() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);
        committed(8, "node-a", "node-b");

        assertThat(admission.admits("orders")).isTrue();
        submitRunUnder(8);
        assertThat(admission.admits("orders"))
                .as("nothing is missing from the run submitted in its place, and the spacing has not "
                        + "elapsed either")
                .isFalse();

        // A second loss, out of the members this run does have. Written as a real one because the only
        // membership a cluster ever commits is one whose node set differs from the last: a bare revision
        // bump over the same members is not an input the membership controller can produce.
        committed(9, "node-a");

        assertThat(admission.admits("orders"))
                .as("a rebuild a moment after the last one goes into the same half-formed membership - "
                        + "which is what the spacing is for, and a pass that happened to see an intact "
                        + "run in between is not a reason to have spent none of it")
                .isFalse();
        nanos.addAndGet(BACKOFF.toNanos());
        assertThat(admission.admits("orders")).isTrue();
    }

    /**
     * The same, on the road the reported failure actually takes: the member that went away is the one
     * that was driving, so the member picking the pipeline up never saw the absence - it has no run of
     * its own to compare a membership against, and the claim carrying somebody else's execution is the
     * whole of what tells it anything happened.
     *
     * <p>Held apart from the case above because they are two different readings of the same question,
     * and only this one is what a killed driver produces. A stretch that started only where a member
     * compared its own run would cover the pipelines a member kept and none of the ones it inherited.
     */
    @Test
    void theReplacementForAnInheritedRunIsRebuiltTooWhileTheDepartureIsStillSettling() {
        committed(7, "node-a", "node-b");
        PipelineActuationOwnership driver = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-b", "boot-b"), membership,
                new ClusterWorkloadClaims(claims, membership), TTL, RENEW, nanos::get);
        assertThat(driver.permit("orders").granted()).isTrue();
        assertThat(driver.beginExecution("orders").allowed()).isTrue();

        // It is killed: it stops renewing, the lease it never released runs out, and it leaves the
        // committed set. What it does not leave is anything saying what its run was planned over.
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());
        committed(8, "node-a");
        assertThat(ownership.permit("orders").granted())
                .as("the pipeline changes hands once the dead holder's lease expires").isTrue();

        assertThat(admission.admits("orders")).isTrue();
        assertThat(ownership.beginExecution("orders").allowed()).isTrue();
        nanos.addAndGet(BACKOFF.toNanos());

        assertThat(admission.admits("orders"))
                .as("the run this member submitted in place of the dead one is planned over itself "
                        + "alone, so nothing is ever missing from it - and what ended it is what the "
                        + "killed member left behind")
                .isTrue();
    }

    @Test
    void onceTheSettlingIsOverADeathIsThePipelinesOwnAgain() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a");

        assertThat(admission.admits("orders")).isTrue();
        submitRunUnder(8);

        // Well past the whole stretch the departure answers for, which is as long as spending the budget
        // takes. Not up to its edge: where that edge falls depends on how many round trips the handover
        // above took, and this is about what is true after it, not about the boundary itself.
        nanos.addAndGet(BACKOFF.multipliedBy(2L * ClusterRebuildAdmission.MAX_ATTEMPTS).toNanos());

        assertThat(admission.admits("orders"))
                .as("a run that kept dying long after the cluster stopped moving is dying of its own "
                        + "trouble, and restarting it every backoff for ever is the restart loop this "
                        + "budget exists to stop")
                .isFalse();
    }

    /** Installs a committed membership at {@code revision} and lets the gate see those nodes. */
    private void committed(long revision, String... nodeIds) {
        membership.install(new ClusterMembership("cluster-a", revision, Set.of(nodeIds)));
        membership.canCommit(Set.of(nodeIds));
    }

    /**
     * Puts this member in the state it is in just after submitting a run: holding the pipeline's claim at
     * the committed revision, with an execution generation taken under it.
     */
    private void submitRunUnder(long revision) {
        // The same path a real handover takes, and it takes two passes when the revision moved: one where
        // the renew is refused because the claim was granted under a cluster that is gone, which drops it,
        // and the next where a fresh claim is taken under the revision committed now. Both are a round
        // trip apart, so the clock moves between them exactly as the reconcile interval would.
        boolean driving = false;
        for (int pass = 0; pass < 3 && !driving; pass++) {
            nanos.addAndGet(RENEW.toNanos() + 1);
            driving = ownership.permit("orders").granted();
        }
        assertThat(driving)
                .as("this member has to be driving the pipeline at revision " + revision)
                .isTrue();
        assertThat(ownership.beginExecution("orders").allowed()).isTrue();
    }

    private static ClusterProperties production() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        return properties;
    }
}
