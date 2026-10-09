package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.Duration;
import java.util.List;
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
    private static final Duration DETECTION = Duration.ofSeconds(30);
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterProperties properties = production();
    private final ClusterMembershipGate membership = new ClusterMembershipGate(properties);
    private final AtomicLong nanos = new AtomicLong();
    private final PipelineActuationOwnership ownership = new PipelineActuationOwnership(
            "cluster-a", NODE_A, membership, new ClusterWorkloadClaims(claims, membership), TTL, RENEW,
            nanos::get);
    private final ClusterRebuildAdmission admission =
            new ClusterRebuildAdmission(ownership, BACKOFF, DETECTION, nanos::get);

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
    void aRunThatFailedBeforeItsDriverRestartedIsNotRebuiltOnClaimTakeover() {
        committed(7, "node-a", "node-b", "node-c");
        assertThat(ownership.permit("orders").granted()).isTrue();
        assertThat(ownership.beginExecution("orders").allowed()).isTrue();

        // Admission is asked for a FAILED run. The intact view must survive failure detection.
        assertThat(admission.admits("orders"))
                .as("recovery waits for the member-loss detection window")
                .isFalse();
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        assertThat(admission.admits("orders"))
                .as("an early intact publication cannot make the verdict durable")
                .isFalse();
        nanos.addAndGet(DETECTION.toNanos());
        assertThat(admission.admits("orders"))
                .as("the last publication may predate failure detection")
                .isFalse();
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        assertThat(admission.admits("orders"))
                .as("an intact view after the detection window records an independent failure")
                .isFalse();

        // The driver restarts after that failure. Its stable node id remains in the committed and
        // visible membership, but its new boot id must take over the expired claim.
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());
        PipelineActuationOwnership restarted = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a-restarted"), membership,
                new ClusterWorkloadClaims(claims, membership), TTL, RENEW, nanos::get);
        PipelineActuationOwnership.Permit taken = restarted.permit("orders");
        assertThat(taken.granted()).isTrue();
        assertThat(taken.claim().claimGeneration()).isEqualTo(2);
        assertThat(taken.claim().executionGeneration()).isEqualTo(1);
        assertThat(membership.visibleNodeIds()).containsExactlyInAnyOrder("node-a", "node-b", "node-c");

        assertThat(new ClusterRebuildAdmission(restarted, BACKOFF, DETECTION, nanos::get).admits("orders"))
                .as("the run was already FAILED for its own reason before the claim changed hands")
                .isFalse();
    }

    @Test
    void aFailureRecordedBeforeAMemberLeavesDoesNotBecomeRecoverableLater() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);

        assertThat(admission.admits("orders")).isFalse();
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        assertThat(admission.admits("orders"))
                .as("an early intact view cannot confirm the failure was independent")
                .isFalse();
        nanos.addAndGet(DETECTION.toNanos());
        assertThat(admission.admits("orders")).isFalse();
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        assertThat(admission.admits("orders"))
                .as("the intact view after failure detection confirms the independent failure")
                .isFalse();
        membership.canCommit(Set.of("node-a", "node-b"));

        assertThat(admission.admits("orders"))
                .as("the missing member appeared only after the failure was recorded")
                .isFalse();
    }

    @Test
    void aFailureSeenBeforeMembershipRefreshCanRecoverWhenTheMissingMemberBecomesVisible() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);

        admission.recordFailure("orders");
        assertThat(admission.admits("orders"))
                .as("the last visible snapshot still includes every member, so recovery waits")
                .isFalse();
        assertThat(admission.admits("orders"))
                .as("another pass over the same view is not new membership evidence")
                .isFalse();
        membership.canCommit(Set.of("node-a", "node-b"));

        assertThat(admission.admits("orders"))
                .as("the refreshed view reveals the member lost under the failed run")
                .isTrue();
    }

    @Test
    void aStaleUnchangedPublicationCannotMakeTheFailureIndependentlyPermanent() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);

        admission.recordFailure("orders");
        // The membership reconciler can publish its old view again before Hazelcast detects the loss.
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));
        assertThat(admission.admits("orders")).isFalse();
        nanos.addAndGet(DETECTION.toNanos());
        assertThat(admission.admits("orders"))
                .as("the stale view cannot confirm a no-loss verdict at the detection boundary")
                .isFalse();
        membership.canCommit(Set.of("node-a", "node-b"));

        assertThat(admission.admits("orders"))
                .as("an unchanged pre-detection view cannot make the later loss unrecoverable")
                .isTrue();
    }

    @Test
    void anUnmarkedFailureCanBeRebuiltWhenItsDriverLeavesDuringDetection() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);
        admission.recordFailure("orders");

        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());
        membership.canCommit(Set.of("node-b", "node-c"));
        PipelineActuationOwnership survivor = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-b", "boot-b"), membership,
                new ClusterWorkloadClaims(claims, membership), TTL, RENEW, nanos::get);
        assertThat(survivor.permit("orders").granted()).isTrue();

        assertThat(new ClusterRebuildAdmission(survivor, BACKOFF, DETECTION, nanos::get).admits("orders"))
                .as("the failure was still unclassified when its driver left")
                .isTrue();
    }

    @Test
    void aKnownSinkFailureMarkerStaysIndependentWhenMemberLossAppearsDuringDetection() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);
        assertThat(claims.recordExecutionFailure(ownership.permit("orders").claim(), false))
                .as("a sink member records its known failure under the run before reporting it")
                .isPresent();

        membership.canCommit(Set.of("node-a", "node-b"));

        assertThat(admission.admits("orders"))
                .as("the later loss must not override the sink failure recorded under this run")
                .isFalse();
    }

    @Test
    void aFailureRecordedAfterMemberLossRemainsRecoverableAcrossTakeoverAndReturn() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);
        membership.canCommit(Set.of("node-a", "node-b"));

        assertThat(admission.admits("orders")).isTrue();
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));

        PipelineActuationOwnership restarted = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a-restarted"), membership,
                new ClusterWorkloadClaims(claims, membership), TTL, RENEW, nanos::get);
        assertThat(restarted.permit("orders").granted()).isTrue();
        assertThat(new ClusterRebuildAdmission(restarted, BACKOFF, DETECTION, nanos::get).admits("orders"))
                .as("the departure had already been recorded with the failure, even though the member returned")
                .isTrue();
    }

    @Test
    void aRunThatFailsWhileItsDriverIsClosingIsRecoveredAfterTakeover() {
        committed(7, "node-a", "node-b", "node-c");
        assertThat(ownership.permit("orders").granted()).isTrue();
        assertThat(ownership.beginExecution("orders").allowed()).isTrue();

        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.registerBean("ownership", PipelineActuationOwnership.class, () -> ownership);
            context.refresh();
        }
        assertThat(ownership.permit("orders").granted()).isFalse();
        admission.recordFailure("orders");
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());

        PipelineActuationOwnership restarted = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a-restarted"), membership,
                new ClusterWorkloadClaims(claims, membership), TTL, RENEW, nanos::get);
        assertThat(restarted.permit("orders").granted()).isTrue();
        assertThat(new ClusterRebuildAdmission(restarted, BACKOFF, DETECTION, nanos::get).admits("orders"))
                .as("the run died after its driver began shutting down, even though every node id is in sight")
                .isTrue();
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

    /**
     * A run refused before it started because a member joined between its plan and its start ran nothing,
     * so its death is not the pipeline's own - and a joining member is exactly what the departure reading
     * above rightly ignores. Without its own answer such a run stays failed for a person over the moment it
     * happened to be submitted in; with it, the run is rebuilt against the members present, and only within
     * the same budget every rebuild is held to.
     */
    @Test
    void aRunRefusedForAChangedMembershipBeforeItStartedIsRebuiltWithinTheBudget() {
        ClusterRebuildAdmission afterARefusedStart = new ClusterRebuildAdmission(
                ownership, pipelineId -> true, BACKOFF, DETECTION, nanos::get);
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a", "node-b", "node-c");

        assertThat(afterARefusedStart.admits("orders")).isTrue();
        for (int attempt = 1; attempt < ClusterRebuildAdmission.MAX_ATTEMPTS; attempt++) {
            nanos.addAndGet(BACKOFF.toNanos());
            assertThat(afterARefusedStart.admits("orders")).as("attempt %s", attempt + 1).isTrue();
        }
        nanos.addAndGet(BACKOFF.toNanos());
        assertThat(afterARefusedStart.admits("orders"))
                .as("the budget is spent: a start refused over and over is left for a person")
                .isFalse();
    }

    /**
     * The second half of what this class was reported for. A member that restarted took over a run another boot
     * of it had submitted, and every start it made was refused with a code before it took a run of its own.
     * Judged by the run the claim still named - inherited, so admitted - each refusal spent a rebuild, and the
     * pipeline was started into the same refusal until the budget ran out.
     */
    @Test
    void aStartRefusedBeforeItTookARunIsNotRebuiltAsTheRunItTookOver() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());
        PipelineActuationOwnership restarted = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a-restarted"), membership,
                new ClusterWorkloadClaims(claims, membership), TTL, RENEW, nanos::get);
        assertThat(restarted.permit("orders").granted()).isTrue();
        ClusterRebuildAdmission afterRestart = new ClusterRebuildAdmission(restarted, BACKOFF, DETECTION, nanos::get);

        List<String> logged = loggedBy(() -> {
            restarted.startRefusedBeforeItsRun("orders");
            for (int pass = 0; pass <= ClusterRebuildAdmission.MAX_ATTEMPTS; pass++) {
                assertThat(afterRestart.admits("orders"))
                        .as("pass %s: the start was refused, and no member leaving answers for that", pass)
                        .isFalse();
                nanos.addAndGet(BACKOFF.toNanos());
            }
        });

        assertThat(logged).hasSize(1).allSatisfy(line -> assertThat(line).contains("orders")
                .contains("its last start was refused before it took a run"));
    }

    /**
     * A departure's rebuilds are for the runs it ends. A replacement refused before it took a run was ended by
     * its refusal alone, so it spends none of them, and the next run that does start and is then cut short by a
     * member leaving has the whole budget.
     */
    @Test
    void aReplacementRefusedBeforeItTookARunDoesNotSpendTheDeparturesRebuilds() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a");
        assertThat(admission.admits("orders")).as("node-b left under the run").isTrue();

        ownership.startRefusedBeforeItsRun("orders");
        nanos.addAndGet(BACKOFF.toNanos());
        assertThat(admission.admits("orders"))
                .as("the replacement was refused before it took a run, with node-b still gone")
                .isFalse();

        // Somebody clears what refused it and starts the pipeline, and that run is cut short by a member leaving.
        committed(9, "node-a", "node-c");
        submitRunUnder(9);
        committed(10, "node-a");
        List<String> logged = loggedBy(() -> assertThat(admission.admits("orders")).isTrue());
        assertThat(logged).hasSize(1).allSatisfy(line -> assertThat(line).contains("attempt 1 of 3"));
    }

    /**
     * The engine keeps the failure of the run it last refused. A start refused before it took a run leaves that
     * record where it is, about the run before - which says nothing of this refusal and must not admit it.
     */
    @Test
    void aStartRefusedBeforeItTookARunIsNotTakenForTheRunAChangedMembershipRefused() {
        ClusterRebuildAdmission afterARefusedStart = new ClusterRebuildAdmission(
                ownership, pipelineId -> true, BACKOFF, DETECTION, nanos::get);
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a", "node-b", "node-c");
        assertThat(afterARefusedStart.admits("orders")).as("the engine refused the run for its members").isTrue();

        ownership.startRefusedBeforeItsRun("orders");
        nanos.addAndGet(BACKOFF.toNanos());

        assertThat(afterARefusedStart.admits("orders"))
                .as("what failed now is the replacement's own refusal, not the run the engine refused")
                .isFalse();
    }

    @Test
    void theRefusalIsReadOffItsCodeWhetherTheCauseOrOnlyItsRenderingSurvived() {
        io.tapstate.core.common.TapstateException refused = new io.tapstate.core.common.TapstateException(
                io.tapstate.runtime.engine.EngineError.MEMBERSHIP_CHANGED_BEFORE_START,
                java.util.Map.of("pipeline", "orders", "planned", 2, "actual", 3), null);

        assertThat(ClusterRebuildAdmission.isMembershipChangedBeforeStart(refused)).isTrue();
        assertThat(ClusterRebuildAdmission.isMembershipChangedBeforeStart(
                new RuntimeException("com.hazelcast.jet.JetException: " + refused))).isTrue();
        assertThat(ClusterRebuildAdmission.isMembershipChangedBeforeStart(
                new RuntimeException("connector refused the write"))).isFalse();
    }

    /**
     * How a member actually leaves a running cluster: the committed set keeps naming it, because that
     * set only ever grows, and what changes is who this member can see. Asked of the committed set, the
     * departure never shows -- and a run whose driver survived the loss of another member it was running
     * on stays failed for a person, which is what a two-machine run found.
     */
    @Test
    void aMemberThatGoesOutOfSightHasLeftTheRunThoughTheCommittedSetStillNamesIt() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);

        membership.canCommit(Set.of("node-a", "node-b"));

        assertThat(membership.committed().activeNodeIds())
                .as("the committed set is not what moved -- it never drops a member")
                .contains("node-c");
        assertThat(admission.admits("orders"))
                .as("node-c carried part of this run and is gone, so this death is the cluster's and the "
                        + "run is replaced, whatever the committed set still says")
                .isTrue();
    }

    /**
     * The other half of reading departures off who is in sight: a run is planned over the members in
     * sight when it is submitted, so a committed member that was already gone then was never part of it.
     * Counting it would make every death of the run read as the cluster's for as long as that member
     * stays away, which is a restart loop with a budget.
     */
    @Test
    void aCommittedMemberAlreadyOutOfSightWhenTheRunWasPlannedWasNeverPartOfIt() {
        committed(7, "node-a", "node-b", "node-c");
        membership.canCommit(Set.of("node-a", "node-b"));
        submitRunUnder(7);

        assertThat(admission.admits("orders"))
                .as("nothing this run was planned over has gone anywhere, so this death is the "
                        + "pipeline's own")
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

    @Test
    void aPipelineCreatedUnderTheIdOfOneThatSpentItsBudgetGetsABudgetOfItsOwn() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a");
        for (int attempt = 0; attempt < ClusterRebuildAdmission.MAX_ATTEMPTS; attempt++) {
            assertThat(admission.admits("orders")).as("attempt " + attempt).isTrue();
            nanos.addAndGet(BACKOFF.toNanos());
        }
        assertThat(admission.admits("orders")).isFalse();

        // Deleted, and created again under the same id before the departure that spent the budget is over.
        admission.retain(List.of());

        assertThat(admission.admits("orders"))
                .as("what a deleted pipeline spent is not the budget of the one created under its id")
                .isTrue();
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

    /**
     * A member that takes a pipeline back judges the run the claim carries now, not the one it submitted
     * itself before it lost the claim.
     *
     * <p>What a three-machine run found: a member submitted a run while a third member was still joining,
     * so that run was planned over two members. Its lease ran out while it was busy starting the run, and
     * the member that picked the pipeline up replaced the run with one planned over all three. When that
     * driver was killed, the first member took the claim back and still compared who is in sight against
     * its own older run - which never included the killed member - so nothing it was planned over had
     * gone, and the pipeline was left failed for a person with nothing in any log saying why.
     */
    @Test
    void aTakeoverJudgesTheRunTheClaimCarriesRatherThanAnOlderOneThisMemberSubmitted() {
        // node-c is committed but not in sight yet, so the run this member submits is planned over two.
        membership.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        membership.canCommit(Set.of("node-a", "node-b"));
        assertThat(ownership.permit("orders").granted()).isTrue();
        assertThat(ownership.beginExecution("orders").allowed()).isTrue();
        membership.canCommit(Set.of("node-a", "node-b", "node-c"));

        // This member's lease runs out while it is busy, and node-c replaces the run with one over all three.
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());
        PipelineActuationOwnership nodeC = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-c", "boot-c"), membership,
                new ClusterWorkloadClaims(claims, membership), TTL, RENEW, nanos::get);
        assertThat(nodeC.permit("orders").granted()).isTrue();
        assertThat(nodeC.beginExecution("orders").allowed()).isTrue();
        assertThat(ownership.permit("orders").granted())
                .as("this member no longer drives the pipeline").isFalse();

        // node-c is killed: it goes out of sight, its lease runs out, and this member takes the pipeline back.
        membership.canCommit(Set.of("node-a", "node-b"));
        claims.elapse(TTL.plusSeconds(1));
        nanos.addAndGet(RENEW.toNanos());
        PipelineActuationOwnership.Permit taken = ownership.permit("orders");
        assertThat(taken.granted()).isTrue();
        assertThat(taken.claim().executionNodeIds())
                .as("the run the claim carries is node-c's, planned over all three")
                .containsExactlyInAnyOrder("node-a", "node-b", "node-c");

        // That run died with node-c, and its failure is recorded under the claim this member holds now.
        admission.recordFailure("orders");

        assertThat(admission.admits("orders"))
                .as("node-c carried part of the run this claim carries and is gone; the run this member "
                        + "planned before node-c was in sight is not the run that died")
                .isTrue();
    }

    /**
     * The other half: taking the claim back is not by itself a sign the run is somebody else's. A member
     * joining moves the committed revision, and every holder takes its claim once more under the new one -
     * the same owner, so the same generation, still carrying the run it submitted. Forgetting what that run
     * was planned over there would make its every death read as inherited, and a member starting up would
     * restart a connector defect that happened to die around then.
     */
    @Test
    void aRunThisMemberTakesBackUnderANewRevisionIsStillJudgedByWhatItWasPlannedOver() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);

        committed(8, "node-a", "node-b", "node-c");
        nanos.addAndGet(RENEW.toNanos() + 1);
        assertThat(ownership.permit("orders").granted())
                .as("the claim was granted under a cluster that has changed, so it has to be taken again")
                .isFalse();
        nanos.addAndGet(RENEW.toNanos() + 1);
        PipelineActuationOwnership.Permit again = ownership.permit("orders");
        assertThat(again.granted()).isTrue();
        assertThat(again.claim().topologyRevision()).isEqualTo(8);
        assertThat(again.claim().executionGeneration())
                .as("still the run this member submitted")
                .isEqualTo(1);

        assertThat(admission.admits("orders"))
                .as("every member this run was planned over is still here, and the one that joined takes "
                        + "nothing away from it, so this death is the pipeline's own")
                .isFalse();
    }

    /**
     * A failed run that is not rebuilt says why, once for each reason rather than on every pass: a pipeline
     * left failed with nothing saying why cannot be told apart from one this question was never asked about.
     */
    @Test
    void aRefusalSaysWhyOnceForEachReason() {
        List<String> logged = loggedBy(() -> {
            committed(7, "node-a", "node-b");
            submitRunUnder(7);

            assertThat(admission.admits("orders")).isFalse();
            assertThat(admission.admits("orders")).isFalse();
            nanos.addAndGet(DETECTION.toNanos());
            assertThat(admission.admits("orders")).isFalse();
            membership.canCommit(Set.of("node-a", "node-b"));
            assertThat(admission.admits("orders"))
                    .as("the view published after the detection window records the failure as the run's own")
                    .isFalse();
            assertThat(admission.admits("orders")).isFalse();
        });

        assertThat(logged).satisfiesExactly(
                first -> assertThat(first).contains("orders")
                        .contains("every member its run was planned over is still in sight"),
                second -> assertThat(second).contains("orders")
                        .contains("recorded as its own before any member it was planned over left"));
    }

    /** A later run of the same pipeline refused for the same reason is a new failure, and says so again. */
    @Test
    void aLaterRunRefusedForTheSameReasonSaysWhyAgain() {
        List<String> logged = loggedBy(() -> {
            committed(7, "node-a", "node-b");
            submitRunUnder(7);
            assertThat(admission.admits("orders")).isFalse();

            // Somebody starts the pipeline again, and the run submitted for it fails the same way.
            assertThat(ownership.beginExecution("orders").allowed()).isTrue();
            assertThat(admission.admits("orders")).isFalse();
        });

        assertThat(logged)
                .as("one line for each run, not one for the pipeline's whole life")
                .hasSize(2)
                .allSatisfy(line -> assertThat(line).contains("orders")
                        .contains("every member its run was planned over is still in sight"));
    }

    /**
     * The budget is spent on one departure, not on a pipeline's whole life. A rebuilt run that goes on
     * running long past the stretch a departure answers for has closed the episode it was submitted for,
     * and a member lost after that starts another. Counted over the life of the process instead, every
     * pipeline of a cluster that has lost members three times is left failed at the fourth - which is what
     * a three-machine run showed: a rebuild after a healthy quarter of an hour counted as the second attempt.
     */
    @Test
    void eachDepartureLongAfterTheLastRebuildRecoveredGetsABudgetOfItsOwn() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);
        long revision = 7;
        for (int loss = 1; loss <= ClusterRebuildAdmission.MAX_ATTEMPTS + 1; loss++) {
            String staying = loss % 2 == 1 ? "node-b" : "node-c";
            committed(++revision, "node-a", staying);

            assertThat(admission.admits("orders"))
                    .as("departure %s, long after the run rebuilt for the one before it recovered", loss)
                    .isTrue();

            // The rebuild; then the member comes back, and the rebuilt run goes on running well.
            submitRunUnder(revision);
            committed(++revision, "node-a", "node-b", "node-c");
            nanos.addAndGet(BACKOFF.multipliedBy(2L * ClusterRebuildAdmission.MAX_ATTEMPTS).toNanos());
        }
    }

    /**
     * The other half: replacements the cluster keeps killing while it has not settled are one departure's
     * attempts, however many new runs they were. Resetting the budget for every new run would be the restart
     * loop the budget exists to stop.
     */
    @Test
    void replacementsTheClusterKeepsKillingWithinTheStretchShareOneBudget() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);
        long revision = 7;
        for (int loss = 1; loss <= ClusterRebuildAdmission.MAX_ATTEMPTS; loss++) {
            String staying = loss % 2 == 1 ? "node-b" : "node-c";
            committed(++revision, "node-a", staying);
            assertThat(admission.admits("orders")).as("attempt %s", loss).isTrue();
            assertThat(ownership.beginExecution("orders").allowed()).as("replacement %s", loss).isTrue();
            nanos.addAndGet(BACKOFF.toNanos());
        }
        // The last replacement is planned over node-a and node-b; node-b goes too.
        committed(++revision, "node-a", "node-c");

        assertThat(admission.admits("orders"))
                .as("each replacement died within the stretch of the last rebuild, so the budget is spent")
                .isFalse();
    }

    /**
     * A spent budget stays spent for the run it was spent on, however long that run then stays failed. What
     * gives a departure a budget of its own is the run a rebuild put in place running past the stretch before
     * it fails; a replacement that died at once has not done that, and waiting long enough beside it is not
     * the same thing - nothing would then stop the cycle of rebuilds starting again, every stretch, for ever.
     */
    @Test
    void aSpentBudgetIsNotGivenBackByWaitingBesideTheRunItWasSpentOn() {
        committed(7, "node-a", "node-b", "node-c");
        submitRunUnder(7);
        long revision = 7;
        for (int loss = 1; loss <= ClusterRebuildAdmission.MAX_ATTEMPTS; loss++) {
            String staying = loss % 2 == 1 ? "node-b" : "node-c";
            committed(++revision, "node-a", staying);
            assertThat(admission.admits("orders")).as("attempt %s", loss).isTrue();
            assertThat(ownership.beginExecution("orders").allowed()).as("replacement %s", loss).isTrue();
            nanos.addAndGet(BACKOFF.toNanos());
        }
        // The last replacement is planned over node-a and node-b; node-b goes too, and stays gone.
        committed(++revision, "node-a", "node-c");
        assertThat(admission.admits("orders")).as("the budget is spent").isFalse();

        nanos.addAndGet(BACKOFF.multipliedBy(2L * ClusterRebuildAdmission.MAX_ATTEMPTS).toNanos());

        assertThat(admission.admits("orders"))
                .as("the same failed run, long after: nothing was started and nothing moved since the budget "
                        + "ran out")
                .isFalse();
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

    /** What the admission logged while {@code scenario} ran. */
    private static List<String> loggedBy(Runnable scenario) {
        Logger logger = (Logger) LoggerFactory.getLogger(ClusterRebuildAdmission.class);
        ListAppender<ILoggingEvent> written = new ListAppender<>();
        written.start();
        logger.addAppender(written);
        try {
            scenario.run();
        } finally {
            logger.detachAppender(written);
            written.stop();
        }
        return written.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
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
