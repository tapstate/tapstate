package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A run is planned over the members that were committed when it was submitted, and a member that joins
 * afterwards is given none of it. Leaving the run alone is the decision; naming the new member's state
 * is what keeps that decision readable from outside, so that a member sitting idle because nobody has
 * rebalanced does not look like a member sitting idle because something is wrong.
 */
class AMemberJoiningLeavesTheRunAloneTest {

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration RENEW = Duration.ofSeconds(10);
    private static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a");

    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final ClusterMembershipGate membership = new ClusterMembershipGate(production());
    private final AtomicLong nanos = new AtomicLong();
    private final PipelineActuationOwnership ownership = new PipelineActuationOwnership(
            "cluster-a", NODE_A, membership, new ClusterWorkloadClaims(claims, membership), TTL, RENEW,
            nanos::get);

    @Test
    void aMemberCommittedAfterTheRunWasPlannedIsAwaitingRebalance() {
        committed(7, "node-a", "node-b");
        submitARun();

        committed(8, "node-a", "node-b", "node-c");

        assertThat(ownership.runMembership("orders"))
                .as("the run keeps the members it was planned over; the one that joined carries none of "
                        + "it and waits for a rebalance nobody has asked for yet")
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "node-a", MemberRunState.PARTICIPATING,
                        "node-b", MemberRunState.PARTICIPATING,
                        "node-c", MemberRunState.AWAITING_REBALANCE));
    }

    @Test
    void aMemberDrivingNoRunOfThePipelineSaysNothingRatherThanSayingNobodyIsWaiting() {
        committed(7, "node-a", "node-b");
        assertThat(ownership.permit("orders").granted()).isTrue();

        assertThat(ownership.runMembership("orders"))
                .as("holding the claim is not having submitted a run: with no run there is no set of "
                        + "members it was planned over, and an empty answer here reads as 'cannot say'")
                .isEmpty();
    }

    @Test
    void theNextRunIsPlannedOverTheMemberThatJoined() {
        committed(7, "node-a", "node-b");
        submitARun();
        committed(8, "node-a", "node-b", "node-c");

        // What a rebalance is, at this level: not a redistribution of the run that is going, but the
        // next run being planned over the cluster that is there when it is submitted.
        takeTheClaimAgainUnderTheClusterThatIsHereNow();
        assertThat(ownership.beginExecution("orders").allowed()).isTrue();

        assertThat(ownership.runMembership("orders").values())
                .as("so the state is not sticky: it describes a run, and this is a different run")
                .containsOnly(MemberRunState.PARTICIPATING);
    }

    /** Installs a committed membership at {@code revision} and lets the gate see those nodes. */
    private void committed(long revision, String... nodeIds) {
        membership.install(new ClusterMembership("cluster-a", revision, Set.of(nodeIds)));
        membership.canCommit(Set.of(nodeIds));
    }

    private void submitARun() {
        assertThat(ownership.permit("orders").granted()).isTrue();
        assertThat(ownership.beginExecution("orders").allowed()).isTrue();
    }

    /** A claim granted under a cluster that has since moved is refused on renew, then taken afresh. */
    private void takeTheClaimAgainUnderTheClusterThatIsHereNow() {
        nanos.addAndGet(RENEW.toNanos() + 1);
        assertThat(ownership.permit("orders").granted())
                .as("the renew is refused: this claim was granted under a cluster that is gone")
                .isFalse();
        nanos.addAndGet(RENEW.toNanos() + 1);
        assertThat(ownership.permit("orders").granted()).isTrue();
    }

    private static ClusterProperties production() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        return properties;
    }
}
