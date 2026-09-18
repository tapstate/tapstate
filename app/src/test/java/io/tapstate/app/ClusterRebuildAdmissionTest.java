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
 * real membership change rather than a stubbed answer -- the question this decides is "did the cluster
 * move under this run", and a stub would be deciding it in the test instead of measuring it.
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
    void aRebuiltRunGivesTheBudgetBackByBeingFencedUnderTheClusterThatIsThereNow() {
        committed(7, "node-a", "node-b");
        submitRunUnder(7);
        committed(8, "node-a");
        assertThat(admission.admits("orders")).isTrue();

        // What a rebuild does: the holder takes the next execution generation, and it takes it under the
        // membership committed now. Nothing has to clear the count -- the comparison stops being true.
        submitRunUnder(8);

        assertThat(admission.admits("orders")).isFalse();

        committed(9, "node-a");

        assertThat(admission.admits("orders"))
                .as("and the next change under it is a fresh budget, not the tail of the previous one")
                .isTrue();
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
