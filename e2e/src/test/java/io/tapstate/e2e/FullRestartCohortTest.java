package io.tapstate.e2e;

import io.tapstate.control.core.ClusterClaimView;
import io.tapstate.control.core.ClusterPipelineView;
import io.tapstate.control.core.ClusterProcessorView;
import io.tapstate.control.core.ClusterTopologyView;
import io.tapstate.control.core.ClusterVertexView;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Waiting for a readable projection must not make partial placement or expired authority ready. */
class FullRestartCohortTest {
    private static final String PIPELINE = "full-restart-pipeline";
    private static final List<String> MEMBERS = List.of("full-restart-a", "full-restart-b", "full-restart-c");

    @Test
    void returnsTheClaimFromTheCompleteObservedCohort() {
        ClusterClaimView claim = claim(true);
        assertThat(AFullClusterRestartRebuildsDesiredPipelinesFromMongoTruthIT.cohortClaim(
                view(PIPELINE, claim, MEMBERS), PIPELINE)).containsSame(claim);
    }

    @Test
    void aPartialCohortIsNotReady() {
        assertThat(AFullClusterRestartRebuildsDesiredPipelinesFromMongoTruthIT.cohortClaim(
                view(PIPELINE, claim(true), MEMBERS.subList(0, 2)), PIPELINE)).isEmpty();
    }

    @Test
    void aForeignParticipantDoesNotSatisfyTheExactCohort() {
        assertThat(AFullClusterRestartRebuildsDesiredPipelinesFromMongoTruthIT.cohortClaim(
                view(PIPELINE, claim(true), List.of("full-restart-a", "full-restart-b", "foreign")), PIPELINE))
                .isEmpty();
    }

    @Test
    void missingOrExpiredAuthorityDoesNotSatisfyPlacement() {
        assertThat(AFullClusterRestartRebuildsDesiredPipelinesFromMongoTruthIT.cohortClaim(
                view(PIPELINE, null, MEMBERS), PIPELINE)).isEmpty();
        assertThat(AFullClusterRestartRebuildsDesiredPipelinesFromMongoTruthIT.cohortClaim(
                view(PIPELINE, claim(false), MEMBERS), PIPELINE)).isEmpty();
    }

    @Test
    void anotherPipelineCannotSupplyItsPlacementOrClaim() {
        assertThat(AFullClusterRestartRebuildsDesiredPipelinesFromMongoTruthIT.cohortClaim(
                view("another-pipeline", claim(true), MEMBERS), PIPELINE)).isEmpty();
    }

    private static ClusterClaimView claim(boolean leased) {
        return new ClusterClaimView(PIPELINE, MEMBERS.getFirst(), "original-boot", 2, 4, 6, leased);
    }

    private static ClusterTopologyView view(String pipeline, ClusterClaimView claim, List<String> participants) {
        List<ClusterProcessorView> processors = participants.stream().map(node -> new ClusterProcessorView(
                participants.indexOf(node), 0, "native-" + node, node, null, Map.of(), Map.of())).toList();
        ClusterVertexView vertex = new ClusterVertexView("source", 3, participants.size(), 1, "execution", processors);
        ClusterPipelineView observed = new ClusterPipelineView(pipeline, claim, List.of(), null, List.of(),
                List.of(), List.of(vertex));
        return new ClusterTopologyView("full-restart-cluster", 6L, List.of(), List.of(observed));
    }
}
