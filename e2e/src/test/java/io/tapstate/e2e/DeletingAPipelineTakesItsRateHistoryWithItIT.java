package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The samples a pipeline took while it ran leave with the pipeline, and with nothing else.
 *
 * <p>Two verbs are driven against one history and asked opposite things. A stop - the one that clears
 * the run's state, which is the verb that takes the most - leaves the history standing: the samples are
 * what a rate is drawn from, and a stop that took them would empty a pipeline's past every time it was
 * stopped, which is every time someone changed it. A removal takes them: a history left behind would be
 * read as the past of whatever is applied under the id next.
 *
 * <p>The history is read off the store's own collection, because it has no read face, and it is asserted
 * present before the stop as well as absent after the removal. A pipeline that was never sampled would
 * satisfy "nothing left" on every implementation, including one that reclaims nothing at all.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class DeletingAPipelineTakesItsRateHistoryWithItIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aStopLeavesTheHistoryStandingAndARemovalTakesIt(Tiers tier, @TempDir Path directory) throws Exception {
        String storeUri = storeUri("delete_history", tier);
        try (ServerHandle server = tier.launch(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            RunningPipeline running = RunningPipeline.started(server, directory);
            ControlPlane control = running.control();
            String pipelineId = running.pipelineId();

            // The first sample is drawn from the first observation that carries a counter, so it exists
            // soon after the pipeline reports running. Waiting for it is what makes "none left" below a
            // statement about the removal rather than about a run that was never sampled.
            Await.until(
                    "a sample of " + pipelineId + " to be recorded",
                    () -> documents.rateSamplesOf(pipelineId) > 0,
                    () -> "samples=" + documents.rateSamplesOf(pipelineId));
            long sampledBeforeTheStop = documents.rateSamplesOf(pipelineId);

            running.stopAndSettle();
            assertThat(documents.rateSamplesOf(pipelineId))
                    .as("a stop that clears the run's state leaves the samples the run took")
                    .isGreaterThanOrEqualTo(sampledBeforeTheStop);

            control.deleteArtifact(pipelineId, control.contentHash(pipelineId));

            assertThat(control.artifact(pipelineId)).as("the artifact itself").isEmpty();
            assertThat(documents.rateSamplesOf(pipelineId))
                    .as("the samples of a removed pipeline - left behind, they are the past of whatever is "
                            + "applied under the id next")
                    .isZero();
        }
    }

    private static String storeUri(String name, Tiers tier) {
        return SharedMongo.replicaSetUrl(name + "_" + tier.name().toLowerCase(Locale.ROOT));
    }
}
