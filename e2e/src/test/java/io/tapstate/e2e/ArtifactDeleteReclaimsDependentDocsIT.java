package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that removing a pipeline takes its bookkeeping with it, and that the reconciliation set a
 * converger works from no longer names it.
 *
 * <p>Asserting only that the artifact is gone would pass against the failure this case exists to catch.
 * A pipeline whose intent document survives its artifact is a pipeline the converger keeps reconciling
 * forever: it reads an id out of the desired set, goes looking for the resource that says what to run, and
 * finds nothing - on every pass, with no error anywhere, because each individual read is behaving exactly
 * as designed. Nothing above the store can see that state, which is why the assertion is taken against the
 * store's own documents rather than through a read face.
 *
 * <p>The three documents are asserted individually rather than as a group, because they are reclaimed by
 * three separate calls and a reclaim that dropped one of them would otherwise hide behind the two that
 * worked. The published observation is also checked through the wire, where its disappearance is the
 * visible half: a status read of a removed pipeline has to answer "nothing published" rather than serving
 * the last observation of a pipeline that no longer exists.
 *
 * <p>Each document is asserted present before the removal as well as absent after it. An absence that was
 * never a presence witnesses nothing, and these documents only exist because the pipeline really ran -
 * a case whose run never got far enough to write a checkpoint would otherwise report a clean reclaim of
 * nothing at all.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class ArtifactDeleteReclaimsDependentDocsIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void removingAPipelineReclaimsItsIntentCheckpointAndObservation(Tiers tier, @TempDir Path directory)
            throws Exception {
        String storeUri = storeUri("delete_reclaims", tier);
        try (ServerHandle server = tier.launch(storeUri);
                StoreDocuments documents = StoreDocuments.at(storeUri)) {
            RunningPipeline running = RunningPipeline.started(server, directory);
            ControlPlane control = running.control();
            String pipelineId = running.pipelineId();

            // A checkpoint is written as the run proceeds rather than at the start verb, so the case waits
            // for all three to exist. Removing documents that were never there would witness nothing.
            Await.until(
                    "the bookkeeping of " + pipelineId + " to exist",
                    () -> documents.holds(MongoStorePort.PIPELINE_DESIRED, pipelineId)
                            && documents.holds(MongoStorePort.PIPELINE_STATE, pipelineId)
                            && documents.observationOf(pipelineId).filter(saved ->
                                    pipelineId.equals(saved.observation().pipelineId())
                                            && saved.observation().state() == PipelineState.RUNNING
                                            && saved.scope().filter(scope ->
                                                    documents.holdsObservationCurrent(pipelineId, scope)).isPresent())
                                    .isPresent(),
                    () -> "desired=" + documents.holds(MongoStorePort.PIPELINE_DESIRED, pipelineId)
                            + " state=" + documents.holds(MongoStorePort.PIPELINE_STATE, pipelineId)
                            + " observation=" + documents.observationOf(pipelineId)
                                    .map(saved -> saved.observation().state() + "/" + saved.scope()));
            var observed = documents.observationOf(pipelineId).orElseThrow();
            var scope = observed.scope().orElseThrow();
            assertThat(observed.observation().pipelineId()).isEqualTo(pipelineId);
            assertThat(observed.observation().state()).isEqualTo(PipelineState.RUNNING);
            assertThat(documents.holdsObservationCurrent(pipelineId, scope))
                    .as("the real readable observation has a matching physical current descriptor").isTrue();
            assertThat(documents.ids(MongoStorePort.PIPELINE_DESIRED))
                    .as("the set a converger reconciles from, before the removal")
                    .contains(pipelineId);

            running.stopAndSettle();
            var stopped = documents.observationOf(pipelineId).orElseThrow();
            assertThat(stopped.observation().state()).isEqualTo(PipelineState.STOPPED);
            assertThat(stopped.scope()).as("STOP retains the observation's factual cleanup scope").contains(scope);
            assertThat(documents.holdsObservationCurrent(pipelineId, scope)).isTrue();
            control.deleteArtifact(pipelineId, control.contentHash(pipelineId));

            assertThat(control.artifact(pipelineId))
                    .as("the artifact itself")
                    .isEmpty();
            assertThat(documents.holds(MongoStorePort.PIPELINE_DESIRED, pipelineId))
                    .as("the intent document - left behind, it is what keeps a converger chasing a "
                            + "pipeline whose definition no longer exists, silently and forever")
                    .isFalse();
            assertThat(documents.holds(MongoStorePort.PIPELINE_STATE, pipelineId))
                    .as("the checkpoint document")
                    .isFalse();
            assertThat(documents.holds(MongoStorePort.PIPELINE_OBSERVATION, pipelineId))
                    .as("the legacy observation residue")
                    .isFalse();
            assertThat(documents.ids(MongoStorePort.PIPELINE_DESIRED))
                    .as("the reconciliation set no longer names it, which is the reclaim's whole purpose")
                    .doesNotContain(pipelineId);
            // Telemetry cleanup is asynchronous and removes only the deleted incarnation's descriptors.
            Await.until(
                    "the deleted observation incarnation to leave latest storage",
                    () -> documents.observationOf(pipelineId).isEmpty()
                            && !documents.holdsObservationIncarnation(pipelineId, scope.pipelineIncarnationId()),
                    () -> "observation=" + documents.observationOf(pipelineId)
                            + " old incarnation retained=" + documents.holdsObservationIncarnation(
                                    pipelineId, scope.pipelineIncarnationId()));
            assertThat(documents.observationOf(pipelineId)).as("the logical latest observation").isEmpty();
            assertThat(documents.holdsObservationIncarnation(pipelineId, scope.pipelineIncarnationId()))
                    .as("the deleted incarnation's physical current, pending and continuation descriptors").isFalse();
            // The visible half. Read as a refusal rather than an absence on purpose: the product answers
            // its own "no such pipeline" code here, which is a different statement from "applied, nothing
            // published yet" - and a case that accepted either would pass while the status face served the
            // last observation of a pipeline that no longer exists.
            assertThat(control.stateExpectingRefusal(pipelineId).code())
                    .as("a status read of a removed pipeline names it as unknown rather than answering "
                            + "out of a stale observation")
                    .isEqualTo("lifecycle.unknown-pipeline");
        }
    }

    private static String storeUri(String name, Tiers tier) {
        return SharedMongo.replicaSetUrl(name + "_" + tier.name().toLowerCase(Locale.ROOT));
    }
}
