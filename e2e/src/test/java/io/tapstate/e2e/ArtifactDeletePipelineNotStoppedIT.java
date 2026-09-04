package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
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
 * The witness that a pipeline which is actually executing cannot be removed, and that stopping it is what
 * makes the removal available.
 *
 * <p>The pipeline here is really running - a connector was registered, a source discovered, rows seeded
 * and a start verb driven, and the case waits for the runtime to publish {@code RUNNING} before it tries
 * anything. That is the point of spending an end-to-end run on this: the gate reads the lifecycle
 * documents a live runtime writes, and a case that planted those documents by hand would witness the gate
 * reading its own fixture rather than the product's state.
 *
 * <p>Both halves are here because either alone permits an outcome this test exists to exclude. The
 * refusal alone is satisfied by a product that refuses every pipeline removal, which would make pipelines
 * undeletable; the success alone is satisfied by a product with no gate at all. Together they place the
 * boundary where the design put it: at whether the pipeline is at rest.
 *
 * <p>The refusal is asserted on the stored bytes as well as the code, for the reason every refusal in this
 * plan is: "refused" and "refused, having already deleted it" answer the same code, and only reading the
 * resource back separates them.
 *
 * <p>What this case deliberately does <em>not</em> try to catch is the window between a stop being
 * requested and the runtime reaching it - the window where the desired state already reads {@code STOPPED}
 * while the pipeline is still executing, and where an implementation consulting desired alone would let the
 * removal through. That window cannot be entered on demand from out here, and a case that raced for it
 * would fail for timing reasons far more often than for the defect. It is pinned deterministically one
 * layer down instead, where both halves of the verdict can simply be set:
 * {@code aRunningPipelineIsRefusedEvenAfterStopWasRequested} and
 * {@code aStoppedPipelineThatWasAskedToStartIsRefused}. What is witnessed here is that the gate is wired to
 * a real runtime's readings at all.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class ArtifactDeletePipelineNotStoppedIT {

    private static final String NOT_STOPPED = "artifact.pipeline-not-stopped";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRunningPipelineIsRefusedAndBecomesRemovableOnceItIsStopped(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (ServerHandle server = tier.launch(storeUri("delete_not_stopped", tier))) {
            RunningPipeline running = RunningPipeline.started(server, directory);
            ControlPlane control = running.control();
            String pipelineId = running.pipelineId();

            ControlPlane.StoredArtifact before = control.artifact(pipelineId).orElseThrow();

            ControlPlane.Refusal refusal =
                    control.deleteArtifactExpectingRefusal(pipelineId, before.contentHash());

            assertThat(refusal.code())
                    .as("the code refusing to remove a pipeline that is still executing")
                    .isEqualTo(NOT_STOPPED);
            assertThat(refusal.params())
                    .as("the refusal reports the state the runtime is actually in, so the author knows "
                            + "what to stop rather than being told only that they may not proceed")
                    .containsEntry("id", pipelineId)
                    .containsEntry("actual", PipelineState.RUNNING.name());
            assertThat(control.artifact(pipelineId))
                    .as("the pipeline after a refused removal - a gate that destroys and then objects "
                            + "answers this same code")
                    .contains(before);

            control.stop(pipelineId, true);
            Await.until(
                    pipelineId + " to reach " + PipelineState.STOPPED,
                    () -> control.state(pipelineId).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));

            // Stopping is not a side effect of the removal; it is the thing the refusal asked the author to
            // do, and having done it the same call goes through.
            control.deleteArtifact(pipelineId, control.contentHash(pipelineId));

            assertThat(control.artifact(pipelineId))
                    .as("the pipeline is removable once it is at rest")
                    .isEmpty();
        }
    }

    private static String storeUri(String name, Tiers tier) {
        return SharedMongo.replicaSetUrl(name + "_" + tier.name().toLowerCase(Locale.ROOT));
    }
}
