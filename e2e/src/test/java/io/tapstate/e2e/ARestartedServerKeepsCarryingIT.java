package io.tapstate.e2e;

import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pipeline that was carrying changes before the server was restarted is still carrying them after it.
 *
 * <p>Every other case that brings a server up twice gives each run a store of its own, so no case has ever
 * asked what happens when the second run reads the first one's store. That is the shape a user meets first:
 * the process is restarted -- to upgrade it, or for any other reason -- and comes back onto the state it
 * left behind. What the store carries across is the pipeline's desired intent and its checkpoint, both
 * saying RUNNING, so the second run adopts a pipeline it did not start.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>Carrying is proven live before the restart.</b> A change is laid down and watched to the target
 *       while the first server is still up. Without it, a target that does not move after the restart
 *       cannot be told apart from a pipeline that never carried anything at all, and the case would report
 *       the same failure for a bug it was not written to find.</li>
 *   <li><b>The second server is a different server.</b> It is launched on a port of its own, so the control
 *       plane below cannot be answered by the first one; the first is closed before it is launched. An
 *       observation that outlives the process it was watching is how a restart case reports RUNNING for
 *       ever while asserting nothing.</li>
 *   <li><b>Both tiers, because one of them can only pretend to die.</b> In this JVM a close shuts the
 *       context down but leaves everything class loading and static state hold; only the real-process tier
 *       actually ends the process the state was in. A restart case that ran embedded alone would leave
 *       "it never really went away" as an untested explanation of its own green.</li>
 *   <li><b>The target is read before the last change is laid down.</b> Held to exactly what the pre-restart
 *       half left there, so the wait that follows can only be satisfied by a row that crossed after the
 *       restart. A count that already met the bar would pass without carrying anything.</li>
 *   <li><b>The pipeline is asked what it thinks it is doing.</b> RUNNING here is not a nicety: a pipeline
 *       that came back STOPPED or FAILED would be a different defect, visible to its user and refusable by
 *       a verb, and this case would be describing the wrong one.</li>
 * </ul>
 *
 * <p>The source is the change-capture read over a directory that {@link RunningPipeline} sets up, so the
 * source and target outlive the process the way a database would, and no real connector is needed.
 */
class ARestartedServerKeepsCarryingIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aChangeMadeAfterTheServerIsRestartedStillReachesTheTarget(Tiers tier) throws Exception {
        Path directory = Files.createTempDirectory("restarted-server-keeps-carrying");
        // Suffixed by tier: the two runs are the same specification, not the same state, and a shared
        // store would have the second one adopt what the first left rather than what it set up itself.
        String storeUri = SharedMongo.replicaSetUrl("restart_carries_store_" + tier.name().toLowerCase());

        RunningPipeline running;
        long carriedBeforeTheRestart;
        try (ServerHandle first = tier.launch(storeUri)) {
            running = RunningPipeline.started(first, directory);
            long seeded = awaitAtLeast(running, 1, "the seeded rows to reach the target");
            // Live before the restart, so the wait after it is measuring the restart and nothing else.
            running.insertAtSource(1);
            carriedBeforeTheRestart = awaitAtLeast(running, seeded + 1, "a change made before the restart");
        }

        // The process is gone. The store it wrote, the source directory and the target directory are not.
        try (ServerHandle second = tier.launch(storeUri)) {
            ControlPlane control = new ControlPlane(second.baseUrl());
            control.login("e2e", "e2e-password");

            assertThat(control.state(running.pipelineId()))
                    .as("the pipeline the restarted server adopted from the store it read")
                    .contains(PipelineState.RUNNING);
            assertThat(running.rowsAtTarget())
                    .as("the target before anything new is laid down - the wait below has to be earned")
                    .isEqualTo(carriedBeforeTheRestart);

            running.insertAtSource(1);
            awaitAtLeast(running, carriedBeforeTheRestart + 1, "a change made after the restart");
        }
    }

    /** Waits for the target to hold at least {@code rows}, and answers what it held once it did. */
    private static long awaitAtLeast(RunningPipeline running, long rows, String what) {
        Await.until(what, () -> running.rowsAtTarget() >= rows, () -> running.rowsAtTarget() + " rows");
        return running.rowsAtTarget();
    }
}
