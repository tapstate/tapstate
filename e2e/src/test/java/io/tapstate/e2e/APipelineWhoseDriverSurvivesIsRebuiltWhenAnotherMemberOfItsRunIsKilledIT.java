package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A member carrying part of a pipeline's run is killed while the member driving that pipeline stays, and
 * the pipeline carries on with nobody asking it to.
 *
 * <p>The other failover case kills the driver, and there the run is picked up by whoever inherits the
 * pipeline. Here nothing changes hands: the driver is alive and still holds the pipeline, and what it has
 * to notice is that the run it submitted lost a member it was planned over. The engine ends that run the
 * moment the connection drops -- before the cluster has finished deciding the member is gone -- so at the
 * instant of the death nothing is missing yet; and the committed membership never drops a member at all.
 * A driver that judged the death only by what was committed recorded it as the pipeline's own and left
 * the pipeline failed for a person, which is what a run on two real machines found.
 *
 * <p><b>Which member to kill is read off the cluster.</b> The engine places each piece of a run for
 * itself, and where it puts one is not up to anybody, so the case starts a few pipelines and looks for
 * one whose run has a piece on a member other than its driver; when none has, it stops and starts them so
 * their pieces are placed again. The member killed is that other member, and only once a run is known to
 * be on it -- killing a member that carries nothing of the run would pass against any product.
 *
 * <p><b>Nothing is asked of the product between the kill and the assertions.</b>
 *
 * <p>What this lane cannot see, said as the other failover case says it: the harness connector hands the
 * product no position and replays its table on every run, so where the replacement run resumes is
 * asserted a level down. What this case adds is the replacement happening at all while the driver stays.
 */
class APipelineWhoseDriverSurvivesIsRebuiltWhenAnotherMemberOfItsRunIsKilledIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    /** Enough pipelines that a split placement is the ordinary outcome of one start, not a lucky one. */
    private static final int PIPELINES = 3;

    /** How many times the pipelines are placed before the case says it never saw a run split. */
    private static final int PLACEMENTS = 6;

    /** Bound on the replacement: the cluster has to notice the member is gone, then the run is rebuilt. */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRunThatLosesAMemberIsReplacedByTheDriverThatDidNotGoAnywhere(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));

        try (FileEndpoints files = new FileEndpoints()) {
            List<Lane> lanes = new ArrayList<>();
            Map<String, String> resources = new LinkedHashMap<>();
            for (int i = 1; i <= PIPELINES; i++) {
                Lane lane = new Lane("kept_driver_pipe_" + i, "kept_driver_src_" + i, "kept_driver_tgt_" + i,
                        Files.createDirectories(directory.resolve("src" + i)),
                        Files.createDirectories(directory.resolve("tgt" + i)));
                files.seed(lane.sourceAddress(), TABLE, SeedRows.generated(SEEDED_ROWS));
                resources.put(lane.sourceId() + ".tap.yml", Workspaces.cdcSourceYaml(lane.sourceId(), lane.source()));
                resources.put(lane.targetId() + ".tap.yml", Workspaces.targetYaml(lane.targetId(), lane.target()));
                resources.put(lane.pipeline() + ".tap.yml",
                        Workspaces.pipelineYaml(lane.pipeline(), lane.sourceId(), lane.targetId(), TABLE));
                lanes.add(lane);
            }

            String store = SharedMongo.replicaSetUrl("e2e_kept_driver_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-kept-driver")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                for (Lane lane : lanes) {
                    control.discoverSchema(lane.sourceId(), E2eConnectorJar.CONNECTOR_ID,
                            Map.of("uri", lane.source().toString()));
                }
                control.apply(resources);
                lanes.forEach(lane -> control.lifecycle(lane.pipeline(), LifecycleVerb.START));

                Split split = null;
                for (int placement = 1; placement <= PLACEMENTS && split == null; placement++) {
                    if (placement > 1) {
                        placeAgain(control, lanes);
                    }
                    for (Lane lane : lanes) {
                        awaitRunningAndPlaced(control, files, lane);
                    }
                    split = aSplitRun(control, lanes);
                }
                assertThat(split)
                        .describedAs("after %d placements no pipeline had a piece of its run on a member other "
                                + "than its driver, so there was no member this case could kill that the "
                                + "run was on", PLACEMENTS)
                        .isNotNull();

                Split chosen = split;
                Lane lane = chosen.lane();
                ControlPlane driver = cluster.memberOtherThan(chosen.victim());
                long generationBefore = driver.executionGenerationOf(lane.pipeline()).orElseThrow();
                cluster.processCarrying(chosen.victim()).kill();

                // Nothing is asked of the product from here until the assertions below.
                long rowsAtTheKill = files.count(lane.targetAddress(), TABLE);
                files.cdc(lane.sourceAddress(), TABLE, CdcOp.INSERT, 1);

                Await.until("a change made after the kill to reach " + lane.pipeline() + "'s target, which "
                                + "only a replaced run can carry", TAKEOVER,
                        () -> files.count(lane.targetAddress(), TABLE) > rowsAtTheKill,
                        () -> "rows at target = " + files.count(lane.targetAddress(), TABLE)
                                + ", was " + rowsAtTheKill + " when " + chosen.victim() + " was killed; the "
                                + "pipeline is " + driver.state(lane.pipeline()) + " and is driven by "
                                + driver.pipelineControllerOf(lane.pipeline()));

                assertThat(driver.state(lane.pipeline()))
                        .describedAs("the pipeline is running again, and nobody asked it to be")
                        .contains(PipelineState.RUNNING);
                assertThat(driver.pipelineControllerOf(lane.pipeline()))
                        .describedAs("the member driving it never went anywhere, so the pipeline did not change "
                                + "hands: this is the driver replacing its own run, not an heir picking one up")
                        .contains(chosen.driver());
                assertThat(driver.executionGenerationOf(lane.pipeline()))
                        .describedAs("the dead run was replaced by exactly one new execution")
                        .contains(generationBefore + 1);
            }
        }
    }

    /** Stops every pipeline and starts it again, which places the pieces of each run afresh. */
    private static void placeAgain(ControlPlane control, List<Lane> lanes) {
        for (Lane lane : lanes) {
            control.stop(lane.pipeline(), false);
        }
        for (Lane lane : lanes) {
            Await.until(lane.pipeline() + " to stop", Duration.ofMinutes(1),
                    () -> control.state(lane.pipeline()).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(lane.pipeline())));
        }
        lanes.forEach(lane -> control.lifecycle(lane.pipeline(), LifecycleVerb.START));
    }

    private static void awaitRunningAndPlaced(ControlPlane control, FileEndpoints files, Lane lane) {
        Await.until(lane.pipeline() + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(lane.pipeline()).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(lane.pipeline())));
        Await.until("the seeded rows of " + lane.pipeline() + " to cross", Duration.ofMinutes(1),
                () -> files.count(lane.targetAddress(), TABLE) >= SEEDED_ROWS,
                () -> "rows at target = " + files.count(lane.targetAddress(), TABLE));
        Await.until("the cluster to say where " + lane.pipeline() + "'s run is placed", Duration.ofMinutes(1),
                () -> !control.membersCarryingPartOf(lane.pipeline()).isEmpty(),
                () -> "carried by " + control.membersCarryingPartOf(lane.pipeline()));
    }

    /** A pipeline whose run has a piece on a member other than the one driving it, or null when none has. */
    private static Split aSplitRun(ControlPlane control, List<Lane> lanes) {
        for (Lane lane : lanes) {
            String driver = control.pipelineControllerOf(lane.pipeline()).orElse(null);
            if (driver == null) {
                continue;
            }
            Set<String> others = new TreeSet<>(control.membersCarryingPartOf(lane.pipeline()));
            others.remove(driver);
            if (!others.isEmpty()) {
                return new Split(lane, driver, others.iterator().next());
            }
        }
        return null;
    }

    private record Lane(String pipeline, String sourceId, String targetId, Path source, Path target) {
        EndpointAddress sourceAddress() {
            return EndpointAddress.uri(source.toString());
        }

        EndpointAddress targetAddress() {
            return EndpointAddress.uri(target.toString());
        }
    }

    private record Split(Lane lane, String driver, String victim) {
    }
}
