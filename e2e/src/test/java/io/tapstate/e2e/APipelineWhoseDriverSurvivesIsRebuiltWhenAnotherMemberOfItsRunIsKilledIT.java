package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
 * <p><b>Which member to kill is read off the cluster.</b> Where each piece of a run goes is not up to this
 * case, and for as long as the membership holds it is the same every time: stopping a pipeline and
 * starting it again puts every piece back where it was, so a restart is no second chance of a split. The
 * case therefore starts pipelines one at a time, each with pieces of its own, and stops at the first
 * whose run has a piece on a member other than its driver. The member killed is that other member, and
 * only once a run is known to be on it -- killing a member that carries nothing of the run would pass
 * against any product.
 *
 * <p>A placement is judged only once the cluster's readings of the run have arrived from every member.
 * Each member reports on its own schedule, seconds apart, and a piece on a member that has not reported
 * yet is missing from the answer: read before then, a split run looks carried entirely by its driver.
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

    /**
     * How many pipelines are started, one at a time, before the case says none of their runs was split.
     * Each brings pieces of its own to be placed, so each is a fresh chance of a split.
     */
    private static final int MOST_PIPELINES = 10;

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
            String store = SharedMongo.replicaSetUrl("e2e_kept_driver_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-kept-driver")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);

                // Applied as a whole each time a pipeline is added, the way a workspace is applied.
                Map<String, String> resources = new LinkedHashMap<>();
                Split split = null;
                for (int i = 1; i <= MOST_PIPELINES && split == null; i++) {
                    Lane lane = new Lane("kept_driver_pipe_" + i, "kept_driver_src_" + i, "kept_driver_tgt_" + i,
                            Files.createDirectories(directory.resolve("src" + i)),
                            Files.createDirectories(directory.resolve("tgt" + i)));
                    files.seed(lane.sourceAddress(), TABLE, SeedRows.generated(SEEDED_ROWS));
                    control.discoverSchema(lane.sourceId(), E2eConnectorJar.CONNECTOR_ID,
                            Map.of("uri", lane.source().toString()));
                    resources.put(lane.sourceId() + ".tap.yml",
                            Workspaces.cdcSourceYaml(lane.sourceId(), lane.source()));
                    resources.put(lane.targetId() + ".tap.yml", Workspaces.targetYaml(lane.targetId(), lane.target()));
                    resources.put(lane.pipeline() + ".tap.yml",
                            Workspaces.pipelineYaml(lane.pipeline(), lane.sourceId(), lane.targetId(), TABLE));
                    control.apply(resources);
                    control.lifecycle(lane.pipeline(), LifecycleVerb.START);
                    awaitRunningAndMeasuredEverywhere(control, files, lane);
                    split = aSplitRun(control, lane);
                }
                assertThat(split)
                        .describedAs("none of the %d pipelines started had a piece of its run on a member other "
                                + "than its driver, so there was no member this case could kill that a run was "
                                + "on", MOST_PIPELINES)
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

    private static void awaitRunningAndMeasuredEverywhere(ControlPlane control, FileEndpoints files, Lane lane) {
        Await.until(lane.pipeline() + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(lane.pipeline()).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(lane.pipeline())));
        Await.until("the seeded rows of " + lane.pipeline() + " to cross", Duration.ofMinutes(1),
                () -> files.count(lane.targetAddress(), TABLE) >= SEEDED_ROWS,
                () -> "rows at target = " + files.count(lane.targetAddress(), TABLE));
        List<String> everyMember = control.clusterMembers().stream().map(ClusterMemberFacts::memberUuid).toList();
        Await.until("the cluster's readings of " + lane.pipeline() + "'s run to arrive from every member",
                Duration.ofMinutes(1),
                () -> control.membersMeasuring(lane.pipeline()).containsAll(everyMember),
                () -> "measured from " + control.membersMeasuring(lane.pipeline()) + " of " + everyMember);
        Await.until("the cluster to say where " + lane.pipeline() + "'s run is placed", Duration.ofMinutes(1),
                () -> !control.membersCarryingPartOf(lane.pipeline()).isEmpty(),
                () -> "carried by " + control.membersCarryingPartOf(lane.pipeline()));
    }

    /** This pipeline, when its run has a piece on a member other than the one driving it; null otherwise. */
    private static Split aSplitRun(ControlPlane control, Lane lane) {
        String driver = control.pipelineControllerOf(lane.pipeline()).orElse(null);
        if (driver == null) {
            return null;
        }
        Set<String> others = new TreeSet<>(control.membersCarryingPartOf(lane.pipeline()));
        others.remove(driver);
        return others.isEmpty() ? null : new Split(lane, driver, others.iterator().next());
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
