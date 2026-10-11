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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A run that loses a member is replaced once, by a run worked out again for the members left, and the new run's plan
 * says which run it replaced and that the member count is what moved its width.
 *
 * <p>A target of three on two members is two writers, one on each: three does not share out over two, and of the
 * two nearest widths - two and four - the smaller is taken. On the one member left it shares out exactly, as three.
 * So the width moves with the members, and it moves back towards the target rather than staying at what the run was
 * built for: a replacement that kept the old width, or that kept working to the old member count, reads as two here.
 *
 * <p>The member killed is the one not driving the pipeline, so the pipeline does not change hands and what is
 * witnessed is the replacement itself. With one writer on each member it carries a piece of the run for certain, and
 * the case still reads that off the cluster before killing it: a kill of a member carrying nothing of the run would
 * pass against any product.
 *
 * <p>Nothing is asked of the product between the kill and the assertions.
 */
class ARunThatLosesAMemberIsPlannedAgainForTheMembersLeftIT {

    private static final String TABLE = "orders";
    private static final String PIPELINE = "replanned_pipe";
    private static final String SOURCE = "replanned_src";
    private static final String TARGET = "replanned_tgt";
    private static final int TARGET_WRITERS = 3;
    private static final long SEEDED_ROWS = 12;

    /** Bound on the replacement: the cluster has to notice the member is gone, then the run is rebuilt. */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theReplacementRunsAtTheWidthTheMembersLeftAllowAndSaysWhatItReplaced(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_replanned_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-replanned")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source));
                resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
                resources.put(PIPELINE + ".tap.yml",
                        Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE, TARGET_WRITERS));
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", failure " + control.failure(PIPELINE));
                Await.until("the seeded rows to cross", Duration.ofMinutes(1),
                        () -> files.count(targetAddress, TABLE) == SEEDED_ROWS,
                        () -> "rows at the target = " + files.count(targetAddress, TABLE));

                ControlPlane.Plan before = Await.answered("the status to carry the run's plan",
                        () -> control.executionPlan(PIPELINE));
                ControlPlane.PlannedNode writersBefore = before.nodeRequested(TARGET_WRITERS);
                assertThat(before.members())
                        .containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(writersBefore.memberCount()).isEqualTo(2);
                assertThat(writersBefore.computedLocal()).as("three does not share out over two").isEqualTo(1);
                assertThat(writersBefore.effective()).as("the smaller of two and four").isEqualTo(2);

                String driver = control.pipelineControllerOf(PIPELINE).orElseThrow();
                String victim = TwoMemberCluster.NODE_A.equals(driver) ? TwoMemberCluster.NODE_B
                        : TwoMemberCluster.NODE_A;
                List<String> everyMember = control.clusterMembers().stream()
                        .map(ClusterMemberFacts::memberUuid).toList();
                Await.until("the cluster to place a piece of the run on " + victim, Duration.ofMinutes(1),
                        () -> control.membersMeasuring(PIPELINE).containsAll(everyMember)
                                && control.membersCarryingPartOf(PIPELINE).contains(victim),
                        () -> "measured from " + control.membersMeasuring(PIPELINE) + " of " + everyMember
                                + ", carried by " + control.membersCarryingPartOf(PIPELINE));
                ControlPlane survivor = cluster.memberOtherThan(victim);
                long generation = survivor.executionGenerationOf(PIPELINE).orElseThrow();
                cluster.processCarrying(victim).kill();

                // Nothing is asked of the product from here until the assertions below.
                long rowsAtTheKill = files.count(targetAddress, TABLE);
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                Await.until("a change made after the kill to reach the target, which only a replaced run can carry",
                        TAKEOVER,
                        () -> files.count(targetAddress, TABLE) > rowsAtTheKill,
                        () -> "rows at the target = " + files.count(targetAddress, TABLE) + ", was " + rowsAtTheKill
                                + " when " + victim + " was killed; the pipeline is " + survivor.state(PIPELINE));

                assertThat(survivor.state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(survivor.pipelineControllerOf(PIPELINE))
                        .as("the pipeline did not change hands").contains(driver);
                assertThat(survivor.executionGenerationOf(PIPELINE))
                        .as("the dead run was replaced by exactly one new execution").contains(generation + 1);

                ControlPlane.Plan after = survivor.executionPlan(PIPELINE).orElseThrow();
                assertThat(after.executionGeneration()).isEqualTo(generation + 1);
                assertThat(after.members()).as("planned for the member left").containsExactly(driver);
                ControlPlane.PlannedNode writersAfter = after.nodeRequested(TARGET_WRITERS);
                assertThat(writersAfter.memberCount()).isEqualTo(1);
                assertThat(writersAfter.computedLocal()).as("three shares out over one exactly").isEqualTo(3);
                assertThat(writersAfter.effective()).isEqualTo(TARGET_WRITERS);
                assertThat(after.replaces())
                        .as("the plan names the run it replaced, and the two members that run was planned over")
                        .isEqualTo(new ControlPlane.Replaced(generation, before.members()));
                assertThat(writersAfter.change()).as("the width moved, and says from what").isNotNull();
                assertThat(writersAfter.change().previousEffective()).isEqualTo(2);
                assertThat(writersAfter.change().causes())
                        .as("the member count moved it; its author did not")
                        .contains("members-changed")
                        .doesNotContain("target-changed");
                assertThat(survivor.awaitingRebalance(PIPELINE))
                        .as("a member that is gone is not one the run is waiting to take in")
                        .isEmpty();
            }
        }
    }
}
