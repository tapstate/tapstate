package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A target of eight writers on three members runs three on each - nine in all - and the status and the cluster say
 * so alike.
 *
 * <p>Eight is a target for the whole cluster, not for each member. The engine runs the same number of processors on
 * every member, so eight cannot be run on three: the two nearest widths are two on each and three on each, six and
 * nine, and nine is the nearer. What the status carries is the plan the run was submitted under - the target, the
 * members, the width worked out from them and that it was rounded up - and what the cluster reports is where each of
 * the run's processors really runs. The two are read apart and held to each other: a plan that said three each and a
 * run of eight, or of two each, would each pass a case that read only one of them.
 *
 * <p>Each member's writers are numbered on that member from nought, and hold one unbroken run of the numbers the whole
 * run gives them, in the same order: the engine numbers a run's processors member by member, so a member's block of
 * numbers is how it tells its own writers from another member's. And every row still arrives, the load and a change
 * after it, which nine writers sharing one target file have to do without losing any of each other's.
 */
class ATargetOfEightRunsThreeWritersOnEachOfThreeMembersIT {

    private static final String TABLE = "orders";
    private static final String PIPELINE = "wide_sink_pipe";
    private static final String SOURCE = "wide_sink_src";
    private static final String TARGET = "wide_sink_tgt";
    private static final String THIRD = "node-c";
    private static final int TARGET_WRITERS = 8;
    private static final int WRITERS = 9;
    private static final int ON_EACH = 3;
    private static final long SEEDED_ROWS = 45;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aTargetOfEightRunsNineWritersThreeOnEachMemberAsPlannedAndEveryRowArrives(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_wide_sink_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-wide-sink")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                RealProcessServer third = cluster.launching(THIRD);
                try {
                    cluster.awaitMembers(3);
                    Await.until(THIRD + " to be committed to the cluster", Duration.ofMinutes(2),
                            () -> control.clusterMembers().stream().anyMatch(member ->
                                    THIRD.equals(member.nodeId()) && "ACTIVE".equals(member.state())),
                            () -> "the members were " + control.clusterMembers());
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

                    ControlPlane.Plan plan = Await.answered("the status to carry the run's plan",
                            () -> control.executionPlan(PIPELINE));
                    ControlPlane.PlannedNode writers = plan.nodeRequested(TARGET_WRITERS);
                    assertThat(plan.members())
                            .as("the run was planned over all three members")
                            .containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, THIRD);
                    assertThat(writers.scope()).as("a target of eight runs at the engine's own width")
                            .isEqualTo("native");
                    assertThat(writers.memberCount()).isEqualTo(3);
                    assertThat(writers.computedLocal()).as("three on each is the nearer of six and nine")
                            .isEqualTo(ON_EACH);
                    assertThat(writers.effective()).isEqualTo(WRITERS);
                    assertThat(writers.reasons()).as("and the plan says it rounded up").containsExactly("rounded-up");
                    assertThat(plan.executionGeneration())
                            .as("the plan is the one the run holding the pipeline was submitted under")
                            .isEqualTo(control.executionGenerationOf(PIPELINE).orElseThrow());

                    List<String> everyMember = control.clusterMembers().stream()
                            .map(ClusterMemberFacts::memberUuid).toList();
                    Await.until("the cluster's readings of the run to arrive from every member",
                            Duration.ofMinutes(1),
                            () -> control.membersMeasuring(PIPELINE).containsAll(everyMember)
                                    && wide(control).stream()
                                            .allMatch(vertex -> vertex.processors().size() == WRITERS),
                            () -> "measured from " + control.membersMeasuring(PIPELINE) + " of " + everyMember
                                    + "; the vertices planned at " + WRITERS + " were " + wide(control));
                    List<ControlPlane.PlacedVertex> wide = wide(control);
                    assertThat(wide).as("the cluster places the writers the plan gave nine to").isNotEmpty();
                    for (ControlPlane.PlacedVertex vertex : wide) {
                        assertThat(vertex.requested()).isEqualTo(TARGET_WRITERS);
                        assertThat(vertex.computedLocal()).isEqualTo(ON_EACH);
                        assertThat(vertex.processors())
                                .extracting(ControlPlane.PlacedProcessor::index)
                                .as("%s runs nine processors, numbered once each across the run", vertex.name())
                                .containsExactlyInAnyOrderElementsOf(IntStream.range(0, WRITERS).boxed().toList());
                        Map<String, List<ControlPlane.PlacedProcessor>> byMember = vertex.processors().stream()
                                .sorted(Comparator.comparingInt(ControlPlane.PlacedProcessor::index))
                                .collect(Collectors.groupingBy(ControlPlane.PlacedProcessor::nodeId,
                                        LinkedHashMap::new, Collectors.toList()));
                        assertThat(byMember.keySet())
                                .as("%s runs on every member", vertex.name())
                                .containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, THIRD);
                        byMember.forEach((member, processors) -> {
                            assertThat(processors)
                                    .extracting(ControlPlane.PlacedProcessor::localIndex)
                                    .as("%s on %s: three writers numbered on the member from nought, in the order "
                                            + "of the run's numbers", vertex.name(), member)
                                    .containsExactly(0, 1, 2);
                            int first = processors.get(0).index();
                            assertThat(processors)
                                    .extracting(ControlPlane.PlacedProcessor::index)
                                    .as("%s on %s holds one unbroken block of the run's numbers", vertex.name(), member)
                                    .containsExactly(first, first + 1, first + 2);
                        });
                    }

                    long beforeTheChange = files.count(targetAddress, TABLE);
                    files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after the load to arrive", Duration.ofMinutes(1),
                            () -> files.count(targetAddress, TABLE) > beforeTheChange,
                            () -> "rows at the target = " + files.count(targetAddress, TABLE) + ", was "
                                    + beforeTheChange);
                    Await.until("the snapshot read to say the load landed", Duration.ofMinutes(1),
                            () -> control.snapshotTables(PIPELINE).containsKey(TABLE)
                                    && control.snapshotTables(PIPELINE).get(TABLE).landed(),
                            () -> "the snapshot read says " + control.snapshotTables(PIPELINE));
                } finally {
                    third.close();
                }
            }
        }
    }

    /** The vertices of the run the plan gave a width of nine: the target's writers and what routes rows to them. */
    private static List<ControlPlane.PlacedVertex> wide(ControlPlane control) {
        return control.placedVertices(PIPELINE).stream()
                .filter(vertex -> vertex.effective() != null && vertex.effective() == WRITERS)
                .toList();
    }
}
