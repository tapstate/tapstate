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
 * A target written by eight writers across two members runs four of them on each, and the status and the cluster
 * say so alike.
 *
 * <p>Eight is a target for the whole cluster, not for each member, so two members run four each: the engine's own
 * width on a member is the target shared out over the members the run was planned on, and eight shares out exactly.
 * What the status carries is the plan the run was submitted under - the target, the members and the width worked
 * out from them - and what the cluster reports is where each of the run's processors really runs. The two are read
 * apart and held to each other: a plan that said four and a run of eight on one member, or of two on each, would
 * each pass a case that read only one of them.
 *
 * <p>Each member's writers are numbered on that member from nought, and hold one unbroken run of the numbers the
 * whole run gives them, in the same order: the engine numbers a run's processors member by member, so a member's
 * block of numbers is how it tells its own writers from the other member's. And every row still arrives, the load
 * and a change after it, which eight writers sharing one target file have to do without losing any of each other's.
 */
class ATargetOfEightRunsFourWritersOnEachOfTwoMembersIT {

    private static final String TABLE = "orders";
    private static final String PIPELINE = "wide_sink_pipe";
    private static final String SOURCE = "wide_sink_src";
    private static final String TARGET = "wide_sink_tgt";
    private static final int WRITERS = 8;
    private static final long SEEDED_ROWS = 40;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void eightWritersRunFourOnEachMemberAsPlannedAndEveryRowArrives(@TempDir Path directory) throws IOException {
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
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source));
                resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE, WRITERS));
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
                ControlPlane.PlannedNode writers = plan.nodeRequested(WRITERS);
                assertThat(plan.members())
                        .as("the run was planned over both members")
                        .containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(writers.scope()).as("a target of eight runs at the engine's own width").isEqualTo("native");
                assertThat(writers.memberCount()).isEqualTo(2);
                assertThat(writers.computedLocal()).as("eight shared out over two members").isEqualTo(4);
                assertThat(writers.effective()).as("eight shares out exactly, so eight run").isEqualTo(WRITERS);
                assertThat(writers.reasons()).as("nothing was rounded or held back").isEmpty();
                assertThat(plan.executionGeneration())
                        .as("the plan is the one the run holding the pipeline was submitted under")
                        .isEqualTo(control.executionGenerationOf(PIPELINE).orElseThrow());

                List<String> everyMember = control.clusterMembers().stream()
                        .map(ClusterMemberFacts::memberUuid).toList();
                Await.until("the cluster's readings of the run to arrive from every member", Duration.ofMinutes(1),
                        () -> control.membersMeasuring(PIPELINE).containsAll(everyMember)
                                && wide(control).stream().allMatch(vertex -> vertex.processors().size() == WRITERS),
                        () -> "measured from " + control.membersMeasuring(PIPELINE) + " of " + everyMember
                                + "; the vertices planned at " + WRITERS + " were " + wide(control));
                List<ControlPlane.PlacedVertex> wide = wide(control);
                assertThat(wide).as("the cluster places the writers the plan gave eight to").isNotEmpty();
                for (ControlPlane.PlacedVertex vertex : wide) {
                    assertThat(vertex.requested()).isEqualTo(WRITERS);
                    assertThat(vertex.computedLocal()).isEqualTo(4);
                    assertThat(vertex.processors())
                            .extracting(ControlPlane.PlacedProcessor::index)
                            .as("%s runs eight processors, numbered once each across the run", vertex.name())
                            .containsExactlyInAnyOrderElementsOf(IntStream.range(0, WRITERS).boxed().toList());
                    Map<String, List<ControlPlane.PlacedProcessor>> byMember = vertex.processors().stream()
                            .sorted(Comparator.comparingInt(ControlPlane.PlacedProcessor::index))
                            .collect(Collectors.groupingBy(ControlPlane.PlacedProcessor::nodeId, LinkedHashMap::new,
                                    Collectors.toList()));
                    assertThat(byMember.keySet())
                            .as("%s runs on both members", vertex.name())
                            .containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                    byMember.forEach((member, processors) -> {
                        assertThat(processors)
                                .extracting(ControlPlane.PlacedProcessor::localIndex)
                                .as("%s on %s: four writers numbered on the member from nought, in the order of "
                                        + "the run's numbers", vertex.name(), member)
                                .containsExactly(0, 1, 2, 3);
                        int first = processors.get(0).index();
                        assertThat(processors)
                                .extracting(ControlPlane.PlacedProcessor::index)
                                .as("%s on %s holds one unbroken block of the run's numbers", vertex.name(), member)
                                .containsExactly(first, first + 1, first + 2, first + 3);
                    });
                }

                long beforeTheChange = files.count(targetAddress, TABLE);
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                Await.until("a change made after the load to arrive", Duration.ofMinutes(1),
                        () -> files.count(targetAddress, TABLE) > beforeTheChange,
                        () -> "rows at the target = " + files.count(targetAddress, TABLE) + ", was " + beforeTheChange);
            }
        }
    }

    /** The vertices of the run the plan gave a width of eight: the target's writers and what routes rows to them. */
    private static List<ControlPlane.PlacedVertex> wide(ControlPlane control) {
        return control.placedVertices(PIPELINE).stream()
                .filter(vertex -> vertex.effective() != null && vertex.effective() == WRITERS)
                .toList();
    }
}
