package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A member that joins while a pipeline runs is named as one the run was not planned for, and the run carries on as
 * it was planned: same execution, same members, same width.
 *
 * <p>Taking the new member in would mean a new run - the width is shared out over the members a run is planned
 * on, and a run cannot change that part way - and a new run is a pause and a replay the user did not ask for. So the
 * run is left alone, and the status and the cluster both say which member it is not using, so that nobody reads an
 * idle member as a broken one.
 *
 * <p>Eight writers is the discriminating width: on two members it shares out as four each, and on three it would be
 * worked out again as three each - nine - so a run that took the new member in would say so in its width as well
 * as in its execution.
 *
 * <p><b>Why the case waits as long as it does.</b> The engine will rebuild a running job onto a member that joins
 * once the member has been in the cluster for its scale-up delay, unless the job was submitted to never do that, and
 * the product submits every job that way. A reading taken before that delay has passed would be the same reading
 * whether the product did or not. The case waits for a reading of the run taken well after it, and reads the
 * engine's own id for the run's execution from it: a rebuild by the engine changes that id even where the product's
 * own execution count does not move.
 */
class AMemberJoiningARunningPipelineIsNamedAwaitingRebalanceIT {

    private static final String TABLE = "orders";
    private static final String PIPELINE = "joining_member_pipe";
    private static final String SOURCE = "joining_member_src";
    private static final String TARGET = "joining_member_tgt";
    private static final String JOINING = "node-c";
    private static final int WRITERS = 8;
    private static final long SEEDED_ROWS = 20;

    /** Twice the engine's default scale-up delay: the delay is ten seconds, and a rebuild takes time to show. */
    private static final Duration WELL_PAST_THE_SCALE_UP_DELAY = Duration.ofSeconds(20);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aRunningRunKeepsItsExecutionAndWidthAndNamesTheMemberItIsNotUsing(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_joining_member_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-joining-member")) {
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

                ControlPlane.Plan before = Await.answered("the status to carry the run's plan",
                        () -> control.executionPlan(PIPELINE));
                assertThat(before.nodeRequested(WRITERS).effective()).as("four writers on each of two").isEqualTo(8);
                long generation = control.executionGenerationOf(PIPELINE).orElseThrow();
                Set<String> executionBefore = Await.answered("the cluster to say which execution the run is",
                        () -> Optional.of(executionIds(control)).filter(ids -> !ids.isEmpty()));
                assertThat(control.awaitingRebalance(PIPELINE)).as("nobody has joined yet").isEmpty();

                RealProcessServer third = cluster.launching(JOINING);
                try {
                    cluster.awaitMembers(3);
                    Await.until(JOINING + " to be committed to the cluster", Duration.ofMinutes(2),
                            () -> control.clusterMembers().stream().anyMatch(member ->
                                    JOINING.equals(member.nodeId()) && "ACTIVE".equals(member.state())),
                            () -> "the members were " + control.clusterMembers());
                    Instant joined = Instant.now();

                    Await.until("the status to name " + JOINING + " as a member the run was not planned for",
                            Duration.ofMinutes(1),
                            () -> control.awaitingRebalance(PIPELINE).equals(List.of(JOINING)),
                            () -> "the status names " + control.awaitingRebalance(PIPELINE));
                    Await.until("the cluster to name " + JOINING + " the same way", Duration.ofMinutes(1),
                            () -> control.clusterAwaitingRebalance(PIPELINE).equals(List.of(JOINING)),
                            () -> "the cluster names " + control.clusterAwaitingRebalance(PIPELINE));
                    Await.until("a reading of the run taken " + WELL_PAST_THE_SCALE_UP_DELAY + " after " + JOINING
                                    + " joined", Duration.ofMinutes(2),
                            () -> control.measuredAt(PIPELINE)
                                    .filter(taken -> taken.isAfter(joined.plus(WELL_PAST_THE_SCALE_UP_DELAY)))
                                    .isPresent(),
                            () -> "the latest reading was taken at " + control.measuredAt(PIPELINE));

                    assertThat(executionIds(control))
                            .as("the engine did not rebuild the run onto the member that joined")
                            .isEqualTo(executionBefore);
                    assertThat(control.membersCarryingPartOf(PIPELINE))
                            .as("no piece of the run is on the member that joined")
                            .doesNotContain(JOINING);
                    assertThat(control.executionGenerationOf(PIPELINE))
                            .as("nor did the product start a new run")
                            .contains(generation);
                    ControlPlane.Plan after = control.executionPlan(PIPELINE).orElseThrow();
                    assertThat(after.executionGeneration()).isEqualTo(before.executionGeneration());
                    assertThat(after.members())
                            .as("the plan is still the one made for the two members it started on")
                            .containsExactlyInAnyOrderElementsOf(before.members());
                    assertThat(after.nodeRequested(WRITERS).effective())
                            .as("eight writers, not the nine three members would be worked out for")
                            .isEqualTo(8);
                    assertThat(control.awaitingRebalance(PIPELINE)).containsExactly(JOINING);

                    long beforeTheChange = files.count(targetAddress, TABLE);
                    files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after " + JOINING + " joined to arrive", Duration.ofMinutes(1),
                            () -> files.count(targetAddress, TABLE) > beforeTheChange,
                            () -> "rows at the target = " + files.count(targetAddress, TABLE) + ", was "
                                    + beforeTheChange + "; the pipeline is " + control.state(PIPELINE));
                    assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                } finally {
                    third.close();
                }
            }
        }
    }

    /** The engine's ids of the execution each placed vertex of the run belongs to - one id while nothing rebuilt it. */
    private static Set<String> executionIds(ControlPlane control) {
        return control.placedVertices(PIPELINE).stream()
                .map(ControlPlane.PlacedVertex::executionId)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}
