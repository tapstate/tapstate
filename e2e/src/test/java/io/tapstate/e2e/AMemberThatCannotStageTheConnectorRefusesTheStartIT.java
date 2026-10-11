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
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A start is refused before anything runs when one member of the cluster cannot load a connector the run needs, and
 * the refusal names that member and that connector.
 *
 * <p>A pipeline's writers run on every member it is planned on, so every one of them has to be able to load the
 * target's connector. A member that cannot would otherwise find out only when the run reached it - with the run
 * already reading, and the reason left on that member - so every member is asked first, and the start is refused
 * with the member, the connector and the reason in hand.
 *
 * <p>The member that cannot load it is given a staging directory it cannot create: a path under a regular file.
 * Every other part of the cluster is ordinary, and the other member stages the same connector where it always does -
 * so the refusal is about the one member, which is what the named member has to say.
 *
 * <p>Then the directory is made possible and the pipeline is stopped and started again - the way out of a failed
 * pipeline - and it runs. A refusal that left anything behind - a claim, a half-made run, a count of attempts -
 * would show here as a second start that does not come up.
 */
class AMemberThatCannotStageTheConnectorRefusesTheStartIT {

    private static final String TABLE = "orders";
    private static final String PIPELINE = "unstaged_pipe";
    private static final String SOURCE = "unstaged_src";
    private static final String TARGET = "unstaged_tgt";
    private static final String UNABLE = TwoMemberCluster.NODE_B;
    private static final long SEEDED_ROWS = 6;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theStartIsRefusedNamingTheMemberAndConnectorAndGoesAheadOnceTheMemberCan(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        Path inTheWay = Files.createFile(directory.resolve("a-file-where-a-directory-goes"));
        Path staging = inTheWay.resolve("plugins");

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_unstaged_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.startStaging(store, "e2e-unstaged", UNABLE, staging)) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.memberOtherThan(UNABLE);
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source));
                resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE));
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);

                ControlPlane.Failure refused = Await.answered("the start to be refused", Duration.ofMinutes(3),
                        () -> control.failure(PIPELINE));
                assertThat(refused.code()).isEqualTo("actuation.connector-unavailable-on-member");
                assertThat(refused.params())
                        .as("the refusal names the member that cannot load the connector, and the connector")
                        .containsEntry("member", UNABLE)
                        .containsEntry("connector", E2eConnectorJar.CONNECTOR_ID)
                        .containsEntry("pipeline", PIPELINE)
                        .containsKey("reason");
                assertThat(control.state(PIPELINE)).contains(PipelineState.FAILED);
                assertThat(files.count(targetAddress, TABLE))
                        .as("nothing was written: the run was refused before it started")
                        .isZero();

                Files.delete(inTheWay);
                control.stop(PIPELINE, false);
                Await.until(PIPELINE + " to reach " + PipelineState.STOPPED, Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                        () -> String.valueOf(control.state(PIPELINE)));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING + " once the member can stage it",
                        Duration.ofMinutes(2),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", failure " + control.failure(PIPELINE));
                Await.until("the seeded rows to cross", Duration.ofMinutes(1),
                        () -> files.count(targetAddress, TABLE) == SEEDED_ROWS,
                        () -> "rows at the target = " + files.count(targetAddress, TABLE));
                assertThat(control.failure(PIPELINE)).as("the refusal is no longer what the status says").isEmpty();
            }
        }
    }
}
