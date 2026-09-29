package io.tapstate.e2e;

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
 * Two pipelines reading one source directly -- with the shared ring switched off -- both carry the changes
 * made to it, on a cluster.
 *
 * <p>A direct tail streams to the one pipeline that opened it and writes no ring another pipeline could
 * read. A cluster claims each capture once, and these two reads used to be filed under one capture: the
 * second pipeline was held to the first one's claim, attached to a tail it could not read from, and ran
 * on its initial load alone -- every change after it missing, with the pipeline running. Each direct read
 * is now a capture of its own, so each pipeline tails the source for itself.
 *
 * <p>The harness's own connector is enough here, unlike for the cases over the shared ring: a direct tail
 * replays nothing to anybody but the pipeline that opened it.
 */
class TwoPipelinesReadingOneSourceDirectlyBothCarryItsChangesIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;
    private static final String SOURCE_ID = "direct_src";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aChangeMadeAfterBothStartedReachesBothTargets(@TempDir Path directory) throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path first = Files.createDirectories(directory.resolve("tgt-1"));
        Path second = Files.createDirectories(directory.resolve("tgt-2"));
        try (FileEndpoints files = new FileEndpoints()) {
            EndpointAddress src = EndpointAddress.uri(source.toString());
            EndpointAddress firstTarget = EndpointAddress.uri(first.toString());
            EndpointAddress secondTarget = EndpointAddress.uri(second.toString());
            files.seed(src, TABLE, SeedRows.generated(SEEDED_ROWS));
            String store = SharedMongo.replicaSetUrl("e2e_direct_pair_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-direct-pair")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, source));
                resources.put("direct_tgt_1.tap.yml", Workspaces.targetYaml("direct_tgt_1", first));
                resources.put("direct_tgt_2.tap.yml", Workspaces.targetYaml("direct_tgt_2", second));
                resources.put("direct_pipe_1.tap.yml", directPipeline("direct_pipe_1", "direct_tgt_1"));
                resources.put("direct_pipe_2.tap.yml", directPipeline("direct_pipe_2", "direct_tgt_2"));
                control.apply(resources);

                start(control, "direct_pipe_1");
                Await.until("the first pipeline's load", () -> files.count(firstTarget, TABLE) >= SEEDED_ROWS,
                        () -> "rows at first = " + files.count(firstTarget, TABLE));
                start(control, "direct_pipe_2");
                Await.until("the second pipeline's load", () -> files.count(secondTarget, TABLE) >= SEEDED_ROWS,
                        () -> "rows at second = " + files.count(secondTarget, TABLE));
                String drivers = control.pipelineControllerOf("direct_pipe_1") + " / "
                        + control.pipelineControllerOf("direct_pipe_2");

                files.cdc(src, TABLE, CdcOp.INSERT, 1);
                Await.until("the change to reach the first target",
                        () -> files.count(firstTarget, TABLE) >= SEEDED_ROWS + 1,
                        () -> "rows at first = " + files.count(firstTarget, TABLE) + "; drivers " + drivers);
                Await.until("the same change to reach the second target, through a tail of its own",
                        () -> files.count(secondTarget, TABLE) >= SEEDED_ROWS + 1,
                        () -> "rows at second = " + files.count(secondTarget, TABLE) + "; drivers " + drivers
                                + "; owners " + control.captureOwnersOf("direct_pipe_1") + " / "
                                + control.captureOwnersOf("direct_pipe_2") + "; second is "
                                + control.state("direct_pipe_2"));
            }
        }
    }

    private static void start(ControlPlane control, String pipeline) {
        control.lifecycle(pipeline, LifecycleVerb.START);
        Await.until(pipeline + " to run", Duration.ofMinutes(1),
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
    }

    private static String directPipeline(String id, String targetId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source:
                  - { id: %s, srs: false }
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: %s_step, from: [ %s ], type: filter, expr: "op != 'x'" }
                serve:
                  from: %s_step
                  sync:
                    - source: %s
                """.formatted(id, SOURCE_ID, id, TABLE, id, targetId);
    }
}
