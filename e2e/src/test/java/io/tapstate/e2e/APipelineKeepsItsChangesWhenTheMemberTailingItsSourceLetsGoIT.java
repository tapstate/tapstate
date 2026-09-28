package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A pipeline goes on receiving its source's changes when the member tailing that source stops the last
 * of its own pipelines on it.
 *
 * <p>One member tails a source for every pipeline reading it, wherever those pipelines are driven. That
 * member lets the capture go with the last pipeline of its own that reads it, and a pipeline another
 * member drives over the same source is then running over a ring nobody writes: healthy, and receiving
 * nothing, because a different pipeline was stopped somewhere else. The member still reading the capture
 * takes the tail over instead, from where the last one had got to.
 *
 * <p>Pipelines are added over the same source until one lands on the member not holding the capture; the
 * pipelines driven by the holder are then stopped with their state cleared, and a change made afterwards
 * has to reach the one left.
 * The source and the targets are real Mongo: the harness's own connector replays its whole table through
 * the ring on every run, which a pipeline left without a tail would receive anyway.
 */
class APipelineKeepsItsChangesWhenTheMemberTailingItsSourceLetsGoIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;
    private static final int MOST_PIPELINES = 6;

    private static final String SOURCE_ID = "let_go_src";

    /**
     * Bound on the handover: the member still reading asks for the claim once per claim renewal, and the
     * tail it opens starts from the durable record. Both are product timings; this decides only how long
     * a broken handover takes to say so.
     */
    private static final Duration HANDOVER = Duration.ofMinutes(2);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void thePipelineLeftReadingACaptureNobodyElseNeedsTakesItsTailOver() {
        String sourceUri = SharedMongo.replicaSetUrl("e2e_let_go_src");
        String store = SharedMongo.replicaSetUrl("e2e_let_go_cluster");

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            mongo.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-let-go")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.discoverSchema(SOURCE_ID, CONNECTOR, Map.of("uri", sourceUri));

                String first = pipelineId(1);
                EndpointAddress firstTarget = startPipelineInto(control, 1, sourceUri);
                Await.until("the first pipeline's load to cross",
                        () -> mongo.count(firstTarget, TABLE) >= SEEDED_ROWS,
                        () -> "rows at the first target = " + mongo.count(firstTarget, TABLE));
                String holder = Await.answered("the cluster to name the member reading the source",
                        () -> control.captureOwnersOf(first).values().stream().findFirst());

                List<String> onTheHolder = new ArrayList<>(List.of(first));
                for (int n = 2; n <= MOST_PIPELINES; n++) {
                    String pipeline = pipelineId(n);
                    EndpointAddress target = startPipelineInto(control, n, sourceUri);
                    String driver = Await.answered("the cluster to name the member driving " + pipeline,
                            () -> control.pipelineControllerOf(pipeline));
                    if (driver.equals(holder)) {
                        onTheHolder.add(pipeline);
                        continue;
                    }
                    Await.until(pipeline + " to receive its load",
                            () -> mongo.count(target, TABLE) >= SEEDED_ROWS,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + " of " + SEEDED_ROWS);

                    // Stopped with their state cleared, which is also the stop that decides whether the
                    // chain's record goes with them: it must not, while this pipeline is still on it.
                    for (String stopping : onTheHolder) {
                        control.stop(stopping, true);
                    }
                    for (String stopping : onTheHolder) {
                        Await.until(stopping + " to stop",
                                () -> control.state(stopping).filter(PipelineState.STOPPED::equals).isPresent(),
                                () -> String.valueOf(control.state(stopping)));
                    }

                    long rowsBefore = mongo.count(target, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after " + holder + " let the capture go to reach " + pipeline,
                            HANDOVER,
                            () -> mongo.count(target, TABLE) > rowsBefore,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + ", was " + rowsBefore
                                    + "; it is " + control.state(pipeline) + " and its capture is owned by "
                                    + control.captureOwnersOf(pipeline).values());
                    assertThat(control.state(pipeline))
                            .describedAs("it ran throughout, and nobody asked it to do anything")
                            .contains(PipelineState.RUNNING);
                    assertThat(control.captureOwnersOf(pipeline).values())
                            .describedAs("its source is now tailed by the member driving it")
                            .containsOnly(driver);
                    return;
                }
                throw new AssertionError("no pipeline of " + MOST_PIPELINES + " landed on the member not "
                        + "holding the capture, so this run could not ask the question");
            }
        }
    }

    private static String pipelineId(int n) {
        return "let_go_pipe_" + n;
    }

    private static EndpointAddress startPipelineInto(ControlPlane control, int n, String sourceUri) {
        String targetUri = SharedMongo.replicaSetUrl("e2e_let_go_tgt_" + n);
        String targetId = "let_go_tgt_" + n;
        String pipeline = pipelineId(n);
        Map<String, String> resources = new LinkedHashMap<>();
        // Sent every time, unchanged: an apply resolves references within what it is given.
        resources.put(SOURCE_ID + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, CONNECTOR, sourceUri, TABLE));
        resources.put(targetId + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(targetId, CONNECTOR, targetUri));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [%s], type: filter, expr: "op == 'r' || op == 'i'" }
                serve:
                  from: rows_through
                  sync:
                    - source: %s
                """.formatted(pipeline, SOURCE_ID, TABLE, targetId));
        control.apply(resources);
        control.lifecycle(pipeline, LifecycleVerb.START);
        Await.until(pipeline + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
        return EndpointAddress.uri(targetUri);
    }
}
