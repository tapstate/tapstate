package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A second pipeline over a source that is already being tailed reads it the way its own settings say,
 * whichever member ends up driving it: the rows that were there before it started, then the changes after.
 *
 * <p>The tail over a source is shared -- one capture, claimed once, running on one member -- and the
 * initial load is not: every pipeline is owed the rows its source already held. A pipeline driven by the
 * member holding the capture attaches to that tail and loads for itself. One driven by the other member
 * does the same there: it loads for itself, then reads the changes the holder's tail writes into the shared
 * ring. Left unattached instead, it ran healthy with none of the rows that were there before it started.
 * The harness's own connector cannot show that, because its tail replays the whole table on every run; a
 * real one's tail carries only what changed after it began.
 *
 * <p>Native first preparation seeds the initial claim on the member receiving START. The second
 * pipeline is started through the other member and its actual ownership is checked before requiring
 * the rows seeded before any pipeline started and a change made afterwards.
 */
class ASecondPipelineOverATailedSourceLoadsWhereverItLandsIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;
    private static final int MOST_PIPELINES = 6;

    private static final String SOURCE_ID = "shared_src";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void thePipelineDrivenAwayFromTheCaptureGetsTheRowsAlreadyThereAndTheChangesAfter() {
        String sourceUri = SharedMongo.replicaSetUrl("e2e_shared_src");

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            mongo.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

            try (PreparedNativeStore storage = PreparedNativeStore.create();
                    TwoMemberCluster cluster = TwoMemberCluster.startPreparedSequentially(storage)) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.discoverSchema(SOURCE_ID, CONNECTOR, Map.of("uri", sourceUri));

                String first = "shared_pipe_1";
                EndpointAddress firstTarget = startPipelineInto(control, 1, sourceUri);
                Await.until("the first pipeline's load to cross",
                        () -> mongo.count(firstTarget, TABLE) >= SEEDED_ROWS,
                        () -> "rows at the first target = " + mongo.count(firstTarget, TABLE));
                String holder = Await.answered("the cluster to name the member reading the source",
                        () -> control.captureOwnersOf(first).values().stream().findFirst());
                assertThat(control.pipelineControllerOf(first)).as("the first pipeline established the capture locally")
                        .contains(holder);
                ControlPlane remote = cluster.memberOtherThan(holder);
                remote.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                String remoteMember = holder.equals(TwoMemberCluster.NODE_A)
                        ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;

                for (int n = 2; n <= MOST_PIPELINES; n++) {
                    String pipeline = "shared_pipe_" + n;
                    EndpointAddress target = startPipelineInto(remote, n, sourceUri);
                    String driver = Await.answered("the cluster to name the member driving " + pipeline,
                            () -> remote.pipelineControllerOf(pipeline));
                    assertThat(driver).as("the native claim was prepared on the other member").isEqualTo(remoteMember);
                    Await.until(pipeline + ", driven by " + driver + " while " + holder + " holds the capture, "
                                    + "to receive the rows seeded before any pipeline started",
                            () -> mongo.count(target, TABLE) >= SEEDED_ROWS,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + " of " + SEEDED_ROWS
                                    + "; it is " + control.state(pipeline));
                    assertThat(control.captureOwnersOf(pipeline).values())
                            .describedAs("it reads the capture the holder runs rather than one of its own")
                            .containsOnly(holder);

                    long firstTargetBefore = mongo.count(firstTarget, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after " + pipeline + " started to reach its target through "
                                    + "the tail on " + holder,
                            () -> mongo.count(target, TABLE) >= SEEDED_ROWS + 1,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + "; it is "
                                    + control.state(pipeline));
                    Await.until("and the same change to reach the first pipeline's target",
                            () -> mongo.count(firstTarget, TABLE) > firstTargetBefore,
                            () -> "rows at the first target = " + mongo.count(firstTarget, TABLE));
                    return;
                }
                throw new AssertionError("no pipeline of " + MOST_PIPELINES + " landed on the member not "
                        + "holding the capture, so this run could not ask the question");
            }
        }
    }

    private static EndpointAddress startPipelineInto(ControlPlane control, int n, String sourceUri) {
        String targetUri = SharedMongo.replicaSetUrl("e2e_shared_tgt_" + n);
        String targetId = "shared_tgt_" + n;
        String pipeline = "shared_pipe_" + n;
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
