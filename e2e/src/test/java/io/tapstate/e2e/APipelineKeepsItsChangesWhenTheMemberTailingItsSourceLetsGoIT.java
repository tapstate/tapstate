package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Remote consumers keep a shared capture alive through local clears and take it over after its owner exits.
 *
 * <p>One member tails a source for every pipeline reading it, wherever those pipelines are driven. That
 * member keeps the independent capture while another member still needs it. Clearing its own pipeline
 * must not leave that consumer without a tail. Once the actual capture-owning process exits, the
 * surviving consumer's member takes the tail over from its durable position.
 *
 * <p>Two pipelines are prepared through the member not holding the capture, and their claims are checked. Its
 * first joiner is stopped with its state cleared before the pipelines driven by the holder are stopped
 * the same way. Changes before and after the owner's exit have to reach the one left, and taking the tail over must not
 * attach the stopped first joiner to the chain again.
 * The source and the targets are real Mongo: the harness's own connector replays its whole table through
 * the ring on every run, which a pipeline left without a tail would receive anyway.
 */
class APipelineKeepsItsChangesWhenTheMemberTailingItsSourceLetsGoIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;
    private static final int MOST_PIPELINES = 12;

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

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            mongo.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

            try (PreparedNativeStore storage = PreparedNativeStore.create();
                    TwoMemberCluster cluster = TwoMemberCluster.startPreparedSequentially(storage);
                    StoreDocuments documents = StoreDocuments.nativeSrsAt(storage.cacheUri())) {
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
                assertThat(control.pipelineControllerOf(first)).as("the first pipeline established the capture locally")
                        .contains(holder);
                ControlPlane remote = cluster.memberOtherThan(holder);
                remote.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                String remoteMember = holder.equals(TwoMemberCluster.NODE_A)
                        ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;

                List<String> onTheHolder = new ArrayList<>(List.of(first));
                Map<String, String> driversByPipeline = new LinkedHashMap<>();
                driversByPipeline.put(first, holder);
                String firstJoiner = null;
                for (int n = 2; n <= MOST_PIPELINES; n++) {
                    String pipeline = pipelineId(n);
                    EndpointAddress target = startPipelineInto(remote, n, sourceUri);
                    String driver = Await.answered("the cluster to name the member driving " + pipeline,
                            () -> remote.pipelineControllerOf(pipeline));
                    driversByPipeline.put(pipeline, driver);
                    assertThat(driver).as("the native claim was prepared on the other member").isEqualTo(remoteMember);
                    Await.until(pipeline + " to receive its load",
                            () -> mongo.count(target, TABLE) >= SEEDED_ROWS,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + " of " + SEEDED_ROWS);
                    if (firstJoiner == null) {
                        firstJoiner = pipeline;
                        continue;
                    }

                    String stoppedJoiner = firstJoiner;
                    String remainingPipeline = pipeline;
                    String stoppedConsumer = SrsConsumerId.of(stoppedJoiner, SOURCE_ID).value();
                    String remainingConsumer = SrsConsumerId.of(remainingPipeline, SOURCE_ID).value();
                    String chain = Await.answered("both joiners to hold cursors on the same chain",
                            () -> documents.miningChainIds().stream()
                                    .filter(id -> documents.consumersOf(id)
                                            .containsAll(List.of(stoppedConsumer, remainingConsumer)))
                                    .findFirst());
                    var stoppedScope = NativePipelineScopes.current(storage, stoppedJoiner);
                    remote.stop(stoppedJoiner, true);
                    NativePipelineScopes.awaitCleared(remote, storage, stoppedScope, Duration.ofSeconds(60));
                    Await.until("the stopped first joiner's cursor to be cleared before takeover",
                            () -> !documents.consumersOf(chain).contains(stoppedConsumer),
                            () -> "consumers on " + chain + " = " + documents.consumersOf(chain));

                    // A shared capture remains needed even after its owner's own pipelines are cleared.
                    for (String stopping : onTheHolder) {
                        var original = NativePipelineScopes.current(storage, stopping);
                        remote.stop(stopping, true);
                        NativePipelineScopes.awaitCleared(remote, storage, original, Duration.ofSeconds(60));
                    }

                    assertThat(remote.captureOwnersOf(remainingPipeline).values())
                            .as("clearing local pipelines does not retire another consumer's capture").containsOnly(holder);
                    long beforeExit = mongo.count(target, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("the original shared tail to remain live after its local pipelines clear", HANDOVER,
                            () -> mongo.count(target, TABLE) > beforeExit,
                            () -> "rows=" + mongo.count(target, TABLE) + ", owner=" + remote.captureOwnersOf(remainingPipeline));

                    cluster.processCarrying(holder).kill();
                    Await.until("the remaining joiner's member to take the capture over", HANDOVER,
                            () -> remote.captureOwnersOf(remainingPipeline).containsValue(driver),
                            () -> "capture owners = " + remote.captureOwnersOf(remainingPipeline));

                    long rowsBefore = mongo.count(target, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after " + holder + " let the capture go to reach " + remainingPipeline,
                            HANDOVER,
                            () -> mongo.count(target, TABLE) > rowsBefore,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + ", was " + rowsBefore
                                    + "; it is " + remote.state(remainingPipeline) + " and its capture is owned by "
                                    + remote.captureOwnersOf(remainingPipeline).values());
                    assertThat(remote.state(remainingPipeline))
                            .describedAs("it ran throughout, and nobody asked it to do anything")
                            .contains(PipelineState.RUNNING);
                    assertThat(remote.captureOwnersOf(remainingPipeline).values())
                            .describedAs("its source is now tailed by the member driving it")
                            .containsOnly(driver);
                    assertThat(documents.consumersOf(chain))
                            .describedAs("taking the tail over must not reattach the cleared first joiner %s",
                                    stoppedJoiner)
                            .doesNotContain(stoppedConsumer);
                    return;
                }
                throw new AssertionError("fewer than two pipelines of " + MOST_PIPELINES + " landed on the member "
                        + "not holding the capture, so this run could not ask the question; capture holder = "
                        + holder + "; pipeline controllers = " + driversByPipeline);
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
