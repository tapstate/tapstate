package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.core.lifecycle.PipelineState;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A finished snapshot leaves a durable mark naming the table it drained.
 *
 * <p>The pipeline's durable cursor already carried a cdc-start position, but that is written before the
 * snapshot drains -- it has to be, or a change made while the snapshot runs would be missed -- so its
 * presence says the snapshot started, not that it finished. Completion is a separate mark, and this is
 * the witness that it lands on the shipped path rather than only where a unit test calls the phase
 * directly.
 *
 * <p>What it discriminates is the <em>name</em>. The unit tests around the snapshot phase hand it a
 * table and read the same string back, so they hold just as green if the product marks the wrong one --
 * the mining chain id, a qualified name, the ring's name. Here the product resolves the table out of the
 * applied source itself and the assertion names what the example declares, so a mark under any other
 * name fails. A reader looking up a table it never finds concludes that table's snapshot has not
 * finished, and waits for something that already happened.
 *
 * <p>It runs the published snapshot example rather than a private fixture: that example is the one shape
 * whose pipeline both snapshots and tails, so a mark appearing there is a mark on the path a user takes.
 */
class SnapshotCompletionIsRecordedIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL = Duration.ofMillis(200);

    /** The example whose read mode snapshots before it tails; its source declares this one table. */
    private static final String EXAMPLE = "the-snapshot-half-reaches-the-target";
    private static final String TABLE = "orders";

    private static final String COMPLETED = "snapshotCompletedTables";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    @TempDir
    private Path targetDirectory;

    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        E2eConnectorJar.buildInto(connectorJars);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDir == null) {
            System.clearProperty("tapstate.e2e.connectors-dir");
        } else {
            System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
        }
    }

    @Test
    void aDrainedSnapshotMarksItsOwnTableCompleteInTheChainRecord() {
        Path workspace = Examples.ROOT.resolve(EXAMPLE);
        Envelope envelope = EnvelopeParser.parse(Examples.read(workspace.resolve("spec.e2e.yml")));
        String pipeline = new FilePipelineLoader(workspace).resolvePipelineId(envelope.pipeline());

        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                NativeSrsDocuments documents = new NativeSrsDocuments(storage);
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.CONNECTOR_ID, files), env());

            // The example's own awaits are what establish the snapshot actually drained: it settles on a
            // count of rows that only a snapshot read produces. Asserting the mark before that would be
            // asserting it of a phase still running -- but it is not enough on its own either, because the
            // rows land at the target strictly before the mark does. See awaitMarkedTables.
            new E2eExecutor(binding, new FilePipelineLoader(workspace), TIMEOUT, POLL).execute(envelope);

            assertThat(awaitMarkedTables(documents, pipeline))
                    .as("the table the drained snapshot marks complete, read out of the chain record by a "
                            + "reader that is not the product; the product resolved this name from the "
                            + "applied source, and the example is what declares it is %s", TABLE)
                    .contains(TABLE);
            assertThat(documents.miningChainIds()).hasSize(1);
            String chain = documents.miningChainIds().iterator().next();
            String consumer = SrsConsumerId.of(pipeline, "src_file").value();
            // This connector replays the three seeded rows once into zero-based CDC slots 0..2.
            // Drain that known overlap so old queued rows cannot witness the later row's capture.
            Await.until("all three initial tail rows to be captured and consumed", TIMEOUT,
                    () -> documents.largestSequence(chain, TABLE) == 2
                            && documents.consumerReadSeq(chain, consumer, TABLE) == 2,
                    () -> "largest=" + documents.largestSequence(chain, TABLE)
                            + " cursor=" + documents.consumerReadSeq(chain, consumer, TABLE));
            assertThat(control.state(pipeline)).contains(PipelineState.RUNNING);
            files.cdc(EndpointAddress.uri(sourceDirectory.toString()), TABLE, CdcOp.INSERT, 1);
            Await.until("the completed snapshot's live tail to consume a later filtered change", TIMEOUT,
                    () -> documents.largestSequence(chain, TABLE) == 3
                            && documents.consumerReadSeq(chain, consumer, TABLE) == 3,
                    () -> String.valueOf(documents.consumerOffset(chain, consumer)));
            assertThat(control.state(pipeline)).contains(PipelineState.RUNNING);
            assertThat(files.count(EndpointAddress.uri(targetDirectory.toString()), TABLE)).isEqualTo(3);
        }
    }

    /**
     * Waits for the chain record to carry a snapshot-completion mark at all, and answers with every mark
     * it found. The wait is deliberately not the assertion: <em>which</em> table is named is what this
     * case discriminates, so that stays with the caller -- a mark under the wrong name ends this wait and
     * fails there, which is where the message explaining it lives.
     *
     * <p>Why a wait is needed when the example has already settled. The mark is not written by the
     * snapshot phase; it is written when the sink acknowledges the rows, and an acknowledgement is reaped
     * on a later pass of the processor than the write it settles. The example's awaits end as soon as the
     * rows are readable at the target, which is strictly earlier, so a single read here races a write that
     * has not happened yet. Measured: the same commit passed one CI run and failed the other on this
     * assertion, reading an empty list.
     */
    private static List<String> awaitMarkedTables(NativeSrsDocuments documents, String pipeline) {
        List<String> marked = new ArrayList<>();
        Await.until("a snapshot-completion mark in the chain record",
                () -> {
                    marked.clear();
                    marked.addAll(completedTables(documents, pipeline));
                    return !marked.isEmpty();
                },
                marked::toString);
        return marked;
    }

    /**
     * Every table any chain in this store has marked snapshot-complete, read straight from the collection
     * rather than through the store port -- the port is the thing under test, and asking it whether it
     * wrote would take its own word for it.
     */
    private static List<String> completedTables(NativeSrsDocuments documents, String pipeline) {
        List<String> tables = new ArrayList<>();
        for (String chain : documents.miningChainIds()) {
            Document consumer = documents.consumerOffset(chain, SrsConsumerId.of(pipeline, "src_file").value());
            if (consumer != null && consumer.get("progress", Document.class).get(COMPLETED) instanceof List<?> entries)
                entries.forEach(entry -> tables.add(String.valueOf(entry)));
        }
        return tables;
    }

    /**
     * The harness is the client, so the example's published references resolve to its own directories. The
     * two are deliberately different: a sink names its target table after the source row's table, so one
     * directory would have the pipeline write back over the file the harness seeded.
     */
    private UnaryOperator<String> env() {
        return Map.of(
                "SRC_DIR", sourceDirectory.toString(),
                "TGT_DIR", targetDirectory.toString())::get;
    }
}
