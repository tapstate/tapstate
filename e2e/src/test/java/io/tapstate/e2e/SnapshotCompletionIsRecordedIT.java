package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.DockerGate;
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
 * <p>The coordination record already carried a cdc-start position, but that is written before the
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

    private static final String SRS_META = "srs_meta";
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
        String storeUri = SharedMongo.replicaSetUrl("e2e_snapshot_completion");

        try (ServerHandle server = InProcessServer.start(storeUri);
                Endpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            HttpTierBinding binding = new HttpTierBinding(
                    control, workspace, Map.of(E2eConnectorJar.CONNECTOR_ID, files), env());

            // The example's own awaits are what establish the snapshot actually drained: it settles on a
            // count of rows that only a snapshot read produces. Asserting the mark before that would be
            // asserting it of a phase still running.
            new E2eExecutor(binding, new FilePipelineLoader(workspace), TIMEOUT, POLL).execute(envelope);

            assertThat(completedTables(storeUri))
                    .as("the table the drained snapshot marks complete, read out of the chain record by a "
                            + "reader that is not the product; the product resolved this name from the "
                            + "applied source, and the example is what declares it is %s", TABLE)
                    .contains(TABLE);
        }
    }

    /**
     * Every table any chain in this store has marked snapshot-complete, read straight from the collection
     * rather than through the store port -- the port is the thing under test, and asking it whether it
     * wrote would take its own word for it.
     */
    private static List<String> completedTables(String storeUri) {
        try (MongoClient client = MongoClients.create(storeUri)) {
            String database = new com.mongodb.ConnectionString(storeUri).getDatabase();
            List<String> tables = new ArrayList<>();
            for (Document chain : client.getDatabase(database).getCollection(SRS_META).find()) {
                Object marked = chain.get(COMPLETED);
                if (marked instanceof List<?> entries) {
                    entries.forEach(entry -> tables.add(String.valueOf(entry)));
                }
            }
            return tables;
        }
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
