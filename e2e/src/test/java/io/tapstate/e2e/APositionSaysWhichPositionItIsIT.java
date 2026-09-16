package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A run that has moved has exactly one position to show for it, and what it prints says which position
 * that is -- with the ones nobody records printed by name beside it rather than left out.
 *
 * <p>Four positions can be meant by an offset on a pipeline: how far the source could be read to, how far
 * the pipeline has processed, how far it has safely checkpointed, and how far the target has confirmed
 * writes. This product records the last of those on this face and nothing else. They move at different
 * times, and the gap between them is the whole diagnosis: a run whose target has stopped accepting writes
 * keeps reading and keeps processing, so the position on screen freezes while the two that are not on
 * screen do not. A reader who takes the frozen one for either of them concludes the source has gone quiet
 * and goes to look at a database that is working.
 *
 * <p>Both halves are asserted, and either alone would pass over the failure this exists to catch. A
 * position that names itself but sits alone still invites a reader to supply the others from expectation,
 * because a field that is absent and a field the product has no concept of are the same blank on screen.
 * Names printed as {@code not collected} beside a position that did not say which one it was would tell a
 * reader which two are missing and leave them guessing at the one that is there.
 *
 * <p>Driven through the shipped front end rather than read off the API, because the words are the
 * deliverable here. The rendering a person reads is one hop further than the document, and a field renamed
 * on the wire while the screen kept printing the old word would leave every reader exactly where they
 * started while a test on the document went green.
 *
 * <p>The position it asserts is the one the run actually acked, read back out of the product first, so the
 * case cannot pass by printing a value it composed itself. Waiting for that ack is also what makes the
 * first half mean anything: before one exists there is no position on screen to be read as the wrong one.
 *
 * <p><b>A real connector, and not by preference.</b> The harness's own file connector hands the product a
 * null offset on every change it streams, so nothing it drives ever acks a position -- measured, not
 * assumed: written first over that connector, this case timed out waiting for one, with the run otherwise
 * healthy. A position that is never recorded cannot witness a position naming itself.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}). Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=APositionSaysWhichPositionItIsIT -Dtest=NoSuchUnitTestOnPurpose
 * </pre>
 */
@DisplayName("the one position a run records says which position it is")
class APositionSaysWhichPositionItIsIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static final String USER = "e2e";
    private static final String PASSWORD = "e2e-password";

    private static final String PIPELINE_ID = "position_identity";
    private static final String SOURCE_ID = "src_mongo";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String COLLECTION = "orders";
    private static final long SEEDED_ROWS = 4;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void theRecordedPositionNamesItselfAndTheUnrecordedOnesAreNamedToo() {
        String database = PIPELINE_ID + "_src";
        String sourceUri = SharedMongo.replicaSetUrl(database);
        String targetUri = SharedMongo.replicaSetUrl(PIPELINE_ID + "_tgt");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoClient source = MongoClients.create(new ConnectionString(sourceUri));
                MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl(PIPELINE_ID + "_state"))) {

            seed(source, database);
            ControlPlane control = start(server, sourceUri, targetUri, database);

            // The load's completion is the barrier: it says the source is being read, so the change made
            // after it is one the tail is in a position to see and to have confirmed.
            Await.until("the target to hold the seeded documents", TIMEOUT,
                    () -> mongo.count(target, COLLECTION) == SEEDED_ROWS,
                    () -> "%d documents".formatted(mongo.count(target, COLLECTION)));
            rename(source, database, 1, "v1");
            Await.until("a change the tail carried to the target", TIMEOUT,
                    () -> mongo.documents(target, COLLECTION).stream()
                            .anyMatch(document -> "v1".equals(document.getString("name"))),
                    () -> mongo.documents(target, COLLECTION).toString());

            Await.until("the pipeline to have acked a source position", TIMEOUT,
                    () -> control.durablePosition(PIPELINE_ID, COLLECTION).isPresent(),
                    () -> String.valueOf(control.durablePosition(PIPELINE_ID, COLLECTION)));
            String acked = control.durablePosition(PIPELINE_ID, COLLECTION).orElseThrow();

            CliOnce.Run run = CliOnce.runSession(PASSWORD, "metrics " + PIPELINE_ID + "\nexit\n",
                    "-c", server.baseUrl().toString(), "-u", USER);

            assertThat(run.exitCode())
                    .as("the session must have run; stdout was:%n%s%nstderr was:%n%s", run.stdout(), run.stderr())
                    .isZero();
            assertThat(run.stdout())
                    .as("the position on screen is the one the run acked, and it says which position that is")
                    .contains("targetAckedPosition." + COLLECTION + "  " + acked);
            assertThat(run.stdout())
                    .as("a position nobody records is printed by name, not left out to be guessed at")
                    .contains("sourceHeadPosition  not collected")
                    .contains("processedPosition  not collected");
            assertThat(run.stdout())
                    .as("the name that said where but never which is gone, not kept beside the new one")
                    .doesNotContain("perTableOffset");
        }
    }

    /** Everything up to and including the start. */
    private static ControlPlane start(
            ServerHandle server, String sourceUri, String targetUri, String database) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin(USER, PASSWORD);
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", sourceYaml(sourceUri, database));
        resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri, PIPELINE_ID + "_tgt"));
        resources.put("pipeline.tap.yml", pipelineYaml());
        control.apply(resources);
        control.discoverSchema(SOURCE_ID, "mongodb", Map.of("uri", sourceUri, "database", database));
        control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
        return control;
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """
                .formatted(PIPELINE_ID, SOURCE_ID, COLLECTION, TARGET_ID);
    }

    private static String sourceYaml(String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(SOURCE_ID, uri, database, COLLECTION);
    }

    private static String targetYaml(String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                """
                .formatted(TARGET_ID, uri, database);
    }

    private static void seed(MongoClient client, String database) {
        client.getDatabase(database).getCollection(COLLECTION).drop();
        for (int id = 1; id <= SEEDED_ROWS; id++) {
            client.getDatabase(database).getCollection(COLLECTION)
                    .insertOne(new Document("_id", id).append("oid", id).append("name", "order-" + id));
        }
    }

    private static void rename(MongoClient client, String database, int id, String name) {
        client.getDatabase(database).getCollection(COLLECTION)
                .updateOne(new Document("_id", id), new Document("$set", new Document("name", name)));
    }
}
