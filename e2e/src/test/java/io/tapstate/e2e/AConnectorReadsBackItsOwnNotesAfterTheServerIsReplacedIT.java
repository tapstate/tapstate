package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.e2e.connector.CsvConnector;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a connector wrote for itself is handed back to it by the run that comes after the server it ran
 * on is gone.
 *
 * <p>The plugin contract gives a connector a notepad of its own, and connectors use it for the one kind
 * of fact they cannot recompute: a name they minted. A PostgreSQL source records the replication slot it
 * created, a change-data reader records the server name its recorded positions are filed under. Handed a
 * fresh notepad, such a connector does not fail - it concludes it has never run here and mints another,
 * which is how a second slot appears on a user's database and how a recorded position ends up filed
 * under a name nothing looks up.
 *
 * <p><b>Why this is a separate witness from the one that shares a notepad across a run's two phases.</b>
 * That one settles scope - the snapshot and the tail of one run get one notepad rather than two - and it
 * is satisfied by a notepad that lives in the process and dies with it. This one settles the other half:
 * the notepad is somewhere that outlives the process. Nothing short of replacing the server can tell the
 * two apart, because a per-process notepad and a persisted one behave identically for as long as the
 * process lives.
 *
 * <p><b>Both tiers, and what the second one is and is not worth.</b> The in-process tier replaces the
 * server without replacing the process; the real-process tier launches the shipped jar, kills it and
 * launches it again, so nothing but the store crosses that boundary - which is what a user actually does.
 * <b>Against the implementation as it stands, no defect separates the two.</b> What is asserted is the
 * value in the store, and the connector mints afresh whenever it is handed an empty notepad, so a notepad
 * that lived anywhere but the store fails on both tiers alike; there is no read cache with a process
 * lifetime for the second tier to catch. It is carried because the cheap ways to make this pass later are
 * process-scoped ones - a client held in a static, a map memoised per JVM, a namespace resolved once at
 * boot - and each of them would leave the in-process tier green. That is insurance, not discrimination
 * today, and stating it the other way round would be a claim this case cannot back.
 *
 * <p><b>The liveness gate comes before the assertion, not after.</b> A second run that never started
 * leaves the stored value exactly as the first run left it, so "the value is unchanged" is satisfied by
 * nothing having happened at all - the same reading the working case produces, for the opposite reason.
 * A change made after the replacement has to reach the target first; only then does what the notepad
 * holds mean anything.
 *
 * <p><b>Compared, not merely counted.</b> The connector mints a fresh value whenever it finds the notepad
 * empty, so a run handed an empty one still leaves a value behind - present, well-formed, and different.
 * Asserting presence alone would pass against exactly the defect this is about; the bytes are compared.
 *
 * <p>The pipeline is stopped with state kept before the replacement. That makes the lifecycle boundary
 * literal: the first run writes its note, the stop preserves it, and the explicit start after the next
 * server opens it. Clearing state instead would be a different request and would correctly leave the
 * connector with nothing to read.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else - no real
 * database, and therefore no gate that skips it outside a nightly run.
 */
class AConnectorReadsBackItsOwnNotesAfterTheServerIsReplacedIT {

    private static final String SOURCE_ID = "notes_src";
    private static final String TARGET_ID = "notes_tgt";
    private static final String PIPELINE_BASE = "connector_notes";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    /** Where the engine files a connector's own notes: one namespace per pipeline node. */
    private static final String NAMESPACE_PREFIX = "pdk.state.";

    private static final String STATE_DATABASE = "tapstate_nest";
    private static final String STATE_COLLECTION = "operator_state";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void theNotepadTheRunAfterTheReplacementOpensIsTheOneTheRunBeforeItWroteIn(
            Tiers tier, @TempDir Path directory) throws Exception {
        // The tier rides on the pipeline id because the namespace is built from it and the state database
        // has a fixed name: on one Mongo, two tiers sharing an id would be writing the same notepad, and
        // the second tier would read back a value the first one minted and call it its own.
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String pipelineId = PIPELINE_BASE + "_" + suffix;
        String namespace = NAMESPACE_PREFIX + pipelineId + "." + SOURCE_ID;
        String storeUri = SharedMongo.replicaSetUrl("connector_notes_store_" + suffix);

        Path sourceDirectory = Files.createDirectories(directory.resolve(SOURCE_ID));
        Path targetDirectory = Files.createDirectories(directory.resolve(TARGET_ID));
        EndpointAddress source = EndpointAddress.uri(sourceDirectory.toString());
        EndpointAddress target = EndpointAddress.uri(targetDirectory.toString());

        FileEndpoints files = new FileEndpoints();
        files.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

        byte[] minted;
        try (ServerHandle first = tier.launch(storeUri)) {
            ControlPlane control = new ControlPlane(first.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));

            control.discoverSchema(
                    SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", sourceDirectory.toString()));
            control.apply(workspace(sourceDirectory, targetDirectory, pipelineId));
            control.lifecycle(pipelineId, LifecycleVerb.START);
            awaitRunning(control, pipelineId);

            // The connector only mints on its stream read, so the rows landing is what says the run is
            // past its snapshot and into the drive that writes the note.
            Await.until(
                    "the first run to carry the seeded rows to the target",
                    () -> files.count(target, TABLE) >= SEEDED_ROWS,
                    () -> "rows at target = " + files.count(target, TABLE));
            Await.until(
                    "the connector to have filed the name it minted",
                    () -> note(storeUri, namespace).isPresent(),
                    () -> "nothing under " + namespace);
            minted = note(storeUri, namespace).orElseThrow();

            control.stop(pipelineId, false);
            awaitStopped(control, pipelineId);
        }

        // The stopped server is gone. On the real-process tier so is the JVM that ran it; the store is not.
        try (ServerHandle second = tier.launch(storeUri)) {
            ControlPlane control = new ControlPlane(second.baseUrl());
            control.login("e2e", "e2e-password");
            control.lifecycle(pipelineId, LifecycleVerb.START);
            awaitRunning(control, pipelineId);

            long rowsBefore = files.count(target, TABLE);
            files.cdc(source, TABLE, CdcOp.INSERT, 1);
            Await.until(
                    "a change made after the server was replaced to reach the target, so the run that "
                            + "came back is reading rather than merely present",
                    () -> files.count(target, TABLE) > rowsBefore,
                    () -> "rows at target = " + files.count(target, TABLE) + ", was " + rowsBefore);

            assertThat(note(storeUri, namespace))
                    .as("the connector's note after a run that came back and is carrying changes")
                    .isPresent();
            assertThat(note(storeUri, namespace).orElseThrow())
                    .as("the run that came back opened the notepad the run before it wrote in, byte for "
                            + "byte: a notepad that did not outlive the server is not empty on the second "
                            + "run either - the connector finds nothing, mints again, and leaves a value "
                            + "that is present, well-formed and not this one")
                    .isEqualTo(minted);
        }
        files.close();
    }

    /** One of the connector's own notes, read straight out of the store as the bytes it was written as. */
    private static Optional<byte[]> note(String storeUri, String namespace) {
        try (MongoClient client = MongoClients.create(storeUri)) {
            Document id = new Document("ns", namespace).append("k", CsvConnector.IDENTITY);
            Document found = client.getDatabase(STATE_DATABASE)
                    .getCollection(STATE_COLLECTION)
                    .find(new Document("_id", id))
                    .first();
            return Optional.ofNullable(found)
                    .map(document -> document.get("state", Binary.class))
                    .map(Binary::getData);
        }
    }

    private static void awaitRunning(ControlPlane control, String pipelineId) {
        Await.until(
                pipelineId + " to reach " + PipelineState.RUNNING,
                () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static void awaitStopped(ControlPlane control, String pipelineId) {
        Await.until(
                pipelineId + " to reach " + PipelineState.STOPPED,
                () -> control.state(pipelineId).filter(PipelineState.STOPPED::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static Map<String, String> workspace(Path sourceDirectory, Path targetDirectory, String pipelineId)
            throws Exception {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, sourceDirectory));
        resources.put(TARGET_ID + ".tap.yml", Workspaces.targetYaml(TARGET_ID, targetDirectory));
        resources.put(pipelineId + ".tap.yml",
                Workspaces.pipelineYaml(pipelineId, SOURCE_ID, TARGET_ID, TABLE));
        return resources;
    }
}
