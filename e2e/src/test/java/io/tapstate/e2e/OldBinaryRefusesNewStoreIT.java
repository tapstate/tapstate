package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.SystemMetaStore;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A binary from the release line, handed a store this build has already migrated, refusing to open it.
 *
 * <p>This is the direction nothing else covers. Every other migration witness moves a store
 * <em>forward</em>; this one is what happens when an operator upgrades and then puts the old image
 * back. There is no way back across a system-data version, so the old binary has to refuse rather than
 * read a shape nobody described to it -- and a refusal that only exists in the build that introduced
 * the migration is no protection at all, because that build is not the one being rolled back to.
 *
 * <p><b>Why it takes a second binary rather than a seeded number.</b> The gate on the release line and
 * the migrator on this one are two codebases that never build together, and they agree on exactly
 * three literals: a collection, a document id, and a field. A case that seeds that field by hand -- as
 * the unit-level gate case does, and rightly -- writes it from the same understanding as the code that
 * reads it, so the two agree by construction and would go on agreeing if both were wrong. Here the
 * number is put there by the real migrator, running in the real server, and read by a binary that was
 * compiled without ever seeing it. A migrator that started recording the version anywhere else would
 * leave every seeded case green and this one red.
 *
 * <p><b>Where the second binary comes from.</b> The lane takes the jar from the published 0.5.0
 * image, the first release with the startup gate. A focused run can supply another published jar
 * through the same input. The same jar's fresh-store control establishes its supported version,
 * rather than assuming every older release carries the same changesets.
 *
 * <p><b>The control is load-bearing.</b> A binary that cannot start for some unrelated reason -- a bad
 * jar, a missing setting, a port already taken -- looks exactly like one that refused, and would pass
 * the first half of this on its own. So the same binary is also started against a store nothing has
 * migrated, where it must serve. Between them the two say the refusal is about the version and not
 * about the binary. The other way round is covered too, and by construction: handed this build's own
 * jar by mistake, the first half goes red, because this build opens its own store quite happily.
 *
 * <p>This is a cold-start rollback refusal. It does not prove that an already-running older member
 * stops when another member migrates the store, or that mixed-version operation is safe.
 */
class OldBinaryRefusesNewStoreIT {

    /** Where the lane leaves the jar it took from the published release image. */
    private static final String OLD_BINARY_JAR = "tapstate.e2e.old-binary-jar";

    /** A refusal happens during startup, before anything slow; this is slack, not an expectation. */
    private static final Duration REFUSAL_BUDGET = Duration.ofSeconds(90);

    private static final String MIGRATED = "e2e_old_binary_migrated";
    private static final String UNTOUCHED = "e2e_old_binary_untouched";
    private static final String SOURCE = "encrypted_rollback_source";
    private static final String SENTINEL = "rollback-config-plaintext-sentinel";

    @BeforeAll
    static void requireTheUpgradeLane() {
        // Asked for before Docker is: this witness needs a build of the product other than the one the
        // reactor makes, so it belongs to the lane that supplies one rather than to every pull request.
        UpgradeLaneGate.require();
        DockerGate.require();
    }

    @Test
    void aBinaryFromTheReleaseLineWillNotOpenAStoreThisBuildHasMigrated() {
        Path oldBinary = oldBinary();
        String migratedStore = SharedMongo.replicaSetUrl(MIGRATED);

        // The real server migrates and writes the Source through its encrypted store. No hand-seeded
        // version or ciphertext stands in for the bytes the older process must refuse to open.
        try (RealProcessServer current = RealProcessServer.start(migratedStore)) {
            current.awaitReady();
            ControlPlane control = new ControlPlane(current.baseUrl());
            control.bootstrapAndLogin("rollback-admin", "rollback-local-password");
            control.apply(Map.of("encrypted.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config:
                      uri: mongodb://fake-user:fake-password@localhost/data
                      nested: { unmarkedValue: %s }
                    """.formatted(SOURCE, SENTINEL)));
        }

        try (MongoClient client = MongoClients.create(migratedStore)) {
            // The premise, asserted rather than assumed: without this the case still passes against a
            // store nothing migrated, where the old binary is right to start and proves nothing.
            assertThat(new SystemMetaStore(client.getDatabase(MIGRATED)).installedVersion())
                    .as("what this build recorded, before the older binary is pointed at it")
                    .contains(MigrationRunner.SUPPORTED_VERSION);
            var database = client.getDatabase(MIGRATED);
            Document schema = SystemCollections.SYSTEM_META.on(database)
                    .find(new Document("_id", "schema")).first();
            Document keyring = SystemCollections.SYSTEM_META.on(database)
                    .find(new Document("_id", "source-config-keyring")).first();
            Document source = SystemCollections.ARTIFACTS.on(database)
                    .find(new Document("_id", SOURCE)).first();
            assertThat(schema).isNotNull();
            assertThat(keyring).isNotNull();
            assertThat(source).isNotNull();
            assertThat(source.get("body", Document.class).get("config"))
                    .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
            assertThat(source.toJson()).doesNotContain(SENTINEL, "fake-password");

            String refusalOutput;
            try (RealProcessServer older = RealProcessServer.launching(migratedStore, oldBinary)) {
                Await.until("the older binary to refuse the store and exit", REFUSAL_BUDGET,
                        () -> !older.isAlive(),
                        () -> "it is still running, so it opened a store it cannot read; it said:\n"
                                + older.tail());

                // Non-zero, not merely stopped. A process that exits 0 has finished as far as anything
                // supervising it is concerned, so an orchestrator would report the rollback as a success.
                assertThat(older.exitValue()).as("the status the older binary exited with").isNotZero();
                refusalOutput = outputOf(older);
                assertThat(refusalOutput)
                        .as("what the older binary said on its way out")
                        .contains("migration.data-newer-than-binary")
                        .contains("installed=" + MigrationRunner.SUPPORTED_VERSION)
                        .doesNotContain("Started Bootstrap", SENTINEL, "fake-password");
                for (Document key : keyring.getList("keys", Document.class)) {
                    assertThat(refusalOutput).doesNotContain(key.getString("material"));
                }
            }

            assertThat(schema.equals(SystemCollections.SYSTEM_META.on(database)
                    .find(new Document("_id", "schema")).first()))
                    .as("the refused old process does not change the schema").isTrue();
            assertThat(keyring.equals(SystemCollections.SYSTEM_META.on(database)
                    .find(new Document("_id", "source-config-keyring")).first()))
                    .as("the refused old process does not change the keyring").isTrue();
            assertThat(source.equals(SystemCollections.ARTIFACTS.on(database)
                    .find(new Document("_id", SOURCE)).first()))
                    .as("the refused old process does not change the encrypted Source").isTrue();

            // The control. Same binary, same everything, a store nothing has migrated: it must serve.
            // Its recorded schema identifies the supported version without breaking earlier release inputs.
            String untouchedStore = SharedMongo.replicaSetUrl(UNTOUCHED);
            try (RealProcessServer control = RealProcessServer.start(untouchedStore, oldBinary);
                    MongoClient controlClient = MongoClients.create(untouchedStore)) {
                Object supported = new SystemMetaStore(controlClient.getDatabase(UNTOUCHED))
                        .installedVersion().orElseThrow();
                assertThat(supported).isInstanceOf(Number.class);
                assertThat(((Number) supported).intValue()).isLessThan(MigrationRunner.SUPPORTED_VERSION);
                assertThat(refusalOutput).contains("supported=" + supported);
            }
        }
    }

    /**
     * The binary the lane built from the release line.
     *
     * <p>Absent, this fails rather than skipping. The lane asking for this witness and the lane not
     * having built its subject are different faults, and only one of them is somebody forgetting to
     * pass a flag; skipping would report them alike, and the lane would stay green over a witness that
     * never ran.
     */
    private static Path oldBinary() {
        String configured = System.getProperty(OLD_BINARY_JAR);
        if (configured == null || configured.isBlank()) {
            throw new AssertionError(
                    "no " + OLD_BINARY_JAR + " system property: this witness needs the jar from the "
                            + "published 0.5.0 image, supplied by the upgrade lane");
        }
        Path jar = Path.of(configured);
        if (!Files.isRegularFile(jar)) {
            throw new AssertionError("no binary at " + jar + ", so there is no older build to point at "
                    + "the store");
        }
        return jar;
    }

    private static String outputOf(RealProcessServer server) {
        try {
            return Files.readString(server.output());
        } catch (IOException e) {
            throw new UncheckedIOException("could not read what the older binary said", e);
        }
    }
}
