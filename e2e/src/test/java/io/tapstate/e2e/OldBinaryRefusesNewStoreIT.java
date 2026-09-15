package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemMetaStore;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

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
 * <p><b>Where the second binary comes from.</b> The lane builds it the way the patch release is made:
 * the release tag, plus the one commit that carries the gate, and nothing else. So it is not a fixture
 * standing in for an old build -- it is that build, assembled by the same recipe, and if that recipe
 * stops working the lane says so before anybody tries to ship by it.
 *
 * <p><b>The control is load-bearing.</b> A binary that cannot start for some unrelated reason -- a bad
 * jar, a missing setting, a port already taken -- looks exactly like one that refused, and would pass
 * the first half of this on its own. So the same binary is also started against a store nothing has
 * migrated, where it must serve. Between them the two say the refusal is about the version and not
 * about the binary. The other way round is covered too, and by construction: handed this build's own
 * jar by mistake, the first half goes red, because this build opens its own store quite happily.
 */
class OldBinaryRefusesNewStoreIT {

    /** Where the lane leaves the binary it built from the release line. Nothing else sets it. */
    private static final String OLD_BINARY_JAR = "tapstate.e2e.old-binary-jar";

    /** A refusal happens during startup, before anything slow; this is slack, not an expectation. */
    private static final Duration REFUSAL_BUDGET = Duration.ofSeconds(90);

    private static final String MIGRATED = "e2e_old_binary_migrated";
    private static final String UNTOUCHED = "e2e_old_binary_untouched";

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

        // Starting is what migrates it. Going through the server rather than calling the runner puts the
        // version in the store by the path a deployment puts it there by. start() returning is the whole
        // assertion here -- it throws, carrying the server's own output, when the server never serves.
        RealProcessServer.start(migratedStore).close();

        try (MongoClient client = MongoClients.create(migratedStore)) {
            // The premise, asserted rather than assumed: without this the case still passes against a
            // store nothing migrated, where the old binary is right to start and proves nothing.
            assertThat(new SystemMetaStore(client.getDatabase(MIGRATED)).installedVersion())
                    .as("what this build recorded, before the older binary is pointed at it")
                    .contains(MigrationRunner.SUPPORTED_VERSION);
        }

        RealProcessServer older = RealProcessServer.launching(migratedStore, oldBinary);
        try {
            Await.until("the older binary to refuse the store and exit", REFUSAL_BUDGET,
                    () -> !older.isAlive(),
                    () -> "it is still running, so it opened a store it cannot read; it said:\n"
                            + older.tail());

            // Non-zero, not merely stopped. A process that exits 0 has finished as far as anything
            // supervising it is concerned, so an orchestrator would report the rollback as a success and
            // move on rather than holding the container down where somebody looks at it.
            assertThat(older.exitValue())
                    .as("the status the older binary exited with")
                    .isNotZero();
            assertThat(outputOf(older))
                    .as("what the older binary said on its way out")
                    .contains("migration.data-newer-than-binary")
                    .contains("installed=" + MigrationRunner.SUPPORTED_VERSION);
        } finally {
            older.close();
        }

        // The control. Same binary, same everything, a store nothing has migrated: it must serve. Again
        // start() returning is the assertion, and it carries the server's own output into the failure
        // when it does not -- so a binary that is simply broken says so here rather than being mistaken
        // for the refusal above.
        RealProcessServer.start(SharedMongo.replicaSetUrl(UNTOUCHED), oldBinary).close();
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
                    "no " + OLD_BINARY_JAR + " system property: this witness needs a build of the product "
                            + "from the release line -- the release tag plus the commit carrying the "
                            + "startup gate -- and the lane is what builds it");
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
