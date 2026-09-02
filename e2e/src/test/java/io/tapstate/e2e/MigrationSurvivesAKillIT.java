package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.SystemMetaStore;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A member killed part way through bringing the store forward, and the member that starts afterwards
 * carrying on from where it stopped rather than from the beginning.
 *
 * <p>The kill is a real one - the process is destroyed outright, with no signal it can handle and
 * nothing of its own run on the way out - because everything this case is about lives in the gap
 * between "recorded" and "finished". A server shut down politely closes that gap itself, which is
 * exactly the state a crash never leaves behind.
 *
 * <p>Where the kill lands is decided by the store rather than by a sleep. The recorded version going
 * to one is the first changeset saying it finished, and the second one starting; that transition is
 * what this waits for, and the process is killed the moment it is seen. So the interruption is inside
 * a changeset by construction, not by timing luck.
 *
 * <p>The store is seeded with enough resources that converting them takes appreciably longer than
 * noticing that it began. That is the one tuned quantity here, and it is tuned in the safe direction:
 * too many only makes the window wider. If the conversion ever outruns the kill this fails saying so
 * rather than passing over a kill that landed after the work.
 *
 * <p>The restart waits out the dead member's lock before it can take over, because a killed holder
 * never released it and a lock is only free once its holder has stopped saying it is alive for longer
 * than the lock lives. That wait is the product behaving correctly, and it is most of this case's
 * running time.
 *
 * <p><b>What this case alone is for.</b> Recording each changeset as it succeeds, rather than all of
 * them at the end, is what makes a resume possible - and that much is already caught deterministically
 * a level down: moving the recording to after the loop reddens two of the adapter's own cases as well
 * as this one. So per-changeset recording is not what this case uniquely holds. What no other case
 * reaches is the rest of the sentence: a process that was <em>killed</em> - not one whose changeset
 * threw, and not one whose lock was taken away while it kept running - and a second process, started
 * afterwards over exactly what the first left behind, finishing the job. The interruption here is a
 * signal the JVM cannot handle, so nothing on the way out gets to tidy the store into a state that
 * happens to be resumable.
 */
class MigrationSurvivesAKillIT {

    /** Its own database: this one gets killed part way through and must not be anybody else's store. */
    private static final String DATABASE = "e2e_migration_kill";

    /**
     * How many stored resources the interrupted changeset has to convert. Large enough that converting
     * them takes appreciably longer than one turn of the wait that watches for it beginning, and no
     * larger - every one of them is parsed twice over the course of this case.
     */
    private static final int SEEDED_RESOURCES = 2_000;

    /** The version the first changeset records, which is the signal that the second one has begun. */
    private static final int AFTER_THE_FIRST_CHANGESET = 1;

    /** Bound on reaching the interrupted changeset. A bound only decides how fast a failure says so. */
    private static final Duration KILL_WINDOW = Duration.ofMinutes(2);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aMemberKilledMidChangesetIsResumedFromTheChangesetItDiedInRatherThanFromTheFirst() {
        String storeUri = SharedMongo.replicaSetUrl(DATABASE);

        try (MongoClient client = MongoClients.create(storeUri)) {
            MongoDatabase store = client.getDatabase(DATABASE);
            seedResourcesInTheOlderShape(store);
            SystemMetaStore meta = new SystemMetaStore(store);

            RealProcessServer dying = RealProcessServer.launching(storeUri);
            try {
                awaitTheSecondChangesetStarting(meta, store, dying);
            } finally {
                dying.kill();
            }

            // Killed in the middle, and the store says so in both halves: the first changeset's result
            // is recorded, and the second one's work is visibly unfinished. Asserting only the recorded
            // version would also pass over a kill that landed before the second changeset touched
            // anything, which is a different history and not the one this case is about.
            assertThat(meta.installedVersion())
                    .as("what the killed member had recorded when it died")
                    .contains(AFTER_THE_FIRST_CHANGESET);
            assertThat(stillInTheOlderShape(store))
                    .as("resources the killed member had not converted yet")
                    .isPositive();

            try (ServerHandle restarted = RealProcessServer.start(storeUri)) {
                assertThat(new ControlPlane(restarted.baseUrl()).healthy())
                        .as("the member that started over the store the killed one left behind")
                        .isTrue();
            }

            assertThat(meta.installedVersion())
                    .as("the store after the second member finished what the first had begun")
                    .contains(MigrationRunner.SUPPORTED_VERSION);
            assertThat(stillInTheOlderShape(store))
                    .as("resources left in the older shape once the migration completed")
                    .isZero();
        }
    }

    /**
     * Waits for the first changeset to have recorded and the second to be running.
     *
     * <p>The two ways this can fail are told apart by what it last read, which is why the reading
     * carries the version, how much is left to convert and whether the member is even alive. Reaching
     * the finished version means the conversion outran the poll and the kill would have landed after
     * the work rather than inside it - seed more resources. A dead member means it never got this far
     * at all, and then its own output is the thing worth reading.
     */
    private static void awaitTheSecondChangesetStarting(
            SystemMetaStore meta, MongoDatabase store, RealProcessServer dying) {
        Await.until("the first changeset to record and the second to begin", KILL_WINDOW,
                () -> meta.installedVersion().orElse(null) instanceof Number version
                        && version.intValue() == AFTER_THE_FIRST_CHANGESET,
                () -> "recorded version " + meta.installedVersion().orElse("nothing")
                        + ", " + stillInTheOlderShape(store) + " resource(s) still to convert, member "
                        + (dying.isAlive() ? "alive" : "dead, its output was:\n" + dying.tail()));
    }

    /**
     * Stored resources written the way the older build wrote them: the canonical text, under the field
     * the conversion selects on. Inserted rather than applied through a running server, because the
     * server that would accept them is the one that no longer writes this shape.
     */
    private static void seedResourcesInTheOlderShape(MongoDatabase store) {
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(store);
        List<Document> seeded = new ArrayList<>(SEEDED_RESOURCES);
        for (int i = 0; i < SEEDED_RESOURCES; i++) {
            String id = "killed_probe_" + i;
            seeded.add(new Document("_id", id)
                    .append("kind", "source")
                    .append("canonical", """
                            version: tapstate/v1
                            kind: source
                            id: %s
                            connector: mongodb
                            config: { uri: "mongodb://127.0.0.1:27017/e2e" }
                            """.formatted(id)));
        }
        artifacts.insertMany(seeded);
    }

    /** How many stored resources still carry a text body, which is what the conversion removes. */
    private static long stillInTheOlderShape(MongoDatabase store) {
        return SystemCollections.ARTIFACTS.on(store)
                .countDocuments(new Document("canonical", new Document("$exists", true)));
    }

}
