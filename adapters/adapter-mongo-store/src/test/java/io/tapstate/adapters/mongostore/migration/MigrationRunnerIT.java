package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The migrator against a real server, in the states that decide whether it is safe to run it on more
 * than one member at once.
 *
 * <p>Most of these cannot be reached with the changeset this build actually ships — it does not fail,
 * and one run is over far too quickly to be waited on or taken over. So the runner is driven with
 * changesets written for the case: a step that throws, a step whose lock is taken away underneath it,
 * a holder that never finishes. Reasoning about those instead of witnessing them is how a lock gets
 * shipped having only ever been run uncontended.
 */
@RequiresDocker
class MigrationRunnerIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final Duration LOCK_TTL = Duration.ofSeconds(30);
    private static final Duration PATIENT = Duration.ofSeconds(30);
    private static final Clock CLOCK = Clock.systemUTC();
    private static final Document SCHEMA_ID = new Document("_id", "schema");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static MongoClient client;

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    // ---- what this build's own changeset does ----

    @Test
    void bringsAnUnmigratedStoreForwardAndRecordsHowFarItGot() {
        MongoDatabase database = freshDatabase("runner_forward");

        MigrationRunner.migrate(database);

        assertThat(installedVersion(database)).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
        assertThat(indexNames(database, SystemCollections.SESSIONS))
                .as("the session lookup runs on every credentialed request; without this index each one "
                        + "is a scan of every session ever issued")
                .contains("secretHash_issuer_idx");
    }

    @Test
    void runningItAgainstTheSameStoreAgainChangesNothing() {
        MongoDatabase database = freshDatabase("runner_twice");

        MigrationRunner.migrate(database);
        List<String> afterOnce = indexNames(database, SystemCollections.SESSIONS);
        MigrationRunner.migrate(database);

        // Re-runnability is not a nicety here: there is one version for the whole store, so a run that
        // died part way is resumed by running the same changeset again.
        assertThat(indexNames(database, SystemCollections.SESSIONS)).isEqualTo(afterOnce);
        assertThat(installedVersion(database)).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
    }

    @Test
    void aStoreAlreadyAtThisVersionIsNotWrittenToAtAll() {
        // This is the path almost every start takes, and what it costs is defined by what it does not
        // do: one read, no writes, no lock. So the store is seeded with a lock an earlier member
        // abandoned -- long since expired, and free for the taking. Nothing here has anything to
        // migrate, so nothing should take it.
        //
        // The tracer matters because the obvious assertion does not discriminate. Measured: removing
        // the short circuit entirely leaves no index built either way, because the step after it skips
        // changesets the store has already had -- so "the index is absent" is true whether the run
        // stopped at the read or went on to take a lock, run nothing, and clear it.
        MongoDatabase database = freshDatabase("runner_uptodate");
        Document abandoned = lock("member-a@host", 4L,
                Instant.now().minusSeconds(600), Instant.now().minus(LOCK_TTL).minusSeconds(60));
        seedSchemaDocument(database, MigrationRunner.SUPPORTED_VERSION, abandoned);

        MigrationRunner.migrate(database);

        Document after = schemaDocuments(database).find(SCHEMA_ID).first();
        assertThat(after.get("lock", Document.class))
                .as("the abandoned lock is exactly as it was: this run did not write to the store")
                .isEqualTo(abandoned);
        assertThat(indexNames(database, SystemCollections.SESSIONS))
                .doesNotContain("secretHash_issuer_idx");
    }

    // ---- what happens when a changeset does not succeed ----

    @Test
    void aChangesetThatFailsLeavesTheStoreAtTheStepBeforeIt() {
        MongoDatabase database = freshDatabase("runner_failure");
        AtomicInteger firstRan = new AtomicInteger();

        TapstateException failure = catchThrowableOfType(
                () -> MigrationRunner.migrate(database,
                        List.of(counting(1, firstRan), throwing(2, "disk is full")),
                        LOCK_TTL, PATIENT, CLOCK),
                TapstateException.class);

        assertThat(failure).isNotNull();
        assertThat(failure.code().code()).isEqualTo("migration.changeset-failed");
        assertThat(failure.args()).containsEntry("cause", "disk is full");
        assertThat(installedVersion(database))
                .as("the step that succeeded is recorded; the one that threw is not")
                .isEqualTo(1);

        // And the next start resumes at the failed step rather than repeating the one before it.
        AtomicInteger secondRan = new AtomicInteger();
        MigrationRunner.migrate(database, List.of(counting(1, firstRan), counting(2, secondRan)),
                LOCK_TTL, PATIENT, CLOCK);
        assertThat(firstRan).hasValue(1);
        assertThat(secondRan).hasValue(1);
        assertThat(installedVersion(database)).isEqualTo(2);
    }

    @Test
    void aChangesetWhoseLockWasTakenAwayDoesNotRecordWhatItDid() {
        MongoDatabase database = freshDatabase("runner_fenced");

        // The changeset runs to completion, but by the time it finishes the lock has moved on -- which
        // is exactly the shape of a holder that stalled, was taken over, and then woke up. Recording
        // its version here would write over whatever the member that replaced it has since done.
        TapstateException failure = catchThrowableOfType(
                () -> MigrationRunner.migrate(database, List.of(stealsTheLock(1, database)),
                        LOCK_TTL, PATIENT, CLOCK),
                TapstateException.class);

        assertThat(failure).isNotNull();
        assertThat(failure.code().code()).isEqualTo("migration.changeset-failed");
        assertThat(failure.args().get("cause").toString()).contains("taken over");
        assertThat(installedVersion(database)).isZero();
    }

    // ---- what happens when more than one member is starting ----

    @Test
    void aMemberThatCannotGetTheLockInTimeRefusesToStartAndSaysWhoHasIt() {
        MongoDatabase database = freshDatabase("runner_timeout");
        Instant heldSince = Instant.parse("2026-09-02T08:00:00Z");
        seedSchemaDocument(database, 0, lock("member-a@host", 4L, heldSince, Instant.now()));

        TapstateException refusal = catchThrowableOfType(
                () -> MigrationRunner.migrate(database, MigrationRunner.changeSets(),
                        LOCK_TTL, Duration.ofMillis(300), CLOCK),
                TapstateException.class);

        assertThat(refusal).isNotNull();
        assertThat(refusal.code().code()).isEqualTo("migration.lock-timeout");
        // Naming the holder is the point of the diagnostic: the answer is on that member, not this one.
        assertThat(refusal.args()).containsEntry("holder", "member-a@host");
        assertThat(refusal.args()).containsEntry("since", heldSince.toString());
    }

    @Test
    void aHolderThatStoppedSayingItIsAliveIsTakenOver() {
        MongoDatabase database = freshDatabase("runner_takeover");
        seedSchemaDocument(database, 0,
                lock("member-a@host", 4L, Instant.now().minusSeconds(600),
                        Instant.now().minus(LOCK_TTL).minusSeconds(60)));

        MigrationRunner.migrate(database, MigrationRunner.changeSets(), LOCK_TTL, PATIENT, CLOCK);

        assertThat(installedVersion(database)).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
        assertThat(indexNames(database, SystemCollections.SESSIONS)).contains("secretHash_issuer_idx");
    }

    @Test
    void aMemberThatLostTheRaceStartsOnceTheHolderHasFinished() {
        MongoDatabase database = freshDatabase("runner_waits");
        seedSchemaDocument(database, 0, lock("member-a@host", 4L, Instant.now(), Instant.now()));

        CompletableFuture<Void> waiting = CompletableFuture.runAsync(() ->
                MigrationRunner.migrate(database, MigrationRunner.changeSets(), LOCK_TTL, PATIENT, CLOCK));

        // The holder finishing is what releases this member -- it does not need the lock itself, and
        // taking it would mean running changesets a second time over the same store.
        schemaDocuments(database).updateOne(SCHEMA_ID, new Document("$set",
                new Document("installedVersion", MigrationRunner.SUPPORTED_VERSION)));

        assertThat(waiting).succeedsWithin(PATIENT);
    }

    @Test
    void theEpochKeepsCountingAcrossHoldersRatherThanRestarting() {
        // The epoch is what refuses a write from a member that has been taken over, so two different
        // holders must never work under the same number. Giving a lock up is the case that can quietly
        // break it: clearing the whole lock leaves the next holder starting at one again, and then a
        // stale write from the holder before last matches.
        MongoDatabase database = freshDatabase("runner_epoch");
        AtomicInteger ran = new AtomicInteger();

        MigrationRunner.migrate(database, List.of(counting(1, ran)), LOCK_TTL, PATIENT, CLOCK);
        long afterFirst = epoch(database);
        MigrationRunner.migrate(database, List.of(counting(1, ran), counting(2, ran)),
                LOCK_TTL, PATIENT, CLOCK);

        assertThat(epoch(database))
                .as("the second holder counts on from the first, it does not start again")
                .isGreaterThan(afterFirst);
    }

    // ---- what the inspection commands read ----

    @Test
    void inspectingReportsWhereTheStoreIsAndWhatHasNotRun() {
        MongoDatabase database = freshDatabase("runner_inspect");

        MigrationRunner.Status before = MigrationRunner.inspect(database);
        assertThat(before.installed()).isZero();
        assertThat(before.supported()).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
        assertThat(before.pending()).containsExactly("V1BaselineIndexes");

        MigrationRunner.migrate(database);

        MigrationRunner.Status after = MigrationRunner.inspect(database);
        assertThat(after.installed()).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
        assertThat(after.pending()).isEmpty();
    }

    // ---- the one guard no shipped declaration reaches yet ----

    @Test
    void aUniqueIndexIsRefusedOverDataThatAlreadyCollides() {
        MongoDatabase database = freshDatabase("runner_duplicates");
        MongoCollection<Document> collection = SystemCollections.SESSIONS.on(database);
        collection.insertOne(new Document("_id", "s1").append("secretHash", "same"));
        collection.insertOne(new Document("_id", "s2").append("secretHash", "same"));

        IllegalStateException refusal = catchThrowableOfType(
                () -> V1BaselineIndexes.build(collection,
                        new SystemCollections.IndexSpec(List.of("secretHash"), true)),
                IllegalStateException.class);

        assertThat(refusal).isNotNull();
        // The driver's own failure names one colliding value and stops, which leaves an operator
        // finding the rest one restart at a time.
        assertThat(refusal).hasMessageContaining("1 duplicated value(s)").hasMessageContaining("same");
        assertThat(indexNames(database, SystemCollections.SESSIONS)).containsExactly("_id_");
    }

    // ---- fixtures ----

    /** A changeset that records that it ran, so a resumed run can be told from a repeated one. */
    private static ChangeSet counting(int version, AtomicInteger runs) {
        return new ChangeSet() {
            @Override
            public int version() {
                return version;
            }

            @Override
            public void up(MongoDatabase database) {
                runs.incrementAndGet();
            }
        };
    }

    /** A changeset that fails the way a real one would: part way, for a reason outside itself. */
    private static ChangeSet throwing(int version, String because) {
        return new ChangeSet() {
            @Override
            public int version() {
                return version;
            }

            @Override
            public void up(MongoDatabase database) {
                throw new IllegalStateException(because);
            }
        };
    }

    /** A changeset that succeeds, but whose lock has moved on by the time it returns. */
    private static ChangeSet stealsTheLock(int version, MongoDatabase database) {
        return new ChangeSet() {
            @Override
            public int version() {
                return version;
            }

            @Override
            public void up(MongoDatabase ignored) {
                schemaDocuments(database).updateOne(SCHEMA_ID,
                        new Document("$inc", new Document("lock.epoch", 1L)));
            }
        };
    }

    private static MongoDatabase freshDatabase(String name) {
        if (client == null) {
            client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        }
        MongoDatabase database = client.getDatabase(name);
        database.drop();
        return database;
    }

    private static MongoCollection<Document> schemaDocuments(MongoDatabase database) {
        return SystemCollections.SYSTEM_META.on(database);
    }

    private static void seedSchemaDocument(MongoDatabase database, int installedVersion, Document held) {
        Document schema = new Document("_id", "schema").append("installedVersion", installedVersion);
        if (held != null) {
            schema.append("lock", held);
        }
        schemaDocuments(database).insertOne(schema);
    }

    private static Document lock(String owner, long epoch, Instant since, Instant heartbeat) {
        return new Document("owner", owner).append("epoch", epoch)
                .append("since", Date.from(since)).append("heartbeat", Date.from(heartbeat));
    }

    /** The epoch on the lock, or zero where the lock itself is gone -- which is the failure to report. */
    private static long epoch(MongoDatabase database) {
        Document schema = schemaDocuments(database).find(SCHEMA_ID).first();
        Document held = schema == null ? null : schema.get("lock", Document.class);
        return held == null ? 0L : held.get("epoch", Number.class).longValue();
    }

    private static int installedVersion(MongoDatabase database) {
        Document schema = schemaDocuments(database).find(SCHEMA_ID).first();
        return schema == null ? 0 : schema.getInteger("installedVersion", 0);
    }

    private static List<String> indexNames(MongoDatabase database, SystemCollections row) {
        List<String> names = new ArrayList<>();
        row.on(database).listIndexes().forEach(index -> names.add(index.getString("name")));
        return names;
    }
}
