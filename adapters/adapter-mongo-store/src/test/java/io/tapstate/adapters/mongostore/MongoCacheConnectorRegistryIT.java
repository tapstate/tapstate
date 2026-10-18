package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real GridFS bytes are published only by their original CACHE metadata transaction. */
@RequiresDocker
class MongoCacheConnectorRegistryIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final CacheAuthScope AUTH = new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1);
    private static final Runnable ACTIVE = () -> {};
    private static final byte[] JAR = "connector-bytes".getBytes(StandardCharsets.UTF_8);
    private static final String HASH = ContentHash.of(JAR);

    @Test void exactBytesRoundTripAndSameHashPreservesItsOriginalRegistrationWithoutAnotherUpload() {
        withFixture(f -> {
            var registry = f.registry(); var first = registry.register("example", null, RegistrationSource.SEED, JAR);
            assertThat(first.newlyRegistered()).isTrue();
            var again = registry.register("other-name", "2.0.0", RegistrationSource.REGISTER, JAR);
            assertThat(again.newlyRegistered()).isFalse(); assertThat(again.registration()).isEqualTo(first.registration());
            assertThat(registry.list()).containsExactly(first.registration());
            assertThat(registry.findAll("other-name")).isEmpty(); assertThat(registry.findAll("example")).containsExactly(first.registration());
            assertThat(registry.artifact(HASH)).contains(JAR); assertThat(registry.hasArtifact(HASH)).isTrue();
            assertThat(registry.artifact("unknown")).isEmpty(); assertThat(registry.hasArtifact("unknown")).isFalse();
            assertThat(f.files().countDocuments()).isEqualTo(1); assertThat(f.chunks().countDocuments()).isEqualTo(1);
            assertThat(f.admissions()).isEqualTo(1);
        });
    }

    @Test void multipleHashesForOneConnectorRemainVisibleInStableHashOrder() {
        withFixture(f -> {
            var registry = f.registry(); registry.register("example", "1", RegistrationSource.SEED, JAR);
            registry.register("example", "2", RegistrationSource.REGISTER, "other-bytes".getBytes(StandardCharsets.UTF_8));
            var all = registry.findAll("example"); assertThat(all).hasSize(2);
            assertThat(all).extracting(r -> r.contentHash()).isSorted();
            assertThat(registry.findAll("example")).containsExactlyElementsOf(all);
            assertThat(registry.list()).containsExactlyElementsOf(all);
        });
    }

    @Test void aJarLargerThanOneBsonDocumentUploadsOutsideThePublicationTransaction() {
        withFixture(f -> {
            byte[] large = new byte[17 * 1024 * 1024]; Arrays.fill(large, (byte) 37);
            var chunks = new AtomicInteger(); var files = new AtomicInteger(); var publication = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (inserts(e, "connector_artifacts.chunks")) {
                        assertThat(e.getCommand().containsKey("autocommit")).isFalse(); chunks.incrementAndGet();
                    }
                    if (inserts(e, "connector_artifacts.files")) {
                        assertThat(e.getCommand().containsKey("autocommit")).isFalse(); files.incrementAndGet();
                    }
                    if (updates(e, "connector_artifacts.files")) {
                        assertThat(e.getCommand().getBoolean("autocommit").getValue()).isFalse(); publication.incrementAndGet();
                    }
                }
            };
            try (var observed = client(listener)) {
                var result = f.registry(observed, ACTIVE).register("large", "1", RegistrationSource.REGISTER, large);
                assertThat(result.newlyRegistered()).isTrue(); assertThat(f.registry().artifact(result.registration().contentHash())).contains(large);
                assertThat(chunks.get()).isGreaterThan(1); assertThat(files).hasValue(1); assertThat(publication).hasValue(1);
                assertThat(f.admissions()).isEqualTo(1);
            }
        });
    }

    @Test void stagedUploadIsAbsentFromEveryPublicReadAndOnlyItsCompleteBytesCanBePublished() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (updates(e, "cache_identity") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var future = worker.submit(() -> f.registry(observed, ACTIVE).register("example", "1", RegistrationSource.SEED, JAR));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); assertThat(f.files().find().first().get("metadata", Document.class)).containsEntry("publication", "STAGED");
                    var reader = f.registry(); assertThat(reader.list()).isEmpty(); assertThat(reader.findAll("example")).isEmpty();
                    assertThat(reader.hasArtifact(HASH)).isFalse(); assertThat(reader.artifact(HASH)).isEmpty();
                } finally { proceed.countDown(); }
                assertThat(future.get(15, TimeUnit.SECONDS).newlyRegistered()).isTrue(); assertThat(f.registry().artifact(HASH)).contains(JAR);
            }
        });
    }

    @Test void twoIndependentClientsPublishOneSameHashWinnerAndCleanOnlyTheLosingUpload() {
        withFixture(f -> {
            var staged = new CountDownLatch(2); var proceed = new CountDownLatch(1);
            try (var one = client(publicationBarrier(staged, proceed)); var two = client(publicationBarrier(staged, proceed));
                 var workers = Executors.newFixedThreadPool(2)) {
                var a = workers.submit(() -> f.registry(one, ACTIVE).register("example", "1", RegistrationSource.SEED, JAR));
                var b = workers.submit(() -> f.registry(two, ACTIVE).register("example", "2", RegistrationSource.REGISTER, JAR));
                try { assertThat(staged.await(10, TimeUnit.SECONDS)).isTrue(); assertThat(f.files().countDocuments()).isEqualTo(2); }
                finally { proceed.countDown(); }
                var aa = a.get(20, TimeUnit.SECONDS); var bb = b.get(20, TimeUnit.SECONDS);
                assertThat(aa.registration()).isEqualTo(bb.registration()); assertThat(aa.newlyRegistered() ^ bb.newlyRegistered()).isTrue();
                assertThat(f.files().countDocuments()).isEqualTo(1); assertThat(f.chunks().countDocuments()).isEqualTo(1);
                assertThat(f.registry().artifact(HASH)).contains(JAR); assertThat(f.admissions()).isEqualTo(2);
            }
        });
    }

    @Test void originalInputIsCapturedBeforeAnyGuardCanMutateTheCallerArray() {
        withFixture(f -> {
            byte[] input = JAR.clone(); var once = new AtomicBoolean();
            var registry = f.registry(f.client, () -> { if (once.compareAndSet(false, true)) input[0] = '!'; });
            assertThat(registry.register("example", "1", RegistrationSource.SEED, input).registration().contentHash()).isEqualTo(HASH);
            assertThat(f.registry().artifact(HASH)).contains(JAR); assertThat(input).isNotEqualTo(JAR);
        });
    }

    @Test void losingTheOriginalGuardAfterAChunkWriteAbortsAndRemovesItsUnpublishedBytes() {
        withFixture(f -> {
            var live = new AtomicBoolean(true); var writingChunks = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) { writingChunks.set(inserts(e, "connector_artifacts.chunks")); }
                @Override public void commandSucceeded(CommandSucceededEvent e) { if (e.getCommandName().equals("insert") && writingChunks.get()) live.set(false); }
            };
            try (var observed = client(listener)) {
                byte[] multi = new byte[600_000]; Arrays.fill(multi, (byte) 7);
                assertCode(() -> f.registry(observed, guard(live)).register("example", "1", RegistrationSource.SEED, multi), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.files().countDocuments()).isZero(); assertThat(f.chunks().countDocuments()).isZero(); assertThat(f.admissions()).isZero();
        });
    }

    @Test void aRetirementCommittedBeforePublicationFencesAndCleansTheCompletedStage() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            try (var observed = client(publicationBarrier(paused, proceed)); var worker = Executors.newSingleThreadExecutor()) {
                var future = worker.submit(() -> f.registry(observed, ACTIVE).register("example", "1", RegistrationSource.SEED, JAR));
                try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.domain.retireBusiness(SCOPE, "retire-artifacts", ACTIVE); }
                finally { proceed.countDown(); }
                assertThatThrownBy(() -> future.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(e -> assertThat(((TapstateException) e.getCause()).code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
                assertThat(f.files().countDocuments()).isZero(); assertThat(f.chunks().countDocuments()).isZero(); assertThat(f.admissions()).isZero();
            }
        });
    }

    @Test void guardLossAfterTheFilesUpdateRollsBackPublicationAndItsRootAdmission() {
        withFixture(f -> {
            var live = new AtomicBoolean(true);
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (updates(e, "connector_artifacts.files")) live.set(false);
            } };
            try (var observed = client(listener)) {
                assertCode(() -> f.registry(observed, guard(live)).register("example", "1", RegistrationSource.SEED, JAR), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.files().countDocuments()).isZero(); assertThat(f.chunks().countDocuments()).isZero(); assertThat(f.admissions()).isZero();
        });
    }

    @Test void anUnknownPublicationCommitReplyRetriesTheSameCommitWithoutUploadingOrPublishingTwice() {
        withFixture(f -> {
            var publishing = new AtomicBoolean(); var armed = new AtomicBoolean(); var commits = new AtomicInteger(); var uploads = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (inserts(e, "connector_artifacts.files")) uploads.incrementAndGet();
                    if (updates(e, "connector_artifacts.files")) publishing.set(true);
                    if (e.getCommandName().equals("commitTransaction") && armed.get()) commits.incrementAndGet();
                }
                @Override public void commandSucceeded(CommandSucceededEvent e) {
                    if (e.getCommandName().equals("update") && publishing.get() && armed.compareAndSet(false, true)) {
                        f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                                .append("data", new Document("failCommands", List.of("commitTransaction"))
                                        .append("writeConcernError", new Document("code", 91).append("errmsg", "test reply lost"))
                                        .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
                    }
                }
            };
            try (var observed = client(listener)) {
                try { assertThat(f.registry(observed, ACTIVE).register("example", "1", RegistrationSource.SEED, JAR).newlyRegistered()).isTrue(); }
                finally { clearFailpoint(f.client); }
                assertThat(uploads).hasValue(1); assertThat(commits.get()).isGreaterThanOrEqualTo(2);
                assertThat(f.admissions()).isEqualTo(1); assertThat(f.files().countDocuments()).isEqualTo(1); assertThat(f.registry().artifact(HASH)).contains(JAR);
            }
        });
    }

    @Test void guardLossAfterASuccessfulCommitNeverCleansThePublishedResult() {
        withFixture(f -> {
            var live = new AtomicBoolean(true); var publishing = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) { if (updates(e, "connector_artifacts.files")) publishing.set(true); }
                @Override public void commandSucceeded(CommandSucceededEvent e) { if (e.getCommandName().equals("commitTransaction") && publishing.get()) live.set(false); }
            };
            try (var observed = client(listener)) {
                assertCode(() -> f.registry(observed, guard(live)).register("example", "1", RegistrationSource.SEED, JAR), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.admissions()).isEqualTo(1); assertThat(f.files().countDocuments()).isEqualTo(1);
            assertThat(f.chunks().countDocuments()).isEqualTo(1); assertThat(f.registry().artifact(HASH)).contains(JAR);
        });
    }

    @Test void missingFilesAfterAnUncertainPublicationDoesNotAuthorizeDeletingItsRemainingChunks() {
        withFixture(f -> {
            var live = new AtomicBoolean(true); var publishing = new AtomicBoolean(); var damaged = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) { if (updates(e, "connector_artifacts.files")) publishing.set(true); }
                @Override public void commandSucceeded(CommandSucceededEvent e) {
                    if (e.getCommandName().equals("commitTransaction") && publishing.get() && damaged.compareAndSet(false, true)) {
                        f.files().deleteOne(new Document()); live.set(false);
                    }
                }
            };
            try (var observed = client(listener)) {
                assertThatThrownBy(() -> f.registry(observed, guard(live)).register("example", "1", RegistrationSource.SEED, JAR))
                        .isInstanceOfSatisfying(TapstateException.class, e -> {
                            assertThat(e.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED);
                            assertThat(e.getSuppressed()).singleElement().isInstanceOfSatisfying(TapstateException.class,
                                    s -> assertThat(s.code()).isEqualTo(IoError.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED));
                        });
            }
            assertThat(f.admissions()).isEqualTo(1); assertThat(f.files().countDocuments()).isZero(); assertThat(f.chunks().countDocuments()).isEqualTo(1);
        });
    }

    @Test void unavailableOwnerReportsUnconfirmedCleanupWithoutGrantingStageVisibility() {
        withFixture(f -> {
            var closing = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) { closing.set(inserts(e, "connector_artifacts.files")); }
                @Override public void commandSucceeded(CommandSucceededEvent e) { if (e.getCommandName().equals("insert") && closing.get()) f.domain.close(); }
            };
            try (var observed = client(listener)) {
                var registry = new MongoCacheConnectorRegistry(observed, observed.getDatabase(f.database.getName()), SCOPE, () -> f.domain.loadBusinessScope(SCOPE), ACTIVE);
                assertThatThrownBy(() -> registry.register("example", "1", RegistrationSource.SEED, JAR)).isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED);
                    assertThat(e.getSuppressed()).singleElement().isInstanceOfSatisfying(TapstateException.class,
                            s -> assertThat(s.code()).isEqualTo(IoError.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED));
                });
            }
            try (var reopened = MongoCacheDomain.connect(f.binding, Duration.ofSeconds(5), "reopened")) {
                assertThat(reopened.connectors(SCOPE, ACTIVE).list()).isEmpty();
            }
            assertThat(f.files().countDocuments()).isEqualTo(1); assertThat(f.chunks().countDocuments()).isEqualTo(1); assertThat(f.admissions()).isZero();
        });
    }

    @Test void previousIncarnationFilesAreAbsentUntilExplicitCurrentRegistration() {
        withFixture(f -> {
            f.registry().register("example", "1", RegistrationSource.SEED, JAR);
            f.files().updateOne(new Document(), new Document("$set", new Document("metadata.cacheScope.cacheIncarnation", "previous-cache")));
            var registry = f.registry(); assertThat(registry.list()).isEmpty(); assertThat(registry.findAll("example")).isEmpty();
            assertThat(registry.artifact(HASH)).isEmpty(); assertThat(registry.hasArtifact(HASH)).isFalse();
            var result = registry.register("example", "2", RegistrationSource.REGISTER, JAR);
            assertThat(result.newlyRegistered()).isTrue(); assertThat(result.registration().source()).isEqualTo(RegistrationSource.REGISTER);
            assertThat(registry.artifact(HASH)).contains(JAR); assertThat(f.files().countDocuments()).isEqualTo(2);
        });
    }

    @Test void corruptFilenameNeverBecomesASensitiveDiagnosticIdentifier() {
        withFixture(f -> {
            var registry = f.registry(); registry.register("example", "1", RegistrationSource.SEED, JAR);
            f.files().updateOne(new Document(), new Document("$set", new Document("filename", "mongodb://user:private-sentinel@host")));
            assertUnreadable(registry::list); assertUnreadable(() -> registry.findAll("example"));
        });
    }

    @Test void damagedMetadataAndChunksFailCodedWithoutRawDriverCauses() {
        withFixture(f -> {
            var registry = f.registry(); registry.register("example", "1", RegistrationSource.SEED, JAR); var saved = f.files().find().first();
            for (var mutation : List.of(new Document("metadata.cacheScope.bindingVersion", 1), new Document("metadata.extra", "private-sentinel"),
                    new Document("metadata.source", "private-sentinel"), new Document("length", 3.5d))) {
                f.files().updateOne(new Document(), new Document("$set", mutation)); assertUnreadable(registry::list);
                assertUnreadable(() -> registry.hasArtifact(HASH)); f.files().replaceOne(new Document(), saved);
            }
            f.chunks().updateOne(new Document(), new Document("$set", new Document("data", new Binary(new byte[]{1}))));
            assertUnreadable(() -> registry.artifact(HASH)); assertThat(f.files().countDocuments()).isEqualTo(1);
        });
    }

    @Test void guardLossDuringDownloadNeverReturnsPreviouslyReadArtifactBytes() {
        withFixture(f -> {
            f.registry().register("example", "1", RegistrationSource.SEED, JAR);
            var live = new AtomicBoolean(true); var chunks = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    chunks.set(e.getCommandName().equals("find") && e.getCommand().getString("find").getValue().equals("connector_artifacts.chunks"));
                }
                @Override public void commandSucceeded(CommandSucceededEvent e) { if (e.getCommandName().equals("find") && chunks.get()) live.set(false); }
            };
            try (var observed = client(listener)) { assertCode(() -> f.registry(observed, guard(live)).artifact(HASH), IoError.CACHE_BUSINESS_FENCED); }
            assertThat(f.registry().artifact(HASH)).contains(JAR); assertThat(f.admissions()).isEqualTo(1);
        });
    }

    @Test void authenticationResetKeepsBusinessArtifactsButRetirementAndMissingStructureNeverRepairThem() {
        withFixture(f -> {
            var registry = f.registry(); registry.register("example", "1", RegistrationSource.SEED, JAR);
            f.domain.advanceAuthGeneration(AUTH, 2, "auth-reset", ACTIVE); assertThat(registry.artifact(HASH)).contains(JAR);
            SystemCollections.CACHE_CONNECTOR_ARTIFACTS.chunksOn(f.database).drop();
            assertCode(() -> f.domain.connectors(SCOPE, ACTIVE), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("connector_artifacts.chunks");
            f.domain.retireBusiness(SCOPE, "retired", ACTIVE); assertCode(registry::list, IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> registry.register("example", "1", RegistrationSource.SEED, JAR), IoError.CACHE_BUSINESS_FENCED);
        });
    }

    private static Runnable guard(AtomicBoolean live) { return () -> { if (!live.get()) throw MongoCacheBusinessIdentity.businessFenced(); }; }
    private static CommandListener publicationBarrier(CountDownLatch staged, CountDownLatch proceed) {
        var once = new AtomicBoolean(); return new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
            if (updates(e, "cache_identity") && once.compareAndSet(false, true)) { staged.countDown(); await(proceed); }
        } };
    }
    private static MongoClient client(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static boolean updates(CommandStartedEvent e, String table) { return e.getCommandName().equals("update") && e.getCommand().getString("update").getValue().equals(table); }
    private static boolean inserts(CommandStartedEvent e, String table) { return e.getCommandName().equals("insert") && e.getCommand().getString("insert").getValue().equals(table); }
    private static void clearFailpoint(MongoClient client) { client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
    private static void await(CountDownLatch latch) { try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); } }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code)); }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> { assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(e.getCause()).isNull(); assertThat(e.toString()).doesNotContain("private-sentinel");
                assertThat(e.args().toString()).doesNotContain("private-sentinel"); }); }
    private static void withFixture(Scenario scenario) {
        String name = "cache_jars_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "jar-test")) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(AUTH, "auth-a", ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareInTransaction(session, ACTIVE);
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY")
                        .append("epoch", 1L).append("operationId", "business-a"));
                scenario.run(new Fixture(client, database, domain, binding));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; }
        catch (Exception e) { throw new AssertionError(e); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding) {
        ConnectorRegistry registry() { return domain.connectors(SCOPE, ACTIVE); }
        ConnectorRegistry registry(MongoClient original, Runnable active) { return new MongoCacheConnectorRegistry(original, original.getDatabase(database.getName()), SCOPE, ACTIVE, active); }
        com.mongodb.client.MongoCollection<Document> files() { return SystemCollections.CACHE_CONNECTOR_ARTIFACTS.indexTargetOn(database); }
        com.mongodb.client.MongoCollection<Document> chunks() { return SystemCollections.CACHE_CONNECTOR_ARTIFACTS.chunksOn(database); }
        long admissions() { return SystemCollections.CACHE_IDENTITY.on(database).find().first().getLong("claimAdmissionFence"); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
