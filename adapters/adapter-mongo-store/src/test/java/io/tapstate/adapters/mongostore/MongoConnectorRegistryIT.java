package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ClientSession;
import com.mongodb.session.ServerSession;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonBoolean;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Witnesses the connector registry against a real Mongo replica-set through the real GridFS store: a
 * registered artifact reads back with its identity and its exact bytes, re-registering the same bytes
 * is a content-hash no-op that stores no second copy, list returns every registration, and an unknown
 * hash has no bytes. Where Docker is absent this aborts on a developer machine and fails in CI, where
 * a skip would be a green build that ran nothing.
 */
@RequiresDocker
class MongoConnectorRegistryIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE)
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");

    private static byte[] jar(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void registerStoresTheRegistrationAndRetrievableBytes() {
        withRegistry(registry -> {
            byte[] jar = jar("mysql-connector-bytes");
            RegistrationOutcome outcome = registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar);

            assertThat(outcome.newlyRegistered()).isTrue();
            ConnectorRegistration registration = outcome.registration();
            assertThat(registration.connectorId()).isEqualTo("mysql");
            assertThat(registration.pdkApiVersion()).isEqualTo("1.3.5");
            assertThat(registration.source()).isEqualTo(RegistrationSource.REGISTER);
            assertThat(registry.artifact(registration.contentHash()).orElseThrow()).isEqualTo(jar);
        });
    }

    @Test
    void reRegisteringTheSameBytesIsAContentHashNoOp() {
        withRegistry(registry -> {
            byte[] jar = jar("mysql-connector-bytes");
            RegistrationOutcome first = registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar);
            RegistrationOutcome again = registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar);

            assertThat(first.newlyRegistered()).isTrue();
            assertThat(again.newlyRegistered()).isFalse();
            // the no-op returns what is stored (the original SEED source), and stores no second copy
            assertThat(again.registration()).isEqualTo(first.registration());
            assertThat(registry.list()).hasSize(1);
        });
    }

    @Test
    void findAnswersAboutOneConnectorAndIsEmptyForAnUnregisteredOne() {
        withRegistry(registry -> {
            registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar("mysql-bytes"));
            registry.register("mongodb", "1.3.5", RegistrationSource.REGISTER, jar("mongodb-bytes"));

            assertThat(registry.findAll("mysql"))
                    .singleElement()
                    .satisfies(found -> assertThat(found.connectorId()).isEqualTo("mysql"));
            assertThat(registry.findAll("postgres")).isEmpty();
        });
    }

    @Test
    void findAllReportsBothArtifactsAndInAStableOrderWhenAnIdCarriesTwo() {
        // One artifact per id is the intended state and a register refuses a second - but two concurrent
        // registers can both pass that check before either stores, and an operator can write out of band.
        // Both have to be reported: a register that saw only one could call a duplicate "already
        // registered", and a read face that saw only one would call an unloadable connector available.
        // And the order has to hold across calls, or anything downstream flips for no observable reason.
        withRegistry(registry -> {
            registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar("mysql-bytes-one"));
            registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar("mysql-bytes-two"));

            List<ConnectorRegistration> first = registry.findAll("mysql");

            // Both are reported - a caller that must refuse the duplicate has to be able to see it.
            assertThat(first).hasSize(2);
            assertThat(registry.list()).hasSize(2);
            assertThat(first).extracting(ConnectorRegistration::contentHash).doesNotHaveDuplicates();
            for (ConnectorRegistration artifact : first) {
                assertThat(registry.artifact(artifact.contentHash()).orElseThrow())
                        .isEqualTo(artifact.source() == RegistrationSource.SEED
                                ? jar("mysql-bytes-one") : jar("mysql-bytes-two"));
            }
            // And reported in the same order every time, so nothing downstream flips between calls.
            assertThat(registry.findAll("mysql")).containsExactlyElementsOf(first);
            assertThat(registry.findAll("mysql")).containsExactlyElementsOf(first);
        });
    }

    @Test
    void listReturnsEveryRegisteredConnector() {
        withRegistry(registry -> {
            registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar("mysql-bytes"));
            registry.register("postgres", "1.3.5", RegistrationSource.REGISTER, jar("postgres-bytes"));

            assertThat(registry.list())
                    .extracting(ConnectorRegistration::connectorId)
                    .containsExactlyInAnyOrder("mysql", "postgres");
        });
    }

    @Test
    void artifactForAnUnknownHashIsEmpty() {
        withRegistry(registry -> assertThat(registry.artifact("deadbeef")).isEmpty());
    }

    @Test
    void threeConcurrentRegistrationsPublishOneFileAndLeaveOnlyWinnerChunks() throws Exception {
        byte[] bytes = new byte[4 * 255 * 1024 + 1];
        Arrays.fill(bytes, (byte) 71);
        String hash = MongoConnectorRegistry.sha256Hex(bytes);
        EmptyHashFindBarrier barrier = new EmptyHashFindBarrier(hash, 3);
        ExecutorService writers = Executors.newFixedThreadPool(3);
        try (MongoClient observer = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = observer.getDatabase("connector_registration_race");
            GridFSBucket bucket = GridFSBuckets.create(database, "connector_artifacts");
            bucket.drop();
            // Prime GridFS's own indexes and retain a control artifact that loser cleanup must not touch.
            MongoConnectorRegistry control = new MongoConnectorRegistry(observer, database);
            RegistrationOutcome retained = control.register(
                    "retained", "1.0", RegistrationSource.SEED, jar("preexisting-artifact"));
            List<Future<RegistrationOutcome>> results = new ArrayList<>();
            for (int member = 0; member < 3; member++) {
                int node = member;
                results.add(writers.submit(() -> {
                    MongoClientSettings settings = MongoClientSettings.builder()
                            .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                            .addCommandListener(barrier)
                            .build();
                    try (MongoClient client = MongoClients.create(settings)) {
                        MongoConnectorRegistry registry = new MongoConnectorRegistry(client, client.getDatabase("connector_registration_race"));
                        return registry.register("mongodb", "1.3." + node,
                                node == 0 ? RegistrationSource.SEED : RegistrationSource.REGISTER, bytes);
                    }
                }));
            }
            List<RegistrationOutcome> outcomes = new ArrayList<>();
            for (Future<RegistrationOutcome> result : results) {
                outcomes.add(result.get(30, TimeUnit.SECONDS));
            }
            assertThat(barrier.released.get()).as("all three real empty replies crossed the barrier").isEqualTo(3);
            List<Document> files = database.getCollection("connector_artifacts.files")
                    .find(new Document("filename", hash)).into(new ArrayList<>());
            assertThat(files).as("the three forced-empty reads must publish exactly one content hash").hasSize(1);
            assertThat(outcomes.stream().filter(RegistrationOutcome::newlyRegistered)).hasSize(1);
            ConnectorRegistration winner = outcomes.stream().filter(RegistrationOutcome::newlyRegistered)
                    .findFirst().orElseThrow().registration();
            assertThat(outcomes).allSatisfy(outcome -> assertThat(outcome.registration()).isEqualTo(winner));
            assertThat(control.findAll("mongodb")).containsExactly(winner);
            assertThat(control.artifact(hash).orElseThrow()).isEqualTo(bytes);
            ObjectId winnerId = files.getFirst().getObjectId("_id");
            assertThat(database.getCollection("connector_artifacts.chunks")
                    .countDocuments(new Document("files_id", winnerId))).isEqualTo(5);
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(6);
            assertThat(control.artifact(retained.registration().contentHash()).orElseThrow())
                    .isEqualTo(jar("preexisting-artifact"));
            assertThat(control.list()).containsExactlyInAnyOrder(winner, retained.registration());
        } finally {
            writers.shutdownNow();
            assertThat(writers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** Holds each real empty lookup reply until every member has observed the same absent hash. */
    private static final class EmptyHashFindBarrier implements CommandListener {
        private final String hash;
        private final CyclicBarrier barrier;
        private final Set<Integer> hashFinds = ConcurrentHashMap.newKeySet();
        private final AtomicInteger released = new AtomicInteger();

        private EmptyHashFindBarrier(String hash, int members) {
            this.hash = hash;
            this.barrier = new CyclicBarrier(members);
        }

        @Override
        public void commandStarted(CommandStartedEvent event) {
            if (event.getCommandName().equals("find")
                    && event.getCommand().getString("find").getValue().equals("connector_artifacts.files")
                    && event.getCommand().getDocument("filter").containsKey("filename")
                    && event.getCommand().getDocument("filter").getString("filename").getValue().equals(hash)) {
                hashFinds.add(event.getRequestId());
            }
        }

        @Override
        public void commandSucceeded(CommandSucceededEvent event) {
            if (hashFinds.remove(event.getRequestId())
                    && event.getResponse().getDocument("cursor").getArray("firstBatch").isEmpty()) {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                    released.incrementAndGet();
                } catch (Exception e) {
                    throw new AssertionError("the forced concurrent lookups did not all reach the barrier", e);
                }
            }
        }
    }

    @Test
    void anotherUniqueConstraintFailureRemainsCodedAndPreservesExistingArtifact() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_registration_constraint");
            GridFSBucket bucket = GridFSBuckets.create(database, "connector_artifacts");
            bucket.drop();
            MongoConnectorRegistry registry = new MongoConnectorRegistry(client, database);
            RegistrationOutcome retained = registry.register(
                    "mongodb", "1.0", RegistrationSource.SEED, jar("retained-bytes"));
            database.getCollection("connector_artifacts.files").createIndex(
                    new Document("metadata.connectorId", 1),
                    new com.mongodb.client.model.IndexOptions().unique(true).name("injected_other_unique"));

            Throwable failure = catchThrowable(() -> registry.register(
                    "mongodb", "1.1", RegistrationSource.REGISTER, jar("different-bytes")));

            assertThat(failure).isInstanceOf(TapstateException.class);
            assertThat(((TapstateException) failure).code()).isEqualTo(IoError.STORE_UNAVAILABLE);
            assertThat(failure.getCause()).isNull();
            assertThat(((TapstateException) failure).args()).containsExactlyEntriesOf(
                    Map.of("detail", "MongoWriteException code=11000"));
            assertThat(failure.toString()).doesNotContain("injected_other_unique");
            assertThat(registry.artifact(retained.registration().contentHash()).orElseThrow())
                    .isEqualTo(jar("retained-bytes"));
            assertThat(registry.list()).containsExactly(retained.registration());
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments())
                    .as("a failed upload cannot leave chunks beside the retained artifact").isEqualTo(1);
        }
    }

    @Test
    void aChunkWriteConcernFailureAbortsOnlyTheUnpublishedAttempt() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_chunk_write_concern");
            GridFSBuckets.create(database, "connector_artifacts").drop();
            MongoConnectorRegistry control = new MongoConnectorRegistry(client, database);
            byte[] retainedBytes = jar("retained-artifact");
            RegistrationOutcome retained = control.register("retained", "1.0", RegistrationSource.SEED, retainedBytes);
            AtomicBoolean fired = new AtomicBoolean();
            MongoConnectorRegistry failing = new MongoConnectorRegistry(client, failInsert(
                    database, "connector_artifacts.chunks", fired, true));
            byte[] bytes = new byte[3 * 255 * 1024 + 1];
            Arrays.fill(bytes, (byte) 19);

            Throwable failure = catchThrowable(() -> failing.register("new", "1.0", RegistrationSource.REGISTER, bytes));

            assertThat(fired).isTrue();
            assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                    coded -> assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(control.list()).containsExactly(retained.registration());
            assertThat(control.artifact(retained.registration().contentHash()).orElseThrow()).isEqualTo(retainedBytes);
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void anAmbiguousFileInsertDoesNotDeleteThePublishedArtifactAndRetryFindsIt() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_file_write_concern");
            GridFSBuckets.create(database, "connector_artifacts").drop();
            MongoConnectorRegistry control = new MongoConnectorRegistry(client, database);
            AtomicBoolean fired = new AtomicBoolean();
            CommitFailure fault = new CommitFailure(client, 2, true, false);
            MongoConnectorRegistry failing = new MongoConnectorRegistry(fault.client(), database);
            byte[] bytes = jar("published-before-write-concern-failed");

            Throwable failure = catchThrowable(() -> failing.register("new", "1.0", RegistrationSource.REGISTER, bytes));

            assertThat(fault.fired).isTrue();
            assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                    coded -> assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            RegistrationOutcome retry = control.register("new", "1.0", RegistrationSource.REGISTER, bytes);
            assertThat(retry.newlyRegistered()).isFalse();
            assertThat(control.artifact(retry.registration().contentHash()).orElseThrow()).isEqualTo(bytes);
            assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(1);
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void anUncommittedAmbiguousPublicationLeavesNoChunks() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_ambiguous_noncommit_probe");
            GridFSBuckets.create(database, "connector_artifacts").drop();
            AtomicBoolean fired = new AtomicBoolean();
            MongoConnectorRegistry registry = new MongoConnectorRegistry(client, failInsert(database, "connector_artifacts.files", fired, false));
            byte[] bytes = jar("ambiguous-uncommitted-upload");
            assertThat(catchThrowable(() -> registry.register("new", "1.0", RegistrationSource.REGISTER, bytes)))
                    .isInstanceOf(TapstateException.class);
            assertThat(fired).isTrue();
            assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isZero();
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments())
                    .as("uncommitted ambiguous close leaves an unlinked attempt requiring reconciliation").isZero();
        }
    }

    @Test
    void delayedMetadataCommitCannotPublishAfterRecoveryWithRetryWritesDisabled() {
        assertLateCommitIsFenced(false, 2, jar("metadata-commit-delayed"));
    }

    @Test
    void delayedMetadataCommitCannotPublishAfterRecoveryWithRetryWritesEnabled() {
        assertLateCommitIsFenced(true, 2, jar("metadata-commit-delayed"));
    }

    @Test
    void delayedChunkCommitCannotRecreateChunksAfterEarlierBatchesWereCleaned() {
        byte[] bytes = new byte[17 * 255 * 1024 + 1];
        Arrays.fill(bytes, (byte) 29);
        assertLateCommitIsFenced(false, 2, bytes);
    }

    private static void assertLateCommitIsFenced(boolean retryWrites, int failingCommit, byte[] bytes) {
        assertLateCommitIsFenced(REPLICA_SET.getReplicaSetUrl(), retryWrites, failingCommit, bytes, database -> { });
    }

    static void assertLateCommitIsFenced(String uri, boolean retryWrites, int failingCommit, byte[] bytes,
                                         java.util.function.Consumer<MongoDatabase> placement) {
        AtomicInteger abortFailures = new AtomicInteger();
        List<BsonDocument> commitCommands = new java.util.concurrent.CopyOnWriteArrayList<>();
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri)).retryWrites(retryWrites)
                .addCommandListener(new CommandListener() {
                    @Override public void commandFailed(CommandFailedEvent event) {
                        if (event.getCommandName().equals("abortTransaction")) abortFailures.incrementAndGet();
                    }
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("commitTransaction")) commitCommands.add(event.getCommand().clone());
                    }
                }).build();
        try (MongoClient client = MongoClients.create(settings)) {
            MongoDatabase database = client.getDatabase("connector_late_commit_" + bytes.length + "_" + retryWrites);
            GridFSBuckets.create(database, "connector_artifacts").drop();
            MongoConnectorRegistry control = new MongoConnectorRegistry(client, database);
            byte[] retainedBytes = jar("unrelated-retained-upload");
            RegistrationOutcome retained = control.register("retained", "1.0", RegistrationSource.SEED, retainedBytes);
            placement.accept(database);
            CommitFailure fault = new CommitFailure(client, failingCommit, false, true);
            MongoConnectorRegistry registry = new MongoConnectorRegistry(fault.client(), database);
            try {
                assertThat(catchThrowable(() -> registry.register("new", "1.0", RegistrationSource.REGISTER, bytes)))
                        .isInstanceOfSatisfying(TapstateException.class,
                                coded -> assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                assertThat(fault.fired).isTrue();
                assertThat(abortFailures.get()).as("the real server refused the attempted abort").isEqualTo(1);
                assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(1);
                assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(1);
                // Preserve the original wire identity, rather than reusing a mutable advanced session.
                Throwable delayed = catchThrowable(() -> replay(client, fault,
                        new BsonDocument("commitTransaction", new BsonInt32(1))
                                .append("txnNumber", new BsonInt64(fault.transactionNumber)).append("autocommit", BsonBoolean.FALSE)));
                assertThat(delayed).isInstanceOfSatisfying(MongoCommandException.class,
                        rejected -> assertThat(rejected.getErrorCodeName()).isIn("TransactionTooOld", "NoSuchTransaction"));
                BsonDocument replayed = commitCommands.getLast();
                assertThat(replayed.getDocument("lsid")).isEqualTo(fault.sessionId);
                assertThat(replayed.getInt64("txnNumber").getValue()).isEqualTo(fault.transactionNumber);
                assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(1);
                assertThat(control.artifact(retained.registration().contentHash()).orElseThrow()).isEqualTo(retainedBytes);
                RegistrationOutcome retry = control.register("new", "1.0", RegistrationSource.REGISTER, bytes);
                assertThat(retry.newlyRegistered()).isTrue();
                assertThat(control.register("new", "1.0", RegistrationSource.REGISTER, bytes).newlyRegistered()).isFalse();
                assertThat(control.artifact(retry.registration().contentHash()).orElseThrow()).isEqualTo(bytes);
                long ownChunks = (bytes.length + 255L * 1024 - 1) / (255 * 1024);
                assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(ownChunks + 1);
            } finally {
                client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        }
    }

    /** Explicit wire identity avoids the driver's implicit-session lsid being appended twice. */
    private static Document replay(MongoClient client, CommitFailure original, BsonDocument command) {
        try (ClientSession delegate = client.startSession()) {
            ServerSession server = (ServerSession) Proxy.newProxyInstance(ServerSession.class.getClassLoader(),
                    new Class<?>[]{ServerSession.class}, (proxy, method, args) -> {
                        if (method.getName().equals("getIdentifier")) return original.sessionId;
                        try { return method.invoke(delegate.getServerSession(), args); }
                        catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                    });
            ClientSession wire = (ClientSession) Proxy.newProxyInstance(ClientSession.class.getClassLoader(),
                    new Class<?>[]{ClientSession.class}, (proxy, method, args) -> {
                        if (method.getName().equals("getServerSession")) return server;
                        try { return method.invoke(delegate, args); }
                        catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                    });
            return client.getDatabase("admin").runCommand(wire, command);
        }
    }

    @Test
    void gridFsReadersAcceptEmptyExactPartialAndMultipleTransactionArtifacts() {
        withRegistry(registry -> {
            for (int length : new int[]{0, 255 * 1024, 255 * 1024 + 1, 17 * 255 * 1024 + 1}) {
                byte[] bytes = new byte[length];
                Arrays.fill(bytes, (byte) 37);
                RegistrationOutcome result = registry.register("boundary-" + length, "1.0", RegistrationSource.REGISTER, bytes);
                assertThat(registry.artifact(result.registration().contentHash()).orElseThrow()).isEqualTo(bytes);
                assertThat(registry.register("boundary-" + length, "1.0", RegistrationSource.SEED, bytes).newlyRegistered()).isFalse();
            }
        });
    }

    @Test
    void anUnresolvedPreparedPublicationIsRetainedAndReportedWithoutDeletingItsChunks() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_prepared_publication");
            GridFSBuckets.create(database, "connector_artifacts").drop();
            CommitFailure fault = new CommitFailure(client, 2, false, true);
            fault.prepare = true;
            MongoConnectorRegistry registry = new MongoConnectorRegistry(fault.client(), database);
            try {
                assertThat(catchThrowable(() -> registry.register("prepared", "1.0", RegistrationSource.REGISTER, jar("prepared"))))
                        .isInstanceOfSatisfying(TapstateException.class,
                                coded -> assertThat(coded.code()).isEqualTo(IoError.CONNECTOR_UPLOAD_UNSETTLED));
                assertThat(fault.prepared).as("a real server participant was prepared before its commit reply was lost").isTrue();
                assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(1);
            } finally {
                client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                if (fault.prepared) replay(client, fault, new BsonDocument("abortTransaction", new BsonInt32(1))
                        .append("txnNumber", new BsonInt64(fault.transactionNumber)).append("autocommit", BsonBoolean.FALSE));
            }
            assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isZero();
        }
    }

    @Test
    void failedRecoveryCommitRemainsExplicitWhetherItsCleanupWasAppliedOrNot() {
        for (boolean applied : new boolean[]{false, true}) {
            try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
                MongoDatabase database = client.getDatabase("connector_recovery_commit_" + applied);
                GridFSBuckets.create(database, "connector_artifacts").drop();
                CommitFailure fault = new CommitFailure(client, 2, applied, false);
                MongoConnectorRegistry registry = new MongoConnectorRegistry(fault.client(),
                        failInsert(database, "connector_artifacts.files", new AtomicBoolean(), false));
                assertThat(catchThrowable(() -> registry.register("new", "1.0", RegistrationSource.REGISTER, jar("cleanup"))))
                        .isInstanceOfSatisfying(TapstateException.class,
                                coded -> assertThat(coded.code()).isEqualTo(IoError.CONNECTOR_UPLOAD_UNSETTLED));
                assertThat(fault.fired).isTrue();
                assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isZero();
                assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(applied ? 0 : 1);
            }
        }
    }

    @Test
    void aFailedRecoveryReadReportsTheRetainedAttemptWithoutClaimingCleanup() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_recovery_unsettled");
            GridFSBuckets.create(database, "connector_artifacts").drop();
            MongoDatabase failing = (MongoDatabase) Proxy.newProxyInstance(MongoDatabase.class.getClassLoader(),
                    new Class<?>[]{MongoDatabase.class}, (proxy, method, args) -> {
                        try {
                            Object result = method.invoke(database, args);
                            if (method.getName().equals("getCollection") && args[0].equals("connector_artifacts.files")) {
                                return failRecoveryRead((MongoCollection<?>) result);
                            }
                            return result;
                        } catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                    });
            MongoConnectorRegistry registry = new MongoConnectorRegistry(client, failing);
            Throwable failure = catchThrowable(() -> registry.register("new", "1.0", RegistrationSource.REGISTER, jar("retained")));
            assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
                assertThat(coded.code()).isEqualTo(IoError.CONNECTOR_UPLOAD_UNSETTLED);
                assertThat(coded.args()).containsEntry("hash", MongoConnectorRegistry.sha256Hex(jar("retained")));
                assertThat(coded.args().get("attempt").toString()).matches("[a-f0-9]{24}");
                assertThat(coded.getCause()).isNull();
                assertThat(coded.toString()).doesNotContain("injected");
            });
            assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isZero();
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(1);
        }
    }

    private static MongoCollection<?> failRecoveryRead(MongoCollection<?> collection) {
        return (MongoCollection<?>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[]{MongoCollection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("insertOne") || method.getName().equals("find")
                            && args != null && args[0] instanceof ClientSession) throw writeConcernFailure();
                    try {
                        Object result = method.invoke(collection, args);
                        return result instanceof MongoCollection<?> next ? failRecoveryRead(next) : result;
                    } catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                });
    }

    /** Loses one commit result, optionally leaving the real server's transaction open after abort fails. */
    private static final class CommitFailure {
        private final MongoClient delegate;
        private final int atCommit;
        private final boolean applied;
        private final boolean refuseAbort;
        private boolean prepare;
        private boolean prepared;
        private final AtomicBoolean fired = new AtomicBoolean();
        private BsonDocument sessionId;
        private long transactionNumber;

        private CommitFailure(MongoClient delegate, int atCommit, boolean applied, boolean refuseAbort) {
            this.delegate = delegate;
            this.atCommit = atCommit;
            this.applied = applied;
            this.refuseAbort = refuseAbort;
        }

        private MongoClient client() {
            return (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(), new Class<?>[]{MongoClient.class},
                    (proxy, method, args) -> {
                        try {
                            Object result = method.invoke(delegate, args);
                            if (result instanceof ClientSession session) return session(session);
                            return result;
                        } catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                    });
        }

        private ClientSession session(ClientSession session) {
            AtomicInteger commits = new AtomicInteger();
            return (ClientSession) Proxy.newProxyInstance(ClientSession.class.getClassLoader(), new Class<?>[]{ClientSession.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("commitTransaction") && commits.incrementAndGet() == atCommit
                                && fired.compareAndSet(false, true)) {
                            sessionId = session.getServerSession().getIdentifier().clone();
                            transactionNumber = session.getServerSession().getTransactionNumber();
                            if (prepare) {
                                delegate.getDatabase("admin").runCommand(session, new Document("prepareTransaction", 1));
                                prepared = true;
                            }
                            if (applied) session.commitTransaction();
                            if (refuseAbort) {
                                delegate.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                                        .append("mode", "alwaysOn")
                                        .append("data", new Document("failCommands", List.of("abortTransaction"))
                                                .append("errorCode", 1)));
                            }
                            throw writeConcernFailure();
                        }
                        try { return method.invoke(session, args); }
                        catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
                    });
        }
    }

    /** Injects one driver failure at the real insert boundary, after the selected write was applied. */
    private static MongoDatabase failInsert(MongoDatabase database, String collection, AtomicBoolean fired, boolean afterWrite) {
        return (MongoDatabase) Proxy.newProxyInstance(MongoDatabase.class.getClassLoader(), new Class<?>[]{MongoDatabase.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(database, args);
                        if (method.getName().equals("getCollection") && args[0].equals(collection)) {
                            return failInsert((MongoCollection<?>) result, fired, afterWrite);
                        }
                        return result;
                    } catch (InvocationTargetException wrapped) {
                        throw wrapped.getCause();
                    }
                });
    }

    private static MongoCollection<?> failInsert(MongoCollection<?> collection, AtomicBoolean fired, boolean afterWrite) {
        return (MongoCollection<?>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(), new Class<?>[]{MongoCollection.class},
                (proxy, method, args) -> {
                    boolean inject = (method.getName().equals("insertOne") || method.getName().equals("insertMany"))
                            && fired.compareAndSet(false, true);
                    try {
                        if (inject && !afterWrite) throw writeConcernFailure();
                        Object result = method.invoke(collection, args);
                        if (inject) throw writeConcernFailure();
                        if (result instanceof MongoCollection<?> next) return failInsert(next, fired, afterWrite);
                        return result;
                    } catch (InvocationTargetException wrapped) {
                        throw wrapped.getCause();
                    }
                });
    }

    private static MongoException writeConcernFailure() {
        return new MongoWriteConcernException(new WriteConcernError(64, "WriteConcernFailed",
                "injected-write-concern-failure", new BsonDocument()), new ServerAddress());
    }

    @Test
    void preexistingIdenticalHashesRefuseBindingWithoutRemovingFilesOrChunks() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_registration_duplicates");
            GridFSBucket bucket = GridFSBuckets.create(database, "connector_artifacts");
            bucket.drop();
            byte[] bytes = jar("preexisting-duplicate-bytes");
            String hash = MongoConnectorRegistry.sha256Hex(bytes);
            com.mongodb.client.gridfs.model.GridFSUploadOptions options =
                    new com.mongodb.client.gridfs.model.GridFSUploadOptions()
                            .metadata(MongoConnectorRegistry.metadata("mongodb", "1.0", RegistrationSource.SEED));
            ObjectId first = bucket.uploadFromStream(hash, new java.io.ByteArrayInputStream(bytes), options);
            ObjectId second = bucket.uploadFromStream(hash, new java.io.ByteArrayInputStream(bytes), options);
            List<Document> before = database.getCollection("connector_artifacts.files")
                    .find().sort(new Document("_id", 1)).into(new ArrayList<>());

            Throwable failure = catchThrowable(() -> new MongoConnectorRegistry(client, database));

            assertThat(database.getCollection("connector_artifacts.files")
                    .find().sort(new Document("_id", 1)).into(new ArrayList<>())).isEqualTo(before);
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(2);
            for (ObjectId id : List.of(first, second)) {
                java.io.ByteArrayOutputStream stored = new java.io.ByteArrayOutputStream();
                bucket.downloadToStream(id, stored);
                assertThat(stored.toByteArray()).isEqualTo(bytes);
            }
            assertThat(failure).isInstanceOf(TapstateException.class);
            assertThat(((TapstateException) failure).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(((TapstateException) failure).args()).containsEntry("id", hash);
        }
    }

    @Test
    void aPartialFilenameConstraintCannotHideExistingDuplicates() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_registration_partial_constraint");
            GridFSBucket bucket = GridFSBuckets.create(database, "connector_artifacts");
            bucket.drop();
            byte[] bytes = jar("excluded-seed-bytes");
            String hash = MongoConnectorRegistry.sha256Hex(bytes);
            database.getCollection("connector_artifacts.files").createIndex(
                    new Document("filename", 1), new com.mongodb.client.model.IndexOptions()
                            .name("filename_idx").unique(true)
                            .partialFilterExpression(new Document("metadata.source", "REGISTER")));
            com.mongodb.client.gridfs.model.GridFSUploadOptions options =
                    new com.mongodb.client.gridfs.model.GridFSUploadOptions()
                            .metadata(MongoConnectorRegistry.metadata("mongodb", "1.0", RegistrationSource.SEED));
            bucket.uploadFromStream(hash, new java.io.ByteArrayInputStream(bytes), options);
            bucket.uploadFromStream(hash, new java.io.ByteArrayInputStream(bytes), options);

            Throwable failure = catchThrowable(() -> new MongoConnectorRegistry(client, database));

            assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(2);
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(2);
            assertThat(failure).isInstanceOf(TapstateException.class);
            assertThat(((TapstateException) failure).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(((TapstateException) failure).args()).containsEntry("field", "index");
        }
    }

    @Test
    void outOfBandSameHashCopiesRemainVisibleAndCannotBeChosenArbitrarily() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("connector_registration_raw_corruption");
            GridFSBucket bucket = GridFSBuckets.create(database, "connector_artifacts");
            bucket.drop();
            MongoConnectorRegistry registry = new MongoConnectorRegistry(client, database);
            byte[] bytes = jar("raw-duplicate-bytes");
            RegistrationOutcome first = registry.register("mongodb", "1.0", RegistrationSource.SEED, bytes);
            database.getCollection("connector_artifacts.files").dropIndex("filename_idx");
            bucket.uploadFromStream(first.registration().contentHash(), new java.io.ByteArrayInputStream(bytes),
                    new com.mongodb.client.gridfs.model.GridFSUploadOptions()
                            .metadata(MongoConnectorRegistry.metadata("mongodb", "1.0", RegistrationSource.SEED)));

            assertThat(registry.list()).containsExactly(first.registration(), first.registration());
            assertThat(registry.findAll("mongodb")).containsExactly(first.registration(), first.registration());
            for (Runnable query : List.<Runnable>of(
                    () -> registry.register("mongodb", "1.0", RegistrationSource.SEED, bytes),
                    () -> registry.artifact(first.registration().contentHash()),
                    () -> registry.hasArtifact(first.registration().contentHash()))) {
                Throwable failure = catchThrowable(query::run);
                assertThat(failure).isInstanceOf(TapstateException.class);
                assertThat(((TapstateException) failure).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            }
            assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(2);
            assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isEqualTo(2);
        }
    }

    private interface RegistryTest {
        void run(MongoConnectorRegistry registry);
    }

    /** Runs a test body against a fresh registry over a clean GridFS bucket on the real replica-set. */
    private static void withRegistry(RegistryTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("tapstate");
            GridFSBucket bucket = GridFSBuckets.create(database, "connector_artifacts");
            bucket.drop();
            test.run(new MongoConnectorRegistry(client, database));
        }
    }
}
