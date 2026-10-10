package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.client.MongoClient;
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
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

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
            MongoConnectorRegistry control = new MongoConnectorRegistry(database);
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
                        MongoConnectorRegistry registry = new MongoConnectorRegistry(client.getDatabase("connector_registration_race"));
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
            MongoConnectorRegistry registry = new MongoConnectorRegistry(database);
            RegistrationOutcome retained = registry.register(
                    "mongodb", "1.0", RegistrationSource.SEED, jar("retained-bytes"));
            database.getCollection("connector_artifacts.files").createIndex(
                    new Document("metadata.connectorId", 1),
                    new com.mongodb.client.model.IndexOptions().unique(true).name("injected_other_unique"));

            Throwable failure = catchThrowable(() -> registry.register(
                    "mongodb", "1.1", RegistrationSource.REGISTER, jar("different-bytes")));

            assertThat(failure).isInstanceOf(TapstateException.class);
            assertThat(((TapstateException) failure).code()).isEqualTo(IoError.STORE_UNAVAILABLE);
            assertThat(failure.getCause()).isInstanceOf(MongoWriteException.class);
            MongoWriteException driver = (MongoWriteException) failure.getCause();
            assertThat(driver.getError().getMessage()).contains("injected_other_unique");
            assertThat(registry.artifact(retained.registration().contentHash()).orElseThrow())
                    .isEqualTo(jar("retained-bytes"));
            assertThat(registry.list()).containsExactly(retained.registration());
        }
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

            Throwable failure = catchThrowable(() -> new MongoConnectorRegistry(database));

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

            Throwable failure = catchThrowable(() -> new MongoConnectorRegistry(database));

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
            MongoConnectorRegistry registry = new MongoConnectorRegistry(database);
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
            test.run(new MongoConnectorRegistry(database));
        }
    }
}
