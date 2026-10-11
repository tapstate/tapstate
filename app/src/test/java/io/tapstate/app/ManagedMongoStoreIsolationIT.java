package io.tapstate.app;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Enforced Mongo authorization, not an unauthenticated simulation of database routing. */
@RequiresDocker
class ManagedMongoStoreIsolationIT {
    enum Missing { OPERATOR, VIEWS }
    private static final String PASSWORD = "controlled-mongo-permissions-password";
    private static final String REPLICA_SET = "permissions-rs";
    @Container
    private static final GenericContainer<?> MONGO = mongo();
    @TempDir Path work;

    private static GenericContainer<?> mongo() {
        byte[] key = new byte[64];
        new java.security.SecureRandom().nextBytes(key);
        return new GenericContainer<>(DockerImageName.parse("mongo:7.0"))
                .withExposedPorts(27017)
                .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
                .withCopyToContainer(Transferable.of(Base64.getEncoder().encode(key), 0400), "/data/configdb/member.key")
                .withCommand("--replSet", REPLICA_SET, "--keyFile", "/data/configdb/member.key", "--bind_ip_all")
                // The image opens a temporary unauthenticated server while creating its administrator.
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*MongoDB init process complete; ready for start up.*", 1))
                        .withStrategy(Wait.forLogMessage("(?i).*waiting for connections.*", 2))
                        .withStrategy(Wait.forListeningPort())).withStartupTimeout(Duration.ofSeconds(90));
    }

    @BeforeAll
    static void initiateAuthenticatedReplicaSet() throws Exception {
        var initiated = MONGO.execInContainer("mongosh", "--quiet", "--username", "root", "--password", PASSWORD,
                "--authenticationDatabase", "admin", "--eval", "rs.initiate({_id:'" + REPLICA_SET
                        + "',members:[{_id:0,host:'localhost:27017'}]})");
        assertThat(initiated.getExitCode()).withFailMessage("authenticated replica initialization failed: %s %s; container: %s",
                initiated.getStdout().replace(PASSWORD, "<redacted>"),
                initiated.getStderr().replace(PASSWORD, "<redacted>"), MONGO.getLogs().replace(PASSWORD, "<redacted>")).isZero();
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                if (Boolean.TRUE.equals(admin.getDatabase("admin").runCommand(new Document("hello", 1))
                        .getBoolean("isWritablePrimary"))) return;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
        throw new AssertionError("the authenticated replica set did not elect a primary");
    }

    @Test
    void twoClusterUsersCanUseOnlyTheirOwnThreeDatabasesAndRecoverAfterRestart() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String a = "isolation_a_" + suffix;
        String b = "isolation_b_" + suffix;
        String userA = "user_a_" + suffix;
        String userB = "user_b_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, userA, List.of(a, a + "_operator", a + "_views"));
            createUser(admin, userB, List.of(b, b + "_operator", b + "_views"));
            try (MongoClient ownA = MongoClients.create(uri(userA, a));
                 MongoClient ownB = MongoClients.create(uri(userB, b));
                 MongoClient anonymous = MongoClients.create("mongodb://" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017)
                         + "/" + a + "?directConnection=true")) {
                assertDenied(anonymous, a);
                for (String other : List.of(b, b + "_operator", b + "_views")) assertDenied(ownA, other);
                for (String other : List.of(a, a + "_operator", a + "_views")) assertDenied(ownB, other);

                for (String metadata : List.of(a, b)) {
                    String user = metadata.equals(a) ? userA : userB;
                    StorageProof proof;
                    try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                        StorePort store = first.getBean(StorePort.class);
                        proof = writeStorageProof(store, metadata);
                        assertStorageProof(admin.getDatabase(metadata), store, proof);
                        store.keyedState().save("permissions-proof", "same-key", metadata.getBytes(StandardCharsets.UTF_8));
                        assertViewsNotSeeded(admin.getDatabase(metadata), store);
                        try (MongoClient view = MongoClients.create(uri(user, metadata + "_views"))) {
                            view.getDatabase(metadata + "_views").getCollection("proof")
                                    .insertOne(new Document("_id", "same-key").append("owner", metadata));
                        }
                        assertNoProbes(admin, metadata);
                    }
                    try (ConfigurableApplicationContext restarted = start(uri(user, metadata), metadata)) {
                        assertStorageProof(admin.getDatabase(metadata), restarted.getBean(StorePort.class), proof);
                        assertViewsNotSeeded(admin.getDatabase(metadata), restarted.getBean(StorePort.class));
                        assertThat(restarted.getBean(StorePort.class).keyedState().load("permissions-proof", "same-key"))
                                .hasValueSatisfying(value -> assertThat(value).isEqualTo(metadata.getBytes(StandardCharsets.UTF_8)));
                        assertThat(admin.getDatabase(metadata + "_views").getCollection("proof")
                                .find(new Document("_id", "same-key")).first().getString("owner")).isEqualTo(metadata);
                        assertNoProbes(admin, metadata);
                    }
                }
            }
        }
    }

    private record StorageProof(CheckpointDoc checkpoint, SrsMeta meta, List<SrsLogRecord> log,
            RateSample sample, List<ConnectorRegistration> connectors) { }

    private static StorageProof writeStorageProof(StorePort store, String cluster) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        store.state().create("permissions-proof", "{\"cluster\":\"" + cluster + "\"}", now);
        store.meta().create("permissions-chain", "7d");
        long epoch = store.meta().openEpoch("permissions-chain");
        ChainPosition position = new ChainPosition(new SourceOrder(epoch, 3), cluster + "/position-3");
        var claim = store.workloadClaims().acquire(
                new WorkloadClaimKey(cluster, WorkloadClaimType.CAPTURE, "permissions-capture"),
                new WorkloadOwner("permissions-node", "permissions-boot"), 1, Duration.ofMinutes(5)).claim();
        WorkloadClaimFence fence = WorkloadClaimFence.from(claim);
        List<SrsLogRecord> log = List.of(
                new SrsLogRecord(cluster + "/position-1", Op.INSERT, 1, null,
                        Map.of("id", 1, "owner", cluster), 0, fence),
                new SrsLogRecord(cluster + "/position-2", Op.UPDATE, 2,
                        Map.of("id", 1, "owner", cluster), Map.of("id", 1, "owner", cluster, "changed", true), 0, fence),
                new SrsLogRecord(cluster + "/position-3", Op.DELETE, 3,
                        Map.of("id", 1, "owner", cluster, "changed", true), null, 0, fence));
        store.srsLog().storeAll("srs.permissions", 1, log);
        store.meta().advanceSourceReadOffset("permissions-chain", position);
        assertThat(store.workloadClaims().release(claim)).isTrue();
        assertThatThrownBy(() -> store.srsLog().storeAll("srs.permissions", 4, List.of(
                new SrsLogRecord("released-owner-position", Op.INSERT, 4, null, Map.of("id", 2), 0, fence))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(store.srsLog().load("srs.permissions", 4)).isEmpty();
        RateSample sample = new RateSample("permissions-proof", now, Map.of("records.out", 3L), Map.of(), now.minusSeconds(60));
        store.rateHistory().append(sample);
        SrsMeta meta = store.meta().read("permissions-chain").orElseThrow();
        assertThat(meta.sourceRead()).isEqualTo(position);
        assertThat(meta.epoch()).isEqualTo(epoch).isEqualTo(1);
        assertThat(meta.retention()).isEqualTo("7d");
        assertThat(meta.consumerOffsets()).isEmpty();
        assertThat(meta.schemaHistory()).isEmpty();
        assertThat(meta.sourceReadAt()).isNotNull().isAfterOrEqualTo(now.minusSeconds(2))
                .isBeforeOrEqualTo(Instant.now().plusSeconds(2));
        return new StorageProof(CheckpointDoc.initial("permissions-proof", "{\"cluster\":\"" + cluster + "\"}", now),
                meta, log, sample, store.connectors().list());
    }

    private static void assertStorageProof(MongoDatabase database, StorePort store, StorageProof proof) throws Exception {
        assertThat(store.state().read("permissions-proof")).contains(proof.checkpoint());
        assertThat(store.meta().read("permissions-chain")).contains(proof.meta());
        assertThat(store.srsLog().largestSequence("srs.permissions")).isEqualTo(3);
        for (int index = 0; index < proof.log().size(); index++) {
            assertThat(store.srsLog().load("srs.permissions", index + 1)).contains(proof.log().get(index));
        }
        assertThat(store.srsLog().load("srs.permissions", 4)).isEmpty();
        assertThat(store.rateHistory().readPage("permissions-proof", proof.sample().observedAt().minusSeconds(1),
                proof.sample().observedAt().plusSeconds(1), null, 1).entries())
                .singleElement().satisfies(entry -> assertThat(entry.sample()).isEqualTo(proof.sample()));
        assertThat(store.connectors().list()).containsExactlyInAnyOrderElementsOf(proof.connectors());
        assertThat(proof.connectors()).extracting(ConnectorRegistration::connectorId).containsExactlyInAnyOrder(
                "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql", "db2");
        for (ConnectorRegistration registration : proof.connectors()) {
            byte[] bytes = store.connectors().artifact(registration.contentHash()).orElseThrow();
            Path seed = CloudConnectorTestInputs.seedDirectory().resolve(registration.connectorId() + "-connector.jar");
            assertThat(bytes.length).isEqualTo(Files.size(seed));
            assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)))
                    .isEqualTo(registration.contentHash());
            MessageDigest expected = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(seed)) {
                byte[] buffer = new byte[8192];
                for (int length; (length = input.read(buffer)) != -1;) expected.update(buffer, 0, length);
            }
            assertThat(HexFormat.of().formatHex(expected.digest())).isEqualTo(registration.contentHash());
        }
        assertThat(SystemCollections.PIPELINE_STATE.on(database).countDocuments(new Document("_id", "permissions-proof"))).isEqualTo(1);
        assertThat(SystemCollections.SRS_META.on(database).countDocuments(new Document("_id", "permissions-chain"))).isEqualTo(1);
        // Change rows share the collection with a private high-water mark; the latter is not an event.
        var logs = SystemCollections.SRS_LOG.on(database);
        assertThat(logs.countDocuments(new Document("_id.ring", "srs.permissions"))).isEqualTo(3);
        assertThat(logs.find(new Document("_id", "srs-log-bounds:srs.permissions")).first())
                .containsEntry("largestSequence", 3L).containsEntry("trimmedThrough", -1L);
        Document history = SystemCollections.PIPELINE_RATE_HISTORY.on(database)
                .find(new Document("pipelineId", "permissions-proof")).first();
        assertThat(history.getDate("observedAt").toInstant()).isEqualTo(proof.sample().observedAt());
        assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(8);
        assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isGreaterThan(8);
        assertThat(historyIndex(database).get("expireAfterSeconds", Number.class).longValue()).isEqualTo(Duration.ofDays(15).toSeconds());
    }

    private ConfigurableApplicationContext start(String metadataUri, String clusterId) {
        return start(metadataUri, clusterId, new AtomicInteger());
    }

    @Test
    void changingHistoryRetentionNeedsOnlyTheHistoryCollectionsAlterPrivilege() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_ttl_" + suffix;
        String user = "user_ttl_" + suffix;
        long originalSeconds = Duration.ofDays(15).toSeconds();
        long changedSeconds = Duration.ofDays(7).toSeconds();
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, metadata + "_operator", metadata + "_views"));
            MongoDatabase database = admin.getDatabase(metadata);
            Instant observedAt = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
            RateSample sample = new RateSample("retention-proof", observedAt,
                    Map.of("records.out", 3L), Map.of(), observedAt.minusSeconds(60));
            try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                first.getBean(StorePort.class).rateHistory().append(sample);
            }
            Document originalIndex = historyIndex(database);
            Document originalCollection = historyCollection(database);
            Document originalSample = SystemCollections.PIPELINE_RATE_HISTORY.on(database).find().first();
            Document originalSource = SystemCollections.ARTIFACTS.on(database).find(new Document("_id", "views")).first();
            assertThat(originalIndex.get("expireAfterSeconds", Number.class).longValue()).isEqualTo(originalSeconds);
            try (ConfigurableApplicationContext unchanged = start(uri(user, metadata), metadata)) {
                assertThat(unchanged.getBean(StorePort.class).rateHistory()
                        .readPage(sample.pipelineId(), observedAt.minusSeconds(1), observedAt.plusSeconds(1), null, 1)
                        .entries()).singleElement().satisfies(entry -> assertThat(entry.sample()).isEqualTo(sample));
            }
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> limited.getDatabase(metadata).runCommand(historyExpiry(changedSeconds)))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger ready = new AtomicInteger();
            Throwable failure = catchThrowable(() -> {
                try (var unexpected = start(uri(user, metadata), metadata, ready,
                        "--tapstate.metrics.history.retention=7d")) {
                    throw new AssertionError("Cloud startup accepted an unchanged TTL under a changed retention");
                }
            });
            assertThat(ready.get()).isZero();
            assertThat(coded(failure)).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                assertThat(error.args()).containsExactlyEntriesOf(Map.of("detail", "MongoCommandException code=13"));
                assertThat(error.getCause()).isNull();
            });
            assertThat(historyIndex(database)).isEqualTo(originalIndex);
            assertThat(historyCollection(database)).isEqualTo(originalCollection);
            assertThat(SystemCollections.PIPELINE_RATE_HISTORY.on(database).find().first()).isEqualTo(originalSample);
            assertThat(SystemCollections.ARTIFACTS.on(database).find(new Document("_id", "views")).first()).isEqualTo(originalSource);

            String role = "alter_history_" + suffix;
            database.runCommand(new Document("createRole", role).append("roles", List.of()).append("privileges", List.of(
                    new Document("resource", new Document("db", metadata)
                            .append("collection", SystemCollections.PIPELINE_RATE_HISTORY.collectionName()))
                            .append("actions", List.of("collMod")))));
            admin.getDatabase("admin").runCommand(new Document("grantRolesToUser", user).append("roles", List.of(
                    new Document("role", role).append("db", metadata))));
            database.getCollection("unrelated_history").createIndex(new Document("observedAt", 1),
                    new IndexOptions().name("observedAt_idx").expireAfter(originalSeconds, TimeUnit.SECONDS));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> limited.getDatabase(metadata).runCommand(
                        historyExpiry(changedSeconds).append("collMod", "unrelated_history")))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger recoveredReady = new AtomicInteger();
            try (ConfigurableApplicationContext recovered = start(uri(user, metadata), metadata, recoveredReady,
                    "--tapstate.metrics.history.retention=7d")) {
                assertThat(recoveredReady.get()).isEqualTo(1);
                assertThat(recovered.getBean(StorePort.class).rateHistory().retention()).isEqualTo(Duration.ofDays(7));
                assertThat(recovered.getBean(StorePort.class).rateHistory()
                        .readPage(sample.pipelineId(), observedAt.minusSeconds(1), observedAt.plusSeconds(1), null, 1)
                        .entries()).singleElement().satisfies(entry -> assertThat(entry.sample()).isEqualTo(sample));
            }
            Document expectedIndex = new Document(originalIndex).append("expireAfterSeconds", changedSeconds);
            Document actualIndex = historyIndex(database);
            assertThat(actualIndex.get("expireAfterSeconds", Number.class).longValue()).isEqualTo(changedSeconds);
            // The server may store the altered expiry as an int rather than the original long.
            assertThat(new Document(actualIndex).append("expireAfterSeconds", changedSeconds)).isEqualTo(expectedIndex);
            assertThat(historyCollection(database)).isEqualTo(originalCollection);
            assertThat(SystemCollections.PIPELINE_RATE_HISTORY.on(database).find().first()).isEqualTo(originalSample);
            assertThat(SystemCollections.ARTIFACTS.on(database).find(new Document("_id", "views")).first()).isEqualTo(originalSource);
            assertNoProbes(admin, metadata);
        }
    }

    @Test
    void missingMigrationIndexPrivilegeIsCodedWithoutTheDriverMessageOrCause() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_migration_" + suffix;
        String user = "user_migration_" + suffix;
        String role = "without_create_index_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            Document builtin = admin.getDatabase("admin").runCommand(new Document("rolesInfo",
                    new Document("role", "readWrite").append("db", metadata)).append("showPrivileges", true))
                    .getList("roles", Document.class).getFirst();
            List<Document> privileges = builtin.getList("privileges", Document.class).stream()
                    .map(privilege -> new Document(privilege).append("actions", privilege.getList("actions", String.class)
                            .stream().filter(action -> !"createIndex".equals(action)).toList())).toList();
            admin.getDatabase(metadata).runCommand(new Document("createRole", role)
                    .append("roles", List.of()).append("privileges", privileges));
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD).append("roles", List.of(
                    new Document("role", role).append("db", metadata),
                    new Document("role", "readWrite").append("db", metadata + "_operator"),
                    new Document("role", "readWrite").append("db", metadata + "_views"))));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> limited.getDatabase(metadata).getCollection("index_denied")
                        .createIndex(new Document("value", 1)))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger ready = new AtomicInteger();
            Throwable failure = catchThrowable(() -> {
                try (var unexpected = start(uri(user, metadata), metadata, ready)) {
                    throw new AssertionError("Cloud startup accepted a failed index migration");
                }
            });
            assertThat(ready.get()).isZero();
            assertThat(coded(failure)).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(MigrationError.CHANGESET_FAILED);
                assertThat(error.args()).containsExactlyEntriesOf(Map.of(
                        "changeset", "V1BaselineIndexes", "cause", "MongoCommandException code=13"));
                assertThat(error.getCause()).isNull();
            });
            Document schema = SystemCollections.SYSTEM_META.on(admin.getDatabase(metadata)).find(new Document("_id", "schema")).first();
            assertThat(schema).doesNotContainKey("installedVersion");
            assertThat(schema.get("lock", Document.class)).doesNotContainKeys("owner", "since", "heartbeat");
            assertNoProbes(admin, metadata);

            admin.getDatabase(metadata).runCommand(new Document("updateRole", role)
                    .append("privileges", builtin.getList("privileges", Document.class)));
            AtomicInteger recoveredReady = new AtomicInteger();
            try (ConfigurableApplicationContext recovered = start(uri(user, metadata), metadata, recoveredReady)) {
                assertThat(recoveredReady.get()).isEqualTo(1);
                assertViewsNotSeeded(admin.getDatabase(metadata), recovered.getBean(StorePort.class));
            }
            assertThat(SystemCollections.SYSTEM_META.on(admin.getDatabase(metadata)).find(new Document("_id", "schema"))
                    .first().getInteger("installedVersion"))
                    .isEqualTo(io.tapstate.adapters.mongostore.migration.MigrationRunner.SUPPORTED_VERSION);
            assertNoProbes(admin, metadata);
        }
    }

    private static Throwable coded(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; depth < 16 && current != null && !(current instanceof TapstateException); depth++) current = current.getCause();
        return current;
    }

    private static Document historyExpiry(long seconds) {
        return new Document("collMod", SystemCollections.PIPELINE_RATE_HISTORY.collectionName())
                .append("index", new Document("name", "observedAt_idx").append("expireAfterSeconds", seconds));
    }

    private static Document historyIndex(MongoDatabase database) {
        return SystemCollections.PIPELINE_RATE_HISTORY.on(database).listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> "observedAt_idx".equals(index.getString("name"))).findFirst().orElseThrow();
    }

    private static Document historyCollection(MongoDatabase database) {
        return database.listCollections().filter(new Document("name", SystemCollections.PIPELINE_RATE_HISTORY.collectionName())).first();
    }

    @Test
    void changingCloudMetadataCredentialsDoesNotCreateOrRenewAViewsSource() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_renewed_" + suffix;
        String user = "user_renewed_" + suffix;
        String renewedPassword = "controlled-renewed-mongo-password";
        SourceResource authored = new SourceResource("views",
                new Metadata(Map.of(), "authored", true, "verified-user"), "mongodb",
                Map.of("isUri", true, "uri", "mongodb://source-user:source-secret@user.example/owned"),
                null, null, null, null);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, metadata + "_operator", metadata + "_views"));
            try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                StorePort store = first.getBean(StorePort.class);
                store.keyedState().save("permissions-proof", "renewed-key", new byte[] {7});
                assertViewsNotSeeded(admin.getDatabase(metadata), store);
                store.artifacts().create(authored);
                try (MongoClient materialization = MongoClients.create(uri(user, metadata + "_views"))) {
                    materialization.getDatabase(metadata + "_views").getCollection("proof")
                            .insertOne(new Document("_id", "before-renewal").append("owner", metadata));
                }
            }
            Document originalSource = SystemCollections.ARTIFACTS.on(admin.getDatabase(metadata))
                    .find(new Document("_id", "views")).first();
            assertThat(originalSource.get("body", Document.class).get("config")).isInstanceOf(String.class);
            admin.getDatabase("admin").runCommand(new Document("updateUser", user).append("pwd", renewedPassword));
            try (MongoClient revoked = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> revoked.getDatabase(metadata).getCollection("proof").countDocuments())
                        .as("the server no longer accepts the original password")
                        .isInstanceOf(MongoSecurityException.class);
            }
            try (ConfigurableApplicationContext restarted = start(uri(user, metadata, renewedPassword), metadata)) {
                StorePort store = restarted.getBean(StorePort.class);
                assertThat(store.keyedState().load("permissions-proof", "renewed-key"))
                        .hasValueSatisfying(value -> assertThat(value).isEqualTo(new byte[] {7}));
                assertThat(store.artifacts().get("views")).contains(authored);
                assertThat(SystemCollections.ARTIFACTS.on(admin.getDatabase(metadata))
                        .find(new Document("_id", "views")).first()).isEqualTo(originalSource);
                try (MongoClient materialization = MongoClients.create(uri(user, metadata + "_views", renewedPassword))) {
                    materialization.getDatabase(metadata + "_views").getCollection("proof")
                            .insertOne(new Document("_id", "after-renewal").append("owner", metadata));
                }
                assertThat(admin.getDatabase(metadata + "_views").getCollection("proof").countDocuments()).isEqualTo(2);
                Object rawConfig = admin.getDatabase(metadata).getCollection(SystemCollections.ARTIFACTS.collectionName())
                        .find(new Document("_id", "views")).first().get("body", Document.class).get("config");
                assertThat(rawConfig).isInstanceOf(String.class);
                assertThat((String) rawConfig).doesNotContain(PASSWORD, renewedPassword);
                assertNoProbes(admin, metadata);
            }
        }
    }

    private static void assertViewsNotSeeded(MongoDatabase metadata, StorePort store) {
        assertThat(store.artifacts().get(ViewTargetResolver.STATE_STORE_SOURCE_ID)).isEmpty();
        assertThat(SystemCollections.ARTIFACTS.on(metadata)
                .countDocuments(new Document("_id", ViewTargetResolver.STATE_STORE_SOURCE_ID))).isZero();
    }

    @ParameterizedTest
    @EnumSource(Missing.class)
    void readableButUnwritableDerivedDatabaseCannotReportReady(Missing readOnly) {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_readonly_" + suffix;
        String denied = metadata + (readOnly == Missing.OPERATOR ? "_operator" : "_views");
        String other = metadata + (readOnly == Missing.OPERATOR ? "_views" : "_operator");
        String user = "user_readonly_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD).append("roles", List.of(
                    new Document("role", "readWrite").append("db", metadata),
                    new Document("role", "readWrite").append("db", other),
                    new Document("role", "read").append("db", denied))));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThat(limited.getDatabase(denied).getCollection("proof").countDocuments()).isZero();
                assertThatThrownBy(() -> limited.getDatabase(denied).getCollection("proof")
                        .insertOne(new Document("_id", "denied-write")))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger ready = new AtomicInteger();
            Throwable failure = catchThrowable(() -> {
                try (var unexpected = start(uri(user, metadata), metadata, ready)) {
                    throw new AssertionError("Cloud startup accepted a read-only deployment database");
                }
            });
            assertThat(ready.get()).isZero();
            Throwable coded = failure;
            for (int depth = 0; depth < 16 && coded != null && !(coded instanceof TapstateException); depth++) coded = coded.getCause();
            assertThat(coded).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(StoreError.DATABASE_ACCESS_FAILED);
                assertThat(error.args()).containsExactlyEntriesOf(java.util.Map.of("database", denied));
                assertThat(error.getCause()).isNull();
            });
            assertNoProbes(admin, metadata);
        }
    }

    private static void assertNoProbes(MongoClient admin, String metadata) {
        for (String database : List.of(metadata, metadata + "_operator", metadata + "_views")) {
            assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                    .noneMatch(name -> name.startsWith("tapstate_access_probe_"));
        }
    }

    @ParameterizedTest
    @EnumSource(Missing.class)
    void missingDerivedDatabaseRightsCannotReportReady(Missing missing) {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_missing_" + suffix;
        String denied = metadata + (missing == Missing.OPERATOR ? "_operator" : "_views");
        String allowed = metadata + (missing == Missing.OPERATOR ? "_views" : "_operator");
        String user = "user_missing_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, allowed));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) { assertDenied(limited, denied); }
            AtomicInteger ready = new AtomicInteger();
            AtomicReference<ConfigurableApplicationContext> started = new AtomicReference<>();
            Throwable failure = catchThrowable(() -> started.set(start(uri(user, metadata), metadata, ready)));
            if (started.get() != null) started.get().close();
            assertThat(failure).as("Cloud startup refuses the missing %s grant before reporting ready", missing).isNotNull();
            assertThat(ready.get()).as("no ApplicationReadyEvent was published").isZero();
            Throwable coded = failure;
            for (int depth = 0; depth < 16 && !(coded instanceof TapstateException) && coded != null; depth++) {
                coded = coded.getCause();
            }
            assertThat(coded).as("a storage authorization failure is diagnosable with a canonical code")
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(StoreError.DATABASE_ACCESS_FAILED);
                        assertThat(error.args()).containsExactlyEntriesOf(java.util.Map.of("database", denied));
                        assertThat(error.getCause()).isNull();
                    });
            for (String database : List.of(metadata, allowed, denied)) {
                assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                        .noneMatch(name -> name.startsWith("tapstate_access_probe_"));
            }
        }
    }

    private ConfigurableApplicationContext start(String metadataUri, String clusterId, AtomicInteger ready, String... overrides) {
        List<String> args = new ArrayList<>(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.store.mongo.server-selection-timeout=2s",
                "--tapstate.connectors.plugins-dir=" + work.resolve(clusterId),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                "--tapstate.cloud.base-url=https://cloud.example.invalid", "--tapstate.cloud.token=controlled-sdk-token",
                "--tapstate.cloud.cluster-id=" + clusterId, "--tapstate.cloud.atlas-uri=" + metadataUri));
        args.addAll(List.of(overrides));
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .listeners((ApplicationListener<ApplicationReadyEvent>) event -> ready.incrementAndGet())
                .run(args.toArray(String[]::new));
    }

    private static void createUser(MongoClient admin, String user, List<String> databases) {
        admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD)
                .append("roles", databases.stream().map(database -> new Document("role", "readWrite").append("db", database)).toList()));
    }

    private static void assertDenied(MongoClient client, String database) {
        assertThatThrownBy(() -> client.getDatabase(database).getCollection("proof").countDocuments())
                .isInstanceOfSatisfying(MongoCommandException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(13));
    }

    private static String uri(String user, String database) {
        return uri(user, database, PASSWORD);
    }

    private static String uri(String user, String database, String password) {
        return "mongodb://" + user + ":" + password + "@" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017)
                + "/" + database + "?replicaSet=" + REPLICA_SET + "&directConnection=true&authSource=admin";
    }
}
