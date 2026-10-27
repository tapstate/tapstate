package io.tapstate.e2e;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.adapters.mongostore.SourceConfigKeyringHandle;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Shipped JVMs reuse native CONFIG keys while node authorization remains in CACHE. */
@RequiresDocker
class SourceConfigKeyringClusterIT {

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(75);
    private static final DslParser PARSER = new DslParser();
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final String JOINING = "node-c";

    @Test
    void aLocallyVisibleAcknowledgementDoesNotSatisfyTheRotationBarrier() throws Exception {
        String name = databaseName("keyring_ack_visibility");
        String clusterId = "visibility-cluster";
        try (MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all",
                        "--setParameter", "enableTestCommands=1")) {
            mongo.start();
            String uri = mongo.getReplicaSetUrl(name);
            try (MongoClient raw = MongoClients.create(uri);
                    MongoConnection writerConnection = maintenance(uri);
                    MongoConnection rotationConnection = maintenance(uri)) {
                MongoDatabase database = metadataDatabase(raw, name);
                MongoDatabase local = raw.getDatabase(name).withReadConcern(ReadConcern.LOCAL)
                        .withReadPreference(ReadPreference.primary());
                SourceConfigKeyringHandle writer = writerConnection.sourceConfigKeyring();
                SourceConfigKeyringHandle rotation = rotationConnection.sourceConfigKeyring();
                WorkloadClaim node = new MongoWorkloadClaimStore(SystemCollections.WORKLOAD_CLAIMS.on(database))
                        .acquire(new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, TwoMemberCluster.NODE_A),
                                new WorkloadOwner(TwoMemberCluster.NODE_A, "visibility-boot"), 0, LEASE)
                        .claim();
                writer.acknowledge(node, LEASE);
                assertThat(rotation.prepareRotation()).isEqualTo(2);

                MongoDatabase admin = raw.getDatabase("admin");
                // Keep the committed ACK at epoch 1 while the real epoch-2 ACK becomes locally
                // visible. Its majority commit is still pending, so it cannot authorize rotation.
                admin.runCommand(new Document("configureFailPoint", "disableSnapshotting")
                        .append("mode", "alwaysOn"));
                CompletableFuture<Void> committed = CompletableFuture.runAsync(() -> writer.acknowledge(node, LEASE));
                try (AutoCloseable pausedSnapshot = () -> {
                    admin.runCommand(new Document("configureFailPoint", "disableSnapshotting")
                            .append("mode", "off"));
                    // A new write resumes committed-snapshot publication after the failpoint.
                    SystemCollections.SYSTEM_META.on(local).withWriteConcern(WriteConcern.W1)
                            .insertOne(new Document("_id", "visibility-resumed"));
                    committed.get(10, TimeUnit.SECONDS);
                }) {
                    Await.until("the epoch-2 ACK to become locally visible before its majority commit",
                            Duration.ofSeconds(10),
                            () -> liveAcknowledgement(local, clusterId, TwoMemberCluster.NODE_A, 2) != null,
                            () -> "the local ACK has not advanced to epoch 2");
                    assertThat(committed.isDone()).as("the majority ACK is still held by the server").isFalse();
                    assertThatThrownBy(rotation::activatePrepared)
                            .isInstanceOfSatisfying(TapstateException.class,
                                    error -> assertThat(error.code())
                                            .isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED));
                    assertThat(liveAcknowledgement(database, clusterId, TwoMemberCluster.NODE_A, 2))
                            .as("the test barrier must not run ahead of the production rotation gate")
                            .isNull();
                }
                awaitAcknowledgements(database, clusterId, 2, TwoMemberCluster.NODE_A);
                assertThat(rotation.activatePrepared()).isEqualTo(3);
            }
        }
    }

    @Test
    void cacheRenewalDoesNotWriteTheConfigurationKeyAfterAdmission() {
        try (MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1")) {
            mongo.start();
            try (PreparedNativeStore storage = PreparedNativeStore.createOn(mongo);
                    MongoClient raw = MongoClients.create(storage.configurationUri());
                    TwoMemberCluster cluster = TwoMemberCluster.startPrepared(storage, LEASE)) {
                var configuration = metadataDatabase(raw, storage.configurationDatabase());
                var cache = metadataDatabase(raw, storage.cacheDatabase());
                assertThat(cluster.awaitBothMembers()).hasSize(2);
                Resource readable = source("survives_key_conflict", "key-conflict-whole-config-sentinel");
                cluster.first().apply(Map.of("key.tap.yml", WRITER.write(readable)));
                Document originalKey = nativeKey(configuration);
                Document owner = nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_A);
                assertThat(owner).isNotNull();
                var admin = raw.getDatabase("admin");
                String application = MongoConnection.processTag(owner.getString("ownerBootId"));
                Document rule = new Document("failCommands", List.of("update"))
                        .append("namespace", storage.configurationDatabase() + "." + SystemCollections.CLUSTER_KEYS.collectionName())
                        .append("appName", application).append("errorCode", 112);
                long entered = admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", "alwaysOn").append("data", rule)).get("count", Number.class).longValue();
                try {
                    Await.until("the original cache lease to renew without a configuration-key write", WAIT, () -> {
                        var renewed = nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_A);
                        return renewed != null && renewed.getDate("leaseUntil").after(owner.getDate("leaseUntil"));
                    }, () -> "the original cache lease has not advanced");
                    var firstRenewal = nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_A);
                    Await.until("a complete subsequent renewal under the armed configuration rule", WAIT, () -> {
                        var next = nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_A);
                        return next != null && next.getDate("leaseUntil").after(firstRenewal.getDate("leaseUntil"));
                    }, () -> "the next original cache lease has not advanced");
                    storage.awaitBusiness(cluster.first());
                    var renewed = nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_A);
                    assertThat(renewed.getString("ownerBootId")).isEqualTo(owner.getString("ownerBootId"));
                    assertThat(renewed.get("claimGeneration")).isEqualTo(owner.get("claimGeneration"));
                    assertThat(nativeKey(configuration)).isEqualTo(originalKey);
                    assertThat(cluster.processCarrying(TwoMemberCluster.NODE_A).isAlive()).isTrue();
                } finally {
                    long finished = admin.runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", "off")).get("count", Number.class).longValue();
                    assertThat(finished - entered).as("ordinary renewal made no configuration-key write").isZero();
                }
                // Positive control: the exact namespace/application rule does reject a real key write.
                long probeCount = admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1)).append("data", rule)).get("count", Number.class).longValue();
                try (var probe = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                        .applyConnectionString(new com.mongodb.ConnectionString(storage.configurationUri()))
                        .applicationName(application).retryWrites(false).build())) {
                    assertThatThrownBy(() -> SystemCollections.CLUSTER_KEYS.on(probe.getDatabase(storage.configurationDatabase()))
                            .updateOne(new Document("_id", "cluster"), new Document("$inc", new Document("sourceWriteFence", 0L))))
                            .isInstanceOfSatisfying(com.mongodb.MongoException.class, failure -> assertThat(failure.getCode()).isEqualTo(112));
                } finally {
                    long finished = admin.runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", "off")).get("count", Number.class).longValue();
                    assertThat(finished - probeCount).isEqualTo(1);
                }
                assertThat(nativeKey(configuration)).isEqualTo(originalKey);
                assertNativeSource(storage, configuration, cluster.second(), readable, originalKey.getString("keyId"));
                assertNoNativeAcknowledgements(configuration, cache);
            }
        }
    }

    @Test
    void concurrentMemberBootsAndWholeClusterRestartKeepTheConfiguredKeyAndReadableSources() {
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                MongoClient raw = MongoClients.create(storage.configurationUri())) {
            var configuration = metadataDatabase(raw, storage.configurationDatabase());
            var cache = metadataDatabase(raw, storage.cacheDatabase());
            Document key = stableNativeKey(nativeKey(configuration));
            TwoMemberCluster original = TwoMemberCluster.startPrepared(storage, LEASE);
            Document firstEnding;
            Document secondEnding;
            Resource first = source("from_a", "first-whole-config-sentinel");
            Resource second = source("from_b", "second-whole-config-sentinel");
            try (original) {
                assertThat(original.awaitBothMembers()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(SystemCollections.CLUSTER_KEYS.on(configuration).countDocuments()).isEqualTo(1);
                assertThat(stableNativeKey(nativeKey(configuration))).isEqualTo(key);
                original.first().apply(Map.of("first.tap.yml", WRITER.write(first)));
                original.second().apply(Map.of("second.tap.yml", WRITER.write(second)));
                assertNativeSource(storage, configuration, original.second(), first, key.getString("keyId"));
                assertNativeSource(storage, configuration, original.first(), second, key.getString("keyId"));
                assertNoNativeAcknowledgements(configuration, cache);
                secondEnding = stopNative(cache, original, storage.clusterId(), TwoMemberCluster.NODE_B);
                firstEnding = stopNative(cache, original, storage.clusterId(), TwoMemberCluster.NODE_A);
            }
            assertThat(stableNativeKey(nativeKey(configuration))).isEqualTo(key);
            try (TwoMemberCluster restarted = original.restarted()) {
                assertThat(restarted.awaitBothMembers()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertNativeReplacement(firstEnding, nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_A));
                assertNativeReplacement(secondEnding, nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_B));
                assertThat(stableNativeKey(nativeKey(configuration))).isEqualTo(key);
                assertNativeSource(storage, configuration, restarted.first(), first, key.getString("keyId"));
                assertNativeSource(storage, configuration, restarted.second(), second, key.getString("keyId"));
                Resource afterRestart = source("after_restart", "restart-write-sentinel");
                restarted.second().apply(Map.of("restart.tap.yml", WRITER.write(afterRestart)));
                assertNativeSource(storage, configuration, restarted.first(), afterRestart, key.getString("keyId"));
                assertNoNativeAcknowledgements(configuration, cache);
            }
        }
    }

    @Test
    void joiningAndGracefulExitReuseTheSameConfiguredKeyWithoutAcknowledgements() {
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                MongoClient raw = MongoClients.create(storage.configurationUri());
                TwoMemberCluster cluster = TwoMemberCluster.startPrepared(storage, LEASE)) {
            var configuration = metadataDatabase(raw, storage.configurationDatabase());
            var cache = metadataDatabase(raw, storage.cacheDatabase());
            Document key = stableNativeKey(nativeKey(configuration));
            Resource beforeJoin = source("before_join", "join-whole-config-sentinel");
            cluster.first().apply(Map.of("before.tap.yml", WRITER.write(beforeJoin)));
            RealProcessServer joining = cluster.launching(JOINING);
            try (joining) {
                joining.awaitReady();
                assertThat(cluster.awaitMembers(3)).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, JOINING);
                ControlPlane added = cluster.signedInAt(joining);
                storage.awaitBusiness(added);
                assertThat(nativeLiveClaim(cache, storage.clusterId(), JOINING)).isNotNull();
                assertNativeSource(storage, configuration, added, beforeJoin, key.getString("keyId"));
                Resource joined = source("after_join", "joined-write-sentinel");
                added.apply(Map.of("after.tap.yml", WRITER.write(joined)));
                assertNativeSource(storage, configuration, cluster.first(), joined, key.getString("keyId"));
                stopNative(cache, cluster, storage.clusterId(), TwoMemberCluster.NODE_B);
                Await.until("the remaining visible members to observe the graceful departure", WAIT,
                        () -> cluster.first().clusterMemberNodeIds().equals(List.of(TwoMemberCluster.NODE_A, JOINING)),
                        () -> "visible members = " + cluster.first().clusterMemberNodeIds());
                assertThat(SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(cache).find().first()
                        .getList("activeNodeIds", String.class))
                        .as("departure does not shrink the committed majority denominator")
                        .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, JOINING);
                assertThat(stableNativeKey(nativeKey(configuration))).isEqualTo(key);
                assertNativeSource(storage, configuration, added, joined, key.getString("keyId"));
                assertNoNativeAcknowledgements(configuration, cache);
            } finally {
                FailureScene.writeMemberLogs(storage.clusterId(), Map.of(JOINING, joining.output()));
            }
        }
    }

    @Test
    void aKilledProcessKeepsItsLeaseHistoryAndReplacementReusesTheConfiguredKey() {
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                MongoClient raw = MongoClients.create(storage.configurationUri());
                TwoMemberCluster cluster = TwoMemberCluster.startPrepared(storage, LEASE)) {
            var configuration = metadataDatabase(raw, storage.configurationDatabase());
            var cache = metadataDatabase(raw, storage.cacheDatabase());
            Document key = stableNativeKey(nativeKey(configuration));
            Resource original = source("survives_crash", "crash-whole-config-sentinel");
            cluster.first().apply(Map.of("crash.tap.yml", WRITER.write(original)));
            Document previous = nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_B);
            Await.until("the second process to renew its actual node session", WAIT,
                    () -> nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_B).getDate("leaseUntil")
                            .after(previous.getDate("leaseUntil")), () -> "the stored lease did not advance");
            Document killedClaim = nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_B);
            RealProcessServer killed = cluster.processCarrying(TwoMemberCluster.NODE_B);
            killed.kill();
            assertThat(killed.isAlive()).isFalse();
            assertThat(nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_B))
                    .as("the crash does not erase the still-live original lease").isNotNull();
            Await.until("the killed node session to expire on the database clock", WAIT,
                    () -> nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_B) == null,
                    () -> "the killed process still has a live node session");
            try (RealProcessServer replacement = cluster.launching(TwoMemberCluster.NODE_B)) {
                replacement.awaitReady();
                ControlPlane replaced = cluster.signedInAt(replacement);
                storage.awaitBusiness(replaced);
                assertNativeReplacement(killedClaim, nativeLiveClaim(cache, storage.clusterId(), TwoMemberCluster.NODE_B));
                assertThat(stableNativeKey(nativeKey(configuration))).isEqualTo(key);
                assertNativeSource(storage, configuration, replaced, original, key.getString("keyId"));
                Resource fresh = source("replacement_write", "replacement-config-sentinel");
                replaced.apply(Map.of("replacement.tap.yml", WRITER.write(fresh)));
                assertNativeSource(storage, configuration, cluster.first(), fresh, key.getString("keyId"));
                assertNoNativeAcknowledgements(configuration, cache);
            }
        }
    }

    private static Document nativeKey(MongoDatabase configuration) {
        Document row = SystemCollections.CLUSTER_KEYS.on(configuration).find(new Document("_id", "cluster")).first();
        assertThat(row).isNotNull();
        return row;
    }

    private static Document stableNativeKey(Document key) {
        Document stable = new Document(key);
        stable.remove("sourceWriteFence");
        return stable;
    }

    private static void assertNoNativeAcknowledgements(MongoDatabase configuration, MongoDatabase cache) {
        assertThat(configuration.listCollectionNames().into(new java.util.ArrayList<>()))
                .isSubsetOf(SystemCollections.physicalNamesIn(SystemCollections.Database.CONFIG));
        assertThat(configuration.getCollection(SystemCollections.WORKLOAD_CLAIMS.collectionName()).countDocuments()).isZero();
        assertThat(SystemCollections.SYSTEM_META.on(configuration).countDocuments()).isZero();
        assertThat(SystemCollections.SYSTEM_META.on(cache).countDocuments()).isZero();
        assertThat(nativeKey(configuration).keySet()).doesNotContain("nodeAckFence", "pendingKeyId");
    }

    private static Document nativeLiveClaim(MongoDatabase cache, String cluster, String node) {
        return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(new Document("clusterId", cluster)
                .append("resourceType", WorkloadClaimType.NODE_SESSION.name()).append("resourceId", node)
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))).first();
    }

    private static Document stopNative(MongoDatabase cache, TwoMemberCluster cluster, String clusterId, String node) {
        Document original = nativeLiveClaim(cache, clusterId, node);
        assertThat(original).isNotNull();
        RealProcessServer process = cluster.processCarrying(node);
        process.close();
        assertThat(process.isAlive()).isFalse();
        assertThat(process.exitValue()).isZero();
        Document ended = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(new Document("_id", original.get("_id"))).first();
        assertThat(ended).isNotNull();
        assertThat(ended.getString("ownerBootId")).isEqualTo(original.getString("ownerBootId"));
        assertThat(ended.get("claimGeneration")).isEqualTo(original.get("claimGeneration"));
        assertThat(ended.getDate("leaseUntil")).as("ordinary shutdown releases the exact original node lease")
                .isBefore(original.getDate("leaseUntil"));
        assertThat(nativeLiveClaim(cache, clusterId, node)).isNull();
        return original;
    }

    private static void assertNativeReplacement(Document old, Document replacement) {
        assertThat(replacement).isNotNull();
        assertThat(replacement.getString("ownerBootId")).isNotEqualTo(old.getString("ownerBootId"));
        assertThat(replacement.get("claimGeneration", Number.class).longValue())
                .isGreaterThan(old.get("claimGeneration", Number.class).longValue());
    }

    private static void assertNativeSource(PreparedNativeStore storage, MongoDatabase configuration,
            ControlPlane control, Resource expected, String keyId) {
        Document document = SystemCollections.SOURCES.on(configuration).find(new Document("_id", expected.id())).first();
        assertThat(document).isNotNull();
        assertThat(document.get("body", Document.class).getString("config")).startsWith("tscfg:1:" + keyId + ":");
        assertThat(document.toJson()).doesNotContain("fake-user", "fake-password", "whole-config-sentinel",
                "joined-write-sentinel", "restart-write-sentinel", "replacement-config-sentinel");
        try (var reader = io.tapstate.adapters.mongostore.MongoConfigurationDomain.openExisting(
                new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)))) {
            assertThat(reader.artifacts().get(expected.id())).contains(expected);
        }
        assertThat(document.getString("contentHash")).isEqualTo(CanonicalHash.of(expected));
        var projection = control.artifact(expected.id()).orElseThrow();
        assertThat(projection.id()).isEqualTo(expected.id());
        assertThat(projection.kind()).isEqualTo("source");
        assertThat(projection.contentHash()).isEqualTo(CanonicalHash.of(expected));
        assertThat(projection.canonicalForm()).isNotBlank().doesNotContain("config:", "tscfg:", "<redacted-source>");
        assertThat(PARSER.parse(projection.canonicalForm()).id()).isEqualTo(expected.id());
    }

    private static MongoConnection maintenance(String uri) {
        MongoConnection connection = new MongoConnection(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)));
        try {
            connection.verify();
            return connection;
        } catch (RuntimeException | Error failed) {
            connection.close();
            throw failed;
        }
    }

    private static Resource source(String id, String sentinel) {
        return PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config:
                  uri: mongodb://fake-user:fake-password@localhost/data
                  publicValue: %s
                  nested: { unmarkedSecret: %s }
                """.formatted(id, sentinel, sentinel));
    }

    private static Document acknowledgement(MongoDatabase database, String clusterId, String nodeId) {
        return SystemCollections.SYSTEM_META.on(database).find(new Document("_id", new Document("kind",
                        "source-config-keyring-node").append("clusterId", clusterId).append("nodeId", nodeId)))
                .first();
    }

    private static Document liveClaim(MongoDatabase database, String clusterId, String nodeId) {
        return SystemCollections.WORKLOAD_CLAIMS.on(database).find(new Document("clusterId", clusterId)
                .append("resourceType", WorkloadClaimType.NODE_SESSION.name()).append("resourceId", nodeId)
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))).first();
    }

    private static Document liveAcknowledgement(MongoDatabase database, String clusterId, String nodeId, long epoch) {
        Document claim = liveClaim(database, clusterId, nodeId);
        Document ack = acknowledgement(database, clusterId, nodeId);
        if (claim == null || ack == null || ack.get("epoch", Number.class).longValue() != epoch
                || !claim.getString("ownerBootId").equals(ack.getString("bootId"))
                || claim.get("claimGeneration", Number.class).longValue()
                        != ack.get("claimGeneration", Number.class).longValue()) return null;
        return SystemCollections.SYSTEM_META.on(database).find(new Document("_id", ack.get("_id"))
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))).first();
    }

    private static void awaitAcknowledgements(MongoDatabase database, String clusterId, long epoch, String... nodes) {
        Await.until("all live node sessions to acknowledge keyring epoch " + epoch, WAIT,
                () -> java.util.Arrays.stream(nodes)
                        .allMatch(node -> liveAcknowledgement(database, clusterId, node, epoch) != null),
                () -> "at least one live boot/generation has not acknowledged the epoch");
    }

    private static String databaseName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static MongoDatabase metadataDatabase(MongoClient client, String name) {
        // Observe the same durable view as rotation; a local ACK can precede its majority commit.
        return client.getDatabase(name).withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY);
    }
}
