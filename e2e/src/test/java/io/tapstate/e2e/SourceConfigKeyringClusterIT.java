package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SourceConfigCipher;
import io.tapstate.adapters.mongostore.SourceConfigKeyringHandle;
import io.tapstate.adapters.mongostore.SourceConfigKeyringStore;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Shipped JVMs, real members and node-session renewal over one shared metadata replica set. */
@RequiresDocker
class SourceConfigKeyringClusterIT {

    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration WAIT = Duration.ofSeconds(75);
    private static final DslParser PARSER = new DslParser();
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final String JOINING = "node-c";

    @Test
    void concurrentFirstBootAndWholeClusterRestartKeepOneKeyringAndReadableSources() {
        String name = databaseName("keyring_restart");
        String uri = SharedMongo.replicaSetUrl(name);
        try (MongoClient raw = MongoClients.create(uri)) {
            MongoDatabase database = raw.getDatabase(name);
            TwoMemberCluster original = TwoMemberCluster.startConcurrently(uri, "keyring-first", LEASE);
            Document keyring;
            EndingSession firstEnding;
            EndingSession secondEnding;
            Resource first = source("from_a", "first-whole-config-sentinel");
            Resource second = source("from_b", "second-whole-config-sentinel");
            try (original) {
                assertThat(original.awaitBothMembers())
                        .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                String clusterId = original.first().clusterId();
                awaitAcknowledgements(database, clusterId, 1, TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                keyring = keyring(database);
                assertThat(SystemCollections.SYSTEM_META.on(database)
                        .countDocuments(new Document("_id", "source-config-keyring"))).isEqualTo(1);
                original.first().apply(Map.of("first.tap.yml", WRITER.write(first)));
                original.second().apply(Map.of("second.tap.yml", WRITER.write(second)));
                assertSource(database, original.second(), first, keyring.getString("activeKeyId"));
                assertSource(database, original.first(), second, keyring.getString("activeKeyId"));
                keyring = keyring(database);
                secondEnding = stopGracefully(database, original, TwoMemberCluster.NODE_B);
                firstEnding = stopGracefully(database, original, TwoMemberCluster.NODE_A);
            }
            assertReleased(database, firstEnding);
            assertReleased(database, secondEnding);
            assertThat(stableKeyring(keyring(database))).as("shutdown does not replace or delete keys")
                    .isEqualTo(stableKeyring(keyring));
            try (TwoMemberCluster restarted = original.restarted()) {
                assertThat(restarted.awaitBothMembers())
                        .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                String clusterId = restarted.first().clusterId();
                awaitAcknowledgements(database, clusterId, 1, TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertReplacement(firstEnding.acknowledgement(),
                        acknowledgement(database, clusterId, TwoMemberCluster.NODE_A));
                assertReplacement(secondEnding.acknowledgement(),
                        acknowledgement(database, clusterId, TwoMemberCluster.NODE_B));
                assertThat(stableKeyring(keyring(database))).as("both restarted nodes load the durable winner")
                        .isEqualTo(stableKeyring(keyring));
                assertSource(database, restarted.first(), first, keyring.getString("activeKeyId"));
                assertSource(database, restarted.second(), second, keyring.getString("activeKeyId"));
                Resource afterRestart = source("after_restart", "restart-write-sentinel");
                restarted.second().apply(Map.of("restart.tap.yml", WRITER.write(afterRestart)));
                assertSource(database, restarted.first(), afterRestart, keyring.getString("activeKeyId"));
            }
        }
    }

    @Test
    void joiningDuringRotationSharesTheEpochAtReadyAndGracefulExitStopsBlocking() {
        String name = databaseName("keyring_join");
        String uri = SharedMongo.replicaSetUrl(name);
        try (MongoClient raw = MongoClients.create(uri);
                TwoMemberCluster cluster = TwoMemberCluster.start(uri, "keyring-join", LEASE);
                MongoConnection maintenance = maintenance(uri)) {
            MongoDatabase database = raw.getDatabase(name);
            assertThat(cluster.awaitBothMembers()).hasSize(2);
            String clusterId = cluster.first().clusterId();
            Resource beforeRotation = source("before_rotation", "rotation-whole-config-sentinel");
            cluster.first().apply(Map.of("before.tap.yml", WRITER.write(beforeRotation)));
            SourceConfigKeyringHandle keys = maintenance.sourceConfigKeyring();
            String initialKey = keyring(database).getString("activeKeyId");
            assertThat(keys.prepareRotation()).isEqualTo(2);

            try (RealProcessServer joining = cluster.launching(JOINING)) {
                joining.awaitReady();
                assertThat(liveAcknowledgement(database, clusterId, JOINING, 2))
                        .as("the ready joining process holds a matching live acknowledgement of the prepared epoch")
                        .isNotNull();
                assertThat(cluster.awaitMembers(3))
                        .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, JOINING);
                ControlPlane added = cluster.signedInAt(joining);
                assertSource(database, added, beforeRotation, initialKey);
                awaitAcknowledgements(database, clusterId, 2,
                        TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B, JOINING);
                assertThat(keys.activatePrepared()).isEqualTo(3);
                String nextKey = keyring(database).getString("activeKeyId");
                assertThat(nextKey).isNotEqualTo(initialKey);
                assertEverySourceUses(database, nextKey);
                assertSource(database, added, beforeRotation, nextKey);
                Resource afterRotation = source("after_rotation", "joined-write-sentinel");
                added.apply(Map.of("after.tap.yml", WRITER.write(afterRotation)));
                assertSource(database, cluster.first(), afterRotation, nextKey);

                stopGracefully(database, cluster, TwoMemberCluster.NODE_B);
                assertThat(keys.prepareRotation()).isEqualTo(4);
                awaitAcknowledgements(database, clusterId, 4, TwoMemberCluster.NODE_A, JOINING);
                assertThat(keys.activatePrepared()).as("an exited historical member is not a rotation participant")
                        .isEqualTo(5);
                String finalKey = keyring(database).getString("activeKeyId");
                assertEverySourceUses(database, finalKey);
                assertSource(database, added, afterRotation, finalKey);
            }
        }
    }

    @Test
    void aKilledProcessBlocksOnlyUntilItsRealLeaseExpiresAndReplacementUsesTheNewEpoch() {
        String name = databaseName("keyring_crash");
        String uri = SharedMongo.replicaSetUrl(name);
        try (MongoClient raw = MongoClients.create(uri);
                TwoMemberCluster cluster = TwoMemberCluster.start(uri, "keyring-crash", LEASE);
                MongoConnection maintenance = maintenance(uri)) {
            MongoDatabase database = raw.getDatabase(name);
            assertThat(cluster.awaitBothMembers()).hasSize(2);
            String clusterId = cluster.first().clusterId();
            awaitAcknowledgements(database, clusterId, 1, TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            Resource source = source("survives_crash", "crash-whole-config-sentinel");
            cluster.first().apply(Map.of("crash.tap.yml", WRITER.write(source)));
            Document previousClaim = liveClaim(database, clusterId, TwoMemberCluster.NODE_B);
            Await.until("the second process to renew its actual node session", WAIT,
                    () -> liveClaim(database, clusterId, TwoMemberCluster.NODE_B).getDate("leaseUntil")
                            .after(previousClaim.getDate("leaseUntil")), () -> "the stored lease did not advance");
            Document killedAck = acknowledgement(database, clusterId, TwoMemberCluster.NODE_B);
            RealProcessServer killed = cluster.processCarrying(TwoMemberCluster.NODE_B);
            killed.kill();
            assertThat(killed.isAlive()).isFalse();
            SourceConfigKeyringHandle keys = maintenance.sourceConfigKeyring();
            assertThat(keys.prepareRotation()).isEqualTo(2);
            awaitAcknowledgements(database, clusterId, 2, TwoMemberCluster.NODE_A);
            assertThat(liveClaim(database, clusterId, TwoMemberCluster.NODE_B))
                    .as("the crashed node still owns a live lease; this is not an expired-node false positive")
                    .isNotNull();
            assertThatThrownBy(keys::activatePrepared).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED));
            Await.until("the killed process's node session to expire on the database clock", WAIT,
                    () -> liveClaim(database, clusterId, TwoMemberCluster.NODE_B) == null,
                    () -> "the killed process still has a live node session");
            assertThat(keys.activatePrepared()).isEqualTo(3);
            String currentKey = keyring(database).getString("activeKeyId");
            assertEverySourceUses(database, currentKey);
            try (RealProcessServer replacement = cluster.launching(TwoMemberCluster.NODE_B)) {
                replacement.awaitReady();
                awaitAcknowledgements(database, clusterId, 3, TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertReplacement(killedAck, acknowledgement(database, clusterId, TwoMemberCluster.NODE_B));
                ControlPlane replaced = cluster.signedInAt(replacement);
                assertSource(database, replaced, source, currentKey);
                Resource fresh = source("replacement_write", "replacement-config-sentinel");
                replaced.apply(Map.of("replacement.tap.yml", WRITER.write(fresh)));
                assertSource(database, cluster.first(), fresh, currentKey);
            }
        }
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

    private static void assertSource(MongoDatabase database, ControlPlane control, Resource expected, String keyId) {
        Document document = SystemCollections.ARTIFACTS.on(database).find(new Document("_id", expected.id())).first();
        assertThat(document).isNotNull();
        String envelope = document.get("body", Document.class).getString("config");
        assertThat(envelope).startsWith("tscfg:1:" + keyId + ":");
        assertThat(document.toJson()).doesNotContain("fake-user", "fake-password", "whole-config-sentinel",
                "joined-write-sentinel", "restart-write-sentinel", "replacement-config-sentinel");
        SourceConfigCipher cipher = new SourceConfigKeyringStore(database).loadExistingCipher();
        assertThat(JsonReader.parse(cipher.decrypt(expected.id(), "mongodb", envelope)))
                .isEqualTo(WRITER.tree(expected).get("config"));
        assertThat(document.getString("contentHash")).isEqualTo(CanonicalHash.of(expected));
        ControlPlane.StoredArtifact projection = control.artifact(expected.id()).orElseThrow();
        assertThat(projection.id()).isEqualTo(expected.id());
        assertThat(projection.kind()).isEqualTo("source");
        assertThat(projection.contentHash()).isEqualTo(CanonicalHash.of(expected));
        assertThat(projection.canonicalForm()).isNotBlank().doesNotContain("config:", "tscfg:", "<redacted-source>");
        assertThat(PARSER.parse(projection.canonicalForm()).id()).isEqualTo(expected.id());
    }

    private static void assertEverySourceUses(MongoDatabase database, String keyId) {
        List<Document> sources = SystemCollections.ARTIFACTS.on(database)
                .find(new Document("kind", "source")).into(new java.util.ArrayList<>());
        assertThat(sources).isNotEmpty();
        for (Document document : sources) {
            assertThat(document.get("body", Document.class).getString("config"))
                    .as("the Source %s was re-encrypted, including managed Sources", document.getString("_id"))
                    .startsWith("tscfg:1:" + keyId + ":");
        }
    }

    private static Document keyring(MongoDatabase database) {
        Document document = SystemCollections.SYSTEM_META.on(database)
                .find(new Document("_id", "source-config-keyring")).first();
        assertThat(document).isNotNull();
        return document;
    }

    private static Document stableKeyring(Document keyring) {
        // Node acknowledgements touch the shared record to fence a concurrent epoch switch.
        // That coordination counter changes; the epoch, key identities and material must not.
        Document stable = new Document(keyring);
        stable.remove("nodeAckFence");
        return stable;
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

    private record EndingSession(Document claim, Document acknowledgement) {}

    private static EndingSession stopGracefully(MongoDatabase database, TwoMemberCluster cluster, String nodeId) {
        String clusterId = cluster.first().clusterId();
        EndingSession ending = new EndingSession(liveClaim(database, clusterId, nodeId),
                acknowledgement(database, clusterId, nodeId));
        assertThat(ending.claim()).as("the process owns a live claim immediately before shutdown").isNotNull();
        assertThat(ending.acknowledgement()).isNotNull();
        RealProcessServer departing = cluster.processCarrying(nodeId);
        departing.close();
        assertThat(departing.isAlive()).isFalse();
        assertThat(departing.exitValue()).as("SIGTERM completed the ordinary application shutdown").isZero();
        assertReleased(database, ending);
        return ending;
    }

    private static void assertReleased(MongoDatabase database, EndingSession ending) {
        String clusterId = ending.acknowledgement().getString("clusterId");
        String nodeId = ending.acknowledgement().getString("nodeId");
        Document endedClaim = SystemCollections.WORKLOAD_CLAIMS.on(database)
                .find(new Document("_id", ending.claim().get("_id"))).first();
        assertThat(endedClaim).isNotNull();
        assertThat(endedClaim.getString("ownerBootId")).isEqualTo(ending.claim().getString("ownerBootId"));
        assertThat(endedClaim.get("claimGeneration")).isEqualTo(ending.claim().get("claimGeneration"));
        assertThat(endedClaim.getDate("leaseUntil"))
                .as("graceful shutdown shortened the claim; merely waiting for its TTL cannot satisfy this")
                .isBefore(ending.claim().getDate("leaseUntil"));
        Document endedAck = SystemCollections.SYSTEM_META.on(database)
                .find(new Document("_id", ending.acknowledgement().get("_id"))).first();
        assertThat(endedAck).isNotNull();
        assertThat(endedAck.getString("bootId")).isEqualTo(ending.acknowledgement().getString("bootId"));
        assertThat(endedAck.get("claimGeneration")).isEqualTo(ending.acknowledgement().get("claimGeneration"));
        assertThat(endedAck.getDate("leaseUntil"))
                .as("graceful shutdown shortened the exact keyring acknowledgement too")
                .isBefore(ending.acknowledgement().getDate("leaseUntil"));
        assertThat(liveClaim(database, clusterId, nodeId)).as("shutdown released the node-session claim").isNull();
        MongoCollection<Document> system = SystemCollections.SYSTEM_META.on(database);
        assertThat(system.find(new Document("_id", ending.acknowledgement().get("_id"))
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))).first())
                .as("shutdown released the matching keyring acknowledgement").isNull();
    }

    private static void assertReplacement(Document oldAck, Document currentAck) {
        assertThat(currentAck).isNotNull();
        assertThat(currentAck.getString("bootId")).isNotEqualTo(oldAck.getString("bootId"));
        assertThat(currentAck.get("claimGeneration", Number.class).longValue())
                .isGreaterThan(oldAck.get("claimGeneration", Number.class).longValue());
    }

    private static String databaseName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }
}
