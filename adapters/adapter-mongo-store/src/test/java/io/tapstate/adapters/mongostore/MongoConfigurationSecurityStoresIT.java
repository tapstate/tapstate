package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration security survives restart without opening either cache authentication or business storage. */
@RequiresDocker
class MongoConfigurationSecurityStoresIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void tokenGrantAndRevocationSurviveReopenAndCompeteWithConfigurationRevision() {
        withConfiguration((domain, database) -> {
            var token = token("token-a", "hash-only");
            domain.tokens().save(token);
            assertThat(revision(database)).isOne();
            domain.tokens().revoke("unknown");
            assertThat(revision(database)).isOne();
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                assertThat(reopened.tokens().find("token-a")).contains(token);
                reopened.tokens().revoke("token-a");
                assertThat(revision(database)).isEqualTo(2);
                assertThat(domain.tokens().find("token-a").orElseThrow().revoked()).isTrue();
                reopened.tokens().revoke("token-a");
                assertThat(revision(database)).isEqualTo(2);
                assertThat(reopened.tokens().list()).hasSize(1);
            }
            assertThat(SystemCollections.CONFIG_TOKENS.on(database).find().first().keySet())
                    .containsExactlyInAnyOrder("_id", "scope", "secretHash", "revoked", "createdAt");
            assertThat(database.listCollectionNames().into(new ArrayList<>()))
                    .doesNotContain("sessions", "op_sessions", "cloud_sessions", "cloud_revocations", "workload_claims");
        });
    }

    @Test
    void rejectedTokenWriteRollsBackItsConfigurationRevision() {
        withConfiguration((domain, database) -> {
            domain.tokens().save(token("token-a", "hash-a"));
            SystemCollections.CONFIG_TOKENS.on(database).createIndex(new Document("scope", 1), new IndexOptions().unique(true));
            assertThatThrownBy(() -> domain.tokens().save(token("token-b", "hash-b"))).isInstanceOf(TapstateException.class);
            assertThat(revision(database)).isOne();
            assertThat(domain.tokens().find("token-b")).isEmpty();
            assertThat(domain.tokens().find("token-a")).contains(token("token-a", "hash-a"));
        });
    }

    @Test
    void missingConfigurationVersionIsNotRecreatedByGrantOrRevocation() {
        withConfiguration((domain, database) -> {
            domain.tokens().save(token("token-a", "hash-a"));
            SystemCollections.CONFIGURATION_VERSION.on(database).deleteMany(new Document());
            assertThatThrownBy(() -> domain.tokens().save(token("token-b", "hash-b"))).satisfies(MongoConfigurationSecurityStoresIT::unreadable);
            assertThatThrownBy(() -> domain.tokens().revoke("token-a")).satisfies(MongoConfigurationSecurityStoresIT::unreadable);
            assertThat(domain.tokens().find("token-a").orElseThrow().revoked()).isFalse();
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(database).countDocuments()).isZero();
        });
    }

    @Test
    void twoClientsPublishTokenChangesThroughTheSameVersionContentionPoint() {
        withConfiguration((domain, database) -> {
            try (var other = MongoConfigurationDomain.open(settings(database), "cluster-a");
                    var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2);
                var start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    domain.tokens().save(token("a", "hash-a")); return null; });
                var b = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    other.tokens().save(token("b", "hash-b")); return null; });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                a.get(20, TimeUnit.SECONDS); b.get(20, TimeUnit.SECONDS);
                assertThat(revision(database)).isEqualTo(2);
                assertThat(domain.tokens().list()).hasSize(2);
            }
        });
    }

    @Test
    void malformedTokenFieldsRefuseFindListOpenAndRevokeWithoutLeakingStoredValues() {
        for (var change : List.of(new Document("secretHash", ""), new Document("revoked", "secret-sentinel"),
                new Document("createdAt", 1), new Document("scope", Map.of("secret", "secret-sentinel")))) {
            withConfiguration((domain, database) -> {
                domain.tokens().save(token("token-a", "hash-only"));
                SystemCollections.CONFIG_TOKENS.on(database).updateOne(new Document(), new Document("$set", change));
                assertThatThrownBy(() -> domain.tokens().find("token-a")).satisfies(MongoConfigurationSecurityStoresIT::unreadable);
                assertThatThrownBy(() -> domain.tokens().list()).satisfies(MongoConfigurationSecurityStoresIT::unreadable);
                assertThatThrownBy(() -> domain.tokens().revoke("token-a")).satisfies(MongoConfigurationSecurityStoresIT::unreadable);
                assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(database), "cluster-a"))
                        .satisfies(MongoConfigurationSecurityStoresIT::unreadable);
                assertThat(revision(database)).isOne();
            });
        }
    }

    @Test
    void auditIsAppendOnlyAndIndependentOfTheAuthoringRevision() {
        withConfiguration((domain, database) -> {
            var record = new AuditRecord(Instant.ofEpochMilli(1000), "operator", "token.create", "token-a");
            domain.audit().record(record);
            domain.audit().record(record);
            assertThat(SystemCollections.CONFIG_AUDIT.on(database).countDocuments()).isEqualTo(2);
            assertThat(revision(database)).isZero();
            SystemCollections.CONFIG_AUDIT.on(database).deleteMany(new Document());
            SystemCollections.CONFIG_AUDIT.on(database).createIndex(new Document("operationId", 1), new IndexOptions().unique(true));
            domain.audit().record(record);
            assertThatThrownBy(() -> domain.audit().record(record)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.CONFIG_AUDIT.on(database).countDocuments()).isOne();
            assertThat(revision(database)).isZero();
        });
    }

    @Test
    void identityUsesTheOriginalWinnerAndNeverRecreatesMissingOrReplacedFacts() {
        withConfiguration((domain, database) -> {
            var before = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).find().first();
            assertThat(domain.clusterIdentity().find()).contains(new ClusterIdentity("cluster-a"));
            assertThat(domain.clusterIdentity().createIfAbsent(new ClusterIdentity("new-proposal"))).isEqualTo(new ClusterIdentity("cluster-a"));
            assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).find().first()).isEqualTo(before);
            assertThat(new ClusterIdentity("cluster-a").issuer()).isEqualTo("urn:tapstate:cluster:cluster-a");
            SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).updateOne(new Document(),
                    new Document("$set", new Document("clusterId", "cluster-b")));
            assertThatThrownBy(() -> domain.clusterIdentity().find()).satisfies(MongoConfigurationSecurityStoresIT::unreadable);
            assertThatThrownBy(() -> domain.tokens().save(token("token-a", "hash-only")))
                    .satisfies(MongoConfigurationSecurityStoresIT::unreadable);
            SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).deleteMany(new Document());
            assertThatThrownBy(() -> domain.clusterIdentity().createIfAbsent(new ClusterIdentity("new-proposal")))
                    .satisfies(MongoConfigurationSecurityStoresIT::unreadable);
            assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).countDocuments()).isZero();
            assertThat(revision(database)).isZero();
        });
    }

    private static TokenRecord token(String id, String hash) { return new TokenRecord(id, "admin", hash, false, Instant.ofEpochMilli(1000)); }
    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.getMessage()).doesNotContain("secret-sentinel", "hash-only");
            assertThat(error.getCause()).isNull();
        });
    }
    private static long revision(MongoDatabase database) {
        return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision");
    }
    private static MongoConnectionSettings settings(MongoDatabase database) {
        return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
    }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("config_security_" + UUID.randomUUID().toString().replace("-", ""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(database), "cluster-a", "installation-a", "setup-a", 1)) {
                scenario.run(domain, database);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, MongoDatabase database) throws Exception; }
}
