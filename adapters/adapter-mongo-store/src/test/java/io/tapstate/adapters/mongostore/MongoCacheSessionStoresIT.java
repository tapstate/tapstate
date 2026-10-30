package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real cache-only session semantics through fixed-generation views of one pooled client. */
@RequiresDocker
class MongoCacheSessionStoresIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final Instant CREATED = Instant.parse("2026-10-17T01:00:00Z");
    private static final CloudSessionIdentity CLOUD = new CloudSessionIdentity("https://cloud.example", "org-a", "cluster-a");
    private static final String HASH = "sha256-secret-fixture";
    private static final Runnable ACTIVE = () -> {};

    @ParameterizedTest
    @EnumSource(value = SystemCollections.class, names = {"CLOUD_SESSIONS", "CLOUD_REVOCATIONS"})
    void aCredentialReadStalledAfterScopeValidationIsBoundedAndRollsBackItsFence(SystemCollections table) {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            assertThat(root.cloudSessions(scope).create(cloud("bounded"))).isTrue();
            var sessions = root.cloudSessions(scope, new MongoReadBudget(Duration.ofSeconds(5)));
            Document original = SystemCollections.CLOUD_SESSIONS.on(database).find().first();
            Document identity = SystemCollections.CACHE_IDENTITY.on(database).find().first();
            try (var adminClient = MongoClients.create(MONGO.getReplicaSetUrl()); var worker = Executors.newSingleThreadExecutor()) {
                var admin = adminClient.getDatabase("admin");
                long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("find"))
                                .append("namespace", database.getName() + "." + table.collectionName())
                                .append("blockConnection", true).append("blockTimeMS", 20000))).get("count")).longValue();
                var result = worker.submit(() -> table == SystemCollections.CLOUD_SESSIONS
                        ? sessions.find(CLOUD, "bounded")
                        : sessions.authenticate(CLOUD, "bounded", HASH, CREATED, CREATED.plusSeconds(60)));
                try {
                    assertThatThrownBy(() -> result.get(15, TimeUnit.SECONDS))
                            .isInstanceOf(java.util.concurrent.ExecutionException.class)
                            .hasCauseInstanceOf(TapstateException.class)
                            .satisfies(failure -> assertThat(((TapstateException) failure.getCause()).code())
                                    .isEqualTo(IoError.STORE_UNAVAILABLE));
                } finally {
                    long finished = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", "off")).get("count")).longValue();
                    assertThat(finished - entered).isPositive();
                }
            }
            assertThat(SystemCollections.CLOUD_SESSIONS.on(database).find().first()).isEqualTo(original);
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).isEqualTo(identity);
            assertThat(SystemCollections.CLOUD_REVOCATIONS.on(database).countDocuments()).isZero();
            assertThat(root.cloudSessions(scope).find(CLOUD, "bounded")).contains(cloud("bounded"));
        });
    }

    @Test
    void opPersistsOnlySessionFactsAndReopensThroughTheOriginalScope() {
        withCache(StorageBinding.Mode.OP, (root, database, binding, scope) -> {
            var sessions = root.sessions(scope);
            sessions.save(op("same-id"));
            assertThat(sessions.find("same-id")).contains(op("same-id"));
            Document raw = SystemCollections.OP_SESSIONS.on(database).find().first();
            assertThat(raw).containsEntry("formatVersion", 1).containsEntry("secretHash", HASH);
            assertThat(raw.getString("_id")).isNotEqualTo("same-id");
            assertThat(raw.toJson()).doesNotContain("raw-secret-sentinel", "password", "userProfile");
            assertThat(database.listCollectionNames().into(new ArrayList<>()))
                    .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "op_sessions");
            try (var reopened = connect(binding)) { assertThat(reopened.sessions(scope).find("same-id")).contains(op("same-id")); }
            assertThatThrownBy(() -> root.cloudSessions(scope)).satisfies(MongoCacheSessionStoresIT::fenced);
        });
    }

    @Test
    void opExchangeAndLogoutPreserveCredentialExpiryAndAbsoluteCap() {
        withCache(StorageBinding.Mode.OP, (root, database, binding, scope) -> {
            var sessions = root.sessions(scope);
            for (String id : List.of("active", "idle-boundary", "absolute-boundary", "logout")) sessions.save(op(id));
            Document original = row(database, SystemCollections.OP_SESSIONS, "active");
            assertThat(sessions.exchange("active", "wrong", "op-issuer", CREATED, CREATED.plusSeconds(20))).isEmpty();
            assertThat(sessions.exchange("active", HASH, "other-issuer", CREATED, CREATED.plusSeconds(20))).isEmpty();
            assertThat(row(database, SystemCollections.OP_SESSIONS, "active")).isEqualTo(original);
            assertThat(sessions.exchange("idle-boundary", HASH, "op-issuer", CREATED.plusSeconds(30), CREATED.plusSeconds(60))).isEmpty();
            sessions.save(new SessionRecord("absolute-boundary", HASH, "admin", "ADMIN", "op-issuer", false,
                    CREATED, CREATED, CREATED.plusSeconds(200), CREATED.plusSeconds(90)));
            assertThat(sessions.exchange("absolute-boundary", HASH, "op-issuer", CREATED.plusSeconds(90), CREATED.plusSeconds(120))).isEmpty();
            assertThat(sessions.exchange("active", HASH, "op-issuer", CREATED.plusSeconds(29), CREATED.plusSeconds(200)))
                    .get().satisfies(record -> {
                        assertThat(record.lastUsedAt()).isEqualTo(CREATED.plusSeconds(29));
                        assertThat(record.idleExpiresAt()).isEqualTo(CREATED.plusSeconds(90));
                    });
            assertThat(sessions.revoke("logout", "wrong", "op-issuer", CREATED)).isFalse();
            assertThat(sessions.revoke("logout", HASH, "op-issuer", CREATED)).isTrue();
            assertThat(sessions.revoke("logout", HASH, "op-issuer", CREATED)).isTrue();
            assertThat(sessions.exchange("logout", HASH, "op-issuer", CREATED, CREATED.plusSeconds(20))).isEmpty();
            assertThat(sessions.revoke("idle-boundary", HASH, "op-issuer", CREATED.plusSeconds(30))).isFalse();
        });
    }

    @Test
    void browserLogoutOnlyRevokesTheMatchingOriginalSessionEvenAfterExpiry() {
        withCache(StorageBinding.Mode.OP, (root, database, binding, scope) -> {
            var sessions = root.sessions(scope);
            sessions.save(op("browser"));
            var before = row(database, SystemCollections.OP_SESSIONS, "browser");
            assertThat(sessions.revokeBrowserSession("browser", "wrong", "op-issuer")).isFalse();
            assertThat(sessions.revokeBrowserSession("browser", HASH, "other-issuer")).isFalse();
            assertThat(row(database, SystemCollections.OP_SESSIONS, "browser")).isEqualTo(before);
            assertThat(sessions.revokeBrowserSession("browser", HASH, "op-issuer")).isTrue();
            assertThat(sessions.revokeBrowserSession("browser", HASH, "op-issuer")).isTrue();
            assertThat(sessions.find("browser").orElseThrow().revoked()).isTrue();
            assertThat(sessions.exchange("browser", HASH, "op-issuer", CREATED, CREATED.plusSeconds(20))).isEmpty();
        });
    }

    @Test
    void browserRevocationRefusesCorruptOriginalFactsWithoutCommittingItsFence() {
        withCache(StorageBinding.Mode.OP, (root, database, binding, scope) -> {
            var sessions = root.sessions(scope);
            for (boolean missing : List.of(false, true)) {
                sessions.save(op("corrupt-browser"));
                var collection = SystemCollections.OP_SESSIONS.on(database);
                var key = row(database, SystemCollections.OP_SESSIONS, "corrupt-browser").get("_id");
                collection.updateOne(new Document("_id", key), missing
                        ? new Document("$unset", new Document("revoked", ""))
                        : new Document("$set", new Document("revoked", "not-a-boolean")));
                var before = collection.find(new Document("_id", key)).first();
                var identity = SystemCollections.CACHE_IDENTITY.on(database).find().first();
                assertThatThrownBy(() -> sessions.revokeBrowserSession("corrupt-browser", HASH, "op-issuer"))
                        .isInstanceOfSatisfying(TapstateException.class, failure ->
                                assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(collection.find(new Document("_id", key)).first()).isEqualTo(before);
                assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).isEqualTo(identity);
                assertThatThrownBy(() -> sessions.revoke("corrupt-browser", HASH, "op-issuer", CREATED))
                        .isInstanceOfSatisfying(TapstateException.class, failure ->
                                assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(collection.find(new Document("_id", key)).first()).isEqualTo(before);
                assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).isEqualTo(identity);
            }
        });
    }

    @Test
    void cloudReopensWithoutLocalUsersAndDuplicateJtiNeverReplacesLoginFacts() {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            var sessions = root.cloudSessions(scope);
            assertThat(sessions.create(cloud("same-id"))).isTrue();
            assertThat(sessions.create(new CloudSessionRecord(CLOUD, "same-id", "replacement-hash", "other-user", "other-scope",
                    false, CREATED, CREATED, CREATED.plusSeconds(600)))).isFalse();
            assertThat(sessions.find(CLOUD, "same-id")).contains(cloud("same-id"));
            assertThat(SystemCollections.CLOUD_REVOCATIONS.on(database).countDocuments()).isZero();
            Document raw = SystemCollections.CLOUD_SESSIONS.on(database).find().first();
            assertThat(raw.toJson()).doesNotContain("raw-secret-sentinel", "raw-jwt-sentinel", "password", "absoluteExpiresAt");
            assertThat(database.listCollectionNames().into(new ArrayList<>()))
                    .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "cloud_sessions", "cloud_revocations");
            try (var reopened = connect(binding)) { assertThat(reopened.cloudSessions(scope).find(CLOUD, "same-id")).contains(cloud("same-id")); }
            assertThatThrownBy(() -> root.sessions(scope)).satisfies(MongoCacheSessionStoresIT::fenced);
        });
    }

    @Test
    void cloudSlidingExpiryIsStrictMonotonicAndLogoutRemainsValidAfterExpiry() {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            var sessions = root.cloudSessions(scope);
            for (String id : List.of("active", "boundary", "expired")) assertThat(sessions.create(cloud(id))).isTrue();
            assertThat(sessions.authenticate(CLOUD, "active", "wrong", CREATED, CREATED.plusSeconds(60))).isEmpty();
            assertThat(sessions.authenticate(CLOUD, "boundary", HASH, CREATED.plusSeconds(30), CREATED.plusSeconds(60))).isEmpty();
            assertThat(sessions.authenticate(CLOUD, "active", HASH, CREATED.plusSeconds(29), CREATED.plusSeconds(59))).isPresent();
            assertThat(sessions.authenticate(CLOUD, "active", HASH, CREATED.plusSeconds(40), CREATED.plusSeconds(70))).isPresent();
            assertThat(sessions.authenticate(CLOUD, "active", HASH, CREATED.plusSeconds(20), CREATED.plusSeconds(50)))
                    .get().satisfies(record -> {
                        assertThat(record.lastUsedAt()).isEqualTo(CREATED.plusSeconds(40));
                        assertThat(record.idleExpiresAt()).isEqualTo(CREATED.plusSeconds(70));
                    });
            assertThat(sessions.logout(CLOUD, "expired", "wrong", CREATED.plusSeconds(300))).isFalse();
            assertThat(sessions.logout(CLOUD, "expired", HASH, CREATED.plusSeconds(300))).isTrue();
            assertThat(sessions.logout(CLOUD, "expired", HASH, CREATED.plusSeconds(400))).isTrue();
            assertThat(sessions.authenticate(CLOUD, "expired", HASH, CREATED, CREATED.plusSeconds(60))).isEmpty();
            assertThat(sessions.create(cloud("expired"))).isFalse();
            assertThat(SystemCollections.CLOUD_REVOCATIONS.on(database).countDocuments()).isOne();
        });
    }

    @Test
    void revokeFirstCreatesOnlyADenialAndCannotBeReplacedByALateLogin() {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            var sessions = root.cloudSessions(scope);
            sessions.invalidate(CLOUD, "late", CREATED);
            sessions.invalidate(CLOUD, "late", CREATED.plusSeconds(1));
            assertThat(sessions.find(CLOUD, "late")).isEmpty();
            assertThat(sessions.create(cloud("late"))).isFalse();
            assertThat(SystemCollections.CLOUD_SESSIONS.on(database).countDocuments()).isZero();
            Document denial = SystemCollections.CLOUD_REVOCATIONS.on(database).find().first();
            assertThat(denial.keySet()).containsExactlyInAnyOrder("_id", "formatVersion", "issuer", "organizationId", "clusterId", "jwtId", "revokedAt");
            assertThat(denial).containsEntry("revokedAt", CREATED.toEpochMilli());
        });
    }

    @Test
    void independentClientsSerializeCreateAgainstRevokeAcrossBothTables() {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            try (var other = connect(binding); var executor = Executors.newFixedThreadPool(2)) {
                var a = root.cloudSessions(scope); var b = other.cloudSessions(scope);
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var create = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); return a.create(cloud("race")); });
                var revoke = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); b.invalidate(CLOUD, "race", CREATED); return true; });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                create.get(20, TimeUnit.SECONDS); assertThat(revoke.get(20, TimeUnit.SECONDS)).isTrue();
                assertThat(a.create(cloud("race"))).isFalse();
                assertThat(a.authenticate(CLOUD, "race", HASH, CREATED, CREATED.plusSeconds(60))).isEmpty();
                a.find(CLOUD, "race").ifPresent(record -> assertThat(record.revoked()).isTrue());
                assertThat(SystemCollections.CLOUD_REVOCATIONS.on(database).countDocuments()).isOne();
            }
        });
    }

    @Test
    void cloudContextIsExactAndCannotMoveToAnotherCluster() {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            var sessions = root.cloudSessions(scope);
            assertThat(sessions.create(cloud("context"))).isTrue();
            for (var wrong : List.of(new CloudSessionIdentity("https://other.example", "org-a", "cluster-a"),
                    new CloudSessionIdentity(CLOUD.issuer(), "org-b", "cluster-a"),
                    new CloudSessionIdentity(CLOUD.issuer(), "org-a", "cluster-b"))) {
                assertThat(sessions.find(wrong, "context")).isEmpty();
                assertThat(sessions.authenticate(wrong, "context", HASH, CREATED, CREATED.plusSeconds(60))).isEmpty();
                assertThat(sessions.logout(wrong, "context", HASH, CREATED)).isFalse();
            }
            var foreign = new CloudSessionIdentity(CLOUD.issuer(), CLOUD.organizationId(), "cluster-b");
            assertThat(sessions.create(new CloudSessionRecord(foreign, "foreign", HASH, "user", "WRITE", false, CREATED, CREATED, CREATED.plusSeconds(30)))).isFalse();
            assertThatThrownBy(() -> sessions.invalidate(foreign, "foreign", CREATED)).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThat(sessions.find(CLOUD, "context")).contains(cloud("context"));
        });
    }

    @Test
    void authResetFencesEveryOldOpOperationAndDoesNotAdoptOldRecords() {
        withCache(StorageBinding.Mode.OP, (root, database, binding, scope) -> {
            var old = root.sessions(scope); old.save(op("same-id"));
            var next = root.advanceAuthGeneration(scope, 2, "reset-a", ACTIVE);
            assertThatThrownBy(() -> old.find("same-id")).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThatThrownBy(() -> old.save(op("late"))).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThatThrownBy(() -> old.exchange("same-id", HASH, "op-issuer", CREATED, CREATED.plusSeconds(60))).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThatThrownBy(() -> old.revoke("same-id", HASH, "op-issuer", CREATED)).satisfies(MongoCacheSessionStoresIT::fenced);
            var current = root.sessions(next); assertThat(current.find("same-id")).isEmpty();
            current.save(op("same-id")); assertThat(current.find("same-id")).contains(op("same-id"));
            assertThat(SystemCollections.OP_SESSIONS.on(database).countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void authResetFencesEveryOldCloudOperationIncludingLateRevocation() {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            var old = root.cloudSessions(scope); assertThat(old.create(cloud("same-id"))).isTrue();
            old.invalidate(CLOUD, "denied", CREATED);
            var next = root.advanceAuthGeneration(scope, 2, "reset-a", ACTIVE);
            assertThatThrownBy(() -> old.find(CLOUD, "same-id")).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThatThrownBy(() -> old.create(cloud("late"))).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThatThrownBy(() -> old.authenticate(CLOUD, "same-id", HASH, CREATED, CREATED.plusSeconds(60))).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThatThrownBy(() -> old.logout(CLOUD, "same-id", HASH, CREATED)).satisfies(MongoCacheSessionStoresIT::fenced);
            assertThatThrownBy(() -> old.invalidate(CLOUD, "same-id", CREATED)).satisfies(MongoCacheSessionStoresIT::fenced);
            var current = root.cloudSessions(next); assertThat(current.find(CLOUD, "same-id")).isEmpty();
            assertThat(current.create(cloud("same-id"))).isTrue();
            assertThat(current.create(cloud("denied"))).isTrue();
            assertThat(current.authenticate(CLOUD, "same-id", HASH, CREATED, CREATED.plusSeconds(60))).isPresent();
        });
    }

    @Test
    void missingAuthNamespaceIsRefusedBeforeNormalOperationsCanRecreateIt() {
        withCache(StorageBinding.Mode.OP, (root, database, binding, scope) -> {
            var sessions = root.sessions(scope); SystemCollections.OP_SESSIONS.on(database).drop();
            assertThatThrownBy(() -> sessions.save(op("late"))).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThat(database.listCollectionNames().into(new ArrayList<>())).doesNotContain("op_sessions");
        });
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            var sessions = root.cloudSessions(scope); SystemCollections.CLOUD_REVOCATIONS.on(database).drop();
            assertThatThrownBy(() -> sessions.create(cloud("late"))).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThatThrownBy(() -> sessions.invalidate(CLOUD, "late", CREATED)).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThat(database.listCollectionNames().into(new ArrayList<>())).doesNotContain("cloud_revocations");
            assertThat(SystemCollections.CLOUD_SESSIONS.on(database).countDocuments()).isZero();
        });
    }

    @Test
    void corruptLoginRollsBackLogoutAndNeverLeaksItsRawValueInACause() {
        withCache(StorageBinding.Mode.OP, (root, database, binding, scope) -> {
            var sessions = root.sessions(scope); sessions.save(op("broken"));
            SystemCollections.OP_SESSIONS.on(database).updateOne(new Document(), new Document("$set", new Document("principal", "")));
            assertThatThrownBy(() -> sessions.find("broken")).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThatThrownBy(() -> sessions.revoke("broken", HASH, "op-issuer", CREATED)).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThat(SystemCollections.OP_SESSIONS.on(database).find().first()).containsEntry("revoked", false);
        });
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            var sessions = root.cloudSessions(scope); assertThat(sessions.create(cloud("broken"))).isTrue();
            SystemCollections.CLOUD_SESSIONS.on(database).updateOne(new Document(), new Document("$set", new Document("userId", "")));
            assertThatThrownBy(() -> sessions.logout(CLOUD, "broken", HASH, CREATED)).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThat(SystemCollections.CLOUD_SESSIONS.on(database).find().first()).containsEntry("revoked", false);
            assertThat(SystemCollections.CLOUD_REVOCATIONS.on(database).countDocuments()).isZero();
        });
    }

    @Test
    void aMalformedDenialIsNeverTreatedAsAnAbsentRevocation() {
        withCache(StorageBinding.Mode.CLOUD, (root, database, binding, scope) -> {
            CloudSessionStore sessions = root.cloudSessions(scope); sessions.invalidate(CLOUD, "broken", CREATED);
            SystemCollections.CLOUD_REVOCATIONS.on(database).updateOne(new Document(), new Document("$set", new Document("revokedAt", "raw-secret-sentinel")));
            assertThatThrownBy(() -> sessions.create(cloud("broken"))).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThatThrownBy(() -> sessions.authenticate(CLOUD, "broken", HASH, CREATED, CREATED.plusSeconds(60))).satisfies(MongoCacheSessionStoresIT::unreadable);
            assertThat(SystemCollections.CLOUD_SESSIONS.on(database).countDocuments()).isZero();
        });
    }

    private static SessionRecord op(String id) { return new SessionRecord(id, HASH, "admin", "ADMIN", "op-issuer", false,
            CREATED, CREATED, CREATED.plusSeconds(30), CREATED.plusSeconds(90)); }
    private static CloudSessionRecord cloud(String id) { return new CloudSessionRecord(CLOUD, id, HASH, "cloud-user", "WRITE", false,
            CREATED, CREATED, CREATED.plusSeconds(30)); }
    private static Document row(MongoDatabase database, SystemCollections table, String id) {
        return table.on(database).find().into(new ArrayList<>()).stream().filter(row -> row.getString("_id").endsWith(",\"" + id + "\"]")).findFirst().orElseThrow();
    }
    private static MongoCacheDomain connect(StorageBinding.Connection binding) { return MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "cache-session-test"); }
    private static void fenced(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
            error -> assertThat(error.code()).isEqualTo(IoError.CACHE_AUTH_FENCED)); }
    private static void unreadable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
        assertThat(error.getMessage()).doesNotContain("raw-secret-sentinel");
    }); }
    private static void withCache(StorageBinding.Mode mode, Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("cache_sessions_" + UUID.randomUUID().toString().replace("-", ""));
            String base = MONGO.getReplicaSetUrl();
            var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    base + (base.contains("?") ? "&" : "?") + "timeoutMS=5000&maxPoolSize=1", database.getName(), null);
            var scope = new CacheAuthScope("cluster-a", 1, mode, "incarnation-a", 1);
            try (var root = connect(binding)) { root.prepareAuth(scope, "setup-a", ACTIVE); scenario.run(root, database, binding, scope); }
            catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario {
        void run(MongoCacheDomain root, MongoDatabase database, StorageBinding.Connection binding, CacheAuthScope scope) throws Exception;
    }
}
