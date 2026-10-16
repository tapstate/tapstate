package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Local Mongo witnesses for managed-session admission and revocation, independent of any Cloud SDK. */
@RequiresDocker
class MongoCloudSessionStoreIT {

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Instant CREATED = Instant.parse("2026-09-28T10:00:00Z");
    private static final Duration IDLE = Duration.ofMinutes(30);
    private static final CloudSessionIdentity IDENTITY =
            new CloudSessionIdentity("https://cloud.example", "org-one", "cluster-one");
    private static final String SECRET = "raw-cookie-secret-sentinel";
    private static final String HASH = hash(SECRET);

    @Test
    void cloudAndOnPremFacetsCoexistAndSurviveANewConnectionWithoutPersistingRawCredentials() {
        String database = database();
        CloudSessionRecord cloud = record("same-jti", HASH);
        SessionRecord local = new SessionRecord("same-jti", hash("local-cookie-secret"), "local-admin", "ADMIN",
                "urn:tapstate:local", false, CREATED, CREATED,
                CREATED.plus(Duration.ofDays(30)), CREATED.plus(Duration.ofDays(90)));
        Document originalLocal;
        try (MongoConnection first = connection(database)) {
            MongoAuthStores stores = new MongoAuthStores(first);
            stores.sessions().save(local);
            assertThat(stores.cloudSessions().create(cloud)).isTrue();
            MongoCollection<Document> collection = SystemCollections.SESSIONS.on(first.database());
            assertThat(collection.countDocuments()).isEqualTo(2);
            originalLocal = collection.find(new Document("_id", local.sessionId())).first();
            Document raw = collection.find(new Document("_id", MongoCloudSessionStore.sessionKey(IDENTITY, cloud.jwtId())))
                    .first();
            assertThat(raw).isNotNull();
            assertThat(raw.get("_id")).isInstanceOf(Binary.class);
            assertThat(raw.getString("origin")).isEqualTo("cloud");
            assertThat(raw.toJson()).doesNotContain(SECRET, "raw-jwt-sentinel", "absoluteExpiresAt", "password");
            assertThat(raw.keySet()).containsExactlyInAnyOrder("_id", "origin", "issuer", "organizationId",
                    "clusterId", "jwtId", "secretHash", "userId", "scope", "revoked", "createdAt",
                    "lastUsedAt", "idleExpiresAt");
        }
        try (MongoConnection restarted = connection(database)) {
            MongoAuthStores stores = new MongoAuthStores(restarted);
            assertThat(stores.cloudSessions().find(IDENTITY, cloud.jwtId())).contains(cloud);
            assertThat(stores.sessions().find(local.sessionId())).contains(local);
            assertThat(stores.cloudSessions().authenticate(IDENTITY, cloud.jwtId(), HASH,
                    CREATED.plusSeconds(1), CREATED.plusSeconds(1).plus(IDLE))).isPresent();
            assertThat(SystemCollections.SESSIONS.on(restarted.database())
                    .find(new Document("_id", local.sessionId())).first()).isEqualTo(originalLocal);
        }
    }

    @Test
    void thirtyMinuteIdleBoundaryIsStrictAndRefreshIsSlidingAndMonotonic() throws Exception {
        withCollection(collection -> {
            MongoCloudSessionStore store = new MongoCloudSessionStore(collection);
            assertThat(store.create(record("active", HASH))).isTrue();
            assertThat(store.create(record("at-boundary", HASH))).isTrue();
            assertThat(store.create(record("after-boundary", HASH))).isTrue();

            Instant before = CREATED.plus(IDLE).minusMillis(1);
            assertThat(store.authenticate(IDENTITY, "active", HASH, before, before.plus(IDLE))).isPresent();
            assertThat(store.authenticate(IDENTITY, "at-boundary", HASH,
                    CREATED.plus(IDLE), CREATED.plus(IDLE).plus(IDLE))).isEmpty();
            assertThat(store.authenticate(IDENTITY, "after-boundary", HASH,
                    CREATED.plus(IDLE).plusMillis(1), CREATED.plus(IDLE).plus(IDLE))).isEmpty();

            // Passing the initial idle deadline does not impose a JWT-derived absolute lifetime.
            Instant later = CREATED.plus(Duration.ofMinutes(31));
            assertThat(store.authenticate(IDENTITY, "active", HASH, later, later.plus(IDLE)))
                    .get().extracting(CloudSessionRecord::idleExpiresAt).isEqualTo(later.plus(IDLE));
            Instant older = CREATED.plus(Duration.ofMinutes(20));
            assertThat(store.authenticate(IDENTITY, "active", HASH, older, older.plus(IDLE)))
                    .get().satisfies(session -> {
                        assertThat(session.lastUsedAt()).isEqualTo(later);
                        assertThat(session.idleExpiresAt()).isEqualTo(later.plus(IDLE));
                    });
            assertThat(store.authenticate(IDENTITY, "active", HASH, later.plus(IDLE), later.plus(IDLE).plus(IDLE)))
                    .isEmpty();
        });
    }

    @Test
    void wrongSecretOrAnyWrongContextCannotAuthenticateOrLogoutOrTouchTheRecord() throws Exception {
        withCollection(collection -> {
            MongoCloudSessionStore store = new MongoCloudSessionStore(collection);
            CloudSessionRecord initial = record("context-bound", HASH);
            assertThat(store.create(initial)).isTrue();
            for (CloudSessionIdentity wrong : new CloudSessionIdentity[] {
                    new CloudSessionIdentity("https://other.example", IDENTITY.organizationId(), IDENTITY.clusterId()),
                    new CloudSessionIdentity(IDENTITY.issuer(), "other-org", IDENTITY.clusterId()),
                    new CloudSessionIdentity(IDENTITY.issuer(), IDENTITY.organizationId(), "other-cluster")}) {
                assertThat(store.authenticate(wrong, initial.jwtId(), HASH, CREATED, CREATED.plus(IDLE))).isEmpty();
                assertThat(store.logout(wrong, initial.jwtId(), HASH, CREATED)).isFalse();
            }
            assertThat(store.authenticate(IDENTITY, initial.jwtId(), hash("wrong-secret"), CREATED, CREATED.plus(IDLE)))
                    .isEmpty();
            assertThat(store.logout(IDENTITY, initial.jwtId(), hash("wrong-secret"), CREATED)).isFalse();
            assertThat(store.authenticate(IDENTITY, "wrong-jti", HASH, CREATED, CREATED.plus(IDLE))).isEmpty();
            assertThat(store.find(IDENTITY, initial.jwtId())).contains(initial);

            collection.updateOne(new Document("_id", MongoCloudSessionStore.sessionKey(IDENTITY, initial.jwtId())),
                    new Document("$set", new Document("origin", "on-prem")));
            assertThat(store.authenticate(IDENTITY, initial.jwtId(), HASH, CREATED, CREATED.plus(IDLE))).isEmpty();
            assertThat(store.logout(IDENTITY, initial.jwtId(), HASH, CREATED)).isFalse();
        });
    }

    @Test
    void duplicateLoginAndEarlyInvalidationCannotResetOrReplaceASecret() throws Exception {
        withCollection(collection -> {
            MongoCloudSessionStore store = new MongoCloudSessionStore(collection);
            CloudSessionRecord initial = record("existing", HASH);
            assertThat(store.create(initial)).isTrue();
            assertThat(store.create(record(initial.jwtId(), hash("replacement-secret")))).isFalse();
            assertThat(store.find(IDENTITY, initial.jwtId())).contains(initial);
            store.invalidate(IDENTITY, initial.jwtId(), CREATED);
            Document revoked = collection.find(new Document("_id", MongoCloudSessionStore.sessionKey(IDENTITY, initial.jwtId())))
                    .first();
            store.invalidate(IDENTITY, initial.jwtId(), CREATED.plusSeconds(30));
            assertThat(collection.find(new Document("_id", revoked.get("_id"))).first()).isEqualTo(revoked);
            assertThat(store.find(IDENTITY, initial.jwtId())).get().satisfies(session -> {
                assertThat(session.revoked()).isTrue();
                assertThat(session.secretHash()).isEqualTo(HASH);
                assertThat(session.createdAt()).isEqualTo(initial.createdAt());
                assertThat(session.lastUsedAt()).isEqualTo(initial.lastUsedAt());
                assertThat(session.idleExpiresAt()).isEqualTo(initial.idleExpiresAt());
            });

            store.invalidate(IDENTITY, "before-login", CREATED);
            store.invalidate(IDENTITY, "before-login", CREATED.plusSeconds(1));
            assertThat(store.create(record("before-login", HASH))).isFalse();
            assertThat(store.find(IDENTITY, "before-login")).isEmpty();
            assertThat(store.authenticate(IDENTITY, "before-login", HASH, CREATED, CREATED.plus(IDLE))).isEmpty();
            Document marker = collection.find(new Document("_id", MongoCloudSessionStore.sessionKey(IDENTITY, "before-login")))
                    .first();
            assertThat(marker.keySet()).containsExactlyInAnyOrder(
                    "_id", "origin", "issuer", "organizationId", "clusterId", "jwtId", "revoked");
            assertThat(marker.getBoolean("revoked")).isTrue();
        });
    }

    @Test
    void logoutRemainsIdempotentEvenAfterIdleExpiryAndRetainsLoginFacts() throws Exception {
        withCollection(collection -> {
            MongoCloudSessionStore store = new MongoCloudSessionStore(collection);
            CloudSessionRecord initial = record("logout", HASH);
            assertThat(store.create(initial)).isTrue();
            Instant expired = CREATED.plus(Duration.ofDays(1));
            assertThat(store.logout(IDENTITY, initial.jwtId(), HASH, expired)).isTrue();
            assertThat(store.logout(IDENTITY, initial.jwtId(), HASH, expired.plusSeconds(1))).isTrue();
            assertThat(store.create(record(initial.jwtId(), hash("new-secret")))).isFalse();
            assertThat(store.find(IDENTITY, initial.jwtId())).get().satisfies(session -> {
                assertThat(session.revoked()).isTrue();
                assertThat(session.secretHash()).isEqualTo(initial.secretHash());
                assertThat(session.lastUsedAt()).isEqualTo(initial.lastUsedAt());
                assertThat(session.idleExpiresAt()).isEqualTo(initial.idleExpiresAt());
            });
            assertThat(store.authenticate(IDENTITY, initial.jwtId(), HASH, CREATED, CREATED.plus(IDLE))).isEmpty();
        });
    }

    @Test
    void concurrentCreateAndInvalidateAlwaysLeaveTheJwtIdDenied() throws Exception {
        withCollection(collection -> {
            MongoCloudSessionStore store = new MongoCloudSessionStore(collection);
            try (var executor = Executors.newFixedThreadPool(2)) {
                for (int attempt = 0; attempt < 12; attempt++) {
                    String jwtId = "racing-" + attempt;
                    CountDownLatch ready = new CountDownLatch(2);
                    CountDownLatch start = new CountDownLatch(1);
                    var created = executor.submit(() -> {
                        ready.countDown();
                        assertThat(start.await(3, TimeUnit.SECONDS)).isTrue();
                        return store.create(record(jwtId, HASH));
                    });
                    var invalidated = executor.submit(() -> {
                        ready.countDown();
                        assertThat(start.await(3, TimeUnit.SECONDS)).isTrue();
                        store.invalidate(IDENTITY, jwtId, CREATED);
                        return true;
                    });
                    assertThat(ready.await(3, TimeUnit.SECONDS)).isTrue();
                    start.countDown();
                    created.get(5, TimeUnit.SECONDS);
                    assertThat(invalidated.get(5, TimeUnit.SECONDS)).isTrue();
                    Document raw = collection.find(new Document("_id", MongoCloudSessionStore.sessionKey(IDENTITY, jwtId)))
                            .first();
                    assertThat(raw.getBoolean("revoked")).isTrue();
                    assertThat(store.create(record(jwtId, hash("second-secret")))).isFalse();
                    assertThat(store.authenticate(IDENTITY, jwtId, HASH, CREATED, CREATED.plus(IDLE))).isEmpty();
                }
            }
        });
    }

    @Test
    void malformedStoredLoginFactsYieldOnlyASafeCodedDiagnostic() throws Exception {
        withCollection(collection -> {
            CloudSessionRecord initial = record("unreadable", HASH);
            Document malformed = MongoCloudSessionStore.toDocument(initial);
            malformed.put("userId", new Document("untrusted", "stored-value-secret-sentinel"));
            collection.insertOne(malformed);
            CloudSessionStore store = new MongoCloudSessionStore(collection);
            assertThatThrownBy(() -> store.find(IDENTITY, initial.jwtId()))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(failure.getCause()).isNull();
                        assertThat(failure.args().toString()).doesNotContain(SECRET, "stored-value-secret-sentinel", HASH);
                    });
            assertThatThrownBy(() -> store.authenticate(IDENTITY, initial.jwtId(), HASH, CREATED, CREATED.plus(IDLE)))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        });
    }

    private static CloudSessionRecord record(String jwtId, String secretHash) {
        return new CloudSessionRecord(IDENTITY, jwtId, secretHash, "verified-user-one", "workload:read workload:write",
                false, CREATED, CREATED, CREATED.plus(IDLE));
    }

    private static String hash(String secret) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(secret.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException unavailable) {
            throw new AssertionError(unavailable);
        }
    }

    private static MongoConnection connection(String database) {
        MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl(database), null, Duration.ofSeconds(5)));
        connection.verify();
        return connection;
    }

    private static String database() {
        return "cloud_sessions_" + Long.toUnsignedString(System.nanoTime(), 16);
    }

    private static void withCollection(CollectionTest body) throws Exception {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            body.run(SystemCollections.SESSIONS.on(client.getDatabase(database())));
        }
    }

    @FunctionalInterface
    private interface CollectionTest {
        void run(MongoCollection<Document> collection) throws Exception;
    }
}
