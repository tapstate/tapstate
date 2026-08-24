package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class MongoSessionStoreIT {

    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Instant CREATED = Instant.parse("2026-08-17T10:00:00Z");

    @Container
    static final MongoDBContainer CONTAINER = MONGO;

    @Test
    void sessionSurvivesStoreRestartWithoutPersistingTheRawSecret() throws Exception {
        withCollection(collection -> {
            SessionRecord record = record();
            new MongoSessionStore(collection).save(record);

            MongoSessionStore restarted = new MongoSessionStore(collection);
            assertThat(restarted.find("s01")).contains(record);
            Document raw = collection.find(new Document("_id", "s01")).first();
            assertThat(raw).isNotNull();
            assertThat(raw.toJson()).doesNotContain("session-secret", "tss_s01.session-secret");
        });
    }

    @Test
    void exchangeConditionAndUpdateAreAtomicAndCapIdleAtAbsolute() throws Exception {
        withCollection(collection -> {
            MongoSessionStore store = new MongoSessionStore(collection);
            Instant now = CREATED.plusSeconds(89L * 24 * 3600);
            Instant desiredIdle = now.plusSeconds(30L * 24 * 3600);
            SessionRecord record = record();
            store.save(new SessionRecord(record.sessionId(), record.secretHash(), record.principal(),
                    record.scope(), record.issuer(), record.revoked(), record.createdAt(), now.minusSeconds(1),
                    record.absoluteExpiresAt(), record.absoluteExpiresAt()));

            try (var executor = Executors.newFixedThreadPool(2)) {
                List<Callable<Boolean>> exchanges = List.of(
                        () -> store.exchange("s01", "sha256-fixture",
                                "urn:tapstate:cluster:01J5FIXTURE", now, desiredIdle).isPresent(),
                        () -> store.exchange("s01", "sha256-fixture",
                                "urn:tapstate:cluster:01J5FIXTURE", now, desiredIdle).isPresent());
                assertThat(executor.invokeAll(exchanges)).allSatisfy(future -> assertThat(future.get()).isTrue());
            }

            SessionRecord touched = store.find("s01").orElseThrow();
            assertThat(touched.lastUsedAt()).isEqualTo(now);
            assertThat(touched.idleExpiresAt()).isEqualTo(record.absoluteExpiresAt());
            assertThat(store.exchange("s01", "wrong", touched.issuer(), now, desiredIdle)).isEmpty();
            assertThat(store.exchange("s01", touched.secretHash(), "urn:other", now, desiredIdle)).isEmpty();
        });
    }

    @Test
    void logoutIsCredentialCheckedAndIdempotent() throws Exception {
        withCollection(collection -> {
            MongoSessionStore store = new MongoSessionStore(collection);
            store.save(record());

            assertThat(store.revoke("s01", "wrong", record().issuer(), CREATED)).isFalse();
            assertThat(store.revoke("s01", record().secretHash(), "urn:other", CREATED)).isFalse();
            assertThat(store.revoke("s01", record().secretHash(), record().issuer(), CREATED)).isTrue();
            assertThat(store.revoke("s01", record().secretHash(), record().issuer(), CREATED)).isTrue();
            assertThat(store.find("s01")).get().extracting(SessionRecord::revoked).isEqualTo(true);
        });
    }

    private static SessionRecord record() {
        return new SessionRecord("s01", "sha256-fixture", "admin", "ADMIN",
                "urn:tapstate:cluster:01J5FIXTURE", false, CREATED, CREATED,
                CREATED.plusSeconds(30L * 24 * 3600), CREATED.plusSeconds(90L * 24 * 3600));
    }

    private static void withCollection(CollectionTest body) throws Exception {
        try (MongoClient client = MongoClients.create(CONTAINER.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("sessions");
            collection.drop();
            body.run(collection);
        }
    }

    @FunctionalInterface
    private interface CollectionTest {
        void run(MongoCollection<Document> collection) throws Exception;
    }
}
