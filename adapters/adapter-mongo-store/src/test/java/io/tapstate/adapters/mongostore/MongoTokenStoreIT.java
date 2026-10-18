package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Witnesses the token store against a real Mongo replica-set: a saved token reads back with the same
 * fields (through the real bson encode), an absent id reads back empty, revocation sets the flag in
 * place, revoking an unknown id is a no-op, and list returns every token including revoked ones. Where
 * Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a green
 * build that ran nothing.
 */
@RequiresDocker
class MongoTokenStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final Instant NOW = Instant.parse("2026-07-08T12:00:00Z");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void savedTokenReadsBackWithItsFields() {
        withStore((store, collection) -> {
            TokenRecord record = new TokenRecord("tok-1", "WRITE", "hash-abc", false, NOW);
            store.save(record);

            assertThat(store.find("tok-1")).contains(record);
        });
    }

    @Test
    void findReturnsEmptyForAnAbsentTokenId() {
        withStore((store, collection) -> assertThat(store.find("nobody")).isEmpty());
    }

    @Test
    void revokeMarksTheTokenRevokedInPlace() {
        withStore((store, collection) -> {
            store.save(new TokenRecord("tok-1", "WRITE", "hash-abc", false, NOW));

            store.revoke("tok-1");

            assertThat(collection.countDocuments()).isEqualTo(1);
            Optional<TokenRecord> read = store.find("tok-1");
            assertThat(read).isPresent();
            assertThat(read.get().revoked()).isTrue();
        });
    }

    @Test
    void revokingAnUnknownTokenIsANoOp() {
        withStore((store, collection) -> {
            assertThatCode(() -> store.revoke("never-issued")).doesNotThrowAnyException();
            assertThat(collection.countDocuments()).isZero();
        });
    }

    @Test
    void listReturnsEveryTokenIncludingRevoked() {
        withStore((store, collection) -> {
            store.save(new TokenRecord("tok-1", "WRITE", "hash-1", false, NOW));
            store.save(new TokenRecord("tok-2", "ADMIN", "hash-2", false, NOW));
            store.revoke("tok-1");

            assertThat(store.list()).containsExactlyInAnyOrder(
                    new TokenRecord("tok-1", "WRITE", "hash-1", true, NOW),
                    new TokenRecord("tok-2", "ADMIN", "hash-2", false, NOW));
        });
    }

    private interface StoreTest {
        void run(MongoTokenStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh token store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("tokens");
            collection.drop();
            test.run(new MongoTokenStore(collection), collection);
        }
    }
}
