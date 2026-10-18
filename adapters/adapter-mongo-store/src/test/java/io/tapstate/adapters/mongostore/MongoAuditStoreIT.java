package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the audit log against a real Mongo replica-set: a recorded entry lands as a document
 * carrying its fields (through the real bson encode), and the log is append-only — recording the same
 * subject / operation / target twice leaves two distinct documents rather than overwriting one. Where
 * Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a green
 * build that ran nothing.
 */
@RequiresDocker
class MongoAuditStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final Instant TS = Instant.parse("2026-07-07T10:15:30Z");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void recordedEntryLandsWithItsFields() {
        withStore((store, collection) -> {
            store.record(new AuditRecord(TS, "alice", "artifact.apply", "orders-source"));

            Document stored = collection.find().first();
            assertThat(stored).isNotNull();
            assertThat(stored.getLong("ts")).isEqualTo(TS.toEpochMilli());
            assertThat(stored.getString("principal")).isEqualTo("alice");
            assertThat(stored.getString("operationId")).isEqualTo("artifact.apply");
            assertThat(stored.getString("resourceId")).isEqualTo("orders-source");
        });
    }

    @Test
    void theLogIsAppendOnly() {
        withStore((store, collection) -> {
            AuditRecord entry = new AuditRecord(TS, "alice", "artifact.apply", "orders-source");
            store.record(entry);
            store.record(entry);

            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    private interface StoreTest {
        void run(MongoAuditStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh audit store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("audit");
            collection.drop();
            test.run(new MongoAuditStore(collection), collection);
        }
    }
}
