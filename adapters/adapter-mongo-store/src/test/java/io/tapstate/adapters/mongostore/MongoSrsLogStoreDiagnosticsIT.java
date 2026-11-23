package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies coded diagnostics when durable log documents cannot be decoded. */
@RequiresDocker
class MongoSrsLogStoreDiagnosticsIT {

    private static final String RING = "srs.mc-1.orders";

    @Container
    private static final MongoDBContainer REPLICA_SET =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void damagedBoundsRetainTheCompleteDiagnosticContract() {
        withStoreCollection((store, collection) -> {
            collection.insertOne(new Document("_id", MongoSrsLogStore.boundsId(RING))
                    .append("largestSequence", "invalid"));
            assertUnreadable(() -> store.bounds(RING), "bounds");
        });
    }

    @Test
    void damagedRecordRetainsTheCompleteDiagnosticContract() {
        withStoreCollection((store, collection) -> {
            collection.insertOne(new Document("_id", new Document("ring", RING).append("seq", 1L))
                    .append("op", "invalid"));
            assertUnreadable(() -> store.load(RING, 1L), "record");
        });
    }

    private static void assertUnreadable(Runnable read, String field) {
        assertThatThrownBy(read::run).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isSameAs(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.args()).containsEntry("field", field);
            assertThat(error.args().keySet()).isEqualTo(error.code().placeholders());
        });
    }

    private static void withStoreCollection(BiConsumer<MongoSrsLogStore, MongoCollection<Document>> body) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getConnectionString())) {
            MongoCollection<Document> collection = client
                    .getDatabase("tapstate_test")
                    .getCollection("srs_log_" + System.nanoTime());
            body.accept(new MongoSrsLogStore(collection), collection);
        }
    }
}
