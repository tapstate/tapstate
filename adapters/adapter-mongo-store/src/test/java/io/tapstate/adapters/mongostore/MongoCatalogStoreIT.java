package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the connection catalog against a real Mongo replica-set: a saved connection reads back
 * equal (settings included, through the real bson encode / decode), an absent id reads back empty,
 * list returns every stored connection, a re-save of the same id replaces in place (last write wins)
 * rather than accumulating documents, and nested settings decode back to plain Java maps and lists.
 * Where Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a
 * green build that ran nothing.
 */
@RequiresDocker
class MongoCatalogStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static ConnectionConfig ordersDb() {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", "db.local");
        settings.put("port", 3306);
        settings.put("tls", true);
        return new ConnectionConfig("orders-db", "mysql", settings);
    }

    @Test
    void savedConnectionReadsBackEqual() {
        withStore((store, collection) -> {
            ConnectionConfig config = ordersDb();
            store.save(config);

            Optional<ConnectionConfig> read = store.get("orders-db");
            assertThat(read).contains(config);
        });
    }

    @Test
    void getReturnsEmptyForAnAbsentId() {
        withStore((store, collection) -> assertThat(store.get("nope")).isEmpty());
    }

    @Test
    void listReturnsEveryStoredConnection() {
        withStore((store, collection) -> {
            store.save(ordersDb());
            store.save(new ConnectionConfig("events", "kafka", Map.of("brokers", "localhost:9092")));

            assertThat(store.list())
                    .extracting(ConnectionConfig::id)
                    .containsExactlyInAnyOrder("orders-db", "events");
        });
    }

    @Test
    void reSaveOfTheSameIdReplacesInPlace() {
        withStore((store, collection) -> {
            store.save(ordersDb());
            ConnectionConfig changed = new ConnectionConfig("orders-db", "mysql", Map.of("host", "replica"));
            store.save(changed);

            assertThat(store.list()).extracting(ConnectionConfig::id).containsExactly("orders-db");
            assertThat(store.get("orders-db")).contains(changed);
        });
    }

    @Test
    void nestedSettingsDecodeBackToPlainJavaTypes() {
        withStore((store, collection) -> {
            Map<String, Object> pool = new LinkedHashMap<>();
            pool.put("min", 1);
            pool.put("max", 8);
            Map<String, Object> settings = new LinkedHashMap<>();
            settings.put("host", "db.local");
            settings.put("pool", pool);
            settings.put("hosts", List.of("a", "b"));
            ConnectionConfig config = new ConnectionConfig("nested", "mysql", settings);
            store.save(config);

            ConnectionConfig read = store.get("nested").orElseThrow();
            assertThat(read).isEqualTo(config);
            assertThat(read.settings().get("pool")).isInstanceOf(Map.class).isNotInstanceOf(Document.class);
        });
    }

    private interface StoreTest {
        void run(MongoCatalogStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh catalog store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("connections");
            collection.drop();
            test.run(new MongoCatalogStore(collection), collection);
        }
    }
}
