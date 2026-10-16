package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the connector spec store against a real Mongo: a stored spec reads back byte-exact, an
 * unknown hash has nothing, and storing the same content twice keeps one document — the store is
 * content-addressed, so the second write is over identical bytes and must not accumulate. Where Docker
 * is absent this aborts on a developer machine and fails in CI, where a skip would be a green build
 * that ran nothing.
 */
@RequiresDocker
class MongoConnectorSpecStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void storedSpecReadsBackByteExact() {
        withStore((store, database) -> {
            // Bytes a String round-trip would not survive unchanged: a lone high codepoint plus the
            // key order and whitespace a re-serializing store would normalize away.
            byte[] spec = "{\"properties\":{\"id\":\"mysql\"},\"zz\":\"\\u00e9\",\"a\": 1}"
                    .getBytes(StandardCharsets.UTF_8);

            store.put("abc123", spec);

            assertThat(store.get("abc123")).contains(spec);
        });
    }

    @Test
    void specForAnUnknownHashIsEmpty() {
        withStore((store, database) -> assertThat(store.get("deadbeef")).isEmpty());
    }

    @Test
    void presenceIsAnsweredWithoutTheSourceComingBack() {
        // The projected lookup has to answer the same question the full read does - a presence test that
        // disagreed with get() would send a re-register either into rewriting what is already filed or
        // into skipping a source that is not there.
        withStore((store, database) -> {
            store.put("hash-present", "{\"properties\":{\"id\":\"mysql\"}}".getBytes(StandardCharsets.UTF_8));

            assertThat(store.has("hash-present")).isTrue();
            assertThat(store.has("hash-absent")).isFalse();
        });
    }

    @Test
    void storingTheSameContentTwiceKeepsOneDocument() {
        withStore((store, database) -> {
            byte[] spec = "{\"properties\":{\"id\":\"mongodb\"}}".getBytes(StandardCharsets.UTF_8);

            store.put("hash-1", spec);
            store.put("hash-1", spec);

            assertThat(database.getCollection(MongoStorePort.CONNECTOR_SPECS).countDocuments()).isEqualTo(1);
            assertThat(store.get("hash-1")).contains(spec);
        });
    }

    private interface SpecStoreTest {
        void run(MongoConnectorSpecStore store, MongoDatabase database);
    }

    /** Runs a test body against a fresh spec store over a clean collection on the real replica-set. */
    private static void withStore(SpecStoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("tapstate");
            database.getCollection(MongoStorePort.CONNECTOR_SPECS).drop();
            test.run(new MongoConnectorSpecStore(database.getCollection(MongoStorePort.CONNECTOR_SPECS)), database);
        }
    }
}
