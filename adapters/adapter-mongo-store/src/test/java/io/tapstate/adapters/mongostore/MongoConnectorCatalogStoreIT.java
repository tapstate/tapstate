package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the derived connector-catalog store against a real Mongo replica-set: an upserted row
 * (with config, options, visibleWhen and provenance) reads back equal through the real bson encode /
 * decode, an unregistered connector reads back empty, every upserted row is listed, and a re-register
 * of the same connector replaces the stored row in place (last write wins) rather than accumulating
 * documents. Where Docker is absent this aborts on a developer machine and fails in CI, where a skip
 * would be a green build that ran nothing.
 */
@RequiresDocker
class MongoConnectorCatalogStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static final String MYSQL_ROW = """
            {
              "id": "mysql", "name": "Mysql", "displayName": "MySQL", "icon": "icons/mysql.png",
              "group": "database", "modes": ["cdc", "snapshot"], "discovery": "catalog",
              "sink": {"capable": true, "writeSemantics": ["upsert", "append"]},
              "pushOut": false,
              "config": [
                {"name": "host", "type": "string", "label": {"en_US": "Host"}, "required": true,
                 "default": null, "secret": false, "options": [],
                 "visibleWhen": {"controllingField": "deploymentMode", "equalsAnyOf": ["standalone"]}}
              ],
              "provenance": {"specPath": "spec.json", "specContentHash": "abc",
                "pdkApiVersion": "1.3.5", "requiredLevel": null, "modeSource": {"cdc": "derived", "snapshot": "derived"}}
            }
            """;

    private static String simpleRow(String id) {
        return """
                {
                  "id": "%s", "name": "%s", "displayName": "%s", "icon": null,
                  "group": "database", "modes": ["snapshot"], "discovery": "catalog",
                  "sink": {"capable": false, "writeSemantics": []}, "pushOut": false, "config": [],
                  "provenance": {"specPath": "spec.json", "specContentHash": "h",
                    "pdkApiVersion": "1.0.0", "requiredLevel": null, "modeSource": {"snapshot": "derived"}}
                }
                """.formatted(id, id, id);
    }

    @Test
    void upsertedRowReadsBackEqualThroughRealBson() {
        withStore((store, collection) -> {
            ConnectorCatalogEntry mysql = CatalogEntryReader.read(MYSQL_ROW);
            store.upsert(mysql);

            Optional<ConnectorCatalogEntry> read = store.get("mysql");
            assertThat(read).contains(mysql);
        });
    }

    @Test
    void getReturnsEmptyForAnUnregisteredConnector() {
        withStore((store, collection) -> assertThat(store.get("never-registered")).isEmpty());
    }

    @Test
    void listReturnsEveryUpsertedRow() {
        withStore((store, collection) -> {
            store.upsert(CatalogEntryReader.read(simpleRow("mysql")));
            store.upsert(CatalogEntryReader.read(simpleRow("postgres")));

            assertThat(store.list()).extracting(ConnectorCatalogEntry::id)
                    .containsExactlyInAnyOrder("mysql", "postgres");
        });
    }

    @Test
    void reRegisterReplacesTheStoredRowInPlace() {
        withStore((store, collection) -> {
            store.upsert(CatalogEntryReader.read(simpleRow("mysql")));
            ConnectorCatalogEntry rich = CatalogEntryReader.read(MYSQL_ROW);
            store.upsert(rich);

            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(store.get("mysql")).contains(rich);
        });
    }

    private interface StoreTest {
        void run(MongoConnectorCatalogStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh row store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("connector_catalog");
            collection.drop();
            test.run(new MongoConnectorCatalogStore(collection), collection);
        }
    }
}
