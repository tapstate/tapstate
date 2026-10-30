package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ControlError;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.bson.Document;

/** A fixture-owned legacy store is evidence to preserve, never an installation permission. */
final class LegacyStoreRefusal implements AutoCloseable {
    private final MongoClient client;
    private final MongoDatabase database;
    private final String uri;
    private final Map<String, CollectionImage> original;
    private record CollectionImage(List<Document> rows, List<Document> indexes) {}

    LegacyStoreRefusal(int resources, boolean interrupted) {
        String name = "e2e_legacy_" + UUID.randomUUID().toString().replace("-", "");
        uri = SharedMongo.replicaSetUrl(name);
        client = MongoClients.create(uri);
        database = client.getDatabase(name);
        try {
            var rows = new ArrayList<Document>();
            for (int i = 0; i < resources; i++) {
                String id = "legacy_source_" + i;
                rows.add(new Document("_id", id).append("kind", "source").append("canonical", """
                        version: tapstate/v1
                        kind: source
                        id: %s
                        connector: mongodb
                        config: { uri: "mongodb://127.0.0.1:27017/legacy" }
                        """.formatted(id)));
            }
            SystemCollections.ARTIFACTS.on(database).insertMany(rows);
            SystemCollections.ARTIFACTS.on(database).createIndex(new Document("kind", 1),
                    new com.mongodb.client.model.IndexOptions().name("legacy_kind"));
            var schema = new Document("_id", "schema").append("installedVersion", interrupted ? 1 : 0);
            if (interrupted) schema.append("lock", new Document("owner", "departed-legacy-process")
                    .append("epoch", 7L).append("since", java.util.Date.from(java.time.Instant.EPOCH))
                    .append("heartbeat", java.util.Date.from(java.time.Instant.EPOCH)));
            SystemCollections.SYSTEM_META.on(database).insertOne(schema);
            original = snapshot();
        } catch (RuntimeException | Error failure) {
            try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    RealProcessServer launch() {
        return RealProcessServer.launchingPrepared(uri, "127.0.0.1",
                port -> List.of("--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2"));
    }

    void requireRefused(RealProcessServer server) throws Exception {
        server.awaitReady();
        try (HttpClient http = HttpClient.newHttpClient()) {
            var entry = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/auth/storage-entry"))
                    .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(entry.statusCode()).isEqualTo(200);
            assertThat(Document.parse(entry.body())).containsEntry("entry", "UNAVAILABLE");
            for (String path : List.of("/auth/login", "/auth/bootstrap")) {
                var response = http.send(HttpRequest.newBuilder(server.baseUrl().resolve(path))
                        .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("""
                                {"username":"legacy-admin","password":"legacy-test-password"}
                                """)).build(), HttpResponse.BodyHandlers.ofString());
                boolean bootstrap = path.equals("/auth/bootstrap");
                assertThat(response.statusCode()).as(path).isEqualTo(bootstrap ? 403 : 503);
                assertThat(ControlPlane.interpretClusterRefusal(response.statusCode(), response.body()))
                        .contains((bootstrap ? ControlError.BOOTSTRAP_FORBIDDEN : ControlError.STORAGE_UNAVAILABLE).code());
            }
            var business = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/api/artifacts"))
                    .timeout(Duration.ofSeconds(5)).header("Authorization", "Bearer legacy-test-token")
                    .GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(business.statusCode()).isEqualTo(503);
            assertThat(ControlPlane.interpretClusterRefusal(business.statusCode(), business.body()))
                    .contains(ControlError.STORAGE_UNAVAILABLE.code());
        }
        requireUnchanged();
    }

    void requireUnchanged() {
        assertThat(snapshot()).as("all original collections, rows and indexes remain unchanged; no native tables appear")
                .isEqualTo(original);
    }

    private Map<String, CollectionImage> snapshot() {
        var image = new TreeMap<String, CollectionImage>();
        for (String name : database.listCollectionNames()) {
            var collection = database.getCollection(name);
            image.put(name, new CollectionImage(collection.find().sort(new Document("_id", 1)).into(new ArrayList<>()),
                    collection.listIndexes().into(new ArrayList<>())));
        }
        return image;
    }

    @Override public void close() {
        try { database.drop(); } finally { client.close(); }
    }
}
