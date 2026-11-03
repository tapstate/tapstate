package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** Fresh Cloud CONFIG startup is outside the declarative pipeline-step vocabulary. */
@RequiresDocker
class CloudConfigurationWriteConcernIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final String CLUSTER = "write-concern-cluster";
    private static final String ORGANIZATION = "write-concern-org";
    private static final String TOKEN = "write-concern-service-token-sentinel";
    private static final String OPERATION = "36ace56d-6eea-4071-8ed0-bd335ad1c6bd";

    @Test
    void aFreshCloudRuntimeWithExplicitWriteConcernReachesTheStorageSetupEntry() throws Exception {
        String database = "cloud_concern_" + UUID.randomUUID().toString().replace("-", "");
        String base = MONGO.getReplicaSetUrl(database);
        String uri = base + (base.contains("?") ? "&" : "?") + "retryWrites=true&w=majority";
        String prefix = "/v1/api/clusters/" + CLUSTER + "/configuration-initialization/";
        List<String> requests = new CopyOnWriteArrayList<>();
        HttpServer cloud = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        cloud.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            requests.add(exchange.getRequestMethod() + " " + path);
            if ("GET".equals(exchange.getRequestMethod())
                    && path.equals("/v1/api/clusters/" + CLUSTER + "/egress-address")
                    && ("Bearer " + TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                byte[] response = JsonWriter.write(Map.of("code", "ok", "data", Map.of(
                        "organizationId", ORGANIZATION, "clusterId", CLUSTER,
                        "egressAddressStatus", "DYNAMIC"))).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                try (var output = exchange.getResponseBody()) { output.write(response); }
                exchange.close(); return;
            }
            boolean claim = path.equals(prefix + "claim");
            boolean complete = path.equals(prefix + "complete");
            if (!"POST".equals(exchange.getRequestMethod()) || !(claim || complete)
                    || !("Bearer " + TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1); exchange.close(); return;
            }
            Map<?, ?> body = (Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            if (complete && !OPERATION.equals(body.get("operationId"))) {
                exchange.sendResponseHeaders(409, -1); exchange.close(); return;
            }
            byte[] response = JsonWriter.write(Map.of("code", "ok", "data", Map.of(
                    "clusterId", CLUSTER, "operationId", OPERATION, "phase", claim ? "CLAIMED" : "COMPLETE",
                    "configurationHash", body.get("configurationHash"), "creationGranted", claim))).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) { output.write(response); }
            exchange.close();
        });
        cloud.start();
        try (var mongo = MongoClients.create(uri);
                var server = RealProcessServer.startPrepared(uri, "127.0.0.1", port -> List.of(
                        "--tapstate.cloud.base-url=http://127.0.0.1:" + cloud.getAddress().getPort(),
                        "--tapstate.cloud.token=" + TOKEN, "--tapstate.cloud.cluster-id=" + CLUSTER,
                        "--tapstate.cloud.atlas-uri=" + uri, "--SDK_STATUS_SENDER_ENABLED=false",
                        "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2"));
                var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            var entry = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/auth/storage-entry"))
                    .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(entry.statusCode()).isEqualTo(200);
            assertThat(Document.parse(entry.body()))
                    .containsEntry("mode", "CLOUD").containsEntry("entry", "SETUP_REQUIRED");
            assertThat(requests).containsExactly("POST " + prefix + "claim",
                    "GET /v1/api/clusters/" + CLUSTER + "/egress-address", "POST " + prefix + "complete");
            var target = mongo.getDatabase(database);
            assertThat(SystemCollections.STORAGE_SETUP.on(target).find(new Document("_id", "cluster")).first())
                    .containsEntry("phase", "CONFIG_PREPARED").containsEntry("operationId", OPERATION);
            assertThat(SystemCollections.CLUSTER_KEYS.on(target).countDocuments()).isEqualTo(1);
            var rules = SystemCollections.ALERT_RULES.on(target).find().into(new java.util.ArrayList<>());
            assertThat(rules).hasSize(5).allSatisfy(row -> assertThat(row.get("_id", Document.class))
                    .containsEntry("authorityId", ORGANIZATION).containsEntry("clusterId", CLUSTER));
            assertThat(Files.readString(server.output())).doesNotContain(TOKEN, uri, "Cloud configuration initialization failed");
        } finally {
            cloud.stop(0);
        }
    }
}
