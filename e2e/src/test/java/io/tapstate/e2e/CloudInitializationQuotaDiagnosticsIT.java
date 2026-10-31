package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** Startup quota injection and pre-authentication HTTP diagnostics are outside the pipeline specification vocabulary. */
@RequiresDocker
class CloudInitializationQuotaDiagnosticsIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final String CLUSTER = "quota-diagnostic-cluster";
    private static final String TOKEN = "quota-test-service-token-sentinel";
    private static final String OPERATION = "a83f26ca-d2ce-4b92-9a0c-0ee331e1682f";
    private static final String PREFIX = "/v1/api/clusters/" + CLUSTER + "/configuration-initialization/";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void initializationReportsOnlyConfirmedQuotaWithoutReclaimingCreation(boolean quota) throws Exception {
        String database = "quota_config_" + UUID.randomUUID().toString().replace("-", "");
        String witness = "quota_witness_" + UUID.randomUUID().toString().replace("-", "");
        String uri = MONGO.getReplicaSetUrl(database);
        List<String> requests = new CopyOnWriteArrayList<>();
        HttpServer cloud = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        cloud.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            requests.add(exchange.getRequestMethod() + " " + path);
            boolean claim = path.equals(PREFIX + "claim"), complete = path.equals(PREFIX + "complete");
            if (!"POST".equals(exchange.getRequestMethod()) || !(claim || complete)
                    || !("Bearer " + TOKEN).equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                exchange.sendResponseHeaders(401, -1); exchange.close(); return;
            }
            Map<?, ?> body = (Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
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
                var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            var retained = new Document("_id", "retained").append("value", "unrelated-user-value-sentinel");
            mongo.getDatabase(witness).getCollection("witness").insertOne(retained);
            if (quota) mongo.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1)).append("data", new Document("failCommands", List.of("insert"))
                            .append("writeConcernError", new Document("code", 8000).append("codeName", "AtlasError")
                                    .append("errmsg", "you are over your space quota, using 513 MB of 512 MB. Writes are blocked on your cluster. Review capacity with the deployment administrator before continuing this original operation. Administrator guidance is available at https://www.mongodb.com/docs/atlas/reference/faq/storage/. Preserve existing databases and saved initialization authority."))));
            try (var server = RealProcessServer.startPrepared(uri, "127.0.0.1", port -> List.of(
                    "--tapstate.cloud.base-url=http://127.0.0.1:" + cloud.getAddress().getPort(),
                    "--tapstate.cloud.token=" + TOKEN, "--tapstate.cloud.cluster-id=" + CLUSTER,
                    "--tapstate.cloud.atlas-uri=" + uri, "--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2"))) {
                var entry = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/auth/storage-entry"))
                        .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(entry.statusCode()).isEqualTo(200);
                assertThat(Document.parse(entry.body())).containsEntry("entry", quota ? "UNAVAILABLE" : "SETUP_REQUIRED");
                if (quota) {
                    for (String code : List.of("not-consumed-a", "not-consumed-b")) {
                        var failure = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/auth/exchange?code=" + code))
                                .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
                        assertThat(failure.statusCode()).isEqualTo(503);
                        assertThat(Document.parse(failure.body())).containsEntry("code", "control.storage-quota-exceeded")
                                .containsEntry("params", new Document());
                        assertThat(failure.headers().firstValue("Set-Cookie")).isEmpty();
                        assertThat(failure.body()).doesNotContain(TOKEN, OPERATION, "unrelated-user-value-sentinel", "513 MB", uri);
                    }
                    assertThat(requests).containsExactly("POST " + PREFIX + "claim");
                } else assertThat(requests).containsExactly("POST " + PREFIX + "claim", "POST " + PREFIX + "complete");
                assertThat(mongo.getDatabase(witness).getCollection("witness").find().first()).isEqualTo(retained);
                assertThat(Files.readString(server.output())).doesNotContain(TOKEN, uri, "unrelated-user-value-sentinel");
            }
        } finally {
            cloud.stop(0);
            try (var mongo = MongoClients.create(uri)) {
                mongo.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                mongo.getDatabase(database).drop(); mongo.getDatabase(witness).drop();
            }
        }
    }
}
