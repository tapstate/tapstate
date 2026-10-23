package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.model.SourceResource;
import io.tapstate.testsupport.RequiresDocker;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.*;

/** A real import commits, its HTTP reply is lost, and another JVM answers the original operation. */
@RequiresDocker
class ACommittedImportSurvivesItsLostResponseIT {
    private static final String SOURCE = "portable_source", PIPELINE = "portable_flow";

    @Test void restartAndReplayReturnTheOriginalReceiptWithoutRewritingLaterEdits(@TempDir Path temporary) throws Exception {
        Path input = Files.createDirectory(temporary.resolve("input"));
        FileEndpoints.replaceTable(input.resolve("orders.csv"), "id,name\n1,first\n");
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(Files.createDirectory(temporary.resolve("connector"))));
        String key = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
        try (var from = PreparedNativeStore.create("portable-admin", "portable-password");
                var to = PreparedNativeStore.create("destination-admin", "destination-password");
                var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
                var raw = MongoClients.create(to.configurationUri())) {
            Map<?, ?> exported;
            try (var source = RealProcessServer.startPrepared(from.configurationUri(), "127.0.0.1", port -> List.of())) {
                var control = new ControlPlane(source.baseUrl()); from.loginAndAwaitBusiness(control);
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                control.apply(Map.of("source.tap.yml", sourceYaml(input, false)));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", input.toString()));
                post(http, source.baseUrl().resolve("/api/pipelines"), control.credential(), Map.of("id", PIPELINE,
                        "sources", List.of(Map.of("id", SOURCE, "srs", false)), "settings", Map.of("readMode", "snapshot_only"),
                        "serve", Map.of("id", "output", "from", "orders", "sync", List.of(Map.of("id", "copy", "source", "views")))), 201);
                exported = post(http, source.baseUrl().resolve("/api/configuration/export"), control.credential(),
                        Map.of("resourceIds", List.of(PIPELINE), "usernames", List.of(from.username()), "transferKey", key));
            }
            var configuration = raw.getDatabase(to.configurationDatabase());
            var cache = raw.getDatabase(to.cacheDatabase());
            String id; Map<String, Object> confirmation; Map<?, ?> receipt; Document oldNode; long afterEdit; long oldPid;
            try (var target = RealProcessServer.startPrepared(to.configurationUri(), "127.0.0.1",
                    port -> List.of())) {
                var control = new ControlPlane(target.baseUrl()); to.loginAndAwaitBusiness(control);
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                var preflight = post(http, target.baseUrl().resolve("/api/configuration/import/preflight"), control.credential(),
                        Map.of("transfer", exported.get("transfer"), "transferKey", key, "defaultSourceMapping", Map.of("views", "views")));
                id = String.valueOf(((Map<?, ?>)((Map<?, ?>)preflight.get("operation")).get("ticket")).get("id"));
                confirmation = Map.of("importId", id, "transfer", preflight.get("preparedTransfer"),
                        "transferKey", key, "defaultSourceMapping", Map.of("views", "views"));
                long before = version(configuration);
                assertThat(SystemCollections.CONFIG_USERS.on(configuration).find(new Document("_id", from.username())).first()).isNull();
                try (var relay = new LostReply(target.baseUrl().resolve("/api/configuration/import/commit"), http)) {
                    var caller = http.sendAsync(request(relay.uri(), control.credential(), confirmation), HttpResponse.BodyHandlers.ofString());
                    var reply = relay.held.get(30, TimeUnit.SECONDS);
                    assertThat(reply.statusCode()).withFailMessage(reply.body()).isEqualTo(200);
                    receipt = (Map<?, ?>) JsonReader.parse(reply.body());
                    var original = SystemCollections.CONFIGURATION_IMPORTS.on(configuration).find(new Document("_id", id)).first();
                    assertThat(original).isNotNull().containsEntry("phase", "COMMITTED");
                    assertThat(version(configuration)).isEqualTo(before + 1);
                    try (var domain = MongoConfigurationDomain.openExisting(settings(to))) {
                        assertThat(domain.artifacts().get(SOURCE)).isPresent();
                        assertThat(domain.artifacts().get(PIPELINE)).isPresent();
                        assertThat(domain.users().find(from.username())).isPresent();
                        assertThat(domain.intents().load(PIPELINE)).isEmpty();
                    }
                    control.apply(Map.of("source.tap.yml", sourceYaml(input, true)));
                    afterEdit = version(configuration);
                    assertThat(afterEdit).isEqualTo(before + 2);
                    assertThat(caller.isDone()).as("the committed response has sent no bytes to its caller").isFalse();
                    // Native standalone identity uses the admitted Cluster id as its stable node id.
                    oldNode = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(
                            new Document("resourceType", "NODE_SESSION").append("ownerNodeId", to.clusterId())).first();
                    assertThat(oldNode).isNotNull().containsEntry("clusterId", to.clusterId()).containsEntry("resourceId", to.clusterId());
                    oldPid = target.pid();
                    target.kill();
                    assertThat(target.isAlive()).isFalse();
                    relay.drop.countDown();
                    assertThatThrownBy(() -> caller.get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(IOException.class);
                    assertThat(relay.calls.get()).isEqualTo(1);
                    assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(configuration).find(new Document("_id", id)).first()).isEqualTo(original);
                }
            }
            Await.until("the original boot lease to expire without deleting or stealing it", Duration.ofSeconds(60), () -> {
                var retained = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(new Document("_id", oldNode.get("_id"))).first();
                assertThat(retained).isNotNull().containsEntry("ownerBootId", oldNode.getString("ownerBootId"));
                var serverNow = raw.getDatabase("admin").runCommand(new Document("hello", 1)).getDate("localTime");
                return !retained.getDate("leaseUntil").after(serverNow);
            }, () -> "the original boot still owns its retained lease");
            try (var restarted = RealProcessServer.startPrepared(to.configurationUri(), "127.0.0.1",
                    port -> List.of())) {
                var control = new ControlPlane(restarted.baseUrl()); to.loginAndAwaitBusiness(control);
                assertThat(restarted.pid()).isNotEqualTo(oldPid);
                var successor = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(new Document("_id", oldNode.get("_id"))).first();
                assertThat(successor).isNotNull().containsEntry("ownerNodeId", oldNode.getString("ownerNodeId"));
                assertThat(successor.getString("ownerBootId")).isNotEqualTo(oldNode.getString("ownerBootId"));
                assertThat(successor.getLong("claimGeneration")).isGreaterThan(oldNode.getLong("claimGeneration"));
                var status = post(http, restarted.baseUrl().resolve("/api/configuration/import/status"), control.credential(), Map.of("importId", id));
                assertThat(status.get("phase")).isEqualTo("COMMITTED");
                assertThat(status.get("receipt")).isEqualTo(receipt);
                assertThat(post(http, restarted.baseUrl().resolve("/api/configuration/import/commit"), control.credential(), confirmation)).isEqualTo(receipt);
                assertThat(version(configuration)).isEqualTo(afterEdit);
                assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(configuration).countDocuments()).isEqualTo(1);
                try (var domain = MongoConfigurationDomain.openExisting(settings(to))) {
                    assertThat(((SourceResource)domain.artifacts().get(SOURCE).orElseThrow()).metadata().description()).isEqualTo("edited-after-import");
                    assertThat(domain.intents().load(PIPELINE)).isEmpty();
                }
                var imported = new ControlPlane(restarted.baseUrl()); imported.login(from.username(), from.password());
                assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(cache).countDocuments()).isZero();
            }
        }
    }

    private static long version(com.mongodb.client.MongoDatabase db) {
        return SystemCollections.CONFIGURATION_VERSION.on(db).find().first().getLong("revision");
    }
    private static MongoConnectionSettings settings(PreparedNativeStore storage) {
        return new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5));
    }
    private static HttpRequest request(URI uri, String credential, Map<String, ?> body) {
        return HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(60)).header("Authorization", "Bearer " + credential)
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build();
    }
    private static Map<?, ?> post(HttpClient http, URI uri, String credential, Map<String, ?> body) throws Exception {
        return post(http, uri, credential, body, 200);
    }
    private static Map<?, ?> post(HttpClient http, URI uri, String credential, Map<String, ?> body, int expectedStatus) throws Exception {
        var response = http.send(request(uri, credential, body), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("HTTP reply: %s", response.body()).isEqualTo(expectedStatus);
        return (Map<?, ?>) JsonReader.parse(response.body());
    }
    private static String sourceYaml(Path input, boolean edited) {
        return """
                version: tapstate/v1
                kind: source
                id: portable_source
                connector: e2e_file
                config: {uri: %s}
                mode: snapshot
                tables: [orders]
                %s
                """.formatted(JsonWriter.write(input.toString()), edited ? "metadata: {description: edited-after-import}" : "");
    }
    private static final class LostReply implements AutoCloseable {
        final CompletableFuture<HttpResponse<String>> held = new CompletableFuture<>();
        final CountDownLatch drop = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        private final HttpServer server;
        private final java.util.concurrent.ExecutorService executor;
        LostReply(URI target, HttpClient upstream) throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            executor = Executors.newVirtualThreadPerTaskExecutor();
            try {
                server.setExecutor(executor);
                server.createContext("/commit", exchange -> {
                    try (exchange) {
                        calls.incrementAndGet();
                        var request = HttpRequest.newBuilder(target).timeout(Duration.ofSeconds(30))
                                .header("Authorization", exchange.getRequestHeaders().getFirst("Authorization"))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes())).build();
                        var response = upstream.send(request, HttpResponse.BodyHandlers.ofString());
                        held.complete(response);
                        if (!drop.await(60, TimeUnit.SECONDS)) throw new IOException("the test did not release its held response");
                        // Closing without headers deliberately makes the original caller lose the response.
                    } catch (Exception failure) {
                        held.completeExceptionally(failure);
                        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                    }
                });
                server.start();
            } catch (RuntimeException | Error failure) {
                server.stop(0); executor.close(); throw failure;
            }
        }
        URI uri() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/commit"); }
        @Override public void close() {
            drop.countDown(); server.stop(0);
            executor.close();
        }
    }
}
