package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot hold the initial alert-rule read across an HTTP request. */
@RequiresDocker
class AlertManagementWaitsForStartupIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withLabel("io.tapstate.issue-loop.owner",
                    System.getenv().getOrDefault("TAPSTATE_TEST_RESOURCE_OWNER", "e2e-alert-startup"))
            .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"))
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");

    @Test
    void theClusterCatalogOmitsTheRetiredNodeAvailabilityType() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.createOn(MONGO);
                var server = InProcessServer.startPrepared(storage)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var types = (List<?>) control.alert("catalog", Map.of()).get("types");
            assertThat(types.stream().map(value -> (String) ((Map<?, ?>) value).get("id")))
                    .containsExactlyInAnyOrder("pipeline.failed", "pipeline.observation-stale",
                            "source.connection-failed", "cluster.storage-unavailable",
                            "cluster.config-structure-drift", "cluster.cache-structure-drift");
        }
    }

    @Test
    void theFirstAuthenticatedAlertRequestWaitsForTheInitialRuleSnapshot() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.createOn(MONGO);
                var raw = MongoClients.create(storage.configurationUri());
                var http = HttpClient.newHttpClient()) {
            var admin = raw.getDatabase("admin");
            long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn")
                    .append("data", new Document("failCommands", List.of("aggregate"))
                            .append("namespace", storage.configurationDatabase() + "."
                                    + SystemCollections.ALERT_RULES.collectionName())
                            .append("errorCode", 2))).get("count")).longValue();
            try (var server = InProcessServer.startPrepared(storage)) {
                var control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                admin.runCommand(new Document("waitForFailPoint", "failCommand")
                        .append("timesEntered", entered + 1).append("maxTimeMS", 15000));

                // One request crosses initialization; retrying HTTP could hide the original 503.
                var pending = http.sendAsync(HttpRequest.newBuilder(server.baseUrl().resolve("/api/alerts/catalog"))
                        .timeout(Duration.ofSeconds(15))
                        .header("Authorization", "Bearer " + control.credential())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString());
                try {
                    Await.until("the catalog handler to wait for the initial alert rules", Duration.ofSeconds(10), () -> {
                        boolean waiting = catalogHandlerIsWaiting();
                        if (pending.isDone()) {
                            var early = pending.join();
                            throw new AssertionError("catalog completed before the readiness wait was observed: "
                                    + early.statusCode() + " " + early.body());
                        }
                        return waiting;
                    }, () -> "no catalog handler observed in the readiness wait");
                } finally {
                    admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
                var response = pending.get(10, TimeUnit.SECONDS);
                assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
                var catalog = new HashMap<Object, Object>((Map<?, ?>) JsonReader.parse(response.body()));
                assertThat(catalog).containsEntry("clusterId", storage.clusterId());
                assertThat(catalog).containsEntry("deployment", "ON_PREM");
                assertThat((List<?>) catalog.get("types")).isNotEmpty();
                assertThat(control.alert("center", Map.of("limit", 1))).containsKey("rows");
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        }
    }

    private static boolean catalogHandlerIsWaiting() {
        // The same server stack must show the catalog handler inside the alert runtime's latch wait.
        for (var stack : Thread.getAllStackTraces().values()) {
            if (Arrays.stream(stack).noneMatch(frame ->
                    isFrame(frame, "io.tapstate.control.restapi.AlertController", "catalog"))) continue;
            for (int i = 1; i < stack.length; i++) {
                if (isFrame(stack[i - 1], CountDownLatch.class.getName(), "await")
                        && isFrame(stack[i], "io.tapstate.app.PipelineAlertRuntime", "managementServices")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isFrame(StackTraceElement frame, String className, String methodName) {
        return frame.getClassName().equals(className) && frame.getMethodName().equals(methodName);
    }
}
