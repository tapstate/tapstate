package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
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
                    assertThatThrownBy(() -> {
                        var early = pending.get(1, TimeUnit.SECONDS);
                        throw new AssertionError("request completed before initialization: " + early.statusCode() + " " + early.body());
                    }).isInstanceOf(TimeoutException.class);
                } finally {
                    admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
                var response = pending.get(10, TimeUnit.SECONDS);
                assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
                var catalog = (Map<?, ?>) JsonReader.parse(response.body());
                assertThat(catalog.get("clusterId")).isEqualTo(storage.clusterId());
                assertThat(catalog.get("deployment")).isEqualTo("ON_PREM");
                assertThat((List<?>) catalog.get("types")).isNotEmpty();
                assertThat(control.alert("center", Map.of("limit", 1))).containsKey("rows");
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        }
    }
}
