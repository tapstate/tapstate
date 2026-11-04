package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** The specification vocabulary cannot inject a failure into an individual CONFIG command shape. */
@RequiresDocker
class AStatusReadReusesItsSetupSnapshotIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withLabel("io.tapstate.issue-loop.owner", System.getenv().getOrDefault("TAPSTATE_TEST_RESOURCE_OWNER",
                    "8fe5ac442f7842d8a460f745fa89629f"))
            .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"))
            .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024)
                    .withNanoCPUs(1_000_000_000L))
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25",
                    "--setParameter", "enableTestCommands=1");

    @Test
    void aNativeHttpStatusReusesSetupFactsAndStillRefusesTheirCorruption() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.createOn(MONGO);
             var raw = MongoClients.create(storage.configurationUri());
             ServerHandle server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                     port -> List.of("--tapstate.cluster.id=" + storage.clusterId(), "--tapstate.hz.member-port=0",
                             "--tapstate.hz.jet.cooperative-thread-count=2"));
             var http = HttpClient.newHttpClient()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.awaitInitialAlertMaintenance();
            var setup = SystemCollections.STORAGE_SETUP.on(raw.getDatabase(storage.configurationDatabase()));
            var original = setup.find(new Document("_id", "cluster")).first();
            assertThat(original).isNotNull();
            var admin = raw.getDatabase("admin");
            var fault = new Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                    .append("data", new Document("failCommands", List.of("find"))
                            .append("namespace", storage.configurationDatabase() + "." + SystemCollections.STORAGE_SETUP.collectionName())
                            .append("errorCode", 2));
            admin.runCommand(fault);
            Map<?, ?> status;
            try {
                // The raw read confirms the exact namespace and non-retryable fault. Native
                // alert workers may also reach it, so global hit counts cannot identify this request.
                assertThatThrownBy(() -> setup.find(new Document("_id", "cluster")).first())
                        .isInstanceOfSatisfying(MongoCommandException.class,
                                failure -> assertThat(failure.getErrorCode()).isEqualTo(2));
                status = control.storageStatus();
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
            assertThat(status.get("clusterId")).isEqualTo(storage.clusterId());
            assertThat(status.get("businessPhase")).isEqualTo("READY");
            System.out.println("NATIVE_STATUS_SETUP_SNAPSHOT directFindFault=2 businessPhase=READY");

            setup.updateOne(new Document("_id", "cluster"), new Document("$set", new Document("generation", "invalid")));
            try {
                var response = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/api/storage/status"))
                        .timeout(Duration.ofSeconds(20)).header("Authorization", "Bearer " + control.credential())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(503);
                assertThat(((Map<?, ?>) JsonReader.parse(response.body())).get("code"))
                        .isEqualTo(ControlError.STORAGE_UNAVAILABLE.code());
                assertThat(setup.find(new Document("_id", "cluster")).first().get("generation")).isEqualTo("invalid");
            } finally {
                setup.replaceOne(new Document("_id", "cluster"), original);
            }
        }
    }
}
