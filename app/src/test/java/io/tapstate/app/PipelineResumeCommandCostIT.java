package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorePort;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;

import static io.tapstate.app.PipelineLaunchLatencyIT.request;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RequiresDocker
class PipelineResumeCommandCostIT {
    @Test void measuresAcceptedLifecycleRequestsAndJobReadiness(@TempDir Path work) throws Exception {
        try (var config = PipelineLaunchLatencyIT.mongo(); var cache = PipelineLaunchLatencyIT.mongo()) {
            config.start();
            cache.start();
            measure(config.getReplicaSetUrl("resume_config"), cache.getReplicaSetUrl("resume_cache"),
                    cache.getReplicaSetUrl("resume_master"), work);
        }
    }

    static void measure(String configUri, String cacheUri, String masterUri, Path work) throws Exception {
        var storage = NativeAppStorage.prepare(configUri, cacheUri, masterUri, StorageBinding.Mode.CLOUD,
                NativeCloudAlertManagementIT.CLUSTER);
        var application = new SpringApplicationBuilder(Bootstrap.class, NativeCloudAlertManagementIT.ControlledSdk.class,
                PipelineLaunchLatencyIT.Measurements.class).environment(CloudFixtureEnvironment.isolated()).run(
                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=WARN",
                "--logging.level.io.tapstate.diagnostics.pipeline=DEBUG",
                "--tapstate.store.mongo.uri=" + configUri, "--tapstate.cloud.atlas-uri=" + configUri,
                "--tapstate.cloud.base-url=" + NativeCloudAlertManagementIT.CLOUD,
                "--tapstate.cloud.token=controlled-cloud-machine-token", "--tapstate.cloud.cluster-id=" + storage.clusterId(),
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"));
        try (var fixture = new PipelineLaunchLatencyIT.LaunchFixture(application); var http = HttpClient.newHttpClient();
             var data = MongoClients.create(masterUri)) {
            var business = fixture.awaitBusiness();
            var port = business.getBean(StorePort.class);
            var fields = List.of(new SourceField("_id", "int", TapstateType.INT64),
                    new SourceField("parent_id", "int", TapstateType.INT64));
            var tables = List.of("orders", "customers", "lines");
            var database = data.getDatabase(storage.masterDatabase());
            for (var table : tables) database.getCollection(table).insertOne(new Document("_id", 1L).append("parent_id", 1L));
            port.artifacts().save(new SourceResource("launch-input", null, "mongodb", Map.of("isUri", true, "uri", masterUri),
                    SourceMode.CDC, tables.stream().<TableRef>map(TableRef::literal).toList(), null, null));
            port.schemas().save(new DiscoveredSourceModel("launch-input", "mongodb", System.currentTimeMillis(), new SourceModel(
                    tables.stream().map(t -> new SourceTable(t, fields, List.of("_id"), List.of())).toList())));
            String origin = "http://localhost:" + ((WebServerApplicationContext) application).getWebServer().getPort();
            var login = http.send(HttpRequest.newBuilder(URI.create(origin + "/auth/exchange?code=browser-code")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(login.statusCode()).isEqualTo(302);
            String cookie = login.headers().firstValue("set-cookie").orElseThrow().split(";", 2)[0];
            var engine = business.getBean(Engine.class);
            for (int i = 0; i < 5; i++) {
                String id = "resume-" + i;
                String path = "/api/pipelines/" + id;
                request(http, origin, cookie, "POST", path + "/draft", PipelineLaunchLatencyIT.draft(id), 201);
                request(http, origin, cookie, "POST", path + "/draft:preview", Map.of("revision", 1), 200);
                request(http, origin, cookie, "POST", path + "/draft:publish", Map.of("revision", 1), 200);
                request(http, origin, cookie, "POST", path + ":start", Map.of(), 200);
                var target = database.getCollection("order_views_" + id.replace('-', '_'));
                await().atMost(Duration.ofSeconds(90)).until(() -> target.countDocuments() > 0);
                request(http, origin, cookie, "GET", path + "/status", null, 200);
                request(http, origin, cookie, "POST", path + ":pause", Map.of(), 200);
                await().atMost(Duration.ofSeconds(60)).until(() -> engine.captureRun(id)
                        .filter(run -> run.status() == com.hazelcast.jet.core.JobStatus.SUSPENDED).isPresent());
                long previous = engine.captureRun(id).orElseThrow().jobId();
                long start = System.nanoTime();
                request(http, origin, cookie, "POST", path + ":resume", Map.of(), 200);
                await().atMost(Duration.ofSeconds(60)).until(() -> engine.captureRun(id)
                        .filter(run -> run.status() == com.hazelcast.jet.core.JobStatus.RUNNING).isPresent());
                long job = engine.captureRun(id).orElseThrow().jobId();
                System.out.printf("RESUME_READY %s replaced=%s elapsedMs=%d%n", id, job != previous,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                request(http, origin, cookie, "POST", path + ":stop", Map.of("purgeState", false), 200);
                await().atMost(Duration.ofSeconds(60)).until(() -> !engine.hasLiveJob(id));
            }
        }
    }
}
