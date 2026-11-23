package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenService;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.ConnectorArtifactMetadata.Origin;
import io.tapstate.spi.store.StorePort;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RequiresDocker
class NativeConnectorHandoffIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25");

    @TempDir
    Path work;

    @Test
    @Timeout(180)
    void retainedRestartMovesOnlyTheStoppedReaderAndDeliversItsMissedChange() throws Exception {
        var storage = NativeAppStorage.createOnPremWithAdmin(MONGO, "handoff", "admin", "fixture-handoff-password");
        String fixture = System.getProperty("tapstate.pdk.it.mongodb-jar");
        Path original = fixture == null ? CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar")
                : Path.of(fixture);
        Path replacement = withNewVersion(original);
        try (var mongo = MongoClients.create(storage.configurationUri());
                var app = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                        "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                        "--tapstate.store.mongo.uri=" + storage.configurationUri(), "--tapstate.hz.member-port=0",
                        "--tapstate.hz.jet.cooperative-thread-count=2", "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"),
                        "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                        "--tapstate.connectors.drop-dir=" + work.resolve("no-drops"),
                        "--tapstate.control.auth.jwt-secret=handoff-fixture-signing-key")) {
            NativeAppStorage.awaitBusiness(app);
            var child = NativeAppStorage.business(app);
            var connector = child.getBean(LocalConnectorRuntime.class).artifacts();
            String oldHash;
            try (var bytes = Files.newInputStream(original)) {
                oldHash = connector.install(bytes, Origin.UPLOAD, "mongodb", null).registration().contentHash();
            }
            String sourceDb = storage.masterDatabase() + "_source";
            String secondTargetDb = storage.masterDatabase() + "_other";
            var source = mongo.getDatabase(sourceDb).getCollection("orders");
            var first = mongo.getDatabase(storage.masterDatabase()).getCollection("orders");
            var second = mongo.getDatabase(secondTargetDb).getCollection("orders");
            var cache = mongo.getDatabase(storage.cacheDatabase());
            var config = mongo.getDatabase(storage.configurationDatabase());
            source.insertOne(row(1, "snapshot"));
            defineReaders(child.getBean(StorePort.class), sourceDb, secondTargetDb);

            var token = child.getBean(TokenService.class).create(Scope.WRITE);
            var http = RestClient.builder().baseUrl("http://127.0.0.1:"
                    + ((WebServerApplicationContext) app).getWebServer().getPort())
                    .defaultHeader("Authorization", "Bearer " + token).build();

            command(http, "reader_a", "start", Map.of());
            assertDelivered(first, 1, "snapshot");
            command(http, "reader_b", "start", Map.of());
            assertDelivered(second, 1, "snapshot");
            String oldChain = chain(cache, "reader_b");
            assertThat(chain(cache, "reader_a")).isEqualTo(oldChain);
            long unchangedExecution = execution(cache, "reader_b");

            String newHash;
            try (var bytes = Files.newInputStream(replacement)) {
                newHash = connector.install(bytes, Origin.UPLOAD, "mongodb", null).registration().contentHash();
                assertThat(newHash).isNotEqualTo(oldHash);
                assertThat(connector.resolve("mongodb").contentHash()).isEqualTo(newHash);
            }
            source.insertOne(row(2, "both-original-readers"));
            assertDelivered(first, 2, "both-original-readers");
            assertDelivered(second, 2, "both-original-readers");
            assertThat(chain(cache, "reader_a")).isEqualTo(oldChain);

            command(http, "reader_a", "stop", Map.of("purgeState", false));
            await().atMost(Duration.ofSeconds(45)).untilAsserted(() ->
                    assertThat(http.get().uri("/api/pipelines/reader_a/status").retrieve().body(Map.class))
                            .containsEntry("state", "STOPPED"));
            first.insertOne(row(99, "existing-target-state"));
            source.insertOne(row(3, "while-a-stopped"));
            assertDelivered(second, 3, "while-a-stopped");
            assertThat(first.find(new Document("_id", 3L)).first()).isNull();

            command(http, "reader_a", "start", Map.of());
            assertDelivered(first, 3, "while-a-stopped");
            assertThat(first.find(new Document("_id", 99L)).first()).containsEntry("payload", "existing-target-state");
            await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                assertThat(chain(cache, "reader_a")).isNotEqualTo(oldChain);
                assertThat(effectiveHashes(config, "reader_a")).containsOnly(newHash);
            });
            assertThat(chain(cache, "reader_b")).isEqualTo(oldChain);
            assertThat(effectiveHashes(config, "reader_b")).containsOnly(oldHash);
            assertThat(execution(cache, "reader_b")).isEqualTo(unchangedExecution);
        }
    }

    private static void defineReaders(StorePort store, String sourceDb, String targetDb) {
        var parser = new DslParser();
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: input
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(MONGO.getReplicaSetUrl(sourceDb))));
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: target_b
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                """.formatted(MONGO.getReplicaSetUrl(targetDb))));
        var fields = List.of(new SourceField("_id", "long", TapstateType.INT64),
                new SourceField("payload", "string", TapstateType.STRING));
        store.schemas().save(new DiscoveredSourceModel("input", "mongodb", System.currentTimeMillis(),
                new SourceModel(List.of(new SourceTable("orders", fields, List.of("_id"), List.of())))));
        for (var entry : Map.of("reader_a", "views", "reader_b", "target_b").entrySet()) {
            store.artifacts().save(parser.parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: [ { id: input, srs: true } ]
                    settings: { read_mode: snapshot_and_cdc }
                    serve:
                      from: orders
                      sync:
                        - source: %s
                          on_full_load: clear
                    """.formatted(entry.getKey(), entry.getValue())));
        }
    }

    private static void command(RestClient http, String pipeline, String command, Map<String, ?> body) {
        http.post().uri("/api/pipelines/{pipeline}:{command}", pipeline, command)
                .contentType(MediaType.APPLICATION_JSON).body(body).retrieve().toBodilessEntity();
    }

    private static String chain(MongoDatabase cache, String reader) {
        var members = cache.getCollection("srs_consumers")
                .find(new Document("pipelineId", reader).append("sourceId", "input").append("detached", false))
                .into(new java.util.ArrayList<>());
        assertThat(members).hasSize(1);
        return members.getFirst().get("_id", Document.class).getString("chain");
    }

    private static List<String> effectiveHashes(MongoDatabase config, String reader) {
        var generation = config.getCollection("operator_generations").find(new Document("_id", reader)).first();
        assertThat(generation).isNotNull();
        var bindings = generation.get("connectorBindings", Document.class);
        assertThat(bindings).isNotNull();
        var effective = bindings.getList("effective", Document.class);
        assertThat(effective).hasSize(2);
        return effective.stream().map(node -> node.getString("sha256")).toList();
    }

    private static long execution(MongoDatabase cache, String reader) {
        var workload = cache.getCollection("workload_claims")
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", reader)).first();
        assertThat(workload).isNotNull();
        return workload.getLong("executionGeneration");
    }

    private static Document row(long id, String payload) {
        return new Document("_id", id).append("payload", payload);
    }

    private static void assertDelivered(MongoCollection<Document> target, long id, String payload) {
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() ->
                assertThat(target.find(new Document("_id", id)).first()).containsEntry("payload", payload));
    }

    // Alter only the test artifact identity. Actual inter-release compatibility is tested with released JARs.
    private Path withNewVersion(Path base) throws IOException {
        Path output = work.resolve("replacement.jar");
        try (var original = new JarFile(base.toFile())) {
            var manifest = new Manifest(original.getManifest());
            manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "9999.0");
            try (var next = new JarOutputStream(Files.newOutputStream(output), manifest)) {
                for (var entry : java.util.Collections.list(original.entries())) {
                    if (entry.getName().equals(JarFile.MANIFEST_NAME) || entry.isDirectory()) continue;
                    next.putNextEntry(new JarEntry(entry.getName()));
                    try (var bytes = original.getInputStream(entry)) {
                        bytes.transferTo(next);
                    }
                    next.closeEntry();
                }
            }
        }
        return output;
    }
}
