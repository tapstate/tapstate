package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.app.BCryptPasswordHasher;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.User;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** Real Mongo PDK snapshot and downtime CDC through separate native stores and a packaged JVM replacement. */
@RequiresDocker
class ThreeLayerRestartContinuationIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    @Container private static final MongoDBContainer DATA = mongo();
    private static final String TABLE = "orders", SOURCE = "input", PIPELINE = "native_resume";
    private static final Duration BOUND = Duration.ofSeconds(90);
    @TempDir Path work;

    @BeforeAll static void requireRealConnector() { RealConnectorGate.require("mongodb"); }

    @Test @Timeout(300)
    void aNewJvmCarriesDowntimeChangesFromTheOriginalPositionWithoutReadingTheSnapshotAgain() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "restart-" + suffix;
        String configUri = CONFIG.getReplicaSetUrl("config_" + suffix);
        String cacheDb = "cache_" + suffix;
        String sourceUri = DATA.getReplicaSetUrl("source_" + suffix);
        String targetUri = DATA.getReplicaSetUrl("target_" + suffix);
        var scope = new StorageSetupScope(cluster, "installation", "setup", 1);
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb), cacheDb, null);
        try (var configuration = MongoConfigurationBootstrap.prepare(
                new MongoConnectionSettings(configUri, null, Duration.ofSeconds(5)), cluster, "installation", "setup", 1)) {
            var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", targetUri),
                    null, null, null, null);
            var committed = configuration.bindings().commitWithFirstCache(scope, StorageBinding.Mode.OP, binding, target);
            try (var cache = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "fixture")) {
                cache.prepareFirstAuth(committed.firstCreation().orElseThrow());
                cache.prepareInitialBusiness(committed.preparation(), () -> {});
            }
            configuration.bindings().complete(scope);
            configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                    new User("admin", new BCryptPasswordHasher().hash("native-restart-password"), "admin"));
        }
        EndpointAddress source = EndpointAddress.uri(sourceUri), target = EndpointAddress.uri(targetUri);
        try (var mongo = new MongoEndpoints()) {
            mongo.seed(source, TABLE, SeedRows.generated(8));
            String acknowledged;
            Map<Long, Long> beforeShutdown;
            var first = start(configUri, cluster);
            try (first) {
                var control = login(first);
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                var defaultTarget = control.artifact("views").orElseThrow();
                var workspace = resources(sourceUri);
                workspace.put("views.tap.yml", defaultTarget.canonicalForm());
                control.apply(workspace);
                assertThat(control.artifact("views").orElseThrow().contentHash()).isEqualTo(defaultTarget.contentHash());
                control.discoverSchema(SOURCE, "mongodb", Map.of("isUri", true, "uri", sourceUri));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitRows(mongo, source, target, "the native snapshot to reach the default target");
                String snapshotAck = Await.answered("the native snapshot target confirmation", BOUND,
                        () -> control.durablePosition(PIPELINE, TABLE));
                mongo.update(source, TABLE, Map.of("id", 1L), Map.of("seq", 10L));
                awaitRows(mongo, source, target, "a live update before replacement");
                acknowledged = Await.answered("the pre-restart CDC target confirmation", BOUND,
                        () -> control.durablePosition(PIPELINE, TABLE).filter(value -> !value.equals(snapshotAck)));
                // Initial snapshot/tail overlap may repeat writes; the confirmed position fixes the restart boundary.
                awaitSettledWrites(control, 9, false);
                assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(TABLE, 8L);
                beforeShutdown = rows(mongo, target);
            }
            assertThat(first.isAlive()).as("the original JVM has exited").isFalse();
            mongo.update(source, TABLE, Map.of("id", 2L), Map.of("seq", 22L));
            mongo.delete(source, TABLE, Map.of("id", 3L));
            mongo.insert(source, TABLE, List.of(Map.of("id", 9L, "seq", 90L)));
            assertThat(rows(mongo, target)).as("the old process has exited before downtime changes").isEqualTo(beforeShutdown);
            try (var replacement = start(configUri, cluster)) {
                var control = login(replacement);
                Await.until("the retained running intent to resume", BOUND,
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> "pipeline=" + control.state(PIPELINE));
                awaitRows(mongo, source, target, "all downtime changes to cross without another start or apply");
                String replayAck = Await.answered("downtime changes to advance the original target position", BOUND,
                        () -> control.durablePosition(PIPELINE, TABLE).filter(value -> !value.equals(acknowledged)));
                mongo.update(source, TABLE, Map.of("id", 4L), Map.of("seq", 44L));
                awaitRows(mongo, source, target, "fresh CDC after replacement");
                Await.answered("the replacement target confirmation to advance again", BOUND,
                        () -> control.durablePosition(PIPELINE, TABLE).filter(value -> !value.equals(replayAck)));
                awaitSettledWrites(control, 4, true);
                assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(TABLE, 0L);
                assertThat(rows(mongo, target)).isEqualTo(rows(mongo, source))
                        .containsEntry(1L, 10L).containsEntry(2L, 22L).containsEntry(4L, 44L).containsEntry(9L, 90L)
                        .doesNotContainKey(3L);
            }
        }
    }

    private RealProcessServer start(String configUri, String cluster) {
        return RealProcessServer.startPrepared(configUri, "127.0.0.1", port -> List.of(
                "--tapstate.cluster.id=" + cluster, "--tapstate.cluster.node-id=restart-node",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds")));
    }

    private static ControlPlane login(RealProcessServer server) {
        var control = new ControlPlane(server.baseUrl());
        control.login("admin", "native-restart-password");
        return control;
    }

    private static void awaitSettledWrites(ControlPlane control, long expected, boolean exact) {
        long[] observed = {-1}, stableSince = {System.nanoTime()};
        Await.until("exact confirmed writes to settle on the current run", BOUND, () -> {
            long current = control.recordsOut(PIPELINE).orElse(-1L);
            if (current != observed[0]) { observed[0] = current; stableSince[0] = System.nanoTime(); }
            return (exact ? current == expected : current >= expected)
                    && System.nanoTime() - stableSince[0] >= Duration.ofSeconds(8).toNanos();
        }, () -> "confirmed writes=" + observed[0] + (exact ? ", expected=" : ", minimum=") + expected);
    }

    private static void awaitRows(MongoEndpoints mongo, EndpointAddress source, EndpointAddress target, String phase) {
        Await.until(phase, BOUND, () -> rows(mongo, target).equals(rows(mongo, source)), () -> "target=" + rows(mongo, target));
    }

    private static Map<Long, Long> rows(MongoEndpoints mongo, EndpointAddress endpoint) {
        var rows = new TreeMap<Long, Long>();
        for (var document : mongo.documents(endpoint, TABLE)) {
            long id = ((Number) document.get("id")).longValue(), seq = ((Number) document.get("seq")).longValue();
            assertThat(rows.put(id, seq)).as("one target row per source identity").isNull();
        }
        return rows;
    }

    private static Map<String, String> resources(String sourceUri) {
        var resources = new LinkedHashMap<String, String>();
        resources.put("input.tap.yml", """
                version: tapstate/v1
                kind: source
                id: input
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(sourceUri));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: native_resume
                source: [ { id: input, srs: true } ]
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: views
                      on_full_load: clear
                """);
        return resources;
    }

    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    }
}
