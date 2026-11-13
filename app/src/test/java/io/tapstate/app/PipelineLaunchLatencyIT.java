package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Measures real native Cloud launch requests independently of background convergence. */
@RequiresDocker
class PipelineLaunchLatencyIT {
    private static final Map<String, CompletableFuture<Map<String, Object>>> TRACES = new ConcurrentHashMap<>();
    @TempDir Path work;

    @Test void nativeLaunchStagesKeepTheirRequestCostsVisible() throws Exception { measure(false); }

    @Test void anIndependentNativeStartRunsWhileAnotherPipelinesPositionReadIsHeld() throws Exception { measure(true); }

    private void measure(boolean blockPriorObservation) throws Exception {
        try (var config = mongo(); var cache = mongo()) {
            config.start(); cache.start();
            var storage = NativeAppStorage.prepare(config.getReplicaSetUrl("launch_config"),
                    cache.getReplicaSetUrl("launch_cache"), cache.getReplicaSetUrl("launch_master"),
                    StorageBinding.Mode.CLOUD, NativeCloudAlertManagementIT.CLUSTER);
            try (var application = new SpringApplicationBuilder(Bootstrap.class,
                    NativeCloudAlertManagementIT.ControlledSdk.class, Measurements.class)
                    .environment(CloudFixtureEnvironment.isolated()).run(
                            "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                            "--logging.level.io.tapstate.app.StorageRoot=WARN",
                            "--logging.level.io.tapstate.diagnostics.pipeline=DEBUG",
                            "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                            "--tapstate.cloud.atlas-uri=" + storage.configurationUri(),
                            "--tapstate.cloud.base-url=" + NativeCloudAlertManagementIT.CLOUD,
                            "--tapstate.cloud.token=controlled-cloud-machine-token",
                            "--tapstate.cloud.cluster-id=" + storage.clusterId(),
                            "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                            "--SDK_STATUS_SENDER_ENABLED=false",
                            "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                            "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"));
                    var diagnostics = new DiagnosticRecording()) {
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                        assertThat(application.getBean(StorageRoot.class).runtime().business())
                                .as("phase=" + application.getBean(StorageRoot.class).phase()).isPresent());
                NativeAppStorage.awaitBusiness(application);
                var port = NativeAppStorage.business(application).getBean(StorePort.class);
                try (var data = com.mongodb.client.MongoClients.create(storage.masterUri())) {
                    for (String table : List.of("orders", "customers", "lines"))
                        data.getDatabase(storage.masterDatabase()).getCollection(table)
                                .insertOne(new org.bson.Document("_id", 1L).append("parent_id", 1L));
                }
                port.artifacts().save(new SourceResource("launch-input", null, "mongodb",
                        Map.of("isUri", true, "uri", storage.masterUri()), io.tapstate.core.model.SourceMode.CDC,
                        List.of(io.tapstate.core.model.TableRef.literal("orders"), io.tapstate.core.model.TableRef.literal("customers"),
                                io.tapstate.core.model.TableRef.literal("lines")), null, null));
                var fields = List.of(new SourceField("_id", "int", TapstateType.INT64),
                        new SourceField("parent_id", "int", TapstateType.INT64));
                port.schemas().save(new DiscoveredSourceModel("launch-input", "mongodb", System.currentTimeMillis(),
                        new SourceModel(List.of(new SourceTable("orders", fields, List.of("_id"), List.of()),
                                new SourceTable("customers", fields, List.of("_id"), List.of()),
                                new SourceTable("lines", fields, List.of("_id"), List.of())))));
                try (var raw = com.mongodb.client.MongoClients.create(storage.configurationUri())) {
                    int delay = Integer.getInteger("launch.delay-ms", 0);
                    if (delay > 0) raw.getDatabase("admin").runCommand(new org.bson.Document("configureFailPoint", "failCommand")
                            .append("mode", "alwaysOn").append("data", new org.bson.Document("failCommands", List.of("find", "aggregate", "getMore"))
                                    .append("blockConnection", true).append("blockTimeMS", delay)));
                }
                int cacheDelay = Integer.getInteger("launch.cache-delay-ms", 0);
                if (cacheDelay > 0) {
                    try (var raw = com.mongodb.client.MongoClients.create(storage.cacheUri())) {
                        raw.getDatabase("admin").runCommand(new org.bson.Document("configureFailPoint", "failCommand")
                                .append("mode", "alwaysOn").append("data", new org.bson.Document("failCommands", List.of("find", "aggregate", "getMore"))
                                        .append("blockConnection", true).append("blockTimeMS", cacheDelay)));
                    }
                }
                String origin = "http://localhost:" + ((WebServerApplicationContext) application).getWebServer().getPort();
                try (var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
                    var login = http.send(HttpRequest.newBuilder(URI.create(origin + "/auth/exchange?code=browser-code"))
                            .GET().build(), HttpResponse.BodyHandlers.ofString());
                    assertThat(login.statusCode()).isEqualTo(302);
                    String cookie = login.headers().firstValue("set-cookie").orElseThrow().split(";", 2)[0];
                    int samples = blockPriorObservation ? 2 : Integer.getInteger("launch.samples", 2);
                    for (int sample = 0; sample < samples; sample++) {
                        String id = "launch-" + sample;
                        request(http, origin, cookie, "POST", "/api/pipelines/" + id + "/draft", draft(id), 201);
                        request(http, origin, cookie, "POST", "/api/pipelines/" + id + "/draft:preview", Map.of("revision", 1), 200);
                        var published = publish(http, origin, cookie, id, storage);
                        assertThat(published).containsEntry("published", true);
                        request(http, origin, cookie, "GET", "/api/pipelines/" + id + "/draft", null, 200);
                        request(http, origin, cookie, "GET", "/api/pipelines/" + id + "/status", null, 200);
                    }
                    String running = "launch-" + (samples - 1);
                    var engine = NativeAppStorage.business(application).getBean(io.tapstate.runtime.engine.Engine.class);
                    if (blockPriorObservation) {
                        request(http, origin, cookie, "POST", "/api/pipelines/launch-0:start", Map.of(), 200);
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> engine.captureRun("launch-0")
                                .filter(run -> run.status() == com.hazelcast.jet.core.JobStatus.RUNNING).isPresent());
                    }
                    var publisher = NativeAppStorage.business(application).getBean(io.tapstate.runtime.scheduler.ObservationPublisher.class);
                    try (var barrier = blockPriorObservation ? new PositionReadBarrier(publisher,"launch-0") : null;
                         var raw = com.mongodb.client.MongoClients.create(storage.cacheUri())) {
                        if (barrier != null) assertThat(barrier.entered.await(30,TimeUnit.SECONDS))
                                .as("the prior Pipeline must reach its actual observation position reader").isTrue();
                        var admin = raw.getDatabase("admin");
                        if (!blockPriorObservation) admin.runCommand(new org.bson.Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                                .append("data", new org.bson.Document("failCommands", List.of("update"))
                                        .append("namespace", storage.cacheDatabase() + "." + io.tapstate.adapters.mongostore.SystemCollections.CACHE_PIPELINE_OBSERVATION.collectionName())
                                        .append("blockConnection", true).append("blockTimeMS", 5000)));
                        try {
                            long requested = System.nanoTime();
                            request(http, origin, cookie, "POST", "/api/pipelines/" + running + ":start", Map.of(), 200);
                            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> engine.captureRun(running)
                                    .filter(run -> run.status() == com.hazelcast.jet.core.JobStatus.RUNNING).isPresent());
                            long readyMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requested);
                            System.out.println((blockPriorObservation ? "INDEPENDENT_ENGINE_READY_MS " : "ENGINE_READY_MS ") + readyMillis);
                            if (barrier != null) assertThat(barrier.release.getCount())
                                    .as("the new real engine must run before the unrelated position reader is released").isEqualTo(1);
                            var state = request(http, origin, cookie, "GET", "/api/pipelines/" + running + "/status", null, 200);
                            assertThat(state).containsEntry("state", "RUNNING").containsEntry("displayState", "RUNNING");
                            if (!blockPriorObservation) assertThat(state).doesNotContainKeys("observedAt", "observedState");
                            if (barrier != null) {
                                try (var target = com.mongodb.client.MongoClients.create(storage.masterUri())) {
                                    var rows = target.getDatabase(storage.masterDatabase())
                                            .getCollection("order_views_" + running.replace('-','_'));
                                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                                        var row = rows.find(new org.bson.Document("parent_id",1L)).first();
                                        assertThat(row).as("the independent Pipeline must deliver its root document").isNotNull();
                                        assertThat(row.get("customer",org.bson.Document.class)).containsEntry("parent_id",1L);
                                        assertThat(row.getList("lines",org.bson.Document.class)).hasSize(1)
                                                .allSatisfy(line -> assertThat(line).containsEntry("parent_id",1L));
                                    });
                                    assertThat(barrier.blocked.get()).isTrue();
                                    assertThat(barrier.prematureExit.get()).isFalse();
                                    System.out.println("INDEPENDENT_TARGET_ROW_VISIBLE_MS "
                                            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-requested));
                                }
                            }
                        } finally { admin.runCommand(new org.bson.Document("configureFailPoint", "failCommand").append("mode", "off")); }
                    }
                    if (!blockPriorObservation) {
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15))
                                .until(() -> diagnostics.slowPublication().isPresent());
                        System.out.println("PIPELINE_DIAGNOSTIC_CALIBRATION "
                                + JsonWriter.write(diagnostics.slowPublication().orElseThrow()));
                    }
                }
            }
        }
    }

    private static final class DiagnosticRecording implements AutoCloseable {
        final ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger)
                org.slf4j.LoggerFactory.getLogger(PipelineDiagnostics.LOGGER_NAME);
        final Queue<Map<?, ?>> completed = new ConcurrentLinkedQueue<>();
        final ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.AppenderBase<>() {
                    @Override protected void append(ch.qos.logback.classic.spi.ILoggingEvent event) {
                        var record = (Map<?, ?>) JsonReader.parse(event.getFormattedMessage());
                        if ("PIPELINE_PASS".equals(record.get("phase")) && "end".equals(record.get("edge")))
                            completed.add(record);
                    }
                };
        DiagnosticRecording() { appender.setContext(logger.getLoggerContext()); appender.start(); logger.addAppender(appender); }
        Optional<Map<String, Object>> slowPublication() {
            for (var record : completed) {
                var mongo = (Map<?, ?>) ((Map<?, ?>) record.get("mongo_by_phase")).get("pipeline_publish");
                var phase = (Map<?, ?>) ((Map<?, ?>) record.get("phases")).get("pipeline_publish");
                if (mongo != null && phase != null && Boolean.TRUE.equals(record.get("complete"))
                        && ((Number) mongo.get("max_driver_ms")).doubleValue() >= 4500
                        && ((Number) phase.get("inclusive_ms")).doubleValue() >= 4500) {
                    return Optional.of(Map.of("pass_id", record.get("pass_id"), "pipeline_id", record.get("pipeline_id"),
                            "phase", "PIPELINE_PUBLISH", "inclusive_ms", phase.get("inclusive_ms"),
                            "max_driver_ms", mongo.get("max_driver_ms"), "command_count", mongo.get("started")));
                }
            }
            return Optional.empty();
        }
        @Override public void close() { logger.detachAppender(appender); appender.stop(); }
    }

    /** Holds one real position supplier; all engine, capture and store work still follows production wiring. */
    private static final class PositionReadBarrier implements AutoCloseable {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicBoolean blocked = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicBoolean prematureExit = new java.util.concurrent.atomic.AtomicBoolean();
        final java.lang.reflect.Field field;
        final io.tapstate.runtime.scheduler.ObservationPublisher publisher;
        final Object original;
        @SuppressWarnings("unchecked")
        PositionReadBarrier(io.tapstate.runtime.scheduler.ObservationPublisher publisher,String pipeline) throws Exception {
            this.publisher=publisher;
            field=io.tapstate.runtime.scheduler.ObservationPublisher.class.getDeclaredField("positions");
            field.setAccessible(true);original=field.get(publisher);
            var delegate=(java.util.function.Function<String,Map<String,String>>)original;
            field.set(publisher,(java.util.function.Function<String,Map<String,String>>) id -> {
                if(id.equals(pipeline)) {
                    blocked.set(true);entered.countDown();
                    try { release.await(); }
                    catch(InterruptedException stopped) {Thread.currentThread().interrupt();throw new IllegalStateException(stopped);}
                    finally {if(release.getCount()!=0)prematureExit.set(true);blocked.set(false);}
                }
                return delegate.apply(id);
            });
        }
        @Override public void close() throws Exception {release.countDown();field.set(publisher,original);}
    }

    private static Map<String, Object> publish(HttpClient http, String origin, String cookie, String id,
            NativeAppStorage storage) throws Exception {
        long duration = Long.getLong("launch.contention-ms", 0);
        if (duration == 0) return request(http, origin, cookie, "POST", "/api/pipelines/" + id + "/draft:publish", Map.of("revision", 1), 200);
        var stop = new java.util.concurrent.atomic.AtomicBoolean();
        var ready = new CountDownLatch(1);
        try (var raw = com.mongodb.client.MongoClients.create(storage.configurationUri()); var writer = Executors.newSingleThreadExecutor()) {
            var updates = writer.submit(() -> {
                long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(duration);
                int count = 0;
                while (!stop.get() && System.nanoTime() < end) {
                    raw.getDatabase(storage.configurationDatabase()).getCollection("configuration_version")
                            .updateOne(new org.bson.Document("_id", "configuration"),
                                    new org.bson.Document("$inc", new org.bson.Document("alertWriteFence", 1L)));
                    count++; ready.countDown();
                    java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(80));
                }
                return count;
            });
            try {
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
                return request(http, origin, cookie, "POST", "/api/pipelines/" + id + "/draft:publish", Map.of("revision", 1), 200);
            } finally {
                stop.set(true);
                assertThat(updates.get(10, TimeUnit.SECONDS)).isPositive();
            }
        }
    }

    private static Map<String, Object> draft(String id) {
        return Map.of("pipelineId", id, "mode", "wizard", "name", "Order view", "wizard", Map.of(
                "root", Map.of("id", "root", "sourceId", "launch-input", "table", "orders", "key", List.of("_id"), "preTransforms", List.of()),
                "related", List.of(related("customer", "customers", "object"), related("lines", "lines", "array")),
                "transforms", List.of(), "output", Map.of("kind", "source", "config", Map.of(
                        "sourceId", "atlas-store", "table", "order_views_" + id.replace('-','_'), "writeMode", "upsert", "onFullLoad", "append"))));
    }

    private static Map<String, Object> related(String id, String table, String shape) {
        return Map.of("id", id, "parentId", "root", "sourceId", "launch-input", "table", table,
                "preTransforms", List.of(), "relation", Map.of("on", List.of(Map.of("childField", "parent_id", "parentField", "_id")),
                        "shape", shape, "path", id, "key", List.of("_id"), "arrayKey", shape.equals("array") ? List.of("_id") : List.of()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> request(HttpClient http, String origin, String cookie, String method,
            String path, Object body, int expected) throws Exception {
        String traceId = UUID.randomUUID().toString();
        var trace = new CompletableFuture<Map<String, Object>>(); TRACES.put(traceId, trace);
        try {
            var builder = HttpRequest.newBuilder(URI.create(origin + path)).timeout(Duration.ofSeconds(60))
                    .header("Cookie", cookie).header("Origin", origin).header("X-Launch-Trace", traceId)
                    .header("Content-Type", "application/json");
            long start = System.nanoTime();
            var response = http.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(), HttpResponse.BodyHandlers.ofString());
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Map<String, Object> observation = trace.get(5, TimeUnit.SECONDS);
            System.out.println("LAUNCH_TRACE " + JsonWriter.write(Map.of("method", method, "path", path,
                    "elapsedMs", millis, "status", response.statusCode(), "measurement", observation)));
            assertThat(response.statusCode()).as(path + ": " + response.body()).isEqualTo(expected);
            if (path.endsWith("/draft:publish")) {
                var callers = (Map<String, Integer>) observation.get("callers");
                assertThat(callers.getOrDefault("commit:find:MongoPipelineStateMappings.validationSchema", 0))
                        .as("one publication transaction validates its mapping schema once").isEqualTo(1);
            }
            return (Map<String, Object>) JsonReader.parse(response.body());
        } finally { TRACES.remove(traceId); }
    }

    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25", "--setParameter", "enableTestCommands=1");
    }

    @Configuration(proxyBeanMethods = false)
    static class Measurements {
        @Bean org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter> launchMeasurements() {
            var filter = new org.springframework.web.filter.OncePerRequestFilter() {
                @Override protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest request,
                        jakarta.servlet.http.HttpServletResponse response, jakarta.servlet.FilterChain chain)
                        throws jakarta.servlet.ServletException, java.io.IOException {
                    try (var scope = MongoHttpDiagnosticsScope.open(); var probe = new io.tapstate.adapters.mongostore.PipelineLaunchCommandProbe()) {
                        try { chain.doFilter(request, response); }
                        finally {
                            var trace = TRACES.get(Objects.toString(request.getHeader("X-Launch-Trace"), ""));
                            if (trace != null) {
                                var snapshot = new LinkedHashMap<>(scope.snapshot()); snapshot.put("callers", probe.counts());
                                snapshot.put("successful_command_wall_ms_by_stage", probe.successfulCommandWallMillis());
                                trace.complete(snapshot);
                            }
                        }
                    }
                }
            };
            var registration = new org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter>(filter);
            registration.setOrder(Integer.MIN_VALUE); return registration;
        }
    }
}
