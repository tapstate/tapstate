package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Measures real native Cloud launch requests independently of background convergence. */
@RequiresDocker
class PipelineLaunchLatencyIT {
    private static final Map<String, CompletableFuture<Map<String, Object>>> TRACES = new ConcurrentHashMap<>();
    @TempDir Path work;

    @Test void nativeLaunchStagesKeepTheirRequestCostsVisible() throws Exception { measure(false); }

    @Test void artifactListReadsSharedOutputMetadataOncePerHttpPage() throws Exception {
        try (var config = mongo(); var cache = mongo()) {
            config.start(); cache.start();
            var storage = NativeAppStorage.prepare(config.getReplicaSetUrl("artifact_page_config"),
                    cache.getReplicaSetUrl("artifact_page_cache"), cache.getReplicaSetUrl("artifact_page_master"),
                    StorageBinding.Mode.CLOUD, NativeCloudAlertManagementIT.CLUSTER);
            try (var fixture = new LaunchFixture(new SpringApplicationBuilder(Bootstrap.class,
                    NativeCloudAlertManagementIT.ControlledSdk.class, Measurements.class)
                    .environment(CloudFixtureEnvironment.isolated()).run(
                            "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                            "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                            "--tapstate.cloud.atlas-uri=" + storage.configurationUri(),
                            "--tapstate.cloud.base-url=" + NativeCloudAlertManagementIT.CLOUD,
                            "--tapstate.cloud.token=controlled-cloud-machine-token",
                            "--tapstate.cloud.cluster-id=" + storage.clusterId(),
                            "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                            "--SDK_STATUS_SENDER_ENABLED=false",
                            "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                            "--tapstate.connectors.plugins-dir=" + work.resolve("artifact-page-plugins")));
                 var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
                var application = fixture.application;
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                        assertThat(application.getBean(StorageRoot.class).runtime().business()).isPresent());
                var port = fixture.awaitBusiness().getBean(StorePort.class);
                var parser = new io.tapstate.core.dsl.DslParser();
                port.artifacts().save(new SourceResource("page-input", null, "mongodb",
                        Map.of("isUri", true, "uri", storage.masterUri()), null,
                        List.of(io.tapstate.core.model.TableRef.regex("order.*")), null, null));
                port.schemas().save(new DiscoveredSourceModel("page-input", "mongodb", 0L,
                        new SourceModel(List.of(new SourceTable("orders",
                                List.of(new SourceField("_id", "int", TapstateType.INT64)), List.of("_id"), List.of())))));
                for (int i = 0; i < 8; i++) port.artifacts().save(parser.parse("""
                        version: tapstate/v1
                        kind: pipeline
                        id: page-%d
                        source: page-input
                        serve:
                          from: page-input
                          sync: [{id: sink, source: atlas-store}]
                        """.formatted(i)));
                String origin = "http://localhost:" + ((WebServerApplicationContext) application).getWebServer().getPort();
                var login = http.send(HttpRequest.newBuilder(URI.create(origin + "/auth/exchange?code=browser-code")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(login.statusCode()).as(login.body()).isEqualTo(302);
                String cookie = login.headers().firstValue("set-cookie").orElseThrow().split(";", 2)[0];
                var totals = new ArrayList<Long>();
                java.util.function.Consumer<Map<String, Object>> budget = reading -> {
                    var collections = (Map<?, ?>) reading.get("collections");
                    String modelRead = "find:" + io.tapstate.adapters.mongostore.SystemCollections.CACHE_SOURCE_SCHEMAS.collectionName();
                    assertThat(collections.get(modelRead)).isInstanceOf(Number.class);
                    assertThat(((Number) collections.get(modelRead)).intValue())
                            .as("one discovery header and table batch per distinct source, independently of page size").isEqualTo(2);
                    totals.add(((Number) ((Map<?, ?>) reading.get("mongo")).get("started")).longValue());
                };
                var small = request(http, origin, cookie, "GET", "/api/pipelines:artifacts?limit=1&offset=0", null, 200, budget);
                assertThat((List<?>) small.get("items")).hasSize(1);
                var full = request(http, origin, cookie, "GET", "/api/pipelines:artifacts?limit=8&offset=0", null, 200, budget);
                assertThat((List<?>) full.get("items")).hasSize(8);
                assertThat(totals.get(1) - totals.getFirst()).as("shared output enrichment does not grow by pipeline count").isLessThanOrEqualTo(8);
                for (int i = 0; i < 25; i++) port.artifacts().save(new SourceResource("unrelated-" + i, null, "mongodb",
                        Map.of("isUri", true, "uri", storage.masterUri()), null, null, null, null));
                request(http, origin, cookie, "GET", "/api/pipelines:artifacts?limit=8&offset=0", null, 200, budget);
                assertThat(totals.get(2)).as("unrelated artifacts do not add enrichment commands").isLessThanOrEqualTo(totals.get(1) + 4);
                var shared = (SourceResource) port.artifacts().get("page-input").orElseThrow();
                port.schemas().save(new DiscoveredSourceModel(shared.id(), shared.connector(), 1L,
                        new SourceModel(List.of(new SourceTable("orders_new",
                                List.of(new SourceField("_id", "int", TapstateType.INT64)), List.of("_id"), List.of())))));
                var changed = request(http, origin, cookie, "GET", "/api/pipelines:artifacts?limit=8&offset=0", null, 200, budget);
                assertThat(JsonWriter.write(changed)).contains("orders_new");
                request(http, origin, cookie, "GET", "/api/pipelines:artifacts?limit=8&offset=50", null, 200, reading ->
                        assertThat(((Map<?, ?>) reading.get("collections")).get("find:"
                                + io.tapstate.adapters.mongostore.SystemCollections.CACHE_SOURCE_SCHEMAS.collectionName())).isNull());
            }
        }
    }

    @Test void anIndependentNativeStartRunsWhileAnotherPipelinesPositionReadIsHeld() throws Exception { measure(true); }

    @Test void anIndependentNativeStartRunsWhileSharedCaptureMaintenanceIsHeld() throws Exception { measure(true, true); }

    @Test void anIndependentNativeStartDeliversWhileAnotherPipelinesAlertProofIsHeld() throws Exception { measure(true, false, true); }

    @Test void aNativeRestartPlansOneOwnCursorBatchAndDeliversItsTarget() throws Exception { measure(false,false,false,true); }

    private void measure(boolean blockPriorObservation) throws Exception { measure(blockPriorObservation, false); }

    private void measure(boolean blockPriorObservation, boolean blockMaintenance) throws Exception {
        measure(blockPriorObservation, blockMaintenance, false);
    }

    private void measure(boolean blockPriorObservation, boolean blockMaintenance, boolean blockProof) throws Exception {
        measure(blockPriorObservation,blockMaintenance,blockProof,false);
    }

    private void measure(boolean blockPriorObservation, boolean blockMaintenance, boolean blockProof, boolean restartPlanning) throws Exception {
        try (var config = mongo(); var cache = mongo()) {
            config.start(); cache.start();
            var storage = NativeAppStorage.prepare(config.getReplicaSetUrl("launch_config"),
                    cache.getReplicaSetUrl("launch_cache"), cache.getReplicaSetUrl("launch_master"),
                    StorageBinding.Mode.CLOUD, NativeCloudAlertManagementIT.CLUSTER);
            boolean blockPositionRead = blockPriorObservation && !blockMaintenance && !blockProof;
            try (var fixture = new LaunchFixture(new SpringApplicationBuilder(Bootstrap.class,
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
                            "--tapstate.connectors.plugins-dir=" + work.resolve("plugins")));
                    var diagnostics = new DiagnosticRecording()) {
                var application = fixture.application;
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                        assertThat(application.getBean(StorageRoot.class).runtime().business())
                                .as("phase=" + application.getBean(StorageRoot.class).phase()).isPresent());
                var business = fixture.awaitBusiness();
                var port = business.getBean(StorePort.class);
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
                var captures = (ScopedPipelineCaptureCoordinator) NativeAppStorage.business(application).getBean(PipelineCaptureCoordinator.class);
                // Capture owners retain their SRS ports. Install the barrier before starting the first owner.
                try (var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
                     var maintenance = blockMaintenance ? new PositionReadBarrier(captures) : null) {
                    var login = http.send(HttpRequest.newBuilder(URI.create(origin + "/auth/exchange?code=browser-code"))
                            .GET().build(), HttpResponse.BodyHandlers.ofString());
                    assertThat(login.statusCode()).isEqualTo(302);
                    String cookie = login.headers().firstValue("set-cookie").orElseThrow().split(";", 2)[0];
                    int samples = blockPriorObservation || restartPlanning ? 2 : Integer.getInteger("launch.samples", 2);
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
                    if (blockPriorObservation || restartPlanning) {
                        request(http, origin, cookie, "POST", "/api/pipelines/launch-0:start", Map.of(), 200);
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> engine.captureRun("launch-0")
                                .filter(run -> run.status() == com.hazelcast.jet.core.JobStatus.RUNNING).isPresent());
                    }
                    var publisher = NativeAppStorage.business(application).getBean(io.tapstate.runtime.scheduler.ObservationPublisher.class);
                    if (Boolean.getBoolean("launch.mutate.separate-capture-reads")) {
                        var sample = io.tapstate.runtime.scheduler.ObservationPublisher.class.getDeclaredField("captureMeasurements");
                        sample.setAccessible(true);
                        sample.set(publisher, null);
                    }
                    try (var otherBarrier = blockProof ? new PositionReadBarrier(NativeAppStorage.business(application)
                            .getBean(PipelineActuationOwnership.class), ((ThreeLayerStorePort) port).scope("launch-0"))
                            : blockPositionRead ? new PositionReadBarrier(publisher,"launch-0") : null;
                         var raw = com.mongodb.client.MongoClients.create(storage.cacheUri());
                         var observationWrite = !blockPriorObservation && !restartPlanning
                                 ? new PositionReadBarrier(business.getBean(ProcessObservationWriter.class)) : null) {
                        var barrier = maintenance == null ? otherBarrier : maintenance;
                        if (maintenance != null) maintenance.armMaintenance.set(true);
                        if (barrier != null) assertThat(barrier.entered.await(30,TimeUnit.SECONDS))
                                .as("the prior Pipeline must enter the selected observation, maintenance or proof barrier").isTrue();
                        var admin = raw.getDatabase("admin");
                        try {
                            long requested = System.nanoTime();
                            request(http, origin, cookie, "POST", "/api/pipelines/" + running + ":start", Map.of(), 200);
                            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> engine.captureRun(running)
                                    .filter(run -> run.status() == com.hazelcast.jet.core.JobStatus.RUNNING).isPresent());
                            long readyMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requested);
                            System.out.println((blockPriorObservation ? "INDEPENDENT_ENGINE_READY_MS " : "ENGINE_READY_MS ") + readyMillis);
                            if (barrier != null) assertThat(barrier.release.getCount())
                                    .as("the new real engine must run before the unrelated dependency is released").isEqualTo(1);
                            if (observationWrite != null) {
                                assertThat(observationWrite.entered.await(30, TimeUnit.SECONDS))
                                        .as("the real Mongo observation update is held while the engine runs").isTrue();
                                assertThat(observationWrite.blocked.get()).isTrue();
                            }
                            var state = request(http, origin, cookie, "GET", "/api/pipelines/" + running + "/status", null, 200);
                            assertThat(state).containsEntry("state", "RUNNING").containsEntry("displayState", "RUNNING");
                            if (!blockPriorObservation && !restartPlanning) assertThat(state).doesNotContainKeys("observedAt", "observedState");
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
                            if (observationWrite != null) {
                                // Release only after status was read with the actual update still held.
                                var configured = admin.runCommand(new org.bson.Document("configureFailPoint", "failCommand")
                                        .append("mode", "alwaysOn")
                                        .append("data", new org.bson.Document("failCommands", List.of("update"))
                                                .append("namespace", storage.cacheDatabase() + "." + io.tapstate.adapters.mongostore.SystemCollections.CACHE_PIPELINE_OBSERVATION.collectionName())
                                                .append("blockConnection", true).append("blockTimeMS", 5000)));
                                observationWrite.release.countDown();
                                admin.runCommand(new org.bson.Document("waitForFailPoint", "failCommand")
                                        .append("timesEntered", ((Number) configured.get("count")).longValue() + 1)
                                        .append("maxTimeMS", 15_000));
                                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15))
                                        .until(() -> diagnostics.slowPublication(running).isPresent());
                                var publication = diagnostics.slowPublication(running).orElseThrow();
                                System.out.println("PIPELINE_DIAGNOSTIC_CALIBRATION " + JsonWriter.write(publication));
                                assertThat(((Number) publication.get("command_count")).intValue())
                                        .as("one source with three tables must reuse its original progress across projections")
                                        .isLessThanOrEqualTo(64);
                            }
                        } finally { admin.runCommand(new org.bson.Document("configureFailPoint", "failCommand").append("mode", "off")); }
                    }
                    if (blockProof) org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                            assertThat(diagnostics.claimIo).anySatisfy(record -> {
                                assertThat(record.get("complete")).isEqualTo(true);
                                assertThat(((Number) ((Map<?, ?>) record.get("mongo")).get("started")).longValue()).isPositive();
                                assertThat(((Number) record.get("queue_ms")).doubleValue()).isNotNegative();
                                assertThat(((Number) record.get("execution_ms")).doubleValue()).isNotNegative();
                                assertThat(record.get("caller_thread_id")).isNotEqualTo(record.get("thread_id"));
                            }));
                    if (restartPlanning) {
                        assertNativeTarget(storage, running, 1L);
                        request(http,origin,cookie,"POST","/api/pipelines/" + running + ":stop",Map.of("purgeState",false),200);
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).untilAsserted(() ->
                                assertThat(request(http,origin,cookie,"GET","/api/pipelines/" + running + "/status",null,200))
                                        .containsEntry("state","STOPPED"));
                        long requested=System.nanoTime();
                        var restarted=request(http,origin,cookie,"POST","/api/pipelines/" + running + ":start",Map.of(),200,observation -> {
                            assertThat(observation.get("complete")).isEqualTo(true);
                            var collections=(Map<?,?>)observation.get("collections");
                            String chains = io.tapstate.adapters.mongostore.SystemCollections.SRS_CHAINS.collectionName();
                            assertThat(collections.get("aggregate:" + chains))
                                    .as("restart must batch the chain, its memberships and its cursors once").isEqualTo(1);
                            var callers = (Map<?, ?>) observation.get("callers");
                            assertThat(callers.get("other:aggregate:MongoSrsConsumerStore.startProgressRows")).isEqualTo(1);
                            assertThat(collections.keySet().stream().map(Object::toString).toList()).doesNotContain(
                                    "find:" + io.tapstate.adapters.mongostore.SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.collectionName(),
                                    "find:" + io.tapstate.adapters.mongostore.SystemCollections.SRS_MODELS.collectionName(),
                                    "find:" + io.tapstate.adapters.mongostore.SystemCollections.SRS_EVENTS.collectionName());
                        });
                        var checks=(Map<?,?>)restarted.get("startChecks");
                        assertThat((List<?>)checks.get("plan")).isNotEmpty()
                                .allSatisfy(entry -> assertThat(((Map<?,?>)entry).get("load")).isEqualTo("RESUME"));
                        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> engine.captureRun(running)
                                .filter(run -> run.status()==com.hazelcast.jet.core.JobStatus.RUNNING).isPresent());
                        System.out.println("NATIVE_RESTART_ENGINE_READY_MS " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requested));
                        try (var source = com.mongodb.client.MongoClients.create(storage.masterUri())) {
                            var database = source.getDatabase(storage.masterDatabase());
                            for (String table : List.of("orders", "customers", "lines"))
                                database.getCollection(table).insertOne(new org.bson.Document("_id", 2L).append("parent_id", 2L));
                        }
                        assertNativeTarget(storage, running, 2L);
                        System.out.println("NATIVE_RESTART_TARGET_READY_MS " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requested));
                    }
                }
            }
        }
    }

    private static final class LaunchFixture implements AutoCloseable {
        private final ConfigurableApplicationContext application;
        private ConfigurableApplicationContext business;
        private HazelcastInstance member;

        private LaunchFixture(ConfigurableApplicationContext application) { this.application = application; }

        private ConfigurableApplicationContext awaitBusiness() {
            NativeAppStorage.awaitBusiness(application);
            business = NativeAppStorage.business(application);
            member = business.getBean(HazelcastInstance.class);
            return business;
        }

        @Override public void close() {
            try (application) {
                if (business != null) {
                    // Barriers have already closed. Let admitted work finish while Jet and Mongo are still live.
                    business.getBean(ConvergenceDriver.class).stopForShutdown();
                    var convergences = business.getBean(PipelineConvergences.class);
                    var observations = business.getBean(PeriodicObservations.class);
                    org.awaitility.Awaitility.await("launch fixture finishes convergence and observations before shutdown")
                            .atMost(Duration.ofSeconds(15))
                            .until(() -> convergences.pipelineIds().isEmpty() && observations.pipelineIds().isEmpty());
                }
            }
            if (member != null) {
                assertThat(Hazelcast.getAllHazelcastInstances())
                        .as("the launch fixture must release its own member when the context closes")
                        .doesNotContain(member);
            }
        }
    }

    private static void assertNativeTarget(NativeAppStorage storage, String pipeline, long parentId) throws Exception {
        try (var client=com.mongodb.client.MongoClients.create(storage.masterUri())) {
            var rows=client.getDatabase(storage.masterDatabase()).getCollection("order_views_" + pipeline.replace('-','_'));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var row=rows.find(new org.bson.Document("parent_id", parentId)).first();
                assertThat(row).isNotNull();
                assertThat(row.get("customer",org.bson.Document.class)).containsEntry("parent_id", parentId);
                assertThat(row.getList("lines",org.bson.Document.class)).hasSize(1)
                        .allSatisfy(line -> assertThat(line).containsEntry("parent_id", parentId));
            });
        }
    }

    private static final class DiagnosticRecording implements AutoCloseable {
        final ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger)
                org.slf4j.LoggerFactory.getLogger(PipelineDiagnostics.LOGGER_NAME);
        final Map<Object, Map<?, ?>> samples = new ConcurrentHashMap<>();
        final Queue<Map<?, ?>> publications = new ConcurrentLinkedQueue<>();
        final Queue<Map<?, ?>> claimIo = new ConcurrentLinkedQueue<>();
        final ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.AppenderBase<>() {
                    @Override protected void append(ch.qos.logback.classic.spi.ILoggingEvent event) {
                        var record = (Map<?, ?>) JsonReader.parse(event.getFormattedMessage());
                        if ("end".equals(record.get("edge")) && Boolean.TRUE.equals(record.get("complete"))) {
                            if ("PIPELINE_SAMPLE".equals(record.get("phase"))) samples.put(record.get("pass_id"), record);
                            if ("PIPELINE_PUBLISH".equals(record.get("phase")) && record.containsKey("observation_sample_id"))
                                publications.add(record);
                        }
                        if ("mongo_claim_io_diag".equals(record.get("event"))) claimIo.add(record);
                    }
                };
        DiagnosticRecording() { appender.setContext(logger.getLoggerContext()); appender.start(); logger.addAppender(appender); }
        Optional<Map<String, Object>> slowPublication(String pipeline) {
            for (var write : publications) {
                var sample = samples.get(write.get("observation_sample_id"));
                if (sample == null || !pipeline.equals(sample.get("pipeline_id")) || !pipeline.equals(write.get("pipeline_id"))) continue;
                var groups = (List<?>) write.get("mongo_groups");
                var delayed = groups.stream().map(group -> (Map<?, ?>) group)
                        .filter(group -> "pipeline_observation".equals(group.get("collection")) && "update".equals(group.get("command"))
                                && ((Number) group.get("failures")).longValue() == 0
                                && ((Number) group.get("completed")).longValue() > 0
                                && ((Number) group.get("max_driver_ms")).doubleValue() >= 4500).findFirst();
                if (delayed.isEmpty() || ((Number) write.get("elapsed_ms")).doubleValue() < 4500) continue;
                int reads = commands(sample);
                int writes = commands(write);
                return Optional.of(Map.of("pipeline_id", pipeline, "sample_id", sample.get("pass_id"),
                        "sample_commands", reads, "write_commands", writes, "command_count", reads + writes,
                        "max_driver_ms", delayed.orElseThrow().get("max_driver_ms")));
            }
            return Optional.empty();
        }
        private static int commands(Map<?, ?> record) {
            return ((Number) ((Map<?, ?>) record.get("mongo")).get("started")).intValue();
        }
        @Override public void close() { logger.detachAppender(appender); appender.stop(); }
    }

    /** Holds one real position supplier; all engine, capture and store work still follows production wiring. */
    private static final class PositionReadBarrier implements AutoCloseable {
        final java.util.concurrent.atomic.AtomicBoolean armMaintenance = new java.util.concurrent.atomic.AtomicBoolean();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicBoolean blocked = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicBoolean prematureExit = new java.util.concurrent.atomic.AtomicBoolean();
        final java.lang.reflect.Field field;
        final Object publisher;
        final Object original;
        ExecutorService proofWorker;
        Future<?> proof;

        PositionReadBarrier(ProcessObservationWriter writer) throws Exception {
            publisher = writer;
            field = ProcessObservationWriter.class.getDeclaredField("cache");
            field.setAccessible(true);
            original = field.get(writer);
            var cache = (io.tapstate.adapters.mongostore.MongoCacheDomain) original;
            var observed = org.mockito.Mockito.mock(io.tapstate.adapters.mongostore.MongoCacheDomain.class,
                    org.mockito.AdditionalAnswers.delegatesTo(cache));
            org.mockito.Mockito.doAnswer(call -> {
                try (var command = io.tapstate.adapters.mongostore.PipelineLaunchCommandProbe.holdObservationUpdate(() -> {
                    blocked.set(true);
                    entered.countDown();
                    try { release.await(); }
                    catch (InterruptedException stopped) { Thread.currentThread().interrupt(); throw new IllegalStateException(stopped); }
                    finally { if (release.getCount() != 0) prematureExit.set(true); blocked.set(false); }
                })) {
                    return cache.writeObservations(call.getArgument(0));
                }
            }).when(observed).writeObservations(org.mockito.ArgumentMatchers.anyList());
            field.set(writer, observed);
        }

        PositionReadBarrier(PipelineActuationOwnership ownership, PipelineIntentScope scope) throws Exception {
            var expected = WorkloadClaimFence.from(ownership.permit(scope).claim());
            var claims = PipelineActuationOwnership.class.getDeclaredField("claims");
            claims.setAccessible(true); publisher = claims.get(ownership);
            field = ClusterWorkloadClaims.class.getDeclaredField("store");
            field.setAccessible(true); original = field.get(publisher);
            var selected = new java.util.concurrent.atomic.AtomicReference<Thread>();
            var proxy = java.lang.reflect.Proxy.newProxyInstance(DeadlineWorkloadClaimStore.class.getClassLoader(),
                    new Class<?>[]{DeadlineWorkloadClaimStore.class}, (ignored, method, arguments) -> {
                        if (method.getName().equals("withDeadline") && Thread.currentThread() == selected.get()) {
                            blocked.set(true); entered.countDown();
                            try { release.await(); }
                            finally { if (release.getCount() != 0) prematureExit.set(true); blocked.set(false); }
                        }
                        try { return method.invoke(original, arguments); }
                        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            field.set(publisher, proxy);
            var gate = PipelineActuationOwnership.class.getDeclaredField("ioAdmission");
            gate.setAccessible(true);
            var serial = (java.util.concurrent.locks.ReentrantLock) gate.get(ownership);
            proofWorker = Executors.newSingleThreadExecutor();
            proof = proofWorker.submit(() -> {
                selected.set(Thread.currentThread());
                boolean mutate = Boolean.getBoolean("launch.mutate.serial-proof");
                if (mutate) serial.lock();
                try { ownership.evaluationProof(scope, expected); }
                finally { if (mutate) serial.unlock(); }
            });
        }
        @SuppressWarnings("unchecked")
        PositionReadBarrier(io.tapstate.runtime.scheduler.ObservationPublisher publisher,String pipeline) throws Exception {
            this.publisher=publisher;
            field=io.tapstate.runtime.scheduler.ObservationPublisher.class.getDeclaredField("captureMeasurements");
            field.setAccessible(true);original=field.get(publisher);
            var delegate=(java.util.function.BiFunction<String,java.time.Instant,io.tapstate.runtime.scheduler.CaptureMeasurements>)original;
            field.set(publisher,(java.util.function.BiFunction<String,java.time.Instant,io.tapstate.runtime.scheduler.CaptureMeasurements>) (id, at) -> {
                if(id.equals(pipeline)) {
                    blocked.set(true);entered.countDown();
                    try { release.await(); }
                    catch(InterruptedException stopped) {Thread.currentThread().interrupt();throw new IllegalStateException(stopped);}
                    finally {if(release.getCount()!=0)prematureExit.set(true);blocked.set(false);}
                }
                return delegate.apply(id, at);
            });
        }
        PositionReadBarrier(ScopedPipelineCaptureCoordinator captures) throws Exception {
            publisher = captures;
            field = ScopedPipelineCaptureCoordinator.class.getDeclaredField("stores");
            field.setAccessible(true);
            synchronized (captures) {
                original = field.get(captures);
                var captured = (ScopedPipelineCaptureCoordinator.Stores) original;
                var delegate = captured.sources();
                SrsStoreViews observed = new SrsStoreViews() {
                    public SrsStores srs(CacheBusinessScope scope, Runnable guard) {
                        var ports = delegate.srs(scope, guard);
                        var consumers = (SrsConsumerStore) java.lang.reflect.Proxy.newProxyInstance(SrsConsumerStore.class.getClassLoader(),
                                new Class<?>[]{SrsConsumerStore.class}, (proxy, method, arguments) -> {
                                    if (method.getName().equals("retentionSnapshot") && Thread.currentThread().getName().startsWith("tapstate-capture-owner-")
                                            && armMaintenance.compareAndSet(true, false)) {
                                        blocked.set(true); entered.countDown();
                                        try { release.await(); }
                                        catch (InterruptedException stopped) { Thread.currentThread().interrupt(); throw new IllegalStateException(stopped); }
                                        finally { if (release.getCount() != 0) prematureExit.set(true); blocked.set(false); }
                                    }
                                    try { return method.invoke(ports.consumers(), arguments); }
                                    catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                                });
                        return new SrsStores(ports.chains(), ports.models(), ports.log(), consumers);
                    }
                    public boolean supportsAdmittedCaptureStartup() { return delegate.supportsAdmittedCaptureStartup(); }
                    public <T> T withCaptureStartup(CacheBusinessScope scope, java.util.function.Supplier<T> step) {
                        return delegate.withCaptureStartup(scope, step);
                    }
                    public DirectChannelStore directChannels(CacheBusinessScope scope, Runnable guard) {
                        return delegate.directChannels(scope, guard);
                    }
                };
                field.set(captures, new ScopedPipelineCaptureCoordinator.Stores(captured.scope(), captured.pipelines(), observed,
                        captured.configuration(), captured.preparations(), captured.notes(), captured.recoveryPinned(),
                        captured.boundedLoads(), captured.completedFull()));
            }
        }
        @Override public void close() throws Exception {
            release.countDown(); field.set(publisher,original);
            if (proofWorker != null) {
                proofWorker.shutdown();
                try { proof.get(10, TimeUnit.SECONDS); }
                finally { assertThat(proofWorker.awaitTermination(10, TimeUnit.SECONDS)).isTrue(); }
            }
        }
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
        return request(http,origin,cookie,method,path,body,expected,ignored -> {});
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> request(HttpClient http, String origin, String cookie, String method,
            String path, Object body, int expected, java.util.function.Consumer<Map<String,Object>> verify) throws Exception {
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
            verify.accept(observation);
            if (path.endsWith("/draft:publish")) {
                var callers = (Map<String, Integer>) observation.get("max_callers_per_transaction_attempt");
                assertThat(callers.getOrDefault("commit:find:MongoPipelineStateMappings.validationSchema", 0))
                        .as("each publication transaction attempt validates its mapping schema once").isEqualTo(1);
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
                                snapshot.put("max_callers_per_transaction_attempt", probe.maxAttemptCounts());
                                snapshot.put("collections",probe.collectionCounts());
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
