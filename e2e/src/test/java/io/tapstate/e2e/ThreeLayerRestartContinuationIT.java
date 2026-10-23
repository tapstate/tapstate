package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.*;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import com.mongodb.client.MongoClients;
import org.bson.Document;
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
        prepareStorage(configUri, cacheDb, targetUri, cluster);
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

    private static void prepareStorage(String configUri, String cacheDb, String targetUri, String cluster) {
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheDb), cacheDb, null);
        NativeMongoStorageFixture.prepare(configUri, binding, targetUri, cluster);
    }

    @Test @Timeout(300)
    void aBoundedRunKeepsItsCompletedObservationAndDoesNotRestartItsSourceAfterProcessReplacement() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "bounded-" + suffix, cacheDb = "bounded_cache_" + suffix;
        String configUri = CONFIG.getReplicaSetUrl("bounded_cfg_" + suffix);
        String sourceUri = DATA.getReplicaSetUrl("bounded_source_" + suffix), targetUri = DATA.getReplicaSetUrl("bounded_target_" + suffix);
        prepareStorage(configUri,cacheDb,targetUri,cluster);
        var source=EndpointAddress.uri(sourceUri); var target=EndpointAddress.uri(targetUri);
        try(var mongo=new MongoEndpoints();var cache=MongoClients.create(CACHE.getReplicaSetUrl())) {
            mongo.seed(source,TABLE,SeedRows.generated(8));
            Map<Long,Long> completedRows;
            java.time.Instant lastObservation;
            long execution;
            var claims=cache.getDatabase(cacheDb).getCollection("workload_claims");
            var pipelineClaim=new Document("resourceType","PIPELINE_ACTUATION").append("resourceId",PIPELINE);
            var first=start(configUri,cluster);
            try(first) {
                var control=login(first); control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
                var resources=resources(sourceUri,true);
                resources.put("views.tap.yml",control.artifact("views").orElseThrow().canonicalForm());
                control.apply(resources);control.discoverSchema(SOURCE,"mongodb",Map.of("isUri",true,"uri",sourceUri));
                control.lifecycle(PIPELINE,LifecycleVerb.START);
                Await.until("the real bounded job and target writes to complete",BOUND,
                        () -> control.state(PIPELINE).filter(PipelineState.COMPLETED::equals).isPresent(),() -> control.logs(PIPELINE));
                awaitRows(mongo,source,target,"the completed bounded target values");
                assertThat(control.snapshotTable(PIPELINE,TABLE)).contains(new io.tapstate.core.lifecycle.TableSnapshot(8,8L,100,true));
                var observed=Await.answered("the completed observation timestamp",BOUND,() -> control.observedAt(PIPELINE));
                mongo.insert(source,TABLE,List.of(Map.of("id",99L,"seq",999L)));
                lastObservation=Await.answered("another completed pass after capture cleanup",BOUND,
                        () -> control.observedAt(PIPELINE).filter(at -> at.isAfter(observed)));
                completedRows=rows(mongo,target);assertThat(completedRows).hasSize(8).doesNotContainKey(99L);
                assertThat(control.snapshotTable(PIPELINE,TABLE)).contains(new io.tapstate.core.lifecycle.TableSnapshot(8,8L,100,true));
                execution=claims.find(pipelineClaim).first().getLong("executionGeneration");
            }
            assertThat(first.isAlive()).isFalse();
            try(var replacement=start(configUri,cluster)) {
                var control=login(replacement);
                Await.answered("the replacement to observe the retained completed result",BOUND,
                        () -> control.observedAt(PIPELINE).filter(at -> at.isAfter(lastObservation)));
                assertThat(control.state(PIPELINE)).contains(PipelineState.COMPLETED);
                assertThat(control.snapshotTable(PIPELINE,TABLE)).contains(new io.tapstate.core.lifecycle.TableSnapshot(8,8L,100,true));
                assertThat(rows(mongo,target)).isEqualTo(completedRows);
                assertThat(claims.find(pipelineClaim).first()).containsEntry("executionGeneration",execution);
            }
        }
    }

    @Test @Timeout(300)
    void aCacheOnlyOutageRefusesANewProcessAndResumesTheOriginalStateAfterNormalLogin() throws Exception {
        storageOutage(false);
    }

    @Test @Timeout(300)
    void anOnPremInstanceOutageNeverReopensInstallationAndResumesAfterTheOriginalStoresReturn() throws Exception {
        storageOutage(true);
    }

    @Test @Timeout(300)
    void aCloudCustomerInstanceOutageKeepsConfigurationAvailableAndResumesItsOriginalTargetAndPosition() throws Exception {
        String suffix=UUID.randomUUID().toString().replace("-","");
        String cluster="cloud-outage-"+suffix,configDb="cloud_cfg_"+suffix,cacheDb="cloud_cache_"+suffix;
        String configUri=CONFIG.getReplicaSetUrl(configDb);
        String cacheUri=CACHE.getReplicaSetUrl(cacheDb);
        cacheUri+=(cacheUri.contains("?")?"&":"?")+"socketTimeoutMS=1500&connectTimeoutMS=1500";
        String sourceUri=DATA.getReplicaSetUrl("cloud_source_"+suffix),targetUri=CACHE.getReplicaSetUrl("cloud_target_"+suffix);
        String targetId=StorageBinding.defaultSourceId(StorageBinding.Mode.CLOUD);
        String input=System.getProperty("tapstate.test.cloud-release-dir",System.getenv("TAPSTATE_TEST_CLOUD_RELEASE_DIR"));
        assertThat(input).as("the original locked Cloud release is required").isNotBlank();
        Path seeds=Path.of(input).resolve("connectors");
        var setup=new StorageSetupScope(cluster,"installation","setup",1);
        var binding=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,cacheUri,cacheDb,null);
        try(var configuration=io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                new MongoConnectionSettings(configUri,null,Duration.ofSeconds(5)),cluster,"installation","setup",1)) {
            var target=new io.tapstate.core.model.SourceResource(targetId,null,"mongodb",Map.of("isUri",true,"uri",targetUri),null,null,null,null);
            var initial=configuration.bindings().commitWithFirstCache(setup,StorageBinding.Mode.CLOUD,binding,target);
            try(var cache=MongoCacheDomain.connect(binding,Duration.ofSeconds(5),"cloud-outage-fixture")) {
                cache.prepareFirstAuth(initial.firstCreation().orElseThrow()); cache.prepareInitialBusiness(initial.preparation(),() -> {});
            }
            configuration.bindings().complete(setup);
        }
        var source=EndpointAddress.uri(sourceUri);var target=EndpointAddress.uri(targetUri);
        try(var mongo=new MongoEndpoints();var cacheClient=MongoClients.create(CACHE.getReplicaSetUrl());
                var configClient=MongoClients.create(configUri);var http=java.net.http.HttpClient.newHttpClient()) {
            mongo.seed(source,TABLE,SeedRows.generated(8));
            var configuration=configClient.getDatabase(configDb);var chains=cacheClient.getDatabase(cacheDb).getCollection("srs_chains");
            Map<String,List<Document>> originalConfiguration;Map<Long,Long> beforeOutage;String acknowledged,originalCookie,claimsBefore;Document originalChain;
            boolean paused=false;
            try {
                var first=RealProcessServer.startCloudFixture(configUri,cluster,seeds);
                try(first) {
                    var control=cloudLogin(first);
                    var resources=resources(sourceUri,false,targetId);
                    resources.put(targetId+".tap.yml",control.artifact(targetId).orElseThrow().canonicalForm());
                    control.apply(resources);control.discoverSchema(SOURCE,"mongodb",Map.of("isUri",true,"uri",sourceUri));
                    control.lifecycle(PIPELINE,LifecycleVerb.START);awaitRows(mongo,source,target,"the Cloud snapshot before its customer instance outage");
                    String snapshotAck=Await.answered("the Cloud snapshot target confirmation",BOUND,() -> control.durablePosition(PIPELINE,TABLE));
                    mongo.update(source,TABLE,Map.of("id",1L),Map.of("seq",11L));awaitRows(mongo,source,target,"live Cloud CDC before the outage");
                    acknowledged=Await.answered("live Cloud CDC target confirmation",BOUND,() -> control.durablePosition(PIPELINE,TABLE).filter(value -> !value.equals(snapshotAck)));
                    originalCookie=control.credential();beforeOutage=rows(mongo,target);originalConfiguration=cloudInstallationFacts(configuration);
                    assertThat(configuration.getCollection("users").countDocuments()).isZero();
                    assertThat(chains.countDocuments()).isOne();originalChain=chains.find().first();assertThat(originalChain.get("checkpoint",Document.class)).isNotEmpty();
                    claimsBefore=claimLeaseReading(cacheClient.getDatabase(cacheDb));
                    CACHE.getDockerClient().pauseContainerCmd(CACHE.getContainerId()).exec();paused=true;
                    mongo.update(source,TABLE,Map.of("id",2L),Map.of("seq",222L));mongo.delete(source,TABLE,Map.of("id",3L));
                    Await.until("the Cloud process to revoke its unavailable customer-cache node session",Duration.ofSeconds(45),
                            () -> first.tail().contains("Node session lost:"),first::tail);
                    assertThat(first.isAlive()).isTrue();assertThat(cloudInstallationFacts(configuration)).isEqualTo(originalConfiguration);
                }
                assertThat(first.isAlive()).isFalse();
                try(var blocked=RealProcessServer.startCloudFixture(configUri,cluster,seeds)) {
                    var entry=http.send(java.net.http.HttpRequest.newBuilder(blocked.baseUrl().resolve("/auth/storage-entry"))
                            .timeout(Duration.ofSeconds(30)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString());
                    assertThat(entry.statusCode()).isEqualTo(200);assertThat(Document.parse(entry.body())).containsEntry("entry","UNAVAILABLE");
                    var exchange=http.send(java.net.http.HttpRequest.newBuilder(blocked.baseUrl().resolve("/auth/exchange?code=unavailable-"+suffix))
                            .timeout(Duration.ofSeconds(30)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString());
                    assertThat(exchange.statusCode()).isEqualTo(503);
                    assertThat(Document.parse(exchange.body())).containsEntry("code",io.tapstate.control.core.ControlError.STORAGE_UNAVAILABLE.code());
                    assertThat(cloudInstallationFacts(configuration)).isEqualTo(originalConfiguration);
                    CACHE.getDockerClient().unpauseContainerCmd(CACHE.getContainerId()).exec();paused=false;
                    assertThat(rows(mongo,target)).isEqualTo(beforeOutage);
                    assertThat(chains.find(new Document("_id",originalChain.get("_id"))).first()).containsEntry("checkpoint",originalChain.get("checkpoint"));
                    String claimsAfter=claimLeaseReading(cacheClient.getDatabase(cacheDb));
                    var control=cloudLogin(blocked);
                    var oldSession=new ControlPlane(blocked.baseUrl());oldSession.useCloudSessionCookie(originalCookie);
                    assertThat(oldSession.storageStatus().get("clusterId")).isEqualTo(cluster);
                    var reported=new java.util.concurrent.atomic.AtomicBoolean();
                    Await.until("the retained Cloud running intent to resume",BOUND,() -> {
                        var observed=control.state(PIPELINE);
                        if(observed.filter(PipelineState.FAILED::equals).isPresent() && reported.compareAndSet(false,true))
                            System.out.println("CLOUD_OUTAGE_FAILED before="+claimsBefore+" after="+claimsAfter+" failure="+control.failure(PIPELINE)+" logs="+control.logs(PIPELINE));
                        return observed.filter(PipelineState.RUNNING::equals).isPresent();
                    },
                            () -> "state="+control.state(PIPELINE)+" failure="+control.failure(PIPELINE)+" logs="+control.logs(PIPELINE));
                    awaitRows(mongo,source,target,"all customer-instance outage changes to resume from the retained Cloud position");
                    String replay=Await.answered("the retained Cloud target position to advance",BOUND,
                            () -> control.durablePosition(PIPELINE,TABLE).filter(value -> !value.equals(acknowledged)));
                    mongo.insert(source,TABLE,List.of(Map.of("id",9L,"seq",999L)));awaitRows(mongo,source,target,"fresh Cloud CDC after customer storage returns");
                    Await.answered("fresh Cloud CDC confirmation",BOUND,() -> control.durablePosition(PIPELINE,TABLE).filter(value -> !value.equals(replay)));
                    assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(TABLE,0L);
                    assertThat(cloudInstallationFacts(configuration)).isEqualTo(originalConfiguration);
                    assertThat(configuration.getCollection("operator_generations").find(new Document("_id",PIPELINE)).first()).containsEntry("stateGeneration",1L);
                    assertThat(configuration.getCollection("users").countDocuments()).isZero();
                }
            } finally { if(paused)CACHE.getDockerClient().unpauseContainerCmd(CACHE.getContainerId()).exec(); }
        }
    }

    private static ControlPlane cloudLogin(RealProcessServer server) {
        var control=new ControlPlane(server.baseUrl());control.exchangeCloudCode("outage-"+UUID.randomUUID());
        Await.until("the native Cloud business runtime to be ready",BOUND,() -> "READY".equals(control.storageStatus().get("businessPhase")),
                () -> String.valueOf(control.storageStatus()));
        return control;
    }
    private static String claimLeaseReading(com.mongodb.client.MongoDatabase cache) {
        return io.tapstate.adapters.mongostore.SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).aggregate(List.of(
                new Document("$match",new Document("resourceType",new Document("$in",List.of("NODE_SESSION","PIPELINE_ACTUATION","CAPTURE")))),
                new Document("$project",new Document("_id",0).append("resourceType",1).append("resourceId",1).append("ownerBootId",1)
                        .append("claimGeneration",1).append("leaseUntil",1).append("observedAt","$$NOW")
                        .append("leased",new Document("$gt",List.of("$leaseUntil","$$NOW"))))))
                .into(new java.util.ArrayList<>()).stream().map(Document::toJson).toList().toString();
    }
    private static Map<String,List<Document>> cloudInstallationFacts(com.mongodb.client.MongoDatabase configuration) {
        var result=new LinkedHashMap<String,List<Document>>();
        for(String name:List.of("cluster_keys","storage_binding","storage_setup","sources")) {
            var facts=configuration.getCollection(name).find().sort(new Document("_id",1)).into(new java.util.ArrayList<>());
            assertThat(facts).as(name).isNotEmpty();
            if(name.equals("cluster_keys"))for(var fact:facts)assertThat(fact.remove("sourceWriteFence")).isInstanceOf(Long.class);
            result.put(name,facts);
        }
        return result;
    }

    private void storageOutage(boolean together) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "outage-" + suffix, configDb = "outage_cfg_" + suffix, cacheDb = "outage_cache_" + suffix;
        String configUri = (together ? CACHE : CONFIG).getReplicaSetUrl(configDb);
        configUri += (configUri.contains("?") ? "&" : "?") + "socketTimeoutMS=1500&connectTimeoutMS=1500";
        String sourceUri = DATA.getReplicaSetUrl("outage_source_" + suffix);
        String targetUri = (together ? CACHE : DATA).getReplicaSetUrl("outage_target_" + suffix), cacheUri = CACHE.getReplicaSetUrl(cacheDb);
        cacheUri += (cacheUri.contains("?") ? "&" : "?") + "socketTimeoutMS=1500&connectTimeoutMS=1500";
        NativeMongoStorageFixture.prepare(configUri, new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                cacheUri, cacheDb, null), targetUri, cluster);
        var source = EndpointAddress.uri(sourceUri); var target = EndpointAddress.uri(targetUri);
        try (var mongo = new MongoEndpoints(); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl());
             var configClient = MongoClients.create(configUri); var http = java.net.http.HttpClient.newHttpClient()) {
            mongo.seed(source, TABLE, SeedRows.generated(8));
            var configuration = configClient.getDatabase(configDb);
            Map<String, List<Document>> originalConfiguration;
            var chains = cacheClient.getDatabase(cacheDb).getCollection("srs_chains");
            Map<Long, Long> beforeOutage;
            String acknowledged;
            Document originalChain;
            boolean paused = false;
            try {
                var first = start(configUri, cluster);
                try (first) {
                    var control = login(first);
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    var resources = resources(sourceUri);
                    resources.put("views.tap.yml", control.artifact("views").orElseThrow().canonicalForm());
                    control.apply(resources); control.discoverSchema(SOURCE, "mongodb", Map.of("isUri", true, "uri", sourceUri));
                    control.lifecycle(PIPELINE, LifecycleVerb.START);
                    awaitRows(mongo, source, target, "the snapshot before a storage outage");
                    String snapshotAck = Await.answered("the initial target confirmation", BOUND,
                            () -> control.durablePosition(PIPELINE, TABLE));
                    mongo.update(source, TABLE, Map.of("id", 1L), Map.of("seq", 11L));
                    awaitRows(mongo, source, target, "live CDC before the storage outage");
                    acknowledged = Await.answered("the live CDC confirmation", BOUND,
                            () -> control.durablePosition(PIPELINE, TABLE).filter(value -> !value.equals(snapshotAck)));
                    beforeOutage = rows(mongo, target);
                    // Source registration and apply legitimately fence the key before the outage begins.
                    originalConfiguration = installationFacts(configuration);
                    assertThat(chains.countDocuments()).isOne(); originalChain = chains.find().first();
                    assertThat(originalChain.get("checkpoint", Document.class)).isNotEmpty();
                    CACHE.getDockerClient().pauseContainerCmd(CACHE.getContainerId()).exec(); paused = true;
                    mongo.update(source, TABLE, Map.of("id", 2L), Map.of("seq", 222L));
                    mongo.delete(source, TABLE, Map.of("id", 3L));
                    Await.until("the original process to revoke its unavailable node session", Duration.ofSeconds(45),
                            () -> first.tail().contains("Node session lost:"), first::tail);
                    assertThat(first.isAlive()).isTrue();
                    if (!together) {
                        assertThat(rows(mongo, target)).isEqualTo(beforeOutage);
                        assertThat(installationFacts(configuration)).isEqualTo(originalConfiguration);
                    }
                }
                assertThat(first.isAlive()).isFalse();
                java.util.function.Consumer<RealProcessServer> verifyResume = resumed -> {
                    var control = login(resumed);
                    awaitRows(mongo, source, target, "all outage changes to resume after normal login");
                    Await.answered("the original target position to advance after restoration", BOUND,
                            () -> control.durablePosition(PIPELINE, TABLE).filter(value -> !value.equals(acknowledged)));
                    mongo.insert(source, TABLE, List.of(Map.of("id", 9L, "seq", 999L)));
                    awaitRows(mongo, source, target, "fresh CDC after storage restoration");
                    assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(TABLE, 0L);
                    assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                    assertThat(installationFacts(configuration)).isEqualTo(originalConfiguration);
                    assertThat(configuration.getCollection("operator_generations").find(new Document("_id", PIPELINE)).first())
                            .containsEntry("stateGeneration", 1L);
                };
                try (var blocked = start(configUri, cluster)) {
                    var entry = http.send(java.net.http.HttpRequest.newBuilder(blocked.baseUrl().resolve("/auth/storage-entry"))
                            .timeout(Duration.ofSeconds(30)).GET().build(), java.net.http.HttpResponse.BodyHandlers.ofString());
                    assertThat(entry.statusCode()).isEqualTo(200);
                    assertThat(Document.parse(entry.body())).containsEntry("entry", "UNAVAILABLE");
                    var login = http.send(java.net.http.HttpRequest.newBuilder(blocked.baseUrl().resolve("/auth/login"))
                            .timeout(Duration.ofSeconds(30)).header("Content-Type", "application/json")
                            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(io.tapstate.core.common.JsonWriter.write(
                                    Map.of("username", NativeMongoStorageFixture.USER, "password", NativeMongoStorageFixture.PASSWORD))))
                            .build(), java.net.http.HttpResponse.BodyHandlers.ofString());
                    assertThat(login.statusCode()).isEqualTo(503);
                    assertThat(Document.parse(login.body())).containsEntry("code", io.tapstate.control.core.ControlError.STORAGE_UNAVAILABLE.code());
                    if (!together) {
                        assertThat(installationFacts(configuration)).isEqualTo(originalConfiguration);
                        assertThat(rows(mongo, target)).isEqualTo(beforeOutage);
                    }
                    CACHE.getDockerClient().unpauseContainerCmd(CACHE.getContainerId()).exec(); paused = false;
                    assertThat(installationFacts(configuration)).isEqualTo(originalConfiguration);
                    assertThat(rows(mongo, target)).isEqualTo(beforeOutage);
                    assertThat(chains.find(new Document("_id", originalChain.get("_id"))).first())
                            .containsEntry("checkpoint", originalChain.get("checkpoint"));
                    if (!together) verifyResume.accept(blocked);
                }
                // A process that could not load CONFIG has no authenticated local context to retry.
                if (together) try (var restored = start(configUri, cluster)) { verifyResume.accept(restored); }
            } finally {
                if (paused) CACHE.getDockerClient().unpauseContainerCmd(CACHE.getContainerId()).exec();
            }
        }
    }

    private static Map<String, List<Document>> installationFacts(com.mongodb.client.MongoDatabase configuration) {
        var result = new LinkedHashMap<String, List<Document>>();
        for (var name : List.of("cluster_keys", "storage_binding", "storage_setup", "op_bootstrap", "users")) {
            var facts = configuration.getCollection(name).find().sort(new Document("_id", 1)).into(new java.util.ArrayList<>());
            assertThat(facts).as(name).isNotEmpty();
            if (name.equals("cluster_keys")) {
                // Normal encrypted connector-note writes also fence the existing key on resume.
                // Its material and identity must remain exact; the write counter is operational state.
                for (var fact : facts) assertThat(fact.remove("sourceWriteFence")).isInstanceOf(Long.class);
            }
            result.put(name, facts);
        }
        return result;
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    @Timeout(180)
    void aNewRuntimeReclaimsTheFrozenPayloadOfAnAlreadyDeletedPipeline(boolean unreadableEarlier) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String cluster = "cleanup-" + suffix, configDb = "cleanup_cfg_" + suffix, cacheDb = "cleanup_cache_" + suffix;
        String configUri = CONFIG.getReplicaSetUrl(configDb);
        prepareStorage(configUri, cacheDb, DATA.getReplicaSetUrl("cleanup_target_" + suffix), cluster);
        var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheDb), cacheDb, null);
        var active = (Runnable) () -> {};
        var owner = new WorkloadOwner("fixture-node", "fixture-boot");
        var ttl = Duration.ofMinutes(2);
        try (var configuration = MongoConfigurationDomain.open(new MongoConnectionSettings(configUri, null, Duration.ofSeconds(5)), cluster);
             var cache = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "retired-fixture")) {
            var business = configuration.bindings().cachePreparation().orElseThrow().businessScope();
            var claims = cache.workloadClaims(business);
            var node = claims.acquire(new WorkloadClaimKey(cluster, WorkloadClaimType.NODE_SESSION, owner.nodeId()), owner, 1, ttl).claim();
            var nodeFence = WorkloadClaimFence.from(node);
            var pipeline = (PipelineResource) new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: retired\nsource: []\n");
            configuration.artifacts().create(pipeline);
            var old = new PipelineIntentScope(cluster, business.bindingVersion(), business.cacheIncarnation(), "retired", 1);
            configuration.intents().compareAndSet(old, 0, new DesiredState("retired", PipelineState.STOPPED, CanonicalHash.of(pipeline), true));
            var grant = ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(old, nodeFence)).creation();
            var preparer = cache.pipelinePreparations(business, active);
            configuration.preparations().complete(preparer.seed(preparer.reserveFirst(grant, ttl), configuration.preparations(), java.time.Instant.now()));
            var claim = claims.advanceExecution(claims.readPipeline(old).orElseThrow().claim(), 1, java.util.Set.of(owner.nodeId())).orElseThrow();
            var location = new OperatorPlacement(old, cacheDb, OperatorPlacement.Resource.JOIN_FACTS, "join", "fact", "facts");
            var cold = cache.operatorState(location, WorkloadClaimFence.from(claim), active);
            for (int index = 0; index < 270; index++) cold.save(location.mapName(), "key-" + index, new byte[]{1, 2});
            var successor = new PipelineIntentScope(cluster, business.bindingVersion(), business.cacheIncarnation(), "retired", 2);
            var request = new PipelineRebuildRequest("fixture-full", configuration.intents().load("retired").orElseThrow(),
                    WorkloadClaimFence.from(claim), 0, PipelinePreparationInputs.of(successor, pipeline, Map.of(), Map.of()));
            var pending = configuration.rebuilds().begin(request);
            var receipt = cache.pipelineRebuilds(business, active).prepare(pending, nodeFence, ttl, java.time.Instant.now());
            configuration.rebuilds().complete(receipt);
            var replacementClaim = WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(successor).orElseThrow().claim(),
                    1, java.util.Set.of(owner.nodeId())).orElseThrow());
            var laterLocation = new OperatorPlacement(successor, cacheDb, OperatorPlacement.Resource.JOIN_FACTS, "join", "fact", "facts");
            cache.operatorState(laterLocation, replacementClaim, active).save(laterLocation.mapName(), "later", new byte[]{9});
            var deletion = configuration.retirements().begin(new PipelineRetirementRequest("fixture-delete", successor,
                    CanonicalHash.of(pipeline), java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(replacementClaim,
                    cache.pipelineStates(successor).read("retired").orElseThrow().epoch()))));
            configuration.retirements().complete(cache.pipelineRetirements(business, active).retire(deletion, nodeFence));
            assertThat(configuration.intents().pipelineIds()).doesNotContain("retired");
            assertThat(cache.retiredPipelinePayloads(business, active).find(old)).isPresent();
            assertThat(claims.release(node)).isTrue();
        }
        try (var client = MongoClients.create(CACHE.getReplicaSetUrl()); var configClient = MongoClients.create(CONFIG.getReplicaSetUrl())) {
            var rows = client.getDatabase(cacheDb).getCollection("join_facts");
            assertThat(rows.countDocuments()).isEqualTo(271);
            var mapping = configClient.getDatabase(configDb).getCollection("operator_generations").find(new Document("_id", "retired")).first();
            assertThat(mapping).isNotNull().containsEntry("phase", "RETIRED").containsEntry("stateGeneration", 2L);
            var ledger = client.getDatabase(cacheDb).getCollection("operator_cleanup");
            if (unreadableEarlier) ledger.updateOne(new Document("_id.stateGeneration", 1L),
                    new Document("$set", new Document("retirement.contentHash", "b".repeat(64))));
            var inventories = ledger.find().sort(new Document("_id", 1)).into(new java.util.ArrayList<>());
            assertThat(inventories).hasSize(2);
            try (var server = start(configUri, cluster)) {
                login(server);
                Await.until("the new runtime to reclaim retained deleted-pipeline payloads in bounded passes", Duration.ofSeconds(20),
                        () -> rows.countDocuments() == (unreadableEarlier ? 270 : 0), () -> "retained rows=" + rows.countDocuments());
                assertThat(rows.countDocuments(new Document("_id.placement.scope.stateGeneration", 2L))).isZero();
                assertThat(configClient.getDatabase(configDb).getCollection("operator_generations").find(new Document("_id", "retired")).first()).isEqualTo(mapping);
                assertThat(client.getDatabase(cacheDb).getCollection("operator_cleanup").find().sort(new Document("_id", 1)).into(new java.util.ArrayList<>())).isEqualTo(inventories);
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
        control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
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
        return resources(sourceUri,false);
    }

    private static Map<String, String> resources(String sourceUri,boolean bounded) {
        return resources(sourceUri,bounded,"views");
    }
    private static Map<String,String> resources(String sourceUri,boolean bounded,String targetId) {
        var resources = new LinkedHashMap<String, String>();
        resources.put("input.tap.yml", """
                version: tapstate/v1
                kind: source
                id: input
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                mode: %s
                tables: [ orders ]
                """.formatted(sourceUri,bounded?"snapshot":"cdc"));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: native_resume
                source: [ { id: input, srs: %s } ]
                settings: { read_mode: %s }
                serve:
                  from: orders
                  sync:
                    - source: %s
                      on_full_load: clear
                """.formatted(!bounded,bounded?"snapshot_only":"snapshot_and_cdc",targetId));
        return resources;
    }

    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    }
}
