package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class CaptureAuthorizationSnapshotIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");

    @Test void renewingTheCaptureDuringAReadCannotExpireTheOriginalProof() throws Exception {
        try (var f = new Fixture(); var calls = Executors.newSingleThreadExecutor()) {
            var original = f.acquire(Duration.ofSeconds(3));
            var fence = WorkloadClaimFence.from(original);
            long count = f.fail(new Document("failCommands", List.of("aggregate"))
                    .append("namespace", f.cacheName + ".workload_claims")
                    .append("blockConnection", true).append("blockTimeMS", 4500));
            try (var authorization = ExecutionAuthorization.scoped(f.scope, f.proofClaims, Duration.ofSeconds(10),
                    System::nanoTime, () -> true)) {
                var future = calls.submit(() -> authorization.requireCapture(f.scope, fence));
                f.admin.getDatabase("admin").runCommand(new Document("waitForFailPoint", "failCommand")
                        .append("timesEntered", count + 1).append("maxTimeMS", 5000));
                var ops = f.admin.getDatabase("admin").runCommand(new Document("aggregate", 1)
                        .append("pipeline", List.of(new Document("$currentOp", new Document("allUsers", true).append("truncateOps", false)),
                                new Document("$match", new Document("appName", MongoConnection.processTag(f.proofProcess))),
                                new Document("$project", new Document("command.aggregate", 1))))
                        .append("cursor", new Document()));
                var matching = ops.get("cursor", Document.class).getList("firstBatch", Document.class).stream().filter(row ->
                        row.get("command") instanceof Document command && "workload_claims".equals(command.get("aggregate"))).toList();
                assertThat(matching).as("the paused operation must be the claim aggregate").hasSize(1);
                var renewed = f.claims.renew(original, Duration.ofSeconds(30)).orElseThrow();
                assertThat(renewed.leaseUntil()).isAfter(original.leaseUntil());
                assertThat(WorkloadClaimFence.from(renewed)).isEqualTo(fence);
                Throwable failure = catchThrowable(() -> future.get(8, TimeUnit.SECONDS));
                var durable = f.claims.read(f.key).orElseThrow();
                assertThat(durable.leased()).isTrue();
                assertThat(WorkloadClaimFence.from(durable.claim())).isEqualTo(fence);
                assertThat(failure).isNull();
                assertThat(future.get()).isEqualTo(fence);
                System.out.printf("CAPTURE_SNAPSHOT_FIXED same_fence=true mongo_live=true remaining_ms=%d old_deadline_passed=%s%n",
                        durable.leaseRemaining().toMillis(), Instant.now().isAfter(original.leaseUntil()));
            } finally { f.clear(); }
        }
    }

    @Test void captureProofUsesOneSnapshotCommandWithoutAReadTransaction() {
        try (var f = new Fixture()) {
            var original = f.acquire(Duration.ofSeconds(30));
            var database = f.admin.getDatabase(f.cacheName);
            database.runCommand(new Document("profile", 2).append("slowms", 0));
            try {
                assertThat(f.proofClaims.read(f.key)).hasValueSatisfying(reading -> {
                    assertThat(reading.leased()).isTrue();
                    assertThat(WorkloadClaimFence.from(reading.claim())).isEqualTo(WorkloadClaimFence.from(original));
                });
                var events = database.runCommand(new Document("find", "system.profile")
                        .append("filter", new Document("appName", MongoConnection.processTag(f.proofProcess)))
                        .append("projection", new Document("command", 1)).append("batchSize", 32))
                        .get("cursor", Document.class).getList("firstBatch", Document.class);
                assertThat(events).hasSize(1);
                var command = events.getFirst().get("command", Document.class);
                assertThat(command.get("aggregate")).isEqualTo("workload_claims");
                assertThat(command.get("readConcern", Document.class).get("level")).isEqualTo("snapshot");
                assertThat(command).doesNotContainKeys("txnNumber", "startTransaction");
                System.out.println("CAPTURE_PROOF commands=1 snapshot=true read_transaction=false");
            } finally { database.runCommand(new Document("profile", 0)); }
        }
    }

    @Test void oneFailedReadCommitDoesNotMeanTheLiveCaptureWasLost() {
        try (var f = new Fixture()) {
            var original = f.acquire(Duration.ofSeconds(30));
            var fence = WorkloadClaimFence.from(original);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.admin.getDatabase(f.cacheName)).updateOne(
                    new Document("resourceType", "CAPTURE").append("resourceId", f.key.resourceId()),
                    new Document("$set", new Document("padding", "x".repeat(70 * 1024))));
            long injected = f.fail(new Document("failCommands", List.of("commitTransaction")).append("errorCode", 91)
                    .append("errorLabels", List.of("TransientTransactionError")));
            try (var authorization = ExecutionAuthorization.scoped(f.scope, f.proofClaims, Duration.ofSeconds(10), System::nanoTime, () -> true)) {
                assertThat(authorization.requireCapture(f.scope, fence)).isEqualTo(fence);
                assertThat(f.claims.read(f.key)).hasValueSatisfying(reading -> assertThat(reading.leased()).isTrue());
                var cleared = f.admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                assertThat(((Number) cleared.get("count")).longValue()).isGreaterThan(injected);
                System.out.println("CAPTURE_TRANSIENT_COMMIT fallback=true injected=true live=true guard=accepted");
            } finally { f.clear(); }
        }
    }

    @Test void actualExpiryAndReplacementOwnerRemainRefused() throws Exception {
        try (var f = new Fixture()) {
            var original = f.acquire(Duration.ofMillis(200));
            var fence = WorkloadClaimFence.from(original);
            Thread.sleep(300);
            assertThat(f.claims.read(f.key)).hasValueSatisfying(reading -> assertThat(reading.leased()).isFalse());
            try (var authorization = ExecutionAuthorization.scoped(f.scope, f.proofClaims, Duration.ofSeconds(10), System::nanoTime, () -> true)) {
                assertThatThrownBy(() -> authorization.requireCapture(f.scope, fence)).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            }
            var replacement = f.claims.acquire(f.key, new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
            assertThat(WorkloadClaimFence.from(replacement)).isNotEqualTo(fence);
            try (var authorization = ExecutionAuthorization.scoped(f.scope, f.proofClaims, Duration.ofSeconds(10), System::nanoTime, () -> true)) {
                assertThatThrownBy(() -> authorization.requireCapture(f.scope, fence)).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                assertThat(authorization.requireCapture(f.scope, WorkloadClaimFence.from(replacement))).isEqualTo(WorkloadClaimFence.from(replacement));
            }
            System.out.println("CAPTURE_FENCES expired=rejected replacement=rejected current_owner=accepted");
        }
    }

    @Test void aRetiredCacheAndDamagedPreparationCannotAuthorizeTheCapture() {
        try (var f = new Fixture()) {
            var original = f.acquire(Duration.ofSeconds(30));
            var fence = WorkloadClaimFence.from(original);
            var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.admin.getDatabase(f.cacheName));
            var filter = new Document("resourceType", "CAPTURE").append("resourceId", f.key.resourceId());
            var saved = table.find(filter).first();
            table.updateOne(filter, new Document("$set", new Document("capturePreparation.scope.cacheIncarnation", UUID.randomUUID().toString())));
            assertThatThrownBy(() -> f.proofClaims.read(f.key)).isInstanceOf(TapstateException.class);
            table.replaceOne(filter, saved);
            SystemCollections.CACHE_IDENTITY.on(f.admin.getDatabase(f.cacheName)).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("businessPhase", "RETIRED")));
            assertThatThrownBy(() -> f.proofClaims.read(f.key)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
            try (var authorization = ExecutionAuthorization.scoped(f.scope, f.proofClaims, Duration.ofSeconds(10), System::nanoTime, () -> true)) {
                assertThatThrownBy(() -> authorization.requireCapture(f.scope, fence)).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            }
            System.out.println("CAPTURE_INTEGRITY mismatched_preparation=rejected retired_cache=rejected");
        }
    }

    static final class Fixture implements AutoCloseable {
        final String suffix = UUID.randomUUID().toString().replace("-", "");
        final String configName = "capture_proof_config_" + suffix;
        final String cacheName = "capture_proof_cache_" + suffix;
        final String proofProcess = "capture-proof-reader-" + suffix;
        final com.mongodb.client.MongoClient admin = MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoConfigurationDomain configuration;
        final MongoCacheDomain cache;
        final MongoCacheDomain proofCache;
        final CacheBusinessScope scope;
        final WorkloadClaimStore claims;
        final WorkloadClaimStore proofClaims;
        final WorkloadClaimKey key;
        Fixture() {
            var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    MONGO.getReplicaSetUrl(cacheName), cacheName, null);
            configuration = MongoConfigurationBootstrap.prepare(new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName), null,
                    Duration.ofSeconds(5)), "cluster-a", "installation", "setup", 1);
            cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "capture-proof-writer-" + suffix);
            var binding = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster-a", "installation", "setup", 1),
                    StorageBinding.Mode.OP, endpoint, new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://target.invalid/views"), null, null, null, null));
            cache.prepareFirstAuth(binding.firstCreation().orElseThrow());
            scope = cache.prepareInitialBusiness(binding.preparation(), () -> {});
            claims = cache.workloadClaims(scope);
            var node = WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, OWNER.nodeId()),
                    OWNER, 1, Duration.ofSeconds(30)).claim());
            var source = new SourceResource("input", null, "mysql", Map.of("host", "source.invalid"), SourceMode.CDC,
                    List.of(TableRef.literal("orders")), null, null);
            var pipeline = (PipelineResource) new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: [{id: input, srs: true}]\nsettings: {read_mode: snapshot_and_cdc}\n");
            configuration.artifacts().saveAll(List.of(source, pipeline));
            var pipelineScope = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), "flow", 1);
            configuration.intents().compareAndSet(pipelineScope, 0, new DesiredState("flow", PipelineState.RUNNING, CanonicalHash.of(pipeline), false));
            var inputs = PipelinePreparationInputs.of(pipelineScope, pipeline, Map.of("input", source), Map.of("input", Set.of("orders")));
            var creation = ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(pipelineScope, node, inputs)).creation();
            var preparation = cache.pipelinePreparations(scope, () -> {});
            var reserved = preparation.reserveFirst(creation, Duration.ofSeconds(30));
            configuration.preparations().complete(preparation.seed(reserved, configuration.preparations(), Instant.now()));
            var pipelineClaim = claims.acquirePipeline(pipelineScope, OWNER, 1, Duration.ofSeconds(30)).claim();
            var running = claims.advanceExecution(pipelineClaim, 1, Set.of(OWNER.nodeId())).orElseThrow();
            var request = new CapturePreparationRequest(pipelineScope, WorkloadClaimFence.from(running), node, "input", inputs);
            var issued = ((CaptureCreationResult.Issued) configuration.capturePreparations().issueFirstCreate(request)).creation();
            var captured = cache.capturePreparations(scope, () -> {}).createFirst(issued);
            key = new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.CAPTURE, captured.preparation().captureId());
            proofCache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), proofProcess);
            proofClaims = proofCache.workloadClaims(scope);
        }
        WorkloadClaim acquire(Duration ttl) { return claims.acquire(key, OWNER, 1, ttl).claim(); }
        long fail(Document data) {
            data.append("appName", MongoConnection.processTag(proofProcess));
            return ((Number) admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1)).append("data", data)).get("count")).longValue();
        }
        void clear() { admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
        public void close() {
            clear();
            ((DeadlineWorkloadClaimStore) proofClaims).awaitQuiescence(System.nanoTime() + Duration.ofSeconds(10).toNanos());
            ((DeadlineWorkloadClaimStore) claims).awaitQuiescence(System.nanoTime() + Duration.ofSeconds(10).toNanos());
            proofCache.close(); cache.close(); configuration.close();
            admin.getDatabase(cacheName).drop(); admin.getDatabase(configName).drop(); admin.close();
        }
    }
}
