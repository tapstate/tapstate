package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;

/** Explicit maintenance over actual version-one CONFIG and CACHE preparations on separate servers. */
@RequiresDocker
class MongoValidationSchemaIT {
    @Container static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    static final Duration TTL = Duration.ofSeconds(30);

    @Test void versionOneOrdinaryOpenKeepsSynchronizationAvailableAndNeverCreatesValidationResources() {
        try (var f = new Fixture(1); var reopened = MongoConfigurationDomain.open(f.settings, "cluster")) {
            assertThat(reopened.schemaVersion()).isEqualTo(1);
            assertThatThrownBy(reopened::validationPreparations).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.cache.validationPreparations(f.scope)).isInstanceOf(TapstateException.class);
            var claims = f.cache.workloadClaims(f.scope);
            var held = claims.acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim(); assertThat(claims.release(held)).isTrue();
            assertThat(f.configSchema().getInteger("installedVersion")).isEqualTo(1);
            assertThat(f.cacheSchema().getInteger("installedVersion")).isEqualTo(1);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("validation_tasks", "validation_reports", "validation_differences");
            assertThat(f.cacheDb.listCollectionNames()).doesNotContain("validation_runs");
        }
    }

    @Test void originalMaintenanceResumesAcrossClientsAndEnablesIndependentBirthWithoutChangingProductionFacts() {
        try (var f = new Fixture(2)) {
            var production = f.productionFacts(); var key = SystemCollections.CLUSTER_KEYS.on(f.configDb).find().first(); var operation = UUID.randomUUID();
            try (var preparation = f.prepare(operation)) {
                assertThat(preparation.advance(1).ready()).isFalse();
                assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, "cluster")).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> f.addPipeline("blocked")).isInstanceOf(TapstateException.class);
                assertThat(f.configSchema().get("validation", Document.class).getLong("initializedPipelines")).isEqualTo(1);
            }
            try (var resumed = f.resume(operation)) {
                resumed.prepareCache(f.cache);
                assertThat(resumed.advance(20).ready()).isTrue();
            }
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster")) {
                assertThat(reopened.schemaVersion()).isEqualTo(2);
                assertThat(f.productionFacts()).isEqualTo(production);
                assertThat(SystemCollections.CLUSTER_KEYS.on(f.configDb).find().first()).isEqualTo(key);
                var subject = reopened.validationPreparations().issue("flow0", f.scope);
                var node = f.cache.workloadClaims(f.scope).acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
                var proof = f.cache.validationPreparations(f.scope).prepare(subject, WorkloadClaimFence.from(node));
                reopened.validationPreparations().complete(subject, proof);
                assertThat(reopened.preparations().read("flow0").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.cacheDb).countDocuments()).isZero();
                assertThat(SystemCollections.CACHE_SOURCE_SCHEMAS.on(f.cacheDb).countDocuments()).isZero();
            }
        }
    }

    @Test void newPipelineMarkerIsPartOfAuthoringAndExistingMarkerIsPreserved() {
        try (var f = new Fixture(0)) {
            f.upgrade(); f.addPipeline("fresh");
            var row = SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).find(new Document("_id", "fresh")).first();
            var marker = MongoValidationPreparationFacts.decode(row.get("validationPreparation", Document.class), "cluster", "fresh");
            assertThat(marker.phase()).isEqualTo(MongoValidationPreparationFacts.Phase.UNASSIGNED);
            f.configuration.artifacts().save(new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: fresh\nsource: [input]\n"));
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).find(new Document("_id", "fresh")).first().get("validationPreparation"))
                    .isEqualTo(row.get("validationPreparation"));
        }
    }

    @Test void aDifferentOperationOrMissingOriginalCannotResumeOrAdoptAnUpgrade() {
        try (var f = new Fixture(1)) {
            var operation = UUID.randomUUID();
            assertThatThrownBy(() -> f.resume(operation)).isInstanceOf(TapstateException.class);
            try (var preparation = f.prepare(operation)) { preparation.advance(1); }
            var before = f.configSchema();
            assertThatThrownBy(() -> f.resume(UUID.randomUUID())).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema()).isEqualTo(before);
            SystemCollections.SCHEMA_MIGRATIONS.on(f.configDb).deleteOne(new Document("_id", "config"));
            assertThatThrownBy(() -> f.prepare(operation)).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema()).isNull();
        }
    }

    @Test void aLostMarkerDuringPreparationIsNotSilentlyBackfilled() {
        try (var f = new Fixture(2)) {
            var operation = UUID.randomUUID();
            try (var preparation = f.prepare(operation)) { preparation.advance(1); }
            SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).updateOne(new Document("validationPreparation", new Document("$exists", true)),
                    new Document("$unset", new Document("validationPreparation", "")));
            try (var resumed = f.resume(operation)) {
                assertThatThrownBy(() -> resumed.advance(20)).isInstanceOf(TapstateException.class);
            }
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).countDocuments(new Document("validationPreparation", new Document("$exists", true)))).isZero();
            assertThat(f.configSchema().get("validation", Document.class).getString("phase")).isEqualTo("PREPARING");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"live", "malformed"})
    void liveOrMalformedWriterLeasesPreventCachePreparationAndTheOriginalOperationCanContinueAfterRelease(String lease) {
        try (var f = new Fixture(1)) {
            var claims = f.cache.workloadClaims(f.scope); var node = claims.acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
            if (lease.equals("malformed")) SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb).updateOne(
                    new Document("_id", MongoWorkloadClaimStore.id(f.nodeKey())), new Document("$set", new Document("leaseUntil", new org.bson.types.MaxKey())));
            var operation = UUID.randomUUID();
            assertThatThrownBy(() -> f.prepare(operation)).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema().getInteger("installedVersion")).isEqualTo(1);
            if (lease.equals("malformed")) SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb).updateOne(
                    new Document("_id", MongoWorkloadClaimStore.id(f.nodeKey())), new Document("$set", new Document("leaseUntil", Date.from(node.leaseUntil()))));
            assertThat(claims.release(node)).isTrue();
            try (var preparation = f.prepare(operation)) {
                var late = claims.acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
                assertThatThrownBy(() -> preparation.prepareCache(f.cache)).isInstanceOf(TapstateException.class);
                assertThat(f.cacheSchema().getInteger("installedVersion")).isEqualTo(1);
                assertThat(f.cacheDb.listCollectionNames()).doesNotContain("validation_runs");
                assertThat(claims.release(late)).isTrue();
                preparation.prepareCache(f.cache);
                assertThat(preparation.advance(20).ready()).isTrue();
            }
            assertThat(f.cacheSchema().getInteger("installedVersion")).isEqualTo(2);
        }
    }

    @Test void versionTwoMissingCollectionsOrIndexesNeverCauseDdlOnOpenOrReplay() {
        try (var f = new Fixture(1)) {
            var operation = f.upgrade(); SystemCollections.VALIDATION_REPORTS.on(f.configDb).drop();
            assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, "cluster")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.resume(operation)).isInstanceOf(TapstateException.class);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("validation_reports");
        }
        try (var f = new Fixture(1)) {
            var operation = f.upgrade(); SystemCollections.VALIDATION_RUNS.on(f.cacheDb).dropIndex("validation_runs_pipeline");
            var before = SystemCollections.CACHE_IDENTITY.on(f.cacheDb).find().first();
            try (var resumed = f.resume(operation)) {
                assertThatThrownBy(() -> resumed.prepareCache(f.cache)).isInstanceOf(TapstateException.class);
            }
            assertThat(SystemCollections.CACHE_IDENTITY.on(f.cacheDb).find().first()).isEqualTo(before);
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).listIndexes()).noneMatch(row -> "validation_runs_pipeline".equals(row.get("name")));
        }
    }

    @Test void aVersionOnePartialValidationNamespaceIsUnknownAndNeverAdopted() {
        try (var f = new Fixture(1)) {
            try (var session = f.configClient.startSession()) { SystemCollections.VALIDATION_REPORTS.prepareCollectionOn(f.configDb, session); }
            assertThatThrownBy(() -> f.prepare(UUID.randomUUID())).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema().getInteger("installedVersion")).isEqualTo(1);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("validation_tasks", "validation_differences");
        }
    }

    @Test void twoMaintenanceClientsCountAndInitializeEachOriginalMappingOnceInBoundedBatches() throws Exception {
        try (var f = new Fixture(25); var workers = Executors.newFixedThreadPool(2)) {
            var operation = UUID.randomUUID();
            try (var a = f.prepare(operation); var b = f.resume(operation)) {
                var gate = new CountDownLatch(1);
                var first = workers.submit(() -> { gate.await(); return a.advance(20); });
                var second = workers.submit(() -> { gate.await(); return b.advance(20); }); gate.countDown();
                first.get(20, TimeUnit.SECONDS); second.get(20, TimeUnit.SECONDS);
                assertThat(a.advance(20).ready()).isTrue(); a.prepareCache(f.cache);
                var rows = SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).find().projection(new Document("validationPreparation", 1)).into(new ArrayList<>());
                assertThat(rows).hasSize(25);
                assertThat(rows.stream().map(row -> row.get("validationPreparation", Document.class).getString("generation")).distinct()).hasSize(25);
                assertThat(f.configSchema().get("validation", Document.class).getLong("initializedPipelines")).isEqualTo(25);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"reopen", "existing", "authoring"})
    void foreignClusterReadinessCannotOpenOrIssueThroughAnExistingHandle(String entry) {
        try (var f = new Fixture(1)) {
            f.upgrade();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster")) {
                var preparations = reopened.validationPreparations();
                assertThat(preparations.read("flow0").phase()).isEqualTo(MongoValidationPreparationFacts.Phase.UNASSIGNED);
                SystemCollections.SCHEMA_MIGRATIONS.on(f.configDb).updateOne(new Document("_id", "config"),
                        new Document("$set", new Document("validation.scope.clusterId", "foreign")));
                assertThatThrownBy(() -> {
                    if (entry.equals("reopen")) try (var ignored = MongoConfigurationDomain.open(f.settings, "cluster")) {}
                    else if (entry.equals("existing")) preparations.issue("flow0", f.scope);
                    else f.addPipeline("foreign-authoring");
                }).isInstanceOf(TapstateException.class);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"new-claims", "existing-claims", "new-control", "existing-control", "existing-config"})
    void ordinaryNativeAccessRejectsMissingDeclaredIndexesWithoutRepair(String entry) {
        try (var f = new Fixture(1)) {
            f.upgrade();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster")) {
                var preparations = reopened.validationPreparations(); var permission = preparations.issue("flow0", f.scope);
                var generic = f.cache.workloadClaims(f.scope); var node = generic.acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
                var complete = preparations.complete(permission, f.cache.validationPreparations(f.scope).prepare(permission, WorkloadClaimFence.from(node)));
                var claims = f.cache.validationClaims(complete); var control = f.cache.validationControl(complete);
                var key = ValidationClaims.key("cluster", "flow0"); var acquired = claims.acquire(key, f.nodeOwner(), 1, TTL).claim();
                var executing = claims.advanceExecution(acquired, 1, Set.of(f.nodeOwner().nodeId())).orElseThrow();
                var run = f.reserveRun(reopened, complete, executing, "task", "run");
                if (entry.equals("existing-config")) SystemCollections.VALIDATION_DIFFERENCES.on(f.configDb).dropIndex("validation_differences_report");
                else SystemCollections.VALIDATION_RUNS.on(f.cacheDb).dropIndex("validation_runs_pipeline");
                assertThatThrownBy(() -> {
                    switch (entry) {
                        case "new-claims" -> f.cache.validationClaims(complete).acquire(key, f.nodeOwner(), 1, TTL);
                        case "existing-claims" -> claims.renew(executing, TTL);
                        case "new-control" -> f.cache.validationControl(complete).join(run);
                        case "existing-control" -> control.join(run);
                        default -> preparations.read("flow0");
                    }
                }).isInstanceOf(TapstateException.class);
                if (entry.equals("existing-config")) assertThat(SystemCollections.VALIDATION_DIFFERENCES.on(f.configDb).listIndexes())
                        .noneMatch(row -> "validation_differences_report".equals(row.get("name")));
                else assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).listIndexes())
                        .noneMatch(row -> "validation_runs_pipeline".equals(row.get("name")));
            }
        }
    }

    @Test void anOriginalRetirementCanStillFinishAfterMaintenanceRefusesItsInFlightMapping() {
        try (var f = new Fixture(0)) {
            var definition = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: retiring\nsource: []\n");
            f.configuration.artifacts().create(definition);
            var scope = new PipelineIntentScope("cluster", f.scope.bindingVersion(), f.scope.cacheIncarnation(), "retiring", 1);
            var retirements = f.configuration.retirements();
            var pending = retirements.begin(new PipelineRetirementRequest(UUID.randomUUID().toString(), scope,
                    io.tapstate.core.model.canonical.CanonicalHash.of(definition), Optional.empty()));
            assertThat(pending.privateScopesCount()).isZero();
            var before = f.configSchema();
            assertThatThrownBy(() -> { try (var ignored = f.prepare(UUID.randomUUID())) {} }).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema()).isEqualTo(before);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("validation_tasks", "validation_reports", "validation_differences");
            var claims = f.cache.workloadClaims(f.scope); var node = claims.acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
            var receipt = f.cache.pipelineRetirements(f.scope, () -> {}).retire(pending, WorkloadClaimFence.from(node));
            retirements.complete(receipt);
            assertThat(f.configuration.artifacts().get("retiring")).isEmpty();
            assertThat(f.configuration.pipelineStates().read("retiring").orElseThrow().phase()).isEqualTo(PipelineStateMapping.Phase.RETIRED);
            assertThat(claims.release(node)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"RETIRING", "REBUILDING"})
    void unfinishedMappingPhasesAreRejectedBeforeTheSchemaTransition(String phase) {
        try (var f = new Fixture(1)) {
            SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).updateOne(new Document("_id", "flow0"), new Document("$set", new Document("phase", phase)));
            var before = f.configSchema();
            assertThatThrownBy(() -> { try (var ignored = f.prepare(UUID.randomUUID())) {} }).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema()).isEqualTo(before);
            try (var session = f.configClient.startSession()) { session.withTransaction(() -> {
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(f.configDb)); return null;
            }); }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"admission", "completion"})
    void schemaTwoRetirementRequiresTheOriginalValidationClosingGate(String stage) {
        try (var f = new Fixture(0)) {
            f.upgrade();
            var definition = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: deleting\nsource: []\n");
            f.configuration.artifacts().create(definition);
            var scope = new PipelineIntentScope("cluster", f.scope.bindingVersion(), f.scope.cacheIncarnation(), "deleting", 1);
            var retirements = f.configuration.retirements();
            var pending = retirements.begin(new PipelineRetirementRequest(UUID.randomUUID().toString(), scope,
                    io.tapstate.core.model.canonical.CanonicalHash.of(definition), Optional.empty()));
            if (stage.equals("admission")) {
                assertThat(f.configuration.validationPreparations().read("deleting").closingOperationId()).isNotNull();
            } else {
                var claims = f.cache.workloadClaims(f.scope); var node = claims.acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
                var receipt = f.cache.pipelineRetirements(f.scope, () -> {}).retire(pending, WorkloadClaimFence.from(node));
                assertThatThrownBy(() -> retirements.complete(receipt)).isInstanceOf(TapstateException.class);
                assertThat(f.configuration.artifacts().get("deleting")).isPresent();
            }
        }
    }

    @Test void unusedValidationRetirementNeedsNoCacheViewAndSameIdRecreationGetsANewGeneration() {
        try (var f = new Fixture(0)) {
            f.upgrade();
            var definition = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: unused\nsource: []\n");
            f.configuration.artifacts().create(definition);
            var previous = f.configuration.validationPreparations().read("unused");
            var scope = new PipelineIntentScope("cluster", f.scope.bindingVersion(), f.scope.cacheIncarnation(), "unused", 1);
            var retirements = f.configuration.retirements();
            var pending = retirements.begin(new PipelineRetirementRequest(UUID.randomUUID().toString(), scope,
                    io.tapstate.core.model.canonical.CanonicalHash.of(definition), Optional.empty()));
            assertThat(f.configuration.retireValidation(pending, null, null, () -> {})).isTrue();
            assertThat(f.configuration.validationPreparations().read("unused").phase()).isEqualTo(MongoValidationPreparationFacts.Phase.CLOSED);
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments()).isZero();
            var node = f.cache.workloadClaims(f.scope).acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
            var receipt = f.cache.pipelineRetirements(f.scope, () -> {}).retire(pending, WorkloadClaimFence.from(node));
            retirements.complete(receipt);
            f.configuration.artifacts().create(definition);
            var fresh = f.configuration.validationPreparations().read("unused");
            assertThat(fresh.generation()).isNotEqualTo(previous.generation());
            assertThat(fresh.phase()).isEqualTo(MongoValidationPreparationFacts.Phase.UNASSIGNED);
            assertThat(fresh.previous()).isNull();
        }
    }

    @Test void deletionClosesTheOriginalReportRevokesItsOwnerAndPreservesHistoryForSameIdReplacement() {
        try (var f = new Fixture(1)) {
            f.upgrade(); var preparations = f.configuration.validationPreparations();
            var first = preparations.issue("flow0", f.scope);
            var generic = f.cache.workloadClaims(f.scope); var nodeA = generic.acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
            var complete = preparations.complete(first, f.cache.validationPreparations(f.scope).prepare(first, WorkloadClaimFence.from(nodeA)));
            var claims = f.cache.validationClaims(complete); var key = ValidationClaims.key("cluster", "flow0");
            var acquired = claims.acquire(key, f.nodeOwner(), 1, TTL).claim();
            var executing = claims.advanceExecution(acquired, 1, Set.of(f.nodeOwner().nodeId())).orElseThrow();
            var run = f.reserveRun(f.configuration, complete, executing, "task", "run");
            var control = f.cache.validationControl(complete); assertThat(control.join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
            assertThat(f.configuration.validationAdmissions(complete).open(run.admission())).isEqualTo(ValidationReportAdmission.Birth.CREATED);
            var definition = f.configuration.artifacts().get("flow0").orElseThrow();
            var scope = new PipelineIntentScope("cluster", f.scope.bindingVersion(), f.scope.cacheIncarnation(), "flow0", 1);
            var retirements = f.configuration.retirements();
            var pending = retirements.begin(new PipelineRetirementRequest(UUID.randomUUID().toString(), scope,
                    io.tapstate.core.model.canonical.CanonicalHash.of(definition), Optional.empty()));
            var nodeB = generic.acquire(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node-b"), new WorkloadOwner("node-b", "boot-b"), 1, TTL).claim();
            assertThat(f.configuration.retireValidation(pending, f.cache, WorkloadClaimFence.from(nodeB), () -> {})).isTrue();
            var closed = preparations.read("flow0");
            assertThat(closed.phase()).isEqualTo(MongoValidationPreparationFacts.Phase.CLOSED);
            assertThat(closed.previous().claimGeneration()).isGreaterThan(executing.claimGeneration());
            assertThat(closed.previous().executionGeneration()).isGreaterThan(executing.executionGeneration());
            var ended = SystemCollections.VALIDATION_RUNS.on(f.cacheDb).find(new Document("_id", MongoValidationControlStore.runId(run))).first();
            assertThat(ended).containsEntry("state", "FAILED").containsEntry("retired", true);
            assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).find(new Document("_id", MongoValidationAdmissionStore.reportId(run.identity()))).first())
                    .containsEntry("closed", true);
            assertThatThrownBy(() -> claims.renew(executing, TTL)).isInstanceOf(TapstateException.class);
            var receipt = f.cache.pipelineRetirements(f.scope, () -> {}).retire(pending, WorkloadClaimFence.from(nodeB));
            for (var notes : f.configuration.requiredNotes().retirements(scope, pending.request().operationId())) {
                assertThat(notes.origin()).isEmpty(); notes.completeAfterVerifiedRelease();
            }
            retirements.complete(receipt); f.configuration.artifacts().create(definition);
            var replaced = preparations.read("flow0");
            assertThat(replaced.generation()).isNotEqualTo(closed.generation()); assertThat(replaced.previous()).isEqualTo(closed.previous());
            var next = preparations.issue("flow0", f.scope);
            preparations.complete(next, f.cache.validationPreparations(f.scope).prepare(next, WorkloadClaimFence.from(nodeB)));
            var current = f.cache.validationClaims(next).acquire(key, nodeB.owner(), 1, TTL).claim();
            assertThat(current.claimGeneration()).isGreaterThan(closed.previous().claimGeneration());
            var oldTask = run.admission().task();
            var stale = new ValidationReportAdmission.Start(oldTask, 1, 2, UUID.randomUUID().toString(),
                    new io.tapstate.core.validation.ValidationRunIdentity(ValidationClaims.owner("flow0", executing), "task", "new-run",
                            io.tapstate.core.validation.ValidationMode.FULL, "spec", 1, "rules"));
            assertThat(f.configuration.validationAdmissions(next).reserve(stale)).isEqualTo(ValidationReportAdmission.Reservation.CLOSED);
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient configClient = MongoClients.create(CONFIG.getReplicaSetUrl()), cacheClient = MongoClients.create(CACHE.getReplicaSetUrl());
        final String suffix = UUID.randomUUID().toString().replace("-", "");
        final MongoDatabase configDb = configClient.getDatabase("schema_config_" + suffix), cacheDb = cacheClient.getDatabase("schema_cache_" + suffix);
        final MongoConnectionSettings settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configDb.getName()), null, Duration.ofSeconds(5));
        final StorageBinding.Connection endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheDb.getName()), cacheDb.getName(), null);
        final MongoConfigurationDomain configuration = MongoConfigurationBootstrap.prepare(settings, "cluster", "installation", "setup", 1);
        final MongoCacheDomain cache;
        final CacheBusinessScope scope;
        Fixture(int pipelines) {
            var master = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://business.invalid/master"), null, null, null, null);
            var binding = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster", "installation", "setup", 1), StorageBinding.Mode.OP, endpoint, master);
            scope = binding.preparation().businessScope();
            cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "validation-schema-fixture");
            cache.prepareFirstAuth(binding.firstCreation().orElseThrow());
            cache.prepareInitialBusiness(binding.preparation(), () -> {});
            configuration.artifacts().create(new SourceResource("input", null, "mongodb", Map.of("uri", "mongodb://business.invalid/source"), null, null, null, null));
            for (int index = 0; index < pipelines; index++) addPipeline("flow" + index);
        }
        void addPipeline(String id) { configuration.artifacts().create(new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: " + id + "\nsource: [input]\n")); }
        MongoConfigurationDomain.ValidationSchemaPreparation prepare(UUID operation) { return MongoConfigurationDomain.prepareValidationSchema(settings, "cluster", operation); }
        MongoConfigurationDomain.ValidationSchemaPreparation resume(UUID operation) { return MongoConfigurationDomain.resumeValidationSchema(settings, "cluster", operation); }
        UUID upgrade() {
            var operation = UUID.randomUUID();
            try (var preparation = prepare(operation)) { preparation.prepareCache(cache); while (!preparation.advance(20).ready()) {} }
            return operation;
        }
        Document configSchema() { return SystemCollections.SCHEMA_MIGRATIONS.on(configDb).find(new Document("_id", "config")).first(); }
        Document cacheSchema() { return SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(cacheDb).find(new Document("_id", "business")).first(); }
        WorkloadOwner nodeOwner() { return new WorkloadOwner("node", "boot"); }
        WorkloadClaimKey nodeKey() { return new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"); }
        ValidationControlStore.Run reserveRun(MongoConfigurationDomain context, MongoValidationPreparations.Permission original, WorkloadClaim owner, String task, String run) {
            var taskIdentity = new ValidationReportAdmission.Task("cluster", original.subject().pipelineId(), task, UUID.randomUUID());
            SystemCollections.VALIDATION_TASKS.on(configDb).insertOne(new Document("_id", MongoValidationAdmissionStore.taskId(taskIdentity))
                    .append("formatVersion", 1).append("taskGeneration", taskIdentity.generation().toString()).append("revision", 1L).append("closed", false)
                    .append("modes", new Document("full", new Document("nextStartSequence", 1L)).append("incremental", new Document("nextStartSequence", 1L))));
            var identity = new io.tapstate.core.validation.ValidationRunIdentity(ValidationClaims.owner(original.subject().pipelineId(), owner), task, run,
                    io.tapstate.core.validation.ValidationMode.FULL, "spec", 1, "rules");
            var start = new ValidationReportAdmission.Start(taskIdentity, 1, 1, UUID.randomUUID().toString(), identity);
            assertThat(context.validationAdmissions(original).reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
            return new ValidationControlStore.Run(identity, 1, start);
        }
        List<Document> productionFacts() {
            var rows = SystemCollections.OPERATOR_GENERATIONS.on(configDb).find().sort(new Document("_id", 1)).into(new ArrayList<>());
            rows.forEach(row -> row.remove("validationPreparation")); return rows;
        }
        @Override public void close() { try { configDb.drop(); cacheDb.drop(); } finally { configuration.close(); cache.close(); configClient.close(); cacheClient.close(); } }
    }
}
