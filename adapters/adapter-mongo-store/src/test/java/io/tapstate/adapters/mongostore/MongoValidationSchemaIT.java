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
            for (int unused = 0; unused < 2; unused++) {
                var unusedDeletion = f.beginRetirement();
                assertThat(f.configuration.retireValidation(unusedDeletion, null, null, () -> {})).isTrue();
                assertThat(preparations.read("flow0").previous()).isEqualTo(closed.previous());
                f.completeRetirement(unusedDeletion, nodeB);
                f.configuration.artifacts().create(definition);
                assertThat(preparations.read("flow0").previous()).isEqualTo(closed.previous());
            }
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retirementCancelsAnUnbornReportButRefusesToHideTheLossOfABornReport(boolean born) {
        try (var f = new Fixture(1)) {
            var started = f.startValidation(1, born); var run = started.runs().getFirst();
            var admission = new MongoValidationAdmissionStore(f.configClient, f.configDb, "cluster", () -> {});
            if (born) SystemCollections.VALIDATION_REPORTS.on(f.configDb)
                    .deleteOne(new Document("_id", MongoValidationAdmissionStore.reportId(run.identity())));
            var pending = f.beginRetirement();
            if (born) {
                var rows = SystemCollections.VALIDATION_RUNS.on(f.cacheDb).find().into(new ArrayList<>());
                assertThatThrownBy(() -> f.configuration.retireValidation(pending, f.cache, WorkloadClaimFence.from(started.node()), () -> {}))
                        .isInstanceOf(TapstateException.class).hasMessageContaining("original-report");
                assertThat(SystemCollections.VALIDATION_TASKS.on(f.configDb).find().first()).containsEntry("closed", false);
                assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).find().into(new ArrayList<>())).isEqualTo(rows);
                assertThat(f.configuration.artifacts().get("flow0")).isPresent();
                assertThat(f.configuration.validationPreparations().read("flow0").phase()).isNotEqualTo(MongoValidationPreparationFacts.Phase.CLOSED);
            } else {
                assertThat(f.configuration.retireValidation(pending, f.cache, WorkloadClaimFence.from(started.node()), () -> {})).isTrue();
                assertThat(admission.open(run.admission())).isEqualTo(ValidationReportAdmission.Birth.CLOSED);
                assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).countDocuments()).isZero();
            }
        }
    }

    @Test void originalDeletionResumesAfterConcurrentCleanupTenuresAndAnInterruptedRunBatch() throws Exception {
        try (var f = new Fixture(1); var workers = Executors.newFixedThreadPool(2)) {
            var started = f.startValidation(2, true); var pending = f.beginRetirement();
            var preparation = f.configuration.validationPreparations(); var closing = preparation.forRetirement(pending);
            assertThat(preparation.closeReports(closing, 20)).isTrue();
            var controls = f.cache.validationPreparations(f.scope); controls.cancel(closing, WorkloadClaimFence.from(started.node()));
            var nodeB = f.node("node-b"); var nodeC = f.node("node-c"); var gate = new CountDownLatch(1);
            var first = workers.submit(() -> { gate.await(); return controls.cleanupOwner(closing, WorkloadClaimFence.from(nodeB), TTL); });
            var second = workers.submit(() -> { gate.await(); return controls.cleanupOwner(closing, WorkloadClaimFence.from(nodeC), TTL); }); gate.countDown();
            var a = first.get(10, TimeUnit.SECONDS); var b = second.get(10, TimeUnit.SECONDS);
            assertThat(a.claimGeneration()).isNotEqualTo(b.claimGeneration());
            var newest = a.claimGeneration() > b.claimGeneration() ? a : b;
            var obsolete = a.claimGeneration() < b.claimGeneration() ? a : b;
            var control = f.cache.validationControl(closing); var run = started.runs().getFirst();
            assertThatThrownBy(() -> control.terminateLostOwner(new ValidationControlStore.LostOwner(
                    ValidationClaims.owner("flow0", obsolete), run, run.report()))).isInstanceOf(TapstateException.class);
            assertThat(control.terminateLostOwner(new ValidationControlStore.LostOwner(
                    ValidationClaims.owner("flow0", newest), run, run.report()))).isEqualTo(ValidationControlStore.Mutation.APPLIED);
            // A different client resumes the exact durable operation after one run was already retired.
            try (var resumed = MongoConfigurationDomain.open(f.settings, "cluster")) {
                var original = resumed.retirements().pending(pending.request().scope(), pending.request().operationId()).orElseThrow();
                assertThat(resumed.retireValidation(original, f.cache, WorkloadClaimFence.from(nodeB), () -> {})).isTrue();
                assertThat(resumed.retireValidation(original, f.cache, WorkloadClaimFence.from(nodeB), () -> {})).isTrue();
                assertThat(resumed.validationPreparations().read("flow0").previous().claimGeneration()).isGreaterThan(newest.claimGeneration());
            }
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments(new Document("state", "FAILED").append("retired", true))).isEqualTo(2);
            assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).countDocuments(new Document("closed", true))).isEqualTo(2);
        }
    }

    @Test void moreThanTwentyOriginalTasksAndRunsAreRetiredAcrossBoundedRetrySteps() {
        try (var f = new Fixture(1)) {
            var started = f.startValidation(21, true); var pending = f.beginRetirement();
            var fence = WorkloadClaimFence.from(started.node());
            assertThat(f.configuration.retireValidation(pending, f.cache, fence, () -> {})).isFalse();
            assertThat(SystemCollections.VALIDATION_TASKS.on(f.configDb).countDocuments(new Document("closed", true))).isEqualTo(20);
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments(new Document("retired", true))).isZero();
            assertThat(f.configuration.retireValidation(pending, f.cache, fence, () -> {})).isFalse();
            assertThat(SystemCollections.VALIDATION_TASKS.on(f.configDb).countDocuments(new Document("closed", true))).isEqualTo(21);
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments(new Document("retired", true))).isEqualTo(20);
            assertThat(f.configuration.retireValidation(pending, f.cache, fence, () -> {})).isTrue();
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments(new Document("retired", true))).isEqualTo(21);
            assertThat(f.configuration.validationPreparations().read("flow0").phase()).isEqualTo(MongoValidationPreparationFacts.Phase.CLOSED);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"complete", "schema-preparing", "schema-ready"})
    void explicitClusterReplacementClosesOldRightsAndOnlyCompletesANewValidationScope(String interruption) {
        try (var f = new Fixture(1)) {
            f.configuration.bindings().complete(new StorageSetupScope("cluster", "installation", "setup", 1));
            f.configuration.firstAdmin().create(f.configuration.firstAdmin().load().orElseThrow().scope(), new User("admin", "fixture-hash", "admin"));
            var started = f.startValidation(1, true); var run = started.runs().getFirst();
            var preparations = f.configuration.validationPreparations(); var original = preparations.issue("flow0", f.scope);
            var old = preparations.read("flow0"); var admissions = f.configuration.validationAdmissions(original);
            f.cacheDb.drop();
            try (var target = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-explicit-recovery")) {
                var auth = f.configuration.prepareAuthenticationRecovery(target, () -> {});
                assertThat(auth.businessScope()).isNotEqualTo(f.scope);
                assertThat(preparations.read("flow0")).isEqualTo(old);
                assertThatThrownBy(() -> admissions.open(run.admission())).isInstanceOf(TapstateException.class);
                var hashes = new TreeMap<String, String>();
                f.configuration.artifacts().list().forEach(resource -> hashes.put(resource.id(), io.tapstate.core.model.canonical.CanonicalHash.of(resource)));
                var manifest = new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of()),
                        Set.of("flow0"), hashes, CacheRecoveryStore.clusterDependencyHash(auth.authScope(), f.configuration.cacheRecoveries().inspectCluster(auth.authScope())),
                        f.configuration.configurationRevision());
                var commands = f.configuration.recoveryCommands(); var confirmed = commands.confirm(commands.plan(auth.authScope(), manifest, null, null));
                f.configuration.cacheRecoveries().stageCluster(confirmed);
                var running = commands.current(auth.authScope()).orElseThrow();
                assertThat(preparations.read("flow0").closingOperationId()).isEqualTo(UUID.fromString(running.operationId()));
                assertThatThrownBy(() -> commands.complete(running)).isInstanceOf(TapstateException.class);
                if (!interruption.equals("complete")) {
                    target.prepareConfirmedInitialBusiness(new CachePreparation(running.operationId(), auth.authScope()), () -> {});
                    if (interruption.equals("schema-preparing")) {
                        new MongoCacheIdentity(f.cacheClient, f.cacheDb, auth.authScope())
                                .beginValidationSchema(auth.businessScope(), running.operationId(), () -> {});
                        assertThat(f.cacheSchema().get("validation", Document.class)).containsEntry("phase", "PREPARING");
                    } else target.prepareValidationSchema(new CachePreparation(running.operationId(), auth.authScope()), running.operationId());
                    try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster");
                            var resumedCache = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-recovery-resume")) {
                        assertThatCode(() -> reopened.prepareStoppedRecovery(resumedCache, running, () -> {})).doesNotThrowAnyException();
                    }
                } else f.configuration.prepareStoppedRecovery(target, running, () -> {});
                assertThat(SystemCollections.VALIDATION_TASKS.on(f.configDb).find().first()).containsEntry("closed", false);
                assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).find().first()).containsEntry("closed", true);
                assertThatThrownBy(() -> preparations.issue("flow0", auth.businessScope())).isInstanceOf(TapstateException.class);
                assertThat(commands.complete(running).phase()).isEqualTo(StorageRecoveryCommands.Phase.COMPLETE);
                var replacement = preparations.read("flow0");
                assertThat(replacement.phase()).isEqualTo(MongoValidationPreparationFacts.Phase.UNASSIGNED);
                assertThat(replacement.generation()).isNotEqualTo(old.generation());
                assertThat(replacement.previous()).isNull();
                assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments()).isZero();
                assertThat(f.configuration.preparations().read("flow0").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                assertThatThrownBy(() -> original.requireComplete()).isInstanceOf(TapstateException.class);
                var next = preparations.issue("flow0", auth.businessScope());
                var node = target.workloadClaims(auth.businessScope()).acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
                preparations.complete(next, target.validationPreparations(auth.businessScope()).prepare(next, WorkloadClaimFence.from(node)));
                assertThat(preparations.read("flow0").phase()).isEqualTo(MongoValidationPreparationFacts.Phase.COMPLETE);
                assertThat(commands.complete(running).phase()).isEqualTo(StorageRecoveryCommands.Phase.COMPLETE);
                assertThat(preparations.read("flow0").generation()).isEqualTo(replacement.generation());
            }
        }
    }

    @Test void emptyInitialCloudRecoveryPreparesTheNewValidationSchemaBeforeCompletion() {
        try (var f = new Fixture(0, StorageBinding.Mode.CLOUD)) {
            f.upgrade(); f.cacheDb.drop();
            try (var target = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-empty-cloud-recovery")) {
                var auth = f.configuration.prepareAuthenticationRecovery(target, () -> {});
                var source = f.configuration.initialCloudRecoverySource(auth.authScope());
                var manifest = new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of()),
                        Set.of(), Map.of(source.id(), io.tapstate.core.model.canonical.CanonicalHash.of(source)),
                        CacheRecoveryStore.clusterDependencyHash(auth.authScope(), List.of()), f.configuration.configurationRevision());
                var commands = f.configuration.recoveryCommands(); var running = commands.start(commands.confirm(commands.plan(auth.authScope(), manifest, null, null)));
                f.configuration.prepareInitialCloudRecovery(target, running, () -> {});
                assertThatCode(() -> commands.complete(running)).doesNotThrowAnyException();
                assertThat(MongoValidationSchema.scope(f.configSchema().get("validation", Document.class))).isEqualTo(auth.businessScope());
                assertThat(f.cacheSchema().getInteger("installedVersion")).isEqualTo(2);
                assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments()).isZero();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aSecondCacheLossRequiresItsOwnConfirmedReplacementAndCannotCompleteTheOldOne(boolean prepared) {
        try (var f = new Fixture(1)) {
            f.configuration.bindings().complete(new StorageSetupScope("cluster", "installation", "setup", 1));
            f.configuration.firstAdmin().create(f.configuration.firstAdmin().load().orElseThrow().scope(), new User("admin", "fixture-hash", "admin"));
            var started = f.startValidation(1, true); var oldGeneration = f.configuration.validationPreparations().read("flow0").generation();
            f.cacheDb.drop();
            try (var firstCache = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-first-loss")) {
                var firstAuth = f.configuration.prepareAuthenticationRecovery(firstCache, () -> {});
                var commands = f.configuration.recoveryCommands();
                var first = commands.confirm(commands.plan(firstAuth.authScope(), f.recoveryManifest(firstAuth.authScope()), null, null));
                f.configuration.cacheRecoveries().stageCluster(first); first = commands.current(firstAuth.authScope()).orElseThrow();
                if (prepared) f.configuration.prepareStoppedRecovery(firstCache, first, () -> {});
                f.cacheDb.drop();
                try (var nextCache = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-second-loss")) {
                    var nextAuth = f.configuration.prepareAuthenticationRecovery(nextCache, () -> {}); var obsolete = first;
                    var next = commands.confirm(commands.plan(nextAuth.authScope(), f.recoveryManifest(nextAuth.authScope()), null, first.operationId()));
                    f.configuration.cacheRecoveries().stageCluster(next); next = commands.current(nextAuth.authScope()).orElseThrow();
                    var before = SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).find().first();
                    assertThatThrownBy(() -> commands.complete(obsolete)).isInstanceOf(TapstateException.class);
                    assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).find().first()).isEqualTo(before);
                    f.configuration.prepareStoppedRecovery(nextCache, next, () -> {}); commands.complete(next);
                    var current = f.configuration.validationPreparations().read("flow0");
                    assertThat(current.phase()).isEqualTo(MongoValidationPreparationFacts.Phase.UNASSIGNED);
                    assertThat(current.generation()).isNotEqualTo(oldGeneration); assertThat(current.previous()).isNull();
                    assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).find().first()).containsEntry("closed", true);
                    var permission = f.configuration.validationPreparations().issue("flow0", nextAuth.businessScope());
                    var node = nextCache.workloadClaims(nextAuth.businessScope()).acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
                    f.configuration.validationPreparations().complete(permission, nextCache.validationPreparations(nextAuth.businessScope()).prepare(permission, WorkloadClaimFence.from(node)));
                    assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments(new Document("kind", "run"))).isZero();
                    assertThat(started.runs()).hasSize(1);
                }
            }
        }
    }

    @Test void concurrentClientsContinueTheSameReplacementAndCompleteOneNewGeneration() throws Exception {
        try (var f = new Fixture(1); var workers = Executors.newFixedThreadPool(2)) {
            f.configuration.bindings().complete(new StorageSetupScope("cluster", "installation", "setup", 1));
            f.configuration.firstAdmin().create(f.configuration.firstAdmin().load().orElseThrow().scope(), new User("admin", "fixture-hash", "admin"));
            f.startValidation(1, true); f.cacheDb.drop();
            try (var cache = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-recovery-concurrent")) {
                var auth = f.configuration.prepareAuthenticationRecovery(cache, () -> {}); var commands = f.configuration.recoveryCommands();
                f.configuration.cacheRecoveries().stageCluster(commands.confirm(commands.plan(auth.authScope(), f.recoveryManifest(auth.authScope()), null, null)));
                var running = commands.current(auth.authScope()).orElseThrow();
                try (var other = MongoConfigurationDomain.open(f.settings, "cluster");
                        var otherCache = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-recovery-other")) {
                    var gate = new CountDownLatch(1);
                    var first = workers.submit(() -> { gate.await(); return attemptRecovery(f.configuration, cache, running); });
                    var second = workers.submit(() -> { gate.await(); return attemptRecovery(other, otherCache, running); }); gate.countDown();
                    assertThat(List.of(first.get(20, TimeUnit.SECONDS), second.get(20, TimeUnit.SECONDS))).contains(true);
                    f.configuration.prepareStoppedRecovery(cache, running, () -> {});
                    other.prepareStoppedRecovery(otherCache, running, () -> {});
                    var marker = f.configSchema();
                    assertThat(commands.complete(running)).isEqualTo(other.recoveryCommands().complete(running));
                    var completed = f.configuration.validationPreparations().read("flow0");
                    assertThat(completed.phase()).isEqualTo(MongoValidationPreparationFacts.Phase.UNASSIGNED);
                    assertThat(f.configSchema()).isEqualTo(marker);
                    assertThat(commands.complete(running).phase()).isEqualTo(StorageRecoveryCommands.Phase.COMPLETE);
                    assertThat(other.validationPreparations().read("flow0")).isEqualTo(completed);
                    assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).countDocuments(new Document("closed", true))).isOne();
                }
            }
        }
    }

    @Test void explicitCacheReplacementPreservesConfigurationAndMonotonicStartPermissionsForANewRun() {
        try (var f = new Fixture(1)) {
            f.configuration.bindings().complete(new StorageSetupScope("cluster", "installation", "setup", 1));
            f.configuration.firstAdmin().create(f.configuration.firstAdmin().load().orElseThrow().scope(), new User("admin", "fixture-hash", "admin"));
            var started = f.startValidation(1, true); var old = started.runs().getFirst();
            var originalTask = SystemCollections.VALIDATION_TASKS.on(f.configDb).find().first();
            var permission = f.configuration.validationPreparations().issue("flow0", f.scope);
            var deleted = f.reserveRun(f.configuration, permission, started.owner(), "deleted-config", "never-started");
            f.configuration.validationAdmissions(permission).close(deleted.admission());
            var deletedId = new Document("_id", MongoValidationAdmissionStore.taskId(deleted.admission().task()));
            SystemCollections.VALIDATION_TASKS.on(f.configDb).updateOne(deletedId, new Document("$set", new Document("closed", true)));
            var deletedBefore = SystemCollections.VALIDATION_TASKS.on(f.configDb).find(deletedId).first();
            f.cacheDb.drop();
            try (var cache = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-config-preservation")) {
                var authentication = f.configuration.prepareAuthenticationRecovery(cache, () -> {}); var commands = f.configuration.recoveryCommands();
                f.configuration.cacheRecoveries().stageCluster(commands.confirm(commands.plan(authentication.authScope(), f.recoveryManifest(authentication.authScope()), null, null)));
                var running = commands.current(authentication.authScope()).orElseThrow();
                f.configuration.prepareStoppedRecovery(cache, running, () -> {}); commands.complete(running);
                var retained = SystemCollections.VALIDATION_TASKS.on(f.configDb).find(new Document("_id", originalTask.get("_id"))).first();
                assertThat(retained).containsEntry("closed", false).containsEntry("taskGeneration", originalTask.get("taskGeneration"))
                        .containsEntry("revision", originalTask.get("revision"));
                var full = retained.get("modes", Document.class).get("full", Document.class);
                assertThat(full.getLong("nextStartSequence")).isEqualTo(2);
                assertThat(full.get("permission", Document.class)).containsEntry("phase", "CLOSED");
                assertThat(SystemCollections.VALIDATION_TASKS.on(f.configDb).find(deletedId).first()).isEqualTo(deletedBefore);
                var preparations = f.configuration.validationPreparations(); var next = preparations.issue("flow0", authentication.businessScope());
                var node = cache.workloadClaims(authentication.businessScope()).acquire(f.nodeKey(), f.nodeOwner(), 1, TTL).claim();
                preparations.complete(next, cache.validationPreparations(authentication.businessScope()).prepare(next, WorkloadClaimFence.from(node)));
                var claims = cache.validationClaims(next); var key = ValidationClaims.key("cluster", "flow0");
                var held = claims.acquire(key, node.owner(), 1, TTL).claim(); var owner = claims.advanceExecution(held, 1, Set.of(node.owner().nodeId())).orElseThrow();
                var identity = new io.tapstate.core.validation.ValidationRunIdentity(ValidationClaims.owner("flow0", owner), old.identity().taskId(), "fresh-run",
                        io.tapstate.core.validation.ValidationMode.FULL, "spec", 1, "rules");
                var command = new ValidationReportAdmission.Start(old.admission().task(), old.admission().configurationRevision(), 2, UUID.randomUUID().toString(), identity);
                var admissions = f.configuration.validationAdmissions(next);
                assertThat(admissions.reserve(command)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
                assertThat(admissions.open(command)).isEqualTo(ValidationReportAdmission.Birth.CREATED);
                assertThat(admissions.open(old.admission())).isEqualTo(ValidationReportAdmission.Birth.STALE);
                assertThat(cache.validationControl(next).join(new ValidationControlStore.Run(identity, 1, command))).isEqualTo(ValidationControlStore.Join.JOINED);
                assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).find(new Document("_id", MongoValidationAdmissionStore.reportId(old.identity()))).first())
                        .containsEntry("closed", true);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"closure-missing", "closure-type", "mode-type", "identity-missing", "generation", "counter", "phase"})
    void recoveryCannotIgnoreMalformedClosedPermissionHeadersOutsideItsOpenPermissionBatch(String corruption) {
        try (var f = new Fixture(1)) {
            f.configuration.bindings().complete(new StorageSetupScope("cluster", "installation", "setup", 1));
            f.configuration.firstAdmin().create(f.configuration.firstAdmin().load().orElseThrow().scope(), new User("admin", "fixture-hash", "admin"));
            var started = f.startValidation(1, true); var permission = f.configuration.validationPreparations().issue("flow0", f.scope);
            var bad = f.reserveRun(f.configuration, permission, started.owner(), "bad", "cancelled-before-birth");
            f.configuration.validationAdmissions(permission).close(bad.admission());
            var tasks = SystemCollections.VALIDATION_TASKS.on(f.configDb); var originalId = new Document("_id", MongoValidationAdmissionStore.taskId(bad.admission().task()));
            var row = tasks.find(originalId).first(); row.put("closed", true); var mode = row.get("modes", Document.class).get("full", Document.class);
            switch (corruption) {
                case "closure-missing" -> row.remove("closed");
                case "closure-type" -> row.put("closed", "true");
                case "mode-type" -> mode.put("nextStartSequence", 2.0);
                case "identity-missing" -> mode.get("permission", Document.class).get("identity", Document.class).remove("run");
                case "generation" -> row.put("taskGeneration", "invalid");
                case "counter" -> mode.put("nextStartSequence", 1L);
                default -> mode.get("permission", Document.class).put("phase", "UNKNOWN");
            }
            tasks.replaceOne(originalId, row); f.cacheDb.drop();
            try (var cache = MongoCacheDomain.connect(f.endpoint, Duration.ofSeconds(5), "validation-recovery-bad-header")) {
                var auth = f.configuration.prepareAuthenticationRecovery(cache, () -> {}); var commands = f.configuration.recoveryCommands();
                f.configuration.cacheRecoveries().stageCluster(commands.confirm(commands.plan(auth.authScope(), f.recoveryManifest(auth.authScope()), null, null)));
                var running = commands.current(auth.authScope()).orElseThrow();
                var before = tasks.find().sort(new Document("_id.taskId", 1)).into(new ArrayList<>());
                assertThatThrownBy(() -> f.configuration.prepareStoppedRecovery(cache, running, () -> {})).isInstanceOf(TapstateException.class);
                assertThat(tasks.find().sort(new Document("_id.taskId", 1)).into(new ArrayList<>())).isEqualTo(before);
                assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).find().first()).containsEntry("closed", false);
                assertThat(commands.current(auth.authScope()).orElseThrow().phase()).isEqualTo(StorageRecoveryCommands.Phase.RUNNING);
                assertThat(f.cacheDb.listCollectionNames()).doesNotContain("validation_runs");
            }
        }
    }

    @Test void closedPermissionVerificationHasAnExplicitHeaderCountBound() {
        try (var f = new Fixture(1)) {
            f.upgrade(); var rows = new ArrayList<Document>();
            for (int n = 0; n < MongoValidationAdmissionStore.MAX_TASKS_PER_PIPELINE; n++) rows.add(emptyTaskHeader("task-" + n));
            SystemCollections.VALIDATION_TASKS.on(f.configDb).insertMany(rows);
            try (var session = f.configClient.startSession()) {
                assertThatCode(() -> session.withTransaction(() -> {
                    MongoValidationAdmissionStore.requireClosedPipelinePermissions(session, f.configDb, "cluster", "flow0"); return null;
                })).doesNotThrowAnyException();
                SystemCollections.VALIDATION_TASKS.on(f.configDb).insertOne(emptyTaskHeader("one-too-many"));
                assertThatThrownBy(() -> session.withTransaction(() -> {
                    MongoValidationAdmissionStore.requireClosedPipelinePermissions(session, f.configDb, "cluster", "flow0"); return null;
                })).isInstanceOf(TapstateException.class).hasMessageContaining("validation.taskControlHeaders");
            }
        }
    }

    private static Document emptyTaskHeader(String task) {
        return new Document("_id", new Document("clusterId", "cluster").append("pipelineId", "flow0").append("taskId", task))
                .append("formatVersion", 1).append("taskGeneration", UUID.randomUUID().toString()).append("revision", 1L).append("closed", false)
                .append("modes", new Document("full", new Document("nextStartSequence", 1L)).append("incremental", new Document("nextStartSequence", 1L)));
    }

    private static boolean attemptRecovery(MongoConfigurationDomain configuration, MongoCacheDomain cache, StorageRecoveryCommands.Plan running) {
        try { configuration.prepareStoppedRecovery(cache, running, () -> {}); return true; }
        catch (TapstateException inProgress) {
            assertThat(inProgress.code()).isIn(IoError.CACHE_BUSINESS_FENCED, IoError.WORKLOAD_CLAIM_FENCED, StoreError.INITIALIZATION_REFUSED);
            return false;
        }
    }

    @Test void publicNativeValidationAccessKeepsSeparateOriginalStoresAndNoProductionPreparation() {
        try (var f = new Fixture(1)) {
            f.upgrade(); var before = f.productionFacts(); var node = f.node("node");
            var access = f.configuration.prepareValidationAccess("flow0", f.scope, f.cache, WorkloadClaimFence.from(node), () -> {});
            var replay = f.configuration.prepareValidationAccess("flow0", f.scope, f.cache, WorkloadClaimFence.from(node), () -> {});
            assertThat(replay.generation()).isEqualTo(access.generation()); assertThat(replay.preparationId()).isEqualTo(access.preparationId());
            assertThat(access.scope()).isEqualTo(f.scope); assertThat(access.pipelineId()).isEqualTo("flow0");
            var claims = access.claims(); var held = claims.acquire(ValidationClaims.key("cluster", "flow0"), node.owner(), 1, TTL).claim();
            var executing = claims.advanceExecution(held, 1, Set.of(node.owner().nodeId())).orElseThrow();
            var run = f.reserveRun(f.configuration, f.configuration.validationPreparations().issue("flow0", f.scope), executing, "task", "run");
            var controls = access.control(); assertThat(controls.join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
            assertThat(access.admissions().open(run.admission())).isEqualTo(ValidationReportAdmission.Birth.CREATED);
            assertThat(controls.transition(new ValidationControlStore.Transition(run, io.tapstate.core.validation.ValidationRunState.STARTING,
                    io.tapstate.core.validation.ValidationRunState.RUNNING))).isEqualTo(ValidationControlStore.Mutation.APPLIED);
            assertThat(access.results().seal(run.report())).isEqualTo(ValidationResultStore.Seal.SEALED);
            assertThat(controls.transition(new ValidationControlStore.Transition(run, io.tapstate.core.validation.ValidationRunState.RUNNING,
                    io.tapstate.core.validation.ValidationRunState.COMPLETED))).isEqualTo(ValidationControlStore.Mutation.APPLIED);
            assertThat(controls.retire(new ValidationControlStore.Retirement(run, run.report(), io.tapstate.core.validation.ValidationRunState.COMPLETED)))
                    .isEqualTo(ValidationControlStore.Mutation.APPLIED);
            assertThat(f.productionFacts()).isEqualTo(before);
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.cacheDb).countDocuments()).isZero();
            assertThat(SystemCollections.CACHE_SOURCE_SCHEMAS.on(f.cacheDb).countDocuments()).isZero();
        }
    }

    @Test void publicNativeValidationAccessRejectsAnotherCacheBeforeIssuingItsFirstPreparation() {
        try (var f = new Fixture(1); var wrong = new Fixture(0)) {
            f.upgrade(); var node = f.node("node"); var before = f.configuration.validationPreparations().read("flow0");
            assertThatThrownBy(() -> f.configuration.prepareValidationAccess("flow0", f.scope, wrong.cache, WorkloadClaimFence.from(node), () -> {}))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.configuration.validationPreparations().read("flow0")).isEqualTo(before);
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).countDocuments()).isZero();
        }
    }

    @Test void publicNativeValidationAccessRetainsTheOriginalLifecycleAndDoesNotFollowAReplacement() {
        try (var f = new Fixture(1)) {
            f.upgrade(); var node = f.node("node"); var active = new java.util.concurrent.atomic.AtomicBoolean(true);
            Runnable local = () -> { if (!active.get()) throw new IllegalStateException("local validation context closed"); };
            var access = f.configuration.prepareValidationAccess("flow0", f.scope, f.cache, WorkloadClaimFence.from(node), local);
            var claims = access.claims(); active.set(false);
            assertThatThrownBy(access::requireCurrent).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> claims.acquire(ValidationClaims.key("cluster", "flow0"), node.owner(), 1, TTL)).isInstanceOf(IllegalStateException.class);
            active.set(true); var definition = f.configuration.artifacts().get("flow0").orElseThrow(); var deletion = f.beginRetirement();
            assertThat(f.configuration.retireValidation(deletion, f.cache, WorkloadClaimFence.from(node), () -> {})).isTrue();
            f.completeRetirement(deletion, node); f.configuration.artifacts().create(definition);
            var current = f.configuration.prepareValidationAccess("flow0", f.scope, f.cache, WorkloadClaimFence.from(node), () -> {});
            assertThat(current.generation()).isNotEqualTo(access.generation());
            assertThatThrownBy(access::requireCurrent).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> claims.acquire(ValidationClaims.key("cluster", "flow0"), node.owner(), 1, TTL)).isInstanceOf(TapstateException.class);
        }
    }

    @Test void retainedPublicStoresRejectLocalClosureAndForeignPipelineCommandsWithoutWrites() {
        try (var f = new Fixture(1)) {
            f.upgrade(); var node = f.node("node"); var live = new java.util.concurrent.atomic.AtomicBoolean(true);
            var access = f.configuration.prepareValidationAccess("flow0", f.scope, f.cache, WorkloadClaimFence.from(node),
                    () -> { if (!live.get()) throw new IllegalStateException("local validation context closed"); });
            var claims = access.claims(); var control = access.control(); var admissions = access.admissions(); var results = access.results();
            var acquired = claims.acquire(ValidationClaims.key("cluster", "flow0"), node.owner(), 1, TTL).claim();
            var executing = claims.advanceExecution(acquired, 1, Set.of(node.owner().nodeId())).orElseThrow();
            var run = f.reserveRun(f.configuration, f.configuration.validationPreparations().issue("flow0", f.scope), executing, "task", "run");
            var foreignOwner = new io.tapstate.core.validation.ValidationOwner("cluster", "foreign", node.owner().nodeId(), node.owner().bootId(),
                    executing.claimGeneration(), executing.executionGeneration(), executing.topologyRevision());
            var foreignIdentity = new io.tapstate.core.validation.ValidationRunIdentity(foreignOwner, "task", "foreign", io.tapstate.core.validation.ValidationMode.FULL, "spec", 1, "rules");
            var foreignStart = new ValidationReportAdmission.Start(new ValidationReportAdmission.Task("cluster", "foreign", "task", UUID.randomUUID()),
                    1, 1, UUID.randomUUID().toString(), foreignIdentity);
            var before = SystemCollections.VALIDATION_RUNS.on(f.cacheDb).find().into(new ArrayList<>());
            assertThatThrownBy(() -> admissions.reserve(foreignStart)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> admissions.open(foreignStart)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> admissions.close(foreignStart)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> control.join(new ValidationControlStore.Run(foreignIdentity, 1, foreignStart))).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> results.seal(foreignStart.report())).isInstanceOf(TapstateException.class);
            live.set(false);
            assertThatThrownBy(() -> claims.renew(executing, TTL)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> control.join(run)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> admissions.open(run.admission())).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> results.seal(run.report())).isInstanceOf(IllegalStateException.class);
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).find().into(new ArrayList<>())).isEqualTo(before);
            assertThat(SystemCollections.VALIDATION_REPORTS.on(f.configDb).countDocuments()).isZero();
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
        Fixture(int pipelines) { this(pipelines, StorageBinding.Mode.OP); }
        Fixture(int pipelines, StorageBinding.Mode mode) {
            var master = new SourceResource(StorageBinding.defaultSourceId(mode), null, mode == StorageBinding.Mode.CLOUD ? "mongodb-atlas" : "mongodb",
                    Map.of("uri", "mongodb://business.invalid/master"), null, null, null, null);
            var binding = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster", "installation", "setup", 1), mode, endpoint, master);
            scope = binding.preparation().businessScope();
            cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "validation-schema-fixture");
            cache.prepareFirstAuth(binding.firstCreation().orElseThrow());
            cache.prepareInitialBusiness(binding.preparation(), () -> {});
            if (mode == StorageBinding.Mode.OP || pipelines > 0)
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
        WorkloadClaim node(String id) {
            return cache.workloadClaims(scope).acquire(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, id),
                    new WorkloadOwner(id, "boot"), 1, TTL).claim();
        }
        StorageRecoveryCommands.Manifest recoveryManifest(CacheAuthScope scope) {
            var hashes = new TreeMap<String, String>();
            configuration.artifacts().list().forEach(resource -> hashes.put(resource.id(), io.tapstate.core.model.canonical.CanonicalHash.of(resource)));
            return new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of()), Set.of("flow0"), hashes,
                    CacheRecoveryStore.clusterDependencyHash(scope, configuration.cacheRecoveries().inspectCluster(scope)), configuration.configurationRevision());
        }
        record Started(WorkloadClaim node, WorkloadClaim owner, List<ValidationControlStore.Run> runs) {}
        Started startValidation(int count, boolean born) {
            upgrade(); var preparations = configuration.validationPreparations(); var permission = preparations.issue("flow0", scope);
            var node = node("node");
            var complete = preparations.complete(permission, cache.validationPreparations(scope).prepare(permission, WorkloadClaimFence.from(node)));
            var claims = cache.validationClaims(complete); var acquired = claims.acquire(ValidationClaims.key("cluster", "flow0"), node.owner(), 1, TTL).claim();
            var executing = claims.advanceExecution(acquired, 1, Set.of(node.owner().nodeId())).orElseThrow();
            var runs = new ArrayList<ValidationControlStore.Run>(); var controls = cache.validationControl(complete);
            for (int n = 0; n < count; n++) {
                var run = reserveRun(configuration, complete, executing, "task-" + n, "run-" + n);
                assertThat(controls.join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
                if (born) assertThat(configuration.validationAdmissions(complete).open(run.admission())).isEqualTo(ValidationReportAdmission.Birth.CREATED);
                runs.add(run);
            }
            return new Started(node, executing, List.copyOf(runs));
        }
        PipelineRetirementPreparation beginRetirement() {
            var definition = configuration.artifacts().get("flow0").orElseThrow();
            var original = new PipelineIntentScope("cluster", scope.bindingVersion(), scope.cacheIncarnation(), "flow0",
                    configuration.pipelineStates().read("flow0").orElseThrow().stateGeneration());
            return configuration.retirements().begin(new PipelineRetirementRequest(UUID.randomUUID().toString(), original,
                    io.tapstate.core.model.canonical.CanonicalHash.of(definition), Optional.empty()));
        }
        void completeRetirement(PipelineRetirementPreparation original, WorkloadClaim node) {
            var receipt = cache.pipelineRetirements(scope, () -> {}).retire(original, WorkloadClaimFence.from(node));
            for (var notes : configuration.requiredNotes().retirements(original.request().scope(), original.request().operationId())) {
                assertThat(notes.origin()).isEmpty(); notes.completeAfterVerifiedRelease();
            }
            configuration.retirements().complete(receipt);
        }
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
