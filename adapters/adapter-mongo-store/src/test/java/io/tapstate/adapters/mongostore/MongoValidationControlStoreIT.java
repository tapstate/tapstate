package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.ValidationControlStore.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;

/** Real CACHE transactions over explicitly prepared domains; report closures use a separate CONFIG client. */
@RequiresDocker
class MongoValidationControlStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster", 1, "cache");
    static final PipelineIntentScope PIPELINE = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
    static final WorkloadOwner NODE_A = new WorkloadOwner("node-a", "boot-a"), NODE_B = new WorkloadOwner("node-b", "boot-b");
    static final Duration TTL = Duration.ofSeconds(30);

    @Test void twoClientsCompeteForOneModeWhileFullAndIncrementalRemainIndependent() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            var a = f.run("task", ValidationMode.FULL, "a", 1); var b = f.run("task", ValidationMode.FULL, "b", 1);
            var gate = new CountDownLatch(1);
            var first = workers.submit(() -> { gate.await(); return f.first.join(a); });
            var second = workers.submit(() -> { gate.await(); return f.second.join(b); }); gate.countDown();
            var outcomes = List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            assertThat(outcomes).containsExactlyInAnyOrder(Join.JOINED, Join.MODE_BUSY);
            var winner = outcomes.getFirst() == Join.JOINED ? a : b;
            assertThat(f.first.join(winner)).isEqualTo(Join.ALREADY_JOINED);
            assertThat(f.second.join(f.run("task", ValidationMode.INCREMENTAL, "incremental", 1))).isEqualTo(Join.JOINED);
            assertThat(f.control().get("slots", Document.class)).hasSize(2);
        }
    }

    @Test void ordinaryFailureRetiresOnlyItsOwnRunAndLastExitClosesTheGeneration() {
        try (var f = new Fixture()) {
            var full = f.run("task", ValidationMode.FULL, "full", 1); var incremental = f.run("task", ValidationMode.INCREMENTAL, "incremental", 1);
            f.first.join(full); f.first.join(incremental);
            assertThat(f.first.transition(new Transition(full, ValidationRunState.STARTING, ValidationRunState.FAILED))).isEqualTo(Mutation.APPLIED);
            assertThat(f.second.retire(new Retirement(full, f.sealed(full), ValidationRunState.FAILED))).isEqualTo(Mutation.APPLIED);
            assertThat(f.control().getBoolean("closed")).isFalse(); assertThat(f.state(incremental)).isEqualTo("STARTING");
            f.first.transition(new Transition(incremental, ValidationRunState.STARTING, ValidationRunState.STOPPING));
            f.first.transition(new Transition(incremental, ValidationRunState.STOPPING, ValidationRunState.STOPPED));
            assertThat(f.first.retire(new Retirement(incremental, f.sealed(incremental), ValidationRunState.STOPPED))).isEqualTo(Mutation.APPLIED);
            assertThat(f.control().getBoolean("closed")).isTrue(); assertThat(f.control().get("slots", Document.class)).isEmpty();
            assertThat(f.first.join(full)).isEqualTo(Join.GENERATION_CLOSED);
            assertThat(f.first.join(f.run("next", ValidationMode.FULL, "same-tenure", 2))).isEqualTo(Join.GENERATION_CLOSED);
            f.advance(); assertThat(f.first.join(f.run("next", ValidationMode.FULL, "new-execution", 2))).isEqualTo(Join.JOINED);
        }
    }

    @Test void stopLatchWinsAgainstNewJoinsAndStopsOnlyAssociatedValidationRuns() {
        try (var f = new Fixture()) {
            var production = f.production();
            var full = f.run("one", ValidationMode.FULL, "full", 1); var incremental = f.run("two", ValidationMode.INCREMENTAL, "incremental", 1);
            f.first.join(full); f.first.join(incremental);
            f.first.transition(new Transition(full, ValidationRunState.STARTING, ValidationRunState.RUNNING));
            assertThat(f.second.latchProblemLimit(full)).isEqualTo(Mutation.APPLIED);
            assertThat(f.first.latchProblemLimit(full)).isEqualTo(Mutation.ALREADY_APPLIED);
            assertThat(f.state(full)).isEqualTo("STOPPING"); assertThat(f.state(incremental)).isEqualTo("STOPPING");
            assertThat(f.first.join(f.run("three", ValidationMode.FULL, "late", 1))).isEqualTo(Join.GENERATION_CLOSED);
            assertThat(f.first.transition(new Transition(full, ValidationRunState.STARTING, ValidationRunState.RUNNING))).isEqualTo(Mutation.STALE);
            assertThat(f.production()).isEqualTo(production);
        }
    }

    @Test void successorTerminatesOldRunWithItsOwnFenceAndLateOldCommandsCannotAffectNewWork() {
        try (var f = new Fixture()) {
            var old = f.run("task", ValidationMode.FULL, "old", 1); f.first.join(old);
            f.first.transition(new Transition(old, ValidationRunState.STARTING, ValidationRunState.RUNNING));
            var report = f.sealed(old); f.handoff();
            var fresh = f.run("task", ValidationMode.FULL, "fresh", 2);
            assertThat(f.second.join(fresh)).isEqualTo(Join.MODE_BUSY);
            assertThat(f.second.terminateLostOwner(new LostOwner(f.owner, old, report))).isEqualTo(Mutation.APPLIED);
            assertThat(f.state(old)).isEqualTo("FAILED"); assertThat(f.second.join(fresh)).isEqualTo(Join.JOINED);
            assertThatThrownBy(() -> f.first.transition(new Transition(old, ValidationRunState.RUNNING, ValidationRunState.FAILED))).isInstanceOf(TapstateException.class);
            var before = f.control();
            assertThat(f.second.terminateLostOwner(new LostOwner(f.owner, old, report))).isEqualTo(Mutation.ALREADY_APPLIED);
            assertThat(f.control()).isEqualTo(before); assertThat(f.state(fresh)).isEqualTo("STARTING");
        }
    }

    @Test void successorRetirementPreservesAnAlreadyKnownCompletedOutcome() {
        try (var f = new Fixture()) {
            var old = f.run("task", ValidationMode.FULL, "completed", 1); f.first.join(old);
            f.first.transition(new Transition(old, ValidationRunState.STARTING, ValidationRunState.RUNNING));
            f.first.transition(new Transition(old, ValidationRunState.RUNNING, ValidationRunState.COMPLETED));
            var sealed = f.sealed(old); f.handoff();
            assertThat(f.second.terminateLostOwner(new LostOwner(f.owner, old, sealed))).isEqualTo(Mutation.APPLIED);
            assertThat(f.state(old)).isEqualTo("COMPLETED"); assertThat(f.row(old).getBoolean("retired")).isTrue();
        }
    }

    @Test void everyFenceComponentAndTheSingletonExecutionContextAreRequired() {
        try (var f = new Fixture()) {
            var current = f.owner;
            var invalid = List.of(new ValidationOwner("cluster", "pipeline", "other", current.bootId(), current.claimGeneration(), current.executionGeneration(), current.topologyRevision()),
                    new ValidationOwner("cluster", "pipeline", current.nodeId(), "other", current.claimGeneration(), current.executionGeneration(), current.topologyRevision()),
                    new ValidationOwner("cluster", "pipeline", current.nodeId(), current.bootId(), current.claimGeneration()+1, current.executionGeneration(), current.topologyRevision()),
                    new ValidationOwner("cluster", "pipeline", current.nodeId(), current.bootId(), current.claimGeneration(), current.executionGeneration()+1, current.topologyRevision()),
                    new ValidationOwner("cluster", "pipeline", current.nodeId(), current.bootId(), current.claimGeneration(), current.executionGeneration(), current.topologyRevision()+1));
            var before = f.control();
            for (var owner : invalid) assertThatThrownBy(() -> f.first.join(new Run(new ValidationRunIdentity(owner, "task", "run", ValidationMode.FULL, "spec", 1, "rules"), 1)))
                    .isInstanceOf(TapstateException.class);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(ValidationClaims.key("cluster", "pipeline"))),
                    new Document("$set", new Document("executionNodeIds", List.of(current.nodeId(), "other"))));
            assertThatThrownBy(() -> f.first.join(f.run("task", ValidationMode.FULL, "bad-context", 1))).isInstanceOf(TapstateException.class);
            assertThat(f.control()).isEqualTo(before);
        }
    }

    @Test void lostControlAndClaimCannotBeRecreatedByNormalOperationsOrPreparationReplay() {
        try (var f = new Fixture()) {
            var run = f.run("task", ValidationMode.FULL, "old", 1);
            f.rows().deleteOne(new Document("_id", MongoValidationControlStore.controlId("cluster", "pipeline")));
            assertThatThrownBy(() -> f.first.join(run)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.claims().acquire(ValidationClaims.key("cluster", "pipeline"), NODE_A, 1, TTL)).isInstanceOf(TapstateException.class);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).deleteOne(new Document("_id", MongoWorkloadClaimStore.id(ValidationClaims.key("cluster", "pipeline"))));
            try (var session = f.client.startSession()) {
                assertThatThrownBy(() -> session.withTransaction(() -> {
                    MongoValidationControlStore.prepareInTransaction(session, f.database, PIPELINE, f.preparationId, true); return null;
                })).isInstanceOf(TapstateException.class).hasMessageContaining("missing-original-control");
            }
            assertThat(f.rows().countDocuments()).isZero();
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).countDocuments(new Document("_id", MongoWorkloadClaimStore.id(ValidationClaims.key("cluster", "pipeline"))))).isZero();
        }
    }

    @Test void missingOriginalPipelineFactsCannotBeHiddenByTheCopiedBirthMarker() {
        for (String missing : List.of("scope", "preparation", "state")) try (var f = new Fixture()) {
            var originalKey = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "pipeline");
            if (missing.equals("state")) SystemCollections.CACHE_PIPELINE_STATE.on(f.database).deleteOne(new Document("_id", "pipeline"));
            else SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(originalKey)),
                    new Document("$unset", new Document(missing.equals("scope") ? "pipelineScope" : "pipelinePreparation", "")));
            var before = f.control();
            assertThatThrownBy(() -> f.first.join(f.run("task", ValidationMode.FULL, "missing-original", 1))).as(missing).isInstanceOf(TapstateException.class);
            assertThat(f.control()).isEqualTo(before);
        }
    }

    @Test void retiredPipelineCannotKeepRenewingValidationAuthority() {
        try (var f = new Fixture()) {
            var originalKey = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "pipeline");
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(originalKey)),
                    new Document("$set", new Document("pipelinePreparation.phase", "RETIRED")));
            assertThatThrownBy(() -> f.claims().renew(f.claim, TTL)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.first.join(f.run("task", ValidationMode.FULL, "retired-pipeline", 1))).isInstanceOf(TapstateException.class);
        }
    }

    @Test void aNonDateLeaseCannotUseBsonTypeOrderToAuthorizeAWrite() {
        try (var f = new Fixture()) {
            var key = ValidationClaims.key("cluster", "pipeline");
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(key)),
                    new Document("$set", new Document("leaseUntil", new org.bson.types.MaxKey())));
            var before = f.control();
            assertThatThrownBy(() -> f.first.join(f.run("task", ValidationMode.FULL, "bad-date", 1))).isInstanceOf(TapstateException.class);
            assertThat(f.control()).isEqualTo(before);
        }
    }

    @Test void aRunPayloadCannotRedirectStopLatchingToAnotherPhysicalRun() {
        try (var f = new Fixture()) {
            var a = f.run("task-a", ValidationMode.FULL, "a", 1); var b = f.run("task-b", ValidationMode.FULL, "b", 1);
            f.first.join(a); f.first.join(b);
            f.rows().updateOne(new Document("_id", MongoValidationControlStore.runId(a)), new Document("$set", new Document("identity", MongoValidationControlStore.identity(b))));
            var before = f.control();
            assertThatThrownBy(() -> f.second.latchProblemLimit(b)).isInstanceOf(TapstateException.class);
            assertThat(f.control()).isEqualTo(before); assertThat(f.state(a)).isEqualTo("STARTING"); assertThat(f.state(b)).isEqualTo("STARTING");
        }
    }

    @Test void malformedOriginalBirthScopeIsDiagnosedWithACodedStorageError() {
        try (var f = new Fixture()) {
            var birth = f.control().get("birth", Document.class); birth.get("pipelineScope", Document.class).remove("stateGeneration");
            f.rows().updateOne(new Document("_id", MongoValidationControlStore.controlId("cluster", "pipeline")), new Document("$set", new Document("birth", birth)));
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database);
            claims.updateOne(new Document("_id", MongoWorkloadClaimStore.id(ValidationClaims.key("cluster", "pipeline"))),
                    new Document("$set", new Document("validationPreparation", birth)));
            claims.updateOne(new Document("_id", MongoWorkloadClaimStore.id(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "pipeline"))),
                    new Document("$set", new Document("validationControlBirth", birth)));
            assertThatThrownBy(() -> f.first.join(f.run("task", ValidationMode.FULL, "malformed", 1))).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @Test void stopLatchRacesAJoinWithoutLeavingAnyAdmittedRunOutsideStopping() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            var a = f.run("task-a", ValidationMode.FULL, "a", 1); var b = f.run("task-b", ValidationMode.FULL, "b", 1); f.first.join(a);
            var gate = new CountDownLatch(1);
            var stop = workers.submit(() -> { gate.await(); return f.first.latchProblemLimit(a); });
            var join = workers.submit(() -> { gate.await(); return f.second.join(b); }); gate.countDown();
            assertThat(stop.get(10, TimeUnit.SECONDS)).isEqualTo(Mutation.APPLIED);
            var admitted = join.get(10, TimeUnit.SECONDS); assertThat(admitted).isIn(Join.JOINED, Join.GENERATION_CLOSED);
            assertThat(f.state(a)).isEqualTo("STOPPING"); assertThat(f.control().getBoolean("problemLimit")).isTrue();
            if (admitted == Join.JOINED) assertThat(f.state(b)).isEqualTo("STOPPING"); else assertThat(f.row(b)).isNull();
        }
    }

    @Test void explicitBirthRollsBackWithItsOriginalMarkerAndReplayDoesNotRecreateLostFacts() {
        try (var f = new Fixture()) {
            var pipeline = new PipelineIntentScope("cluster", 1, "cache", "another", 1); String operation = UUID.randomUUID().toString();
            var key = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "another");
            var original = new Document(f.production()); original.put("_id", MongoWorkloadClaimStore.id(key)); original.put("resourceId", "another");
            original.put("pipelineScope", MongoPipelinePreparationFacts.scope(pipeline));
            original.put("pipelinePreparation", MongoCachePipelinePreparations.marker(operation, PipelineCachePreparations.Phase.PREPARED)); original.remove("validationControlBirth");
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database); claims.insertOne(original);
            var state = new Document(SystemCollections.CACHE_PIPELINE_STATE.on(f.database).find().first()); state.put("_id", "another"); state.put("scope", MongoPipelinePreparationFacts.scope(pipeline));
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database).insertOne(state);
            try (var session = f.client.startSession()) {
                assertThatThrownBy(() -> session.withTransaction(() -> { MongoValidationControlStore.prepareInTransaction(session, f.database, pipeline, operation, false); return null; }))
                        .isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> session.withTransaction(() -> {
                    MongoValidationControlStore.prepareInTransaction(session, f.database, pipeline, operation, true); throw new IllegalStateException("birth interrupted");
                })).isInstanceOf(IllegalStateException.class).hasMessage("birth interrupted");
                assertThat(claims.find(new Document("_id", MongoWorkloadClaimStore.id(key))).first()).isEqualTo(original);
                assertThat(f.rows().find(new Document("_id", MongoValidationControlStore.controlId("cluster", "another"))).first()).isNull();
                session.withTransaction(() -> { MongoValidationControlStore.prepareInTransaction(session, f.database, pipeline, operation, true); return null; });
                var control = f.rows().find(new Document("_id", MongoValidationControlStore.controlId("cluster", "another"))).first();
                session.withTransaction(() -> { MongoValidationControlStore.prepareInTransaction(session, f.database, pipeline, operation, false); return null; });
                assertThat(f.rows().find(new Document("_id", MongoValidationControlStore.controlId("cluster", "another"))).first()).isEqualTo(control);
            }
        }
    }

    @Test void lastExitRacingAnExistingGenerationJoinNeverReopensAClosedGeneration() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            var old = f.run("old", ValidationMode.FULL, "old", 1); f.first.join(old);
            f.first.transition(new Transition(old, ValidationRunState.STARTING, ValidationRunState.FAILED)); var sealed = f.sealed(old);
            var next = f.run("other", ValidationMode.FULL, "next", 1); var gate = new CountDownLatch(1);
            var a = workers.submit(() -> { gate.await(); return f.first.retire(new Retirement(old, sealed, ValidationRunState.FAILED)); });
            var b = workers.submit(() -> { gate.await(); return f.second.join(next); }); gate.countDown();
            assertThat(a.get(10, TimeUnit.SECONDS)).isEqualTo(Mutation.APPLIED);
            var joined = b.get(10, TimeUnit.SECONDS); assertThat(joined).isIn(Join.JOINED, Join.GENERATION_CLOSED);
            assertThat(f.control().getBoolean("closed")).isEqualTo(joined == Join.GENERATION_CLOSED);
            assertThat(f.control().get("slots", Document.class).size()).isEqualTo(joined == Join.JOINED ? 1 : 0);
        }
    }

    private static final class Fixture implements AutoCloseable {
        final MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl()), other = MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoDatabase database = client.getDatabase("validation_control_" + UUID.randomUUID().toString().replace("-", ""));
        final MongoDatabase configuration = other.getDatabase("validation_config_" + UUID.randomUUID().toString().replace("-", ""));
        final String preparationId = UUID.randomUUID().toString();
        final MongoCacheDomain domain;
        final MongoValidationControlStore first, second;
        final Map<Run, ValidationResultStore.Report> closedReports = new HashMap<>();
        WorkloadClaim claim; ValidationOwner owner;
        Fixture() {
            var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(database.getName()), database.getName(), null);
            domain = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "validation-control-fixture");
            domain.prepareAuth(new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "cache", 1), "auth", () -> {});
            try (var session = client.startSession()) {
                for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION, SystemCollections.VALIDATION_RUNS))
                    table.prepareCollectionOn(database, session);
            }
            try (var session = other.startSession()) {
                for (var table : List.of(SystemCollections.VALIDATION_TASKS, SystemCollections.VALIDATION_REPORTS, SystemCollections.VALIDATION_DIFFERENCES)) table.prepareCollectionOn(configuration, session);
            }
            var pipelineKey = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "pipeline");
            var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)); var production = raw.acquire(pipelineKey, NODE_A, 1, TTL).claim(); raw.release(production);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(pipelineKey)), new Document("$set",
                    new Document("pipelineScope", MongoPipelinePreparationFacts.scope(PIPELINE)).append("pipelinePreparation", MongoCachePipelinePreparations.marker(preparationId, PipelineCachePreparations.Phase.PREPARED))));
            SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "pipeline").append("scope", MongoPipelinePreparationFacts.scope(PIPELINE))
                    .append("stateJson", "NEW").append("epoch", 0L).append("touchMillis", 0L).append("runtimeWriteFence", 1L));
            SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set",
                    new Document("businessPhase", "READY").append("businessPreparationId", "business").append("claimAdmissionFence", 0L)));
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY").append("epoch", 1L).append("operationId", "business"));
            try (var session = client.startSession()) { session.withTransaction(() -> { MongoValidationControlStore.prepareInTransaction(session, database, PIPELINE, preparationId, true); return null; }); }
            first = new MongoValidationControlStore(client, database, SCOPE, () -> {});
            second = new MongoValidationControlStore(other, other.getDatabase(database.getName()), SCOPE, () -> {});
            claim = claims().acquire(ValidationClaims.key("cluster", "pipeline"), NODE_A, 1, TTL).claim(); advance();
        }
        WorkloadClaimStore claims() { return domain.workloadClaims(SCOPE); }
        void advance() { claim = claims().advanceExecution(claim, 1, Set.of(claim.owner().nodeId())).orElseThrow(); owner = ValidationClaims.owner("pipeline", claim); }
        void handoff() { assertThat(claims().release(claim)).isTrue(); claim = claims().acquire(ValidationClaims.key("cluster", "pipeline"), NODE_B, 1, TTL).claim(); advance(); }
        Run run(String task, ValidationMode mode, String run, long generation) { return new Run(new ValidationRunIdentity(owner, task, run, mode, "spec", 1, "rules"), generation); }
        Document control() { return rows().find(new Document("_id", MongoValidationControlStore.controlId("cluster", "pipeline"))).first(); }
        Document row(Run run) { return rows().find(new Document("_id", MongoValidationControlStore.runId(run))).first(); }
        String state(Run run) { return row(run).getString("state"); }
        Document production() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(new Document("_id", MongoWorkloadClaimStore.id(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "pipeline")))).first(); }
        com.mongodb.client.MongoCollection<Document> rows() { return SystemCollections.VALIDATION_RUNS.on(database); }
        ValidationResultStore.Report sealed(Run run) {
            return closedReports.computeIfAbsent(run, original -> {
                var tasks = SystemCollections.VALIDATION_TASKS.on(configuration); var key = new Document("clusterId", "cluster").append("pipelineId", "pipeline").append("taskId", run.identity().taskId());
                var task = tasks.find(new Document("_id", key)).first();
                if (task == null) {
                    task = new Document("_id", key).append("formatVersion", 1).append("taskGeneration", UUID.randomUUID().toString()).append("revision", 1L)
                            .append("modes", new Document("full", new Document("nextStartSequence", 1L)).append("incremental", new Document("nextStartSequence", 1L))); tasks.insertOne(task);
                }
                String mode = run.identity().mode() == ValidationMode.FULL ? "full" : "incremental";
                long sequence = task.get("modes", Document.class).get(mode, Document.class).getLong("nextStartSequence");
                var start = new ValidationReportAdmission.Start(new ValidationReportAdmission.Task("cluster", "pipeline", run.identity().taskId(), UUID.fromString(task.getString("taskGeneration"))),
                        1, sequence, UUID.randomUUID().toString(), run.identity());
                var admission = new MongoValidationAdmissionStore(other, configuration, "cluster", () -> {});
                assertThat(admission.reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED); admission.open(start);
                assertThat(new MongoValidationResultStore(other, configuration, "cluster", () -> {}).seal(start.report())).isEqualTo(ValidationResultStore.Seal.SEALED);
                return start.report();
            });
        }
        @Override public void close() { try { configuration.drop(); database.drop(); } finally { domain.close(); other.close(); client.close(); } }
    }
}
