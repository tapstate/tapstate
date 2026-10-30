package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandFailedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Explicit business and checkpoint fixtures witness load/write views, not controlled preparation. */
@RequiresDocker
class MongoCachePipelineStoresIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope(SCOPE.clusterId(), 1, SCOPE.cacheIncarnation());
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, SCOPE.pipelineId());
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};
    private static final Instant AT = Instant.parse("2026-10-18T00:00:00Z");

    @Test void initialLoadDisplayRoundTripsAndNewRunReplacesOnlyItsOwnCurrentFacts() {
        withFixture(f -> {
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            var load = new io.tapstate.core.lifecycle.InitialLoad("execution:orders:2:at", AT, "snapshot_and_cdc", Map.of(
                    "orders", new io.tapstate.core.lifecycle.InitialLoad.Table(io.tapstate.core.lifecycle.InitialLoad.Stage.READING, 3L, null)));
            var first = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, AT, List.of(), load);
            observations.save(first);
            assertThat(observations.read("orders")).contains(first);
            var next = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, AT.plusSeconds(1), List.of(),
                    new io.tapstate.core.lifecycle.InitialLoad("execution:orders:3:next", AT.plusSeconds(1), "snapshot_and_cdc", Map.of(
                            "orders", new io.tapstate.core.lifecycle.InitialLoad.Table(io.tapstate.core.lifecycle.InitialLoad.Stage.PENDING, null, null))));
            observations.save(next);
            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(observations.read("orders")).contains(next);
            collection.updateOne(new Document("_id", "orders"), new Document("$unset", new Document("initialLoad", "")));
            assertThat(observations.read("orders").orElseThrow().initialLoad()).isNull();
        });
    }

    @Test void checkpointsAndProjectionsUseTheOriginalScopeAndLiveClaimInOneCacheDomain() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            assertThat(states.read("orders").orElseThrow().epoch()).isZero();
            assertThat(observations.read("orders")).isEmpty();
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var before = state(f.database());
            assertThat(states.compareAndSwap("orders", 0, "PAUSED", AT)).isEqualTo(new CasOutcome.Fenced(1));
            assertThat(state(f.database())).isEqualTo(before);
            var observation = observation(PipelineState.RUNNING);
            observations.save(observation);
            assertThat(observations.read("orders")).contains(observation);
            assertThat(f.domain().pipelineStates(SCOPE).read("orders").orElseThrow().stateJson()).isEqualTo("RUNNING");
            assertThat(state(f.database())).containsEntry("epoch", 1L).containsEntry("runtimeWriteFence", 4L);
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(1);
            f.domain().advanceAuthGeneration(new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "incarnation-a", 1), 2, "reset-auth", ACTIVE);
            observations.save(observation(PipelineState.PAUSED));
            assertThat(observations.read("orders").orElseThrow().state()).isEqualTo(PipelineState.PAUSED);
        });
    }

    @Test void readViewsAndOrdinaryCreateOrDeleteCannotSeedResetOrRetireState() {
        withFixture(f -> {
            var before = snapshot(f.database());
            var read = f.domain().pipelineStates(SCOPE); var observations = f.domain().pipelineObservations(SCOPE);
            assertCode(() -> read.compareAndSwap("orders", 0, "RUNNING", AT), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> observations.save(observation(PipelineState.RUNNING)), IoError.CACHE_BUSINESS_FENCED);
            var writer = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertCode(() -> writer.create("orders", "NEW", AT), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> writer.delete("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> observations.delete("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> writer.read("another"), IoError.CACHE_BUSINESS_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void aKnownMissingCheckpointRefusesInsteadOfBeingSeededOrProjected() {
        withFixture(f -> {
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).deleteOne(new Document("_id", "orders"));
            var before = snapshot(f.database());
            var writer = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertCode(() -> writer.read("orders"), IoError.DOCUMENT_UNREADABLE);
            assertCode(() -> writer.compareAndSwap("orders", 0, "RUNNING", AT), IoError.DOCUMENT_UNREADABLE);
            assertCode(() -> writer.create("orders", "NEW", AT), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.RUNNING)), IoError.DOCUMENT_UNREADABLE);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void oldScopeWithAFreshClaimCannotWriteOrReadTheReplacementGeneration() {
        withFixture(f -> {
            var claims = f.domain().workloadClaims(BUSINESS);
            var held = claims.read(KEY).orElseThrow().claim();
            assertThat(claims.release(held)).isTrue();
            replaceScope(f.database(), scope(2));
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                    new Document("$set", new Document("pipelineScope", header(scope(2))).append("pipelinePreparation",
                            MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
            var fresh = WorkloadClaimFence.from(claims.acquirePipeline(scope(2), OWNER, 1, Duration.ofSeconds(30)).claim());
            assertThat(fresh.claimGeneration()).isGreaterThan(f.fence().claimGeneration());
            var before = snapshot(f.database());
            var stale = f.domain().pipelineStates(SCOPE, fresh, ACTIVE);
            assertCode(() -> stale.read("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> stale.compareAndSwap("orders", 0, "RUNNING", AT), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> f.domain().pipelineObservations(SCOPE, fresh, ACTIVE).save(observation(PipelineState.RUNNING)), IoError.CACHE_BUSINESS_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
            assertThat(f.domain().pipelineStates(scope(2), fresh, ACTIVE).compareAndSwap("orders", 0, "RUNNING", AT))
                    .isInstanceOf(CasOutcome.Applied.class);
        });
    }

    @Test void advancingExecutionFencesThePreviousPassAndTheNextViewUsesOnlyTheReturnedClaim() {
        withFixture(f -> {
            var oldStates = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            var oldObservations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var claims = f.domain().workloadClaims(BUSINESS);
            var advanced = claims.advanceExecution(claims.read(KEY).orElseThrow().claim(), 1, Set.of("node-a")).orElseThrow();
            var before = snapshot(f.database());
            assertCode(() -> oldStates.compareAndSwap("orders", 0, "RUNNING", AT), IoError.WORKLOAD_CLAIM_FENCED);
            assertCode(() -> oldObservations.save(observation(PipelineState.RUNNING)), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
            f.domain().pipelineObservations(SCOPE, WorkloadClaimFence.from(advanced), ACTIVE).save(observation(PipelineState.FAILED));
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders").orElseThrow().state()).isEqualTo(PipelineState.FAILED);
        });
    }

    @Test void wrongRolePipelineOrBusinessCoordinatesNeverUseAnotherAuthority() {
        withFixture(f -> {
            var before = snapshot(f.database());
            for (var key : List.of(new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "orders"),
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "other"),
                    new WorkloadClaimKey("another", WorkloadClaimType.PIPELINE_ACTUATION, "orders"))) {
                var wrong = new WorkloadClaimFence(key, OWNER, 1, 0, 1);
                assertCode(() -> f.domain().pipelineStates(SCOPE, wrong, ACTIVE), IoError.CACHE_BUSINESS_FENCED);
            }
            var wrong = new PipelineIntentScope("cluster-a", 1, "other-incarnation", "orders", 1);
            assertCode(() -> f.domain().pipelineStates(wrong, f.fence(), ACTIVE).read("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void twoSameEpochWritersHaveOneWinnerAndNeitherResetsTheCheckpoint() {
        withFixture(f -> {
            try (var workers = Executors.newFixedThreadPool(2)) {
                var writer = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE); var start = new CountDownLatch(1);
                var a = workers.submit(() -> { start.await(); return writer.compareAndSwap("orders", 0, "RUNNING", AT); });
                var b = workers.submit(() -> { start.await(); return writer.compareAndSwap("orders", 0, "PAUSED", AT); });
                start.countDown();
                var results = List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
                assertThat(results).filteredOn(result -> result instanceof CasOutcome.Applied).hasSize(1);
                assertThat(results).filteredOn(result -> result instanceof CasOutcome.Fenced).containsExactly(new CasOutcome.Fenced(1));
                assertThat(state(f.database())).containsEntry("epoch", 1L).containsEntry("runtimeWriteFence", 3L);
            }
        });
    }

    @Test void anObservationPausedAfterReadingTheOldScopeCannotCommitAcrossReplacement() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (isUpdate(event, "pipeline_state") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = client(listener); var workers = Executors.newSingleThreadExecutor()) {
                var store = new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(observed,
                        observed.getDatabase(f.database().getName()), SCOPE, f.fence(), ACTIVE));
                var result = workers.submit(() -> store.save(observation(PipelineState.RUNNING)));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    // Deliberately keep the claim live: the state header's own write must close this race.
                    replaceScope(f.database(), scope(2));
                } finally { proceed.countDown(); }
                assertThatThrownBy(() -> result.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(error -> assertThat(((TapstateException) error.getCause()).code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
                assertThat(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).countDocuments()).isZero();
                assertThat(state(f.database())).containsEntry("scope", header(scope(2))).containsEntry("runtimeWriteFence", 2L);
                assertThat(claim(f.database())).doesNotContainKey("fencedPipelineWrites");
            }
        });
    }

    @Test void guardRevocationAfterWritesRollsBackClaimStateAndObservationTogether() {
        withFixture(f -> {
            var active = new AtomicBoolean(true);
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) { if (isUpdate(event, "pipeline_observation")) active.set(false); }
            };
            var before = snapshot(f.database());
            try (var observed = client(listener)) {
                var store = new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(observed,
                        observed.getDatabase(f.database().getName()), SCOPE, f.fence(), () -> { if (!active.get()) throw MongoCachePipelineAccess.fenced(); }));
                assertCode(() -> store.save(observation(PipelineState.RUNNING)), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void aTransientRetryRetainsTheOriginalGuardAndDoesNotCommitAfterRevocation() {
        withFixture(f -> {
            var active = new AtomicBoolean(true); var failed = new AtomicInteger(); var writes = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) { if (event.getCommandName().equals("update")) writes.incrementAndGet(); }
                @Override public void commandFailed(CommandFailedEvent event) { if (event.getCommandName().equals("update")) { failed.incrementAndGet(); active.set(false); } }
            };
            var before = snapshot(f.database());
            try (var observed = client(listener)) {
                var store = new MongoCachePipelineStateStore(new MongoCachePipelineAccess(observed,
                        observed.getDatabase(f.database().getName()), SCOPE, f.fence(), () -> { if (!active.get()) throw MongoCachePipelineAccess.fenced(); }));
                f.admin().getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("update")).append("errorCode", 112).append("errorLabels", List.of("TransientTransactionError"))));
                try { assertCode(() -> store.compareAndSwap("orders", 0, "RUNNING", AT), IoError.CACHE_BUSINESS_FENCED); }
                finally { f.admin().getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
                assertThat(failed).hasValue(1); assertThat(writes).hasValue(1);
            }
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void missingCollectionsAndTtlIndexesAreNotRepairedByNormalReads() {
        withFixture(f -> {
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).drop();
            assertThat(f.domain().pipelineStates(SCOPE).read("orders")).isPresent();
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_observation");
            f.database().createCollection("pipeline_observation");
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).createIndex(new Document("touchMillis", 1),
                    new com.mongodb.client.model.IndexOptions().expireAfter(60L, TimeUnit.SECONDS));
            assertCode(() -> f.domain().pipelineStates(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
        });
    }

    @Test void malformedScopeAndCheckpointNumbersFailWithDiagnosticsAndNoRepair() {
        withFixture(f -> {
            var original = state(f.database());
            for (var mutation : List.of(new Document("epoch", 0), new Document("runtimeWriteFence", 0),
                    new Document("epoch", -1L), new Document("stateJson", 7), new Document("touchMillis", "bad"),
                    new Document("scope.bindingVersion", 1), new Document("scope.stateGeneration", 1))) {
                SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id", "orders"), new Document("$set", mutation));
                var before = snapshot(f.database());
                assertCode(() -> f.domain().pipelineStates(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
                assertThat(snapshot(f.database())).isEqualTo(before);
                SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).replaceOne(new Document("_id", "orders"), original);
            }
        });
    }

    @Test void existingWrongScopeObservationIsNotAdoptedOrOverwritten() {
        withFixture(f -> {
            var document = MongoObservationStore.toDocument(observation(PipelineState.RUNNING)).append("scope", header(scope(2)));
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).insertOne(document);
            var before = snapshot(f.database());
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.PAUSED)), IoError.CACHE_BUSINESS_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void saturationRetirementAndClosedOwnersRefuseAllCapturedWriteViews() {
        withFixture(f -> {
            var writer = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id", "orders"), new Document("$set", new Document("runtimeWriteFence", Long.MAX_VALUE - 1)));
            var before = snapshot(f.database());
            assertCode(() -> writer.compareAndSwap("orders", 0, "RUNNING", AT), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> observations.save(observation(PipelineState.RUNNING)), IoError.CACHE_BUSINESS_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
            f.domain().retireBusiness(BUSINESS, "retire-business", ACTIVE);
            assertCode(() -> writer.read("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> observations.save(observation(PipelineState.RUNNING)), IoError.CACHE_BUSINESS_FENCED);
            f.domain().close();
            assertCode(() -> writer.read("orders"), IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void fractionalStoredMetricsAreDiagnosedInsteadOfTruncated() {
        withFixture(f -> {
            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.RUNNING));
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("metrics.rows", 1.75d)));
            var before = snapshot(f.database());
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void malformedStoredStateNeverRetainsItsRawTextInAnExceptionCause() {
        withFixture(f -> {
            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.RUNNING));
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("state", "private-sentinel-state")));
            assertThatThrownBy(() -> f.domain().pipelineObservations(SCOPE).read("orders"))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(error.getCause()).isNull();
                        assertThat(error.getMessage()).doesNotContain("private-sentinel-state");
                    });
        });
    }

    @Test void appliedCheckpointUsesTheSameTimePrecisionAsItsPersistedValue() {
        withFixture(f -> {
            var writer = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            var result = (CasOutcome.Applied) writer.compareAndSwap("orders", 0, "RUNNING", AT.plusNanos(123456789));
            assertThat(result.next()).isEqualTo(writer.read("orders").orElseThrow());
        });
    }

    @Test void currentTypedObservationRoundTripsFactsProgressFailureAndPositions() {
        withFixture(f -> {
            var point = io.tapstate.core.lifecycle.MetricPoint.reading(Map.of(), AT, 2L);
            var fact = io.tapstate.core.lifecycle.MetricFact.single("recordCount", io.tapstate.core.lifecycle.MetricType.GAUGE, "{record}", point);
            var expected = new Observation("orders", PipelineState.FAILED, Map.of("rows", 2L),
                    Map.of("table-a", new io.tapstate.core.lifecycle.TableSnapshot(2L, 2L, 100, true)), Map.of("table-a", "position-a"),
                    new io.tapstate.core.lifecycle.ObservationFailure("engine.job-failed", Map.of("pipeline", "orders")), AT, List.of(fact));
            var writer = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            writer.save(expected);
            assertThat(writer.read("orders")).contains(expected);
        });
    }

    @Test void losingOnlyTheProjectionCollectionDoesNotDisableCheckpointAccess() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).drop();
            assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> states.read("orders")))
                    .as("a derived projection is not a prerequisite of the retained checkpoint").isNull();
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_observation");
        });
    }

    private static Observation observation(PipelineState state) { return new Observation("orders", state, Map.of(), Map.of(), Map.of(), null, AT, List.of()); }
    private static PipelineIntentScope scope(long generation) { return new PipelineIntentScope(SCOPE.clusterId(), 1, SCOPE.cacheIncarnation(), "orders", generation); }
    private static Document header(PipelineIntentScope scope) { return new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
            .append("cacheIncarnation", scope.cacheIncarnation()).append("pipelineId", scope.pipelineId()).append("stateGeneration", scope.stateGeneration()); }
    private static void replaceScope(MongoDatabase database, PipelineIntentScope scope) { SystemCollections.CACHE_PIPELINE_STATE.on(database)
            .updateOne(new Document("_id", "orders"), new Document("$set", new Document("scope", header(scope)))); }
    private static Document state(MongoDatabase database) { return SystemCollections.CACHE_PIPELINE_STATE.on(database).find().first(); }
    private static Document identity(MongoDatabase database) { return SystemCollections.CACHE_IDENTITY.on(database).find().first(); }
    private static Document claim(MongoDatabase database) { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find().first(); }
    private static Map<String, List<Document>> snapshot(MongoDatabase database) {
        var out = new java.util.TreeMap<String, List<Document>>();
        for (var table : List.of(SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION, SystemCollections.CACHE_WORKLOAD_CLAIMS))
            out.put(table.collectionName(), table.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
        return out;
    }
    private static boolean isUpdate(CommandStartedEvent event, String collection) { return event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals(collection); }
    private static MongoClient client(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(code));
    }
    private static void withFixture(Scenario scenario) {
        String name = "cache_pipeline_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "pipeline-test")) {
            var database = admin.getDatabase(name);
            try {
                domain.prepareAuth(new CacheAuthScope(SCOPE.clusterId(), 1, StorageBinding.Mode.OP, SCOPE.cacheIncarnation(), 1), "auth-a", ACTIVE);
                try (var session = admin.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION)) table.prepareCollectionOn(database, session);
                }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "orders").append("scope", header(SCOPE))
                        .append("stateJson", "NEW").append("epoch", 0L).append("touchMillis", AT.toEpochMilli()).append("runtimeWriteFence", 1L));
                var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var retained = raw.acquire(KEY, OWNER, 1, Duration.ofSeconds(30)).claim();
                raw.release(retained);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                        new Document("$set", new Document("pipelineScope", header(SCOPE)).append("pipelinePreparation",
                                MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
                var claim = domain.workloadClaims(BUSINESS).acquirePipeline(SCOPE, OWNER, 1, Duration.ofSeconds(30)).claim();
                scenario.run(new Fixture(admin, database, domain, WorkloadClaimFence.from(claim)));
            } finally { database.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoClient admin, MongoDatabase database, MongoCacheDomain domain, WorkloadClaimFence fence) {}
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
