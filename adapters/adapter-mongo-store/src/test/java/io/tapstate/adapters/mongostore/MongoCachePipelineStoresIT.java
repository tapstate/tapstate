package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandFailedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.DeadlineWorkloadClaimStore;
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

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

    @Test void sharedStartupOrderReadsFreshLeaseAndCounterInOneCommand() {
        withFixture(f -> {
            try (var session = f.admin().startSession()) {
                SystemCollections.CACHE_SNAPSHOT_ORDERS.prepareCollectionOn(f.database(), session);
                MongoSnapshotStateStore.seedInitial(session, f.database(), SCOPE);
            }
            var commands = new ArrayList<String>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommandName()); }
            };
            try (var observed = client(listener)) {
                observed.getDatabase("admin").runCommand(new Document("ping", 1));
                commands.clear();
                var store = new MongoSnapshotStateStore(new MongoCachePipelineAccess(observed,
                        observed.getDatabase(f.database().getName()), SCOPE, f.fence(), ACTIVE));
                assertThat(store.startupOrder()).isZero();
                assertThat(commands).containsExactly("aggregate");

                var state = SystemCollections.CACHE_PIPELINE_STATE.on(f.database());
                var stateId = new Document("_id", "orders");
                assertThat(state.updateOne(stateId, new Document("$set",
                        new Document("padding", "x".repeat(64 * 1024)))).getModifiedCount()).isOne();
                commands.clear();
                assertThat(store.startupOrder()).isZero();
                assertThat(commands).startsWith("aggregate").contains("find").endsWith("commitTransaction");
                assertThat(state.updateOne(stateId, new Document("$unset",
                        new Document("padding", ""))).getModifiedCount()).isOne();

                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                        new Document("$set", new Document("leaseUntil", new java.util.Date(0))));
                assertCode(store::startupOrder, IoError.WORKLOAD_CLAIM_FENCED);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                        new Document("$set", new Document("leaseUntil", new java.util.Date(System.currentTimeMillis() + 30000))
                                .append("ownerBootId", "other-boot")));
                assertCode(store::startupOrder, IoError.WORKLOAD_CLAIM_FENCED);
            }
        });
    }

    @Test void sharedStartupOrderRequiresTheOriginalLiveNodeForANodeBackedPipeline() {
        withFixture(f -> {
            try (var session = f.admin().startSession()) {
                SystemCollections.CACHE_SNAPSHOT_ORDERS.prepareCollectionOn(f.database(), session);
                MongoSnapshotStateStore.seedInitial(session, f.database(), SCOPE);
            }
            var owner = new WorkloadOwner(SCOPE.clusterId(), "node-backed-boot");
            var key = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, owner.nodeId());
            var node = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()))
                    .acquire(key, owner, 1, Duration.ofSeconds(30)).claim();
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                    new Document("$set", new Document("ownerNodeId", owner.nodeId()).append("ownerBootId", owner.bootId())
                            .append("leaseUntil", new java.util.Date(-1)).append(MongoWorkloadClaimStore.NODE_LEASE_GENERATION, node.claimGeneration())));
            var original = new WorkloadClaimFence(KEY, owner, f.fence().claimGeneration(),
                    f.fence().executionGeneration(), f.fence().topologyRevision());
            var store = new MongoSnapshotStateStore(new MongoCachePipelineAccess(f.admin(), f.database(), SCOPE, original, ACTIVE));
            assertThat(store.startupOrder()).isZero();
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(key)),
                    new Document("$set", new Document("leaseUntil", new java.util.Date(0))));
            assertCode(store::startupOrder, IoError.WORKLOAD_CLAIM_FENCED);
        });
    }

    @Test void preparedMetadataReadsKeepTheSameSnapshotAndWriteFences() {
        withFixture(f -> {
            var commands = new java.util.ArrayList<String>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommandName()); }
            };
            try (var observed = client(listener)) {
                var database = observed.getDatabase(f.database().getName());
                database.runCommand(new Document("ping", 1));
                var access = new MongoCachePipelineAccess(observed, database, SCOPE, f.fence(), ACTIVE);
                var store = new MongoCacheDerivedSchemaStore(access);
                commands.clear();
                assertThat(store.latestMany(SCOPE.pipelineId(), java.util.Set.of("source.orders"))).isEmpty();
                var read = java.util.List.copyOf(commands);
                commands.clear();
                store.record(SCOPE.pipelineId(), "source.orders", java.util.Map.of("id", "bigint"), "sql", "input", "source");
                var write = java.util.List.copyOf(commands);
                System.out.println("PREPARED_METADATA read=" + read + " write=" + write);
                assertThat(read).containsExactly("aggregate", "find", "commitTransaction");
                assertThat(write).containsExactly("aggregate", "update", "update", "find", "update", "commitTransaction");
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"),
                        new Document("$set", new Document("businessPhase", "RETIRED")));
                assertCode(() -> store.latestMany(SCOPE.pipelineId(), java.util.Set.of("source.orders")),
                        IoError.CACHE_BUSINESS_FENCED);
            }
        });
    }

    @Test void startCheckpointReadsAdmissionStateAndClaimTogether() {
        withFixture(f -> {
            var expected = f.domain().pipelineStates(SCOPE).read(SCOPE.pipelineId()).orElseThrow();
            var commands = new java.util.concurrent.CopyOnWriteArrayList<String>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getDatabaseName().equals(f.database().getName()) || event.getCommandName().equals("commitTransaction"))
                        commands.add(event.getCommandName());
                }
            };
            try (var observed = client(listener)) {
                var access = new MongoCachePipelineAccess(observed, observed.getDatabase(f.database().getName()), SCOPE, null, ACTIVE);
                assertThat(access.startCheckpoint()).isEqualTo(expected);
                assertThat(commands).containsExactly("aggregate", "commitTransaction");
            }
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).deleteOne(new Document("_id", SCOPE.pipelineId()));
            assertCode(() -> f.domain().startCheckpoint(SCOPE, ACTIVE), IoError.DOCUMENT_UNREADABLE);
        });
    }

    @Test void startCheckpointRequiresItsPreparedClaimAndOriginalCache() {
        withFixture(f -> {
            var wrong = new PipelineIntentScope(SCOPE.clusterId(), SCOPE.bindingVersion(), "other-incarnation", SCOPE.pipelineId(), SCOPE.stateGeneration());
            assertCode(() -> f.domain().startCheckpoint(wrong, ACTIVE), IoError.CACHE_BUSINESS_FENCED);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).deleteOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)));
            assertCode(() -> f.domain().startCheckpoint(SCOPE, ACTIVE), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        });
    }

    @Test void terminalRefreshUsesOneCoherentCheckpointAndProjectionRead() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var original = new Observation("orders", PipelineState.FAILED, Map.of("recordCount", 8L), Map.of(),
                    Map.of("orders", "position"), new io.tapstate.core.lifecycle.ObservationFailure("test.failure", Map.of()),
                    AT, List.of());
            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(original, 1);
            var commands = new java.util.concurrent.CopyOnWriteArrayList<String>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getDatabaseName().equals(f.database().getName()) || e.getCommandName().equals("commitTransaction"))
                        commands.add(e.getCommandName());
                }
            };
            try (var observed = client(listener)) {
                var store = new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(observed,
                        observed.getDatabase(f.database().getName()), SCOPE, f.fence(), ACTIVE));
                var refreshed = store.refreshTerminal(original, 1, AT.plusSeconds(1)).orElseThrow();
                assertThat(refreshed.observedAt()).isEqualTo(AT.plusSeconds(1));
                assertThat(refreshed.metrics()).isEqualTo(original.metrics());
                assertThat(refreshed.positions()).isEqualTo(original.positions());
                assertThat(refreshed.failure()).isEqualTo(original.failure());
                assertThat(commands).hasSize(6).doesNotContain("listCollections", "listIndexes");
                assertThat(java.util.Collections.frequency(commands, "commitTransaction")).isEqualTo(1);
                assertThat(java.util.Collections.frequency(commands, "find")).isEqualTo(2);
                assertThat(store.read("orders")).contains(refreshed);
                assertThat(store.refreshTerminal(original, 1, AT.plusSeconds(2))).isEmpty();
                assertThat(states.compareAndSwap("orders", 1, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
                assertThat(store.refreshTerminal(refreshed, 1, AT.plusSeconds(2))).isEmpty();
                assertThat(store.read("orders")).isEmpty();
            }
        });
    }

    @Test void terminalRefreshDoesNotHideADelayedFailureFromThePreviousClaim() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var previous = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var claims = f.domain().workloadClaims(BUSINESS);
            var next = WorkloadClaimFence.from(claims.advanceExecution(claims.read(KEY).orElseThrow().claim(),
                    1, Set.of("node-a")).orElseThrow());
            var replacement = f.domain().pipelineObservations(SCOPE, next, ACTIVE);
            var noReason = observation(PipelineState.FAILED);
            replacement.save(noReason, 1);
            var reason = new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new io.tapstate.core.lifecycle.ObservationFailure("test.failure", Map.of()), AT, List.of());
            previous.save(reason, 1);
            assertThat(replacement.read("orders")).contains(reason);
            assertThat(replacement.refreshTerminal(noReason, 1, AT.plusSeconds(1))).isEmpty();
            assertThat(replacement.read("orders")).contains(reason);
        });
    }

    @Test void terminalRefreshNeverAdoptsAnotherClaimOrOverwritesANewerSample() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var original = new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new io.tapstate.core.lifecycle.ObservationFailure("test.failure", Map.of()), AT, List.of());
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            observations.save(original, 1);
            var claims = f.domain().workloadClaims(BUSINESS);
            var next = WorkloadClaimFence.from(claims.advanceExecution(claims.read(KEY).orElseThrow().claim(),
                    1, Set.of("node-a")).orElseThrow());
            assertThat(observations.refreshTerminal(original, 1, AT.plusSeconds(1))).isEmpty();
            var replacement = f.domain().pipelineObservations(SCOPE, next, ACTIVE);
            assertThat(replacement.refreshTerminal(original, 1, AT.plusSeconds(1))).isEmpty();
            replacement.save(original, 1);
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (isUpdate(e, SystemCollections.CACHE_PIPELINE_OBSERVATION.collectionName()) && once.compareAndSet(false, true)) {
                        entered.countDown(); await(release);
                    }
                }
            };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var store = new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(observed,
                        observed.getDatabase(f.database().getName()), SCOPE, next, ACTIVE));
                var refresh = worker.submit(() -> store.refreshTerminal(original, 1, AT.plusSeconds(1)));
                try {
                    await(entered);
                    var newer = new Observation("orders", PipelineState.FAILED, Map.of("recordCount", 9L), Map.of(),
                            Map.of(), original.failure(), AT.plusSeconds(2), List.of());
                    replacement.save(newer, 1);
                    release.countDown();
                    assertThat(refresh.get(10, TimeUnit.SECONDS)).isEmpty();
                    assertThat(replacement.read("orders")).contains(newer);
                } finally { release.countDown(); }
            }
        });
    }

    @Test void initialLoadDisplayRoundTripsAndNewRunReplacesOnlyItsOwnCurrentFacts() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            var load = new io.tapstate.core.lifecycle.InitialLoad("execution:orders:2:at", AT, "snapshot_and_cdc", Map.of(
                    "orders", new io.tapstate.core.lifecycle.InitialLoad.Table(io.tapstate.core.lifecycle.InitialLoad.Stage.READING, 3L, null)));
            var first = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, AT, List.of(), load);
            observations.save(first, 1);
            assertThat(observations.read("orders")).contains(first);
            var next = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, AT.plusSeconds(1), List.of(),
                    new io.tapstate.core.lifecycle.InitialLoad("execution:orders:3:next", AT.plusSeconds(1), "snapshot_and_cdc", Map.of(
                            "orders", new io.tapstate.core.lifecycle.InitialLoad.Table(io.tapstate.core.lifecycle.InitialLoad.Stage.PENDING, null, null))));
            observations.save(next, 1);
            observations.save(first, 1);
            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(observations.read("orders")).contains(next);
            collection.updateOne(new Document("_id", collection.find().first().get("_id")), new Document("$unset", new Document("initialLoad", "")));
            assertThat(observations.read("orders").orElseThrow().initialLoad()).isNull();
        });
    }

    @Test void existingObservationUpdatesDoNotCreateOrReplaceAnotherExecutionsProjection() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var observations = (MongoCachePipelineObservationStore) f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var first = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, AT, List.of());
            var later = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, AT.plusSeconds(1), List.of());
            var table = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            assertThat(observations.updateExisting(first, 1)).isFalse();
            observations.save(first, 1);
            assertThat(observations.updateExisting(later, 1)).isTrue();
            assertThat(observations.updateExisting(first, 1)).isTrue();
            assertThat(observations.read("orders")).contains(later);
            assertThat(observations.updateExisting(later, 2)).isFalse();
            var other = new WorkloadClaimFence(f.fence().key(), f.fence().owner(), f.fence().claimGeneration(),
                    f.fence().executionGeneration() + 1, f.fence().topologyRevision());
            var replacement = (MongoCachePipelineObservationStore) f.domain().pipelineObservations(SCOPE, other, ACTIVE);
            assertThat(replacement.updateExisting(later, 1)).isFalse();
            assertThat(table.countDocuments()).isOne();
            table.drop();
            assertThat(observations.updateExisting(later, 1)).isFalse();
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_observation");
        });
    }

    @Test void aBulkObservationPreservesTheSuccessfulEntryAndReportsBothFailuresInOrder() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE)
                    .compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var accepted = observation(PipelineState.RUNNING);
            var rejected = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null,
                    AT.plusSeconds(1), List.of());
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            f.database().runCommand(new Document("collMod", collection.getNamespace().getCollectionName())
                    .append("validator", new Document("observedAt", new Document("$ne", Date.from(rejected.observedAt())))));
            var lostClaim = MongoCachePipelineAccess.fenced();

            var failures = f.domain().writeObservations(List.of(
                    new MongoCacheDomain.ObservationWrite(SCOPE, f.fence(), () -> { throw lostClaim; }, accepted, 1),
                    new MongoCacheDomain.ObservationWrite(SCOPE, f.fence(), ACTIVE, accepted, 1),
                    new MongoCacheDomain.ObservationWrite(SCOPE, f.fence(), ACTIVE, rejected, 1)));

            assertThat(failures).hasSize(3);
            assertThat(failures.get(0)).isSameAs(lostClaim);
            assertThat(failures.get(1)).isNull();
            assertThat(failures.get(2)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(accepted);
            assertThat(collection.countDocuments()).isOne();
        });
    }

    @Test void bulkObservationRetainsTheStorageDiagnosticWhenItsWriteFails() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE)
                    .compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var sample = observation(PipelineState.RUNNING);
            var entry = new MongoCacheDomain.ObservationWrite(SCOPE, f.fence(), ACTIVE, sample, 1);
            var admin = f.admin().getDatabase("admin");
            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                    .append("data", new Document("failCommands", List.of("update")).append("errorCode", 2)));
            try {
                assertThatThrownBy(() -> f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(sample, 1))
                        .isInstanceOfSatisfying(TapstateException.class, failure ->
                                assertThat(failure.code().code()).isEqualTo("TS0480001"));
                assertThat(f.domain().writeObservations(List.of(entry)).getFirst())
                        .isInstanceOfSatisfying(TapstateException.class, failure -> {
                            assertThat(failure.code().code()).isEqualTo("TS0480001");
                            assertThat(failure.args()).containsExactly(Map.entry("detail", "MongoCommandException code=2"));
                            assertThat(failure.getCause()).isNull();
                        });
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
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
            observations.save(observation, 1);
            assertThat(observations.read("orders")).contains(observation);
            assertThat(f.domain().pipelineStates(SCOPE).read("orders").orElseThrow().stateJson()).isEqualTo("RUNNING");
            assertThat(state(f.database())).isEqualTo(before);
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(1);
            f.domain().advanceAuthGeneration(new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "incarnation-a", 1), 2, "reset-auth", ACTIVE);
            assertThat(states.compareAndSwap("orders", 1, "PAUSED", AT)).isInstanceOf(CasOutcome.Applied.class);
            observations.save(observation(PipelineState.PAUSED), 2);
            assertThat(observations.read("orders").orElseThrow().state()).isEqualTo(PipelineState.PAUSED);
        });
    }

    @Test void claimReadsKeepTheirScopeWithAndWithoutSnapshotSessions() {
        withFixture(f -> {
            var claims = (DeadlineWorkloadClaimStore) f.domain().workloadClaims(BUSINESS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            var expected = claims.withDeadline(deadline, () -> claims.readPipeline(SCOPE)).orElseThrow();
            assertThat(WorkloadClaimFence.from(expected.claim())).isEqualTo(f.fence());

            var olderTopology = mock(ClusterDescription.class);
            doReturn(ClusterType.UNKNOWN).when(olderTopology).getType();
            var legacyClient = mock(MongoClient.class, delegatesTo(f.admin()));
            doReturn(olderTopology).when(legacyClient).getClusterDescription();
            var legacy = new MongoCacheWorkloadClaimStore(legacyClient, f.database(), BUSINESS, ACTIVE);
            assertThat(legacy.readPipeline(SCOPE).orElseThrow().claim()).isEqualTo(expected.claim());
            assertCode(() -> legacy.readPipeline(scope(2)), IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void aCurrentRunInitialLoadSurvivesTheStrictCacheCodec() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            observations.save(observation(PipelineState.RUNNING), 1);
            Document load = new Document("runKey", "run-1").append("observedAt", java.util.Date.from(AT))
                    .append("readMode", "snapshot_and_cdc")
                    .append("tables", new Document("orders", new Document("stage", "READING")
                            .append("rowsRead", 7L).append("rowsTotalEstimate", 10L))
                            .append("customers", new Document("stage", "PENDING")));
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            collection.updateOne(new Document("_id", collection.find().first().get("_id")), new Document("$set", new Document("initialLoad", load)));
            Observation decoded = observations.read("orders").orElseThrow();
            observations.save(decoded, 1);
            assertThat(collection.find().first().get("initialLoad")).isEqualTo(load);
        });
    }

    @Test void malformedInitialLoadCannotBeCoercedOrRepairedByARead() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            observations.save(observation(PipelineState.RUNNING), 1);
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            Document original = collection.find().first();
            for (var mutation : List.of(new Document("initialLoad.tables.orders.rowsRead", 7.5d),
                    new Document("initialLoad.tables.orders.rowsRead", -1L),
                    new Document("initialLoad.tables.orders.stage", "UNRECOGNIZED"),
                    new Document("initialLoad.unexpected", "private-sentinel"))) {
                collection.updateOne(new Document("_id", collection.find().first().get("_id")), new Document("$set", mutation));
                var before = snapshot(f.database());
                assertCode(() -> observations.read("orders"), IoError.DOCUMENT_UNREADABLE);
                assertThat(snapshot(f.database())).isEqualTo(before);
                collection.replaceOne(new Document("_id", original.get("_id")), original);
            }
        });
    }

    @Test void readViewsAndOrdinaryCreateOrDeleteCannotSeedResetOrRetireState() {
        withFixture(f -> {
            var before = snapshot(f.database());
            var read = f.domain().pipelineStates(SCOPE); var observations = f.domain().pipelineObservations(SCOPE);
            assertCode(() -> read.compareAndSwap("orders", 0, "RUNNING", AT), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> observations.save(observation(PipelineState.RUNNING), 1), IoError.CACHE_BUSINESS_FENCED);
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
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
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
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.CACHE_BUSINESS_FENCED);
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
            assertThat(snapshot(f.database())).isEqualTo(before);
            var next = WorkloadClaimFence.from(advanced);
            var states = f.domain().pipelineStates(SCOPE, next, ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            oldObservations.save(observation(PipelineState.RUNNING), 1);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).isEmpty();
            assertThat(states.compareAndSwap("orders", 1, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            f.domain().pipelineObservations(SCOPE, next, ACTIVE).save(observation(PipelineState.FAILED), 2);
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

    @Test void anUnrelatedPipelineCheckpointCommitsWhileAnotherPipelineWaitsOnTheSameClient() {
        withFixture(f -> {
            var healthyScope = new PipelineIntentScope(SCOPE.clusterId(), SCOPE.bindingVersion(),
                    SCOPE.cacheIncarnation(), "shipments", SCOPE.stateGeneration());
            var healthyKey = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION,
                    healthyScope.pipelineId());
            var healthyFence = new WorkloadClaimFence(healthyKey, f.fence().owner(), f.fence().claimGeneration(),
                    f.fence().executionGeneration(), f.fence().topologyRevision());
            var checkpoint = new Document(state(f.database()));
            checkpoint.put("_id", healthyScope.pipelineId());
            checkpoint.put("scope", header(healthyScope));
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).insertOne(checkpoint);
            var healthyClaim = new Document(claim(f.database()));
            healthyClaim.put("_id", MongoWorkloadClaimStore.id(healthyKey));
            healthyClaim.put("resourceId", healthyScope.pipelineId());
            healthyClaim.put("pipelineScope", header(healthyScope));
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).insertOne(healthyClaim);

            var held = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var stalled = new AtomicBoolean();
            var healthyEntered = new CountDownLatch(1);
            var healthyCompleted = new CountDownLatch(1);
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (isUpdate(event, "pipeline_state") && SCOPE.pipelineId().equals(event.getCommand()
                            .getArray("updates").get(0).asDocument().getDocument("q").getString("_id").getValue())
                            && stalled.compareAndSet(false, true)) {
                        held.countDown();
                        await(release);
                    }
                }
            };
            try (var observed = client(listener); var workers = Executors.newFixedThreadPool(2)) {
                var database = observed.getDatabase(f.database().getName());
                var slowStates = new MongoCachePipelineStateStore(
                        new MongoCachePipelineAccess(observed, database, SCOPE, f.fence(), ACTIVE), new MongoStructureProof());
                var healthyStates = new MongoCachePipelineStateStore(
                        new MongoCachePipelineAccess(observed, database, healthyScope, healthyFence, ACTIVE), new MongoStructureProof());
                var slow = workers.submit(() -> slowStates.compareAndSwap(SCOPE.pipelineId(), 0, "RUNNING", AT));
                try {
                    assertThat(held.await(10, TimeUnit.SECONDS)).isTrue();
                    var healthy = workers.submit(() -> {
                        healthyEntered.countDown();
                        try { return healthyStates.compareAndSwap(healthyScope.pipelineId(), 0, "RUNNING", AT); }
                        finally { healthyCompleted.countDown(); }
                    });
                    try {
                        assertThat(healthyEntered.await(5, TimeUnit.SECONDS)).isTrue();
                        boolean completedBeforeRelease = healthyCompleted.await(2, TimeUnit.SECONDS);
                        System.out.println("CHECKPOINT_PIPELINE_ISOLATION healthyCompletedBeforeRelease=" + completedBeforeRelease);
                        assertThat(completedBeforeRelease)
                                .as("an independent Pipeline checkpoint must commit while the other Pipeline remains stalled")
                                .isTrue();
                        assertThat(healthy.get(5, TimeUnit.SECONDS)).isInstanceOf(CasOutcome.Applied.class);
                        assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database())
                                .find(new Document("_id", healthyScope.pipelineId())).first()).containsEntry("stateJson", "RUNNING");
                    } finally {
                        release.countDown();
                        assertThat(healthy.get(15, TimeUnit.SECONDS)).isInstanceOf(CasOutcome.Applied.class);
                        System.out.println("CHECKPOINT_PIPELINE_ISOLATION healthyCommittedAfterRelease=true");
                    }
                } finally {
                    release.countDown();
                    assertThat(slow.get(15, TimeUnit.SECONDS)).isInstanceOf(CasOutcome.Applied.class);
                }
            }
        });
    }

    @Test void aMultiPipelineWaitUsesStableOrderAndReleasesItsLanesWhenInterrupted() throws Exception {
        var client = mock(MongoClient.class);
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var waitingThread = new AtomicReference<Thread>();
        try (var workers = Executors.newFixedThreadPool(3)) {
            var owner = workers.submit(() -> MongoCachePipelineStateStore.writeTransaction(client, "orders", () -> {
                held.countDown(); await(release); return true;
            }));
            Future<Boolean> waiting = null;
            try {
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                waiting = waitForBothCheckpointLanes(workers, client, waitingThread);
                Thread waiter = awaitCheckpointLaneWait(waitingThread);
                var independent = workers.submit(() -> MongoCachePipelineStateStore.writeTransaction(client, "shipments", () -> true));
                assertThat(independent.get(2, TimeUnit.SECONDS)).isTrue();
                waiter.interrupt();
                assertThat(waiting.get(5, TimeUnit.SECONDS)).isFalse();
                var afterInterruption = workers.submit(() -> MongoCachePipelineStateStore.writeTransaction(client, "shipments", () -> true));
                assertThat(afterInterruption.get(2, TimeUnit.SECONDS)).isTrue();
            } finally {
                if (waiting != null) waiting.cancel(true);
                release.countDown();
                assertThat(owner.get(15, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test void interruptingAMultiPipelineWaitReleasesItsAlreadyAcquiredLane() throws Exception {
        var client = mock(MongoClient.class);
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var waitingThread = new AtomicReference<Thread>();
        var contenderThread = new AtomicReference<Thread>();
        try (var workers = Executors.newFixedThreadPool(3)) {
            var owner = workers.submit(() -> MongoCachePipelineStateStore.writeTransaction(client, "shipments", () -> {
                held.countDown(); await(release); return true;
            }));
            Future<Boolean> waiting = null;
            Future<Boolean> contender = null;
            try {
                assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
                waiting = waitForBothCheckpointLanes(workers, client, waitingThread);
                Thread waiter = awaitCheckpointLaneWait(waitingThread);
                contender = workers.submit(() -> {
                    contenderThread.set(Thread.currentThread());
                    return MongoCachePipelineStateStore.writeTransaction(client, "orders", () -> true);
                });
                awaitCheckpointLaneWait(contenderThread);
                assertThat(contender.isDone()).as("the multi-Pipeline caller already holds orders").isFalse();
                waiter.interrupt();
                assertThat(waiting.get(5, TimeUnit.SECONDS)).isFalse();
                assertThat(contender.get(2, TimeUnit.SECONDS)).isTrue();
            } finally {
                if (waiting != null) waiting.cancel(true);
                if (contender != null) contender.cancel(true);
                release.countDown();
                assertThat(owner.get(15, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static Future<Boolean> waitForBothCheckpointLanes(ExecutorService workers, MongoClient client,
            AtomicReference<Thread> waitingThread) {
        return workers.submit(() -> {
            waitingThread.set(Thread.currentThread());
            try {
                return MongoCachePipelineStateStore.writeTransaction(client,
                        List.of("shipments", "orders", "shipments"), () -> true);
            } catch (TapstateException failure) {
                assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                return false;
            }
        });
    }

    private static Thread awaitCheckpointLaneWait(AtomicReference<Thread> thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        Thread waiting;
        while ((waiting = thread.get()) == null || waiting.getState() != Thread.State.WAITING) {
            if (System.nanoTime() >= deadline) throw new AssertionError("caller did not wait for the held checkpoint lane");
            Thread.sleep(1);
        }
        return waiting;
    }

    @Test void delayedObservationCanWriteItsOwnRecordButCannotCrossAStopOrReplacement() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            var writes = new java.util.concurrent.CopyOnWriteArrayList<String>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update")) {
                        writes.add(event.getCommand().getString("update").getValue());
                        if (isUpdate(event, "pipeline_observation")) { paused.countDown(); await(proceed); }
                    }
                }
            };
            try (var observed = client(listener); var workers = Executors.newSingleThreadExecutor()) {
                var store = new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(observed,
                        observed.getDatabase(f.database().getName()), SCOPE, f.fence(), ACTIVE));
                var result = workers.submit(() -> store.save(observation(PipelineState.RUNNING), 1));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(states.compareAndSwap("orders", 1, "STOPPED", AT)).isInstanceOf(CasOutcome.Applied.class);
                } finally { proceed.countDown(); }
                result.get(15, TimeUnit.SECONDS);
                assertThat(writes).containsExactly("pipeline_observation");
                assertThat(store.read("orders")).isEmpty();
                assertThat(f.domain().pipelineObservationBatch(List.of(SCOPE), ACTIVE, true).get("orders").observation()).isEmpty();
                store.save(observation(PipelineState.STOPPED), 2);
                assertThat(f.domain().pipelineObservationBatch(List.of(SCOPE), ACTIVE, true).get("orders").observation().orElseThrow().state())
                        .isEqualTo(PipelineState.STOPPED);
                replaceScope(f.database(), scope(2));
                assertCode(() -> store.read("orders"), IoError.CACHE_BUSINESS_FENCED);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                        new Document("$set", new Document("pipelineScope", header(scope(2)))));
                store.save(observation(PipelineState.RUNNING), 1);
                assertThat(f.domain().pipelineObservations(scope(2)).read("orders")).isEmpty();
                assertThat(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).countDocuments()).isEqualTo(2);
            }
        });
    }

    @Test void observationHousekeepingHasOneBoundedBatchAndCannotEraseARecreatedScope() {
        withFixture(f -> {
            var rows = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            for (long epoch = 1; epoch <= 64; epoch++) {
                rows.insertOne(new Document("_id", new Document("scope", header(SCOPE)).append("epoch", epoch)));
            }
            replaceScope(f.database(), scope(2));
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                    new Document("$set", new Document("pipelineScope", header(scope(2)))));
            var paused = new CountDownLatch(1);
            var proceed = new CountDownLatch(1);
            var firstDelete = new AtomicBoolean(true);
            var finds = new AtomicInteger();
            var batches = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (!event.getDatabaseName().equals(f.database().getName())
                            || !Thread.currentThread().getName().equals("tapstate-observation-reap")) return;
                    var command = event.getCommand();
                    if (event.getCommandName().equals("find")) {
                        assertThat(command.getNumber("limit").intValue()).isEqualTo(32);
                        finds.incrementAndGet();
                    } else if (event.getCommandName().equals("delete")) {
                        batches.add(command.getArray("deletes").getFirst().asDocument().getDocument("q")
                                .getDocument("_id").getArray("$in").size());
                        if (firstDelete.compareAndSet(true, false)) { paused.countDown(); await(proceed); }
                    }
                }
            };
            try (var observed = client(listener)) {
                var database = observed.getDatabase(f.database().getName());
                var predecessor = new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(observed,
                        database, scope(2), f.fence(), ACTIVE));
                try {
                    // The cadence counter is process-wide. Each attempt advances a full 64-save interval.
                    awaitCleanup(() -> {
                        for (int i = 0; i < 64; i++) predecessor.save(observation(PipelineState.RUNNING), 1);
                        return paused.getCount() == 0;
                    });
                    assertThat(finds).hasValue(1);
                    var stale = new Document("_id.scope", header(SCOPE));
                    for (int i = 0; i < 10; i++) MongoCachePipelineObservationStore.reap(database, stale);
                    assertThat(finds).hasValue(1);
                    var current = rows.find(new Document("_id.scope", header(scope(2)))).first();
                    assertThat(current).isNotNull();
                    var successor = Document.parse(current.toJson());
                    successor.get("_id", Document.class).put("scope", header(scope(3)));
                    var neighbor = Document.parse(current.toJson());
                    neighbor.get("_id", Document.class).get("scope", Document.class).put("pipelineId", "neighbor");
                    neighbor.put("pipelineId", "neighbor");
                    rows.insertMany(List.of(successor, neighbor));
                    replaceScope(f.database(), scope(3));
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                            new Document("$set", new Document("pipelineScope", header(scope(3)))));
                    proceed.countDown();
                    awaitCleanup(() ->
                            rows.countDocuments(new Document("_id.scope", header(SCOPE))) == 32);
                    assertThat(batches).containsExactly(32);
                    assertThat(rows.find(new Document("_id", successor.get("_id"))).first()).isEqualTo(successor);
                    assertThat(rows.find(new Document("_id", neighbor.get("_id"))).first()).isEqualTo(neighbor);
                    var writer = new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(observed,
                            database, scope(3), f.fence(), ACTIVE));
                    awaitCleanup(() -> {
                        for (int i = 0; i < 64; i++) writer.save(observation(PipelineState.RUNNING), 1);
                        return rows.countDocuments(new Document("_id.scope.pipelineId", "orders")
                                .append("_id.scope.stateGeneration", new Document("$lt", 3))) == 0;
                    });
                    assertThat(batches).containsExactly(32, 32, 1);
                    System.out.println("OBSERVATION_CLEANUP batches=" + batches + " successorKept=true neighborKept=true singleFlight=true");
                    assertThat(rows.countDocuments()).isEqualTo(2);
                    assertThat(rows.find(new Document("_id", successor.get("_id"))).first()).isEqualTo(successor);
                    assertThat(rows.find(new Document("_id", neighbor.get("_id"))).first()).isEqualTo(neighbor);
                } finally { proceed.countDown(); }
            }
        });
    }

    @Test void anInactiveLocalOwnerCannotOpenANewObservationWrite() {
        withFixture(f -> {
            var before = snapshot(f.database());
            var store = f.domain().pipelineObservations(SCOPE, f.fence(), () -> { throw MongoCachePipelineAccess.fenced(); });
            assertCode(() -> store.save(observation(PipelineState.RUNNING), 1), IoError.CACHE_BUSINESS_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void aMissingCurrentObservationKeepsOnlyTheRunningWitnessInTheSameEpoch() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var first = f.domain().pipelineObservationBatch(List.of(SCOPE), ACTIVE, true).get("orders");
            assertThat(first.observation()).isEmpty();
            assertThat(first.runningObservationUnavailable()).isFalse();

            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.RUNNING), 1);
            var claims = f.domain().workloadClaims(BUSINESS);
            var next = WorkloadClaimFence.from(claims.advanceExecution(claims.read(KEY).orElseThrow().claim(),
                    1, Set.of("node-a")).orElseThrow());
            var replaced = f.domain().pipelineObservationBatch(List.of(SCOPE), ACTIVE, true).get("orders");
            assertThat(replaced.observation()).isEmpty();
            assertThat(replaced.runningObservationUnavailable()).isTrue();

            var nextStates = f.domain().pipelineStates(SCOPE, next, ACTIVE);
            assertThat(nextStates.compareAndSwap("orders", 1, "STOPPED", AT)).isInstanceOf(CasOutcome.Applied.class);
            assertThat(nextStates.compareAndSwap("orders", 2, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var restarted = f.domain().pipelineObservationBatch(List.of(SCOPE), ACTIVE, true).get("orders");
            assertThat(restarted.observation()).isEmpty();
            assertThat(restarted.runningObservationUnavailable()).isFalse();
        });
    }

    @Test void aSettledStopCanBeRepublishedOnlyForTheSameCheckpointAndExecution() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "STOPPED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var checkpoint = states.read("orders").orElseThrow();
            var original = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            assertThat(original.republishStopped(checkpoint)).isEmpty();
            var stopped = observation(PipelineState.STOPPED);
            original.save(stopped, checkpoint.epoch());

            var claims = f.domain().workloadClaims(BUSINESS);
            assertThat(claims.release(claims.read(KEY).orElseThrow().claim())).isTrue();
            assertThat(original.republishStopped(checkpoint)).isEmpty();
            var granted = claims.acquirePipeline(SCOPE, new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30));
            assertThat(granted.acquired()).isTrue();
            var next = WorkloadClaimFence.from(granted.claim());
            assertThat(next.executionGeneration()).isEqualTo(f.fence().executionGeneration());
            assertThat(next.claimGeneration()).isGreaterThan(f.fence().claimGeneration());
            var resumed = f.domain().pipelineObservations(SCOPE, next, ACTIVE);
            assertThat(resumed.read("orders")).isEmpty();
            assertThat(original.republishStopped(checkpoint)).isEmpty();
            assertThat(resumed.republishStopped(checkpoint)).contains(stopped);
            assertThat(resumed.read("orders")).contains(stopped);
            var later = new Observation("orders", PipelineState.STOPPED, Map.of(), Map.of(),
                    Map.of("source.orders", "later-position"), null, AT.plusSeconds(1), List.of());
            resumed.save(later, checkpoint.epoch());
            assertThat(resumed.republishStopped(checkpoint)).contains(later);
            assertThat(resumed.read("orders")).contains(later);

            var advanced = claims.advanceExecution(granted.claim(), 1, Set.of("node-b")).orElseThrow();
            var successor = WorkloadClaimFence.from(advanced);
            var newObservations = f.domain().pipelineObservations(SCOPE, successor, ACTIVE);
            assertThat(newObservations.republishStopped(checkpoint)).isEmpty();
            var successorStates = f.domain().pipelineStates(SCOPE, successor, ACTIVE);
            assertThat(successorStates.compareAndSwap("orders", checkpoint.epoch(), "RUNNING", AT))
                    .isInstanceOf(CasOutcome.Applied.class);
            assertThat(newObservations.republishStopped(checkpoint)).isEmpty();
            assertThat(newObservations.republishStopped(successorStates.read("orders").orElseThrow())).isEmpty();
            assertThat(newObservations.read("orders")).isEmpty();
        });
    }

    @Test void aStoppedObservationCanFollowTheSameClaimToItsCurrentTopology() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "STOPPED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var checkpoint = states.read("orders").orElseThrow();
            var original = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var stopped = observation(PipelineState.STOPPED);
            original.save(stopped, checkpoint.epoch());

            var table = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            var future = table.find().first();
            var futureId = future.get("_id", Document.class);
            futureId.put("topologyRevision", f.fence().topologyRevision() + 2);
            future.put("observedAt", Date.from(AT.plusSeconds(2)));
            table.insertOne(future);

            var claims = f.domain().workloadClaims(BUSINESS);
            var reacquired = claims.acquirePipeline(SCOPE, OWNER, f.fence().topologyRevision() + 1, Duration.ofSeconds(30));
            assertThat(reacquired.acquired()).isTrue();
            var current = WorkloadClaimFence.from(reacquired.claim());
            assertThat(current.claimGeneration()).isEqualTo(f.fence().claimGeneration());
            assertThat(current.executionGeneration()).isEqualTo(f.fence().executionGeneration());
            assertThat(current.topologyRevision()).isEqualTo(f.fence().topologyRevision() + 1);
            var observations = f.domain().pipelineObservations(SCOPE, current, ACTIVE);
            assertThat(original.republishStopped(checkpoint)).isEmpty();
            assertThat(observations.read("orders")).isEmpty();
            assertThat(observations.republishStopped(checkpoint)).contains(stopped);
            assertThat(observations.read("orders")).contains(stopped);
        });
    }

    @Test void terminalPayloadStaysVisibleAcrossAClaimChangeAndNeverCrossesTheNextEpoch() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            var running = observation(PipelineState.RUNNING);
            observations.save(running, 1);
            assertThat(states.compareAndSwap("orders", 1, "COMPLETED", AT)).isInstanceOf(CasOutcome.Applied.class);
            assertThat(observations.beforeCompletion("orders", 2)).contains(running);
            var completed = observation(PipelineState.COMPLETED);
            observations.save(completed, 2);
            var claims = f.domain().workloadClaims(BUSINESS);
            var next = WorkloadClaimFence.from(claims.advanceExecution(claims.read(KEY).orElseThrow().claim(),
                    1, Set.of("node-a")).orElseThrow());
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(completed);
            var nextStates = f.domain().pipelineStates(SCOPE, next, ACTIVE);
            assertThat(nextStates.compareAndSwap("orders", 2, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var failed = new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new io.tapstate.core.lifecycle.ObservationFailure("test.failure", Map.of()), AT, List.of());
            f.domain().pipelineObservations(SCOPE, next, ACTIVE).save(failed, 3);
            assertThat(f.domain().pipelineObservationBatch(List.of(SCOPE), ACTIVE, true).get("orders").observation()).contains(failed);
            var takeover = WorkloadClaimFence.from(claims.advanceExecution(claims.read(KEY).orElseThrow().claim(),
                    1, Set.of("node-a")).orElseThrow());
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(failed);
            assertThat(f.domain().pipelineStates(SCOPE, takeover, ACTIVE).compareAndSwap("orders", 3, "STOPPED", AT))
                    .isInstanceOf(CasOutcome.Applied.class);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).isEmpty();
            f.domain().pipelineObservations(SCOPE, takeover, ACTIVE).save(observation(PipelineState.STOPPED), 4);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(observation(PipelineState.STOPPED));
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
                        observed.getDatabase(f.database().getName()), SCOPE, f.fence(), () -> { if (!active.get()) throw MongoCachePipelineAccess.fenced(); }),
                        new MongoStructureProof());
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
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).isEmpty();
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_observation");
            f.database().createCollection("pipeline_observation");
            var nanos = new java.util.concurrent.atomic.AtomicLong();
            var states = new MongoCachePipelineStateStore(
                    new MongoCachePipelineAccess(f.admin(), f.database(), SCOPE, null, ACTIVE),
                    new MongoStructureProof(nanos::get));
            assertThat(states.read("orders")).isPresent();
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).createIndex(new Document("touchMillis", 1),
                    new com.mongodb.client.model.IndexOptions().expireAfter(60L, TimeUnit.SECONDS));
            assertThat(f.domain().pipelineStates(SCOPE).read("orders")).isPresent();
            assertThat(f.domain().inspectStructure(new io.tapstate.spi.store.CacheAuthScope(SCOPE.clusterId(), SCOPE.bindingVersion(),
                    io.tapstate.spi.store.StorageBinding.Mode.OP, SCOPE.cacheIncarnation(), 1), new MongoReadBudget(Duration.ofSeconds(5))))
                    .anySatisfy(issue -> {
                        assertThat(issue.collection()).isEqualTo("pipeline_state");
                        assertThat(issue.problem()).isEqualTo(MongoStructureInspection.Problem.UNEXPECTED_CONSTRAINT);
                    });
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

    @Test void observationsFromAnotherScopeAreInvisibleAndCannotBlockTheCurrentProjection() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            f.domain().pipelineObservations(scope(2), f.fence(), ACTIVE).save(observation(PipelineState.RUNNING), 1);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).isEmpty();
            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.RUNNING), 1);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(observation(PipelineState.RUNNING));
            assertThat(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).countDocuments()).isEqualTo(2);
        });
    }

    @Test void saturatedRetiredAndClosedStateRefusesBusinessAccessAndHidesOldObservations() {
        withFixture(f -> {
            var writer = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            var observations = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id", "orders"), new Document("$set", new Document("runtimeWriteFence", Long.MAX_VALUE - 1)));
            var before = snapshot(f.database());
            assertCode(() -> writer.compareAndSwap("orders", 0, "RUNNING", AT), IoError.CACHE_BUSINESS_FENCED);
            assertThat(snapshot(f.database())).isEqualTo(before);
            f.domain().retireBusiness(BUSINESS, "retire-business", ACTIVE);
            assertCode(() -> writer.read("orders"), IoError.CACHE_BUSINESS_FENCED);
            observations.save(observation(PipelineState.RUNNING), 1);
            assertCode(() -> observations.read("orders"), IoError.CACHE_BUSINESS_FENCED);
            f.domain().close();
            assertCode(() -> writer.read("orders"), IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void fractionalStoredMetricsAreDiagnosedInsteadOfTruncated() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.RUNNING), 1);
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).updateOne(new Document("pipelineId", "orders"),
                    new Document("$set", new Document("metrics.rows", 1.75d)));
            var before = snapshot(f.database());
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void malformedStoredStateNeverRetainsItsRawTextInAnExceptionCause() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(observation(PipelineState.RUNNING), 1);
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).updateOne(new Document("pipelineId", "orders"),
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
                    new io.tapstate.core.lifecycle.ObservationFailure("TS0440008", Map.of("pipeline", "orders")), AT, List.of(fact));
            var writer = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            writer.save(expected, 1);
            assertThat(writer.read("orders")).contains(expected);
        });
    }

    @Test void legacyAndProductFailureIdentitiesUseTheSameLatestObservationContract() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var writer = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            var params = Map.of("captureId", "capture-a");
            var legacy = new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new io.tapstate.core.lifecycle.ObservationFailure("capture.claim-lost", params), AT, List.of());
            writer.save(legacy, 1);
            var oldBytes = collection.find().first();
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(legacy);
            assertThat(collection.find().first()).isEqualTo(oldBytes);

            var current = new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new io.tapstate.core.lifecycle.ObservationFailure(
                            io.tapstate.core.common.CaptureErrorDefinitions.CLAIM_LOST.code(), params),
                    AT.plusSeconds(1), List.of());
            writer.save(current, 1);
            writer.save(legacy, 1);
            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(current);
            var row = collection.find().first();
            assertThat(row.keySet()).isEqualTo(oldBytes.keySet());
            assertThat(row.get("failure", Document.class).keySet())
                    .isEqualTo(oldBytes.get("failure", Document.class).keySet());
            assertThat(row.get("failure", Document.class).getString("code")).isEqualTo("TS0350003");
            assertThat(collection.find().first()).isEqualTo(row);

            assertThat(states.compareAndSwap("orders", 1, "RUNNING", AT.plusSeconds(2)))
                    .isInstanceOf(CasOutcome.Applied.class);
            var recovered = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                    null, AT.plusSeconds(2), List.of());
            writer.save(recovered, 2);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(recovered);
        });
    }

    @Test void runtimeDiagnosticSurvivesIndependentReaderAndPreservesLatestFailure() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var writer = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE);
            var failure = new io.tapstate.core.lifecycle.ObservationFailure("TS0060027",
                    Map.of("connector", "fixture", "detail", "refused"), Map.of("message", "safe cause"));
            var expected = new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    failure, AT.plusSeconds(1), List.of());
            writer.save(expected, 1);
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            var before = collection.find().first();
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(expected);
            String readerClasspath = System.getProperty("runtime.recovery.classpath", System.getProperty("java.class.path"));
            var process = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp", readerClasspath,
                    RuntimeObservationReaderProcess.class.getName(), MONGO.getReplicaSetUrl(), f.database().getName(), "read")
                    .redirectErrorStream(true).start();
            String output = readerOutput(process);
            assertThat(output).contains("RUNTIME_DIAGNOSTIC_RECOVERED");
            String oldClasspath = System.getProperty("runtime.legacy.classpath");
            if (oldClasspath != null) {
                var oldReader = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-cp", oldClasspath,
                        RuntimeObservationReaderProcess.class.getName(), MONGO.getReplicaSetUrl(), f.database().getName(), "unreadable")
                        .redirectErrorStream(true).start();
                String oldOutput = readerOutput(oldReader);
                assertThat(oldOutput).contains("OLD_RUNTIME_READER_REFUSED_EXTENSION");
                assertThat(collection.find().first()).isEqualTo(before);
            }
            writer.save(new Observation("orders", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                    new io.tapstate.core.lifecycle.ObservationFailure("connector.write-failed", Map.of()), AT, List.of()), 1);
            assertThat(collection.find().first()).isEqualTo(before);
            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(states.compareAndSwap("orders", 1, "RUNNING", AT.plusSeconds(2))).isInstanceOf(CasOutcome.Applied.class);
            var recovered = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null,
                    AT.plusSeconds(2), List.of());
            writer.save(recovered, 2);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).contains(recovered);
        });
    }

    @Test void unknownRuntimeDiagnosticVersionIsRefusedWithoutRepair() {
        withFixture(f -> {
            assertThat(f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE).compareAndSwap("orders", 0, "FAILED", AT))
                    .isInstanceOf(CasOutcome.Applied.class);
            f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).save(new Observation("orders", PipelineState.FAILED,
                    Map.of(), Map.of(), Map.of(), new io.tapstate.core.lifecycle.ObservationFailure("TS0060027",
                    Map.of("connector", "fixture", "detail", "refused"), Map.of("message", "safe cause")), AT, List.of()), 1);
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database());
            collection.updateOne(new Document(), new Document("$set", new Document("failure.diagnostic.formatVersion", 99)));
            var damaged = collection.find().first();
            assertCode(() -> f.domain().pipelineObservations(SCOPE).read("orders"), IoError.DOCUMENT_UNREADABLE);
            assertThat(collection.find().first()).isEqualTo(damaged);
        });
    }

    @Test void losingOnlyTheProjectionCollectionDoesNotDisableCheckpointAccess() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).drop();
            assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> states.read("orders")))
                    .as("a derived projection is not a prerequisite of the retained checkpoint").isNull();
            assertThat(states.compareAndSwap("orders", 0, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
            assertThat(f.domain().pipelineObservations(SCOPE).read("orders")).isEmpty();
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_observation");
        });
    }

    private static void awaitCleanup(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new AssertionError("Observation cleanup did not finish within its test budget");
            Thread.sleep(20);
        }
    }

    private static String readerOutput(Process process) throws Exception {
        try {
            assertThat(process.waitFor(20, TimeUnit.SECONDS)).isTrue();
            String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            assertThat(process.exitValue()).as(output).isZero();
            return output;
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private static Observation observation(PipelineState state) {
        return new Observation("orders", state, Map.of(), Map.of(), Map.of(), null, AT, List.of(),
                new io.tapstate.core.lifecycle.InitialLoad("run-1", AT, "snapshot_and_cdc", Map.of(
                        "orders", new io.tapstate.core.lifecycle.InitialLoad.Table(io.tapstate.core.lifecycle.InitialLoad.Stage.READING, 7L, null))));
    }
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
    @Test void exhaustedLeaseRecoveryProjectsTheCurrentEngineIdentityWithoutWritingAnObservation() {
        withFixture(f -> {
            var states = f.domain().pipelineStates(SCOPE, f.fence(), ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "FAILED", AT)).isInstanceOf(CasOutcome.Applied.class);
            var collection = SystemCollections.CACHE_PIPELINE_STATE.on(f.database());
            for (int attempts : List.of(2, 3)) {
                collection.updateOne(new Document("_id", "orders"), new Document("$set", new Document("leaseRecovery",
                        new Document("intentRevision", 1L).append("failedExecution", 1L)
                                .append("attempts", attempts).append("retryAfter", AT.toEpochMilli()))));
                var observed = f.domain().pipelineObservations(SCOPE, f.fence(), ACTIVE).read("orders").orElseThrow();
                assertThat(observed.state()).isEqualTo(PipelineState.FAILED);
                if (attempts < 3) assertThat(observed.failure()).isNull();
                else {
                    assertThat(observed.failure().code()).isEqualTo("TS0440003");
                    assertThat(observed.failure().params()).containsExactly(Map.entry("pipeline", "orders"));
                }
                assertThat(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).countDocuments()).isZero();
            }
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"held", "ended", "expired", "takeover", "checkpoint", "scope", "missing-projection", "retired", "node-held", "node-ended", "node-expired"})
    void stoppedTerminalRecheckIsReadOnlyAndRequiresTheOriginalDurableEvidence(String scenario) {
        withFixture(f -> {
            var claimTable = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database());
            var nodeOwner = new WorkloadOwner(SCOPE.clusterId(), "node-backed-boot");
            var nodeKey = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, nodeOwner.nodeId());
            boolean nodeBacked = scenario.startsWith("node-");
            if (nodeBacked) {
                var node = new MongoWorkloadClaimStore(claimTable).acquire(nodeKey, nodeOwner, 1, Duration.ofSeconds(30)).claim();
                claimTable.updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), new Document("$set",
                        new Document("ownerNodeId", nodeOwner.nodeId()).append("ownerBootId", nodeOwner.bootId())
                                .append("leaseUntil", new Date(-1)).append(MongoWorkloadClaimStore.NODE_LEASE_GENERATION, node.claimGeneration())));
            }
            var original = nodeBacked ? new WorkloadClaimFence(KEY, nodeOwner, f.fence().claimGeneration(),
                    f.fence().executionGeneration(), f.fence().topologyRevision()) : f.fence();
            var states = f.domain().pipelineStates(SCOPE, original, ACTIVE);
            assertThat(states.compareAndSwap("orders", 0, "STOPPED", AT)).isInstanceOf(CasOutcome.Applied.class);
            f.domain().pipelineObservations(SCOPE, original, ACTIVE).save(observation(PipelineState.STOPPED), 1);
            var checkpoint = states.read("orders").orElseThrow();
            var claims = f.domain().workloadClaims(BUSINESS);
            var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database());
            boolean ended = scenario.equals("ended") || scenario.equals("node-ended");
            switch (scenario) {
                case "ended", "node-ended" -> assertThat(claims.release(claims.read(KEY).orElseThrow().claim())).isTrue();
                case "node-expired" -> table.updateOne(new Document("_id", MongoWorkloadClaimStore.id(nodeKey)),
                        new Document("$set", new Document("leaseUntil", new Date(1))));
                case "expired" -> table.updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                        new Document("$set", new Document("leaseUntil", new Date(1))));
                case "takeover" -> {
                    assertThat(claims.release(claims.read(KEY).orElseThrow().claim())).isTrue();
                    assertThat(claims.acquirePipeline(SCOPE, new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).acquired()).isTrue();
                }
                case "checkpoint" -> assertThat(states.compareAndSwap("orders", 1, "RUNNING", AT)).isInstanceOf(CasOutcome.Applied.class);
                case "scope" -> replaceScope(f.database(), scope(2));
                case "missing-projection" -> SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).deleteMany(new Document());
                case "retired" -> f.domain().retireBusiness(BUSINESS, "retire-terminal-test", ACTIVE);
                default -> { }
            }
            var before = snapshot(f.database());
            if (scenario.equals("scope") || scenario.equals("retired")) {
                assertCode(() -> f.domain().stoppedCheckpointUnchanged(SCOPE, checkpoint, original, ended, ACTIVE), IoError.CACHE_BUSINESS_FENCED);
            } else {
                assertThat(f.domain().stoppedCheckpointUnchanged(SCOPE, checkpoint, original, ended, ACTIVE))
                        .isEqualTo(scenario.equals("held") || scenario.equals("ended") || scenario.equals("node-held") || scenario.equals("node-ended"));
                if (scenario.equals("held") || scenario.equals("node-held") || scenario.equals("node-expired"))
                    assertThat(f.domain().stoppedCheckpointUnchanged(SCOPE, checkpoint, original, true, ACTIVE)).isFalse();
                if (scenario.equals("ended") || scenario.equals("node-ended"))
                    assertThat(f.domain().stoppedCheckpointUnchanged(SCOPE, checkpoint, original, false, ACTIVE)).isFalse();
            }
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

}
