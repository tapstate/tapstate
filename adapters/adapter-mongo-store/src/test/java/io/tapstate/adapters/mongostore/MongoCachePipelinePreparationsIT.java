package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.FirstPipelineCreation;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Independent CONFIG and CACHE replica sets exercise the original first-creation authority. */
@RequiresDocker
class MongoCachePipelinePreparationsIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Instant AT = Instant.parse("2026-10-18T02:00:00Z");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static MongoDBContainer mongo() { return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1"); }

    @Test void onlyCommittedCachePreparationCanCompleteConfigAndAllowActuation() {
        withFixture(f -> {
            long revision = f.version();
            FirstPipelineCreation grant = f.issue();
            assertThat(f.configuration.intents().loadForActuation("flow")).isEmpty();
            var reservation = f.preparations().reserveFirst(grant, TTL);
            assertThat(reservation.claim().claimGeneration()).isEqualTo(1);
            assertThat(reservation.claim().executionGeneration()).isZero();
            assertThat(f.state()).isNull();
            assertThat(f.claim()).containsEntry("pipelinePreparation", marker(grant.preparation(), "RESERVED"));
            var receipt = f.preparations().seed(reservation, f.configuration.preparations(), AT);
            assertThat(f.state()).containsEntry("epoch", 0L).containsEntry("runtimeWriteFence", 1L).containsEntry("stateJson", "NEW");
            assertThat(SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.cacheDatabase).find().first())
                    .containsEntry("_id", MongoPipelinePreparationFacts.scope(f.scope)).containsEntry("preparationId", grant.preparation().preparationId())
                    .containsEntry("revision", 0L).containsEntry("shapes", List.of()).containsEntry("placements", List.of());
            assertThat(f.claim()).containsEntry("pipelinePreparation", marker(grant.preparation(), "PREPARED"));
            assertThat(f.configuration.intents().loadForActuation("flow")).isEmpty();
            var complete = f.configuration.preparations().complete(receipt);
            assertThat(complete.phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.configuration.intents().loadForActuation("flow")).isEqualTo(f.configuration.intents().load("flow"));
            assertThat(f.configuration.preparations().complete(receipt)).isEqualTo(complete);
            assertThat(f.version()).isEqualTo(revision);
            assertThat(f.configDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_state", "workload_claims");
            assertThat(f.cacheDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("operator_generations", "pipeline_intents");
        });
    }

    @Test void invocationIsConsumedBeforeTheCacheGuardAndCannotBeRepeatedAfterReentry() {
        withFixture(f -> {
            var grant = f.issue();
            var calls = new AtomicInteger();
            var guarded = new MongoCachePipelinePreparations(f.cacheClient, f.cacheDatabase, f.business(), () -> {
                calls.incrementAndGet();
                assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            });
            guarded.reserveFirst(grant, TTL);
            assertThat(calls.get()).isGreaterThan(0);
            var before = f.snapshot();
            assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void arbitraryGrantImplementationsCannotTurnStatusIntoInsertAuthority() {
        withFixture(f -> {
            var issued = f.issue().preparation(); var callbacks = new AtomicInteger();
            FirstPipelineCreation forged = new FirstPipelineCreation() {
                @Override public PipelinePreparation preparation() { callbacks.incrementAndGet(); return issued; }
                @Override public io.tapstate.spi.store.PipelinePreparationInputs inputs() { callbacks.incrementAndGet(); return null; }
                @Override public void consume() { callbacks.incrementAndGet(); }
                @Override public void requireActive() { callbacks.incrementAndGet(); }
            };
            var before = f.snapshot();
            assertCode(() -> f.preparations().reserveFirst(forged, TTL), IoError.PIPELINE_INTENT_FENCED);
            assertThat(callbacks).hasValue(0); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void issuedAuthorityWithoutAnyCacheFactRequiresExplicitRebuildAfterReopen() {
        withFixture(f -> {
            var original = f.issue().preparation();
            try (var reopened = MongoConfigurationDomain.open(f.settings(), "cluster-a")) {
                assertThat(reopened.preparations().issueFirstCreate(f.scope, f.node)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
                var before = f.snapshot();
                assertCode(() -> f.preparations().resume(original, f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }

    @Test void retainedReservationResumesAndExpiredSameBootGetsTheNextTenure() {
        withFixture(f -> {
            var grant = f.issue(); var first = f.preparations().reserveFirst(grant, TTL);
            var live = f.preparations().resume(grant.preparation(), f.node, TTL);
            assertThat(live.claim()).isEqualTo(first.claim());
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("leaseUntil", new Date(0))));
            var resumed = f.preparations().resume(grant.preparation(), f.node, TTL);
            assertThat(resumed.claim().claimGeneration()).isEqualTo(2);
            assertThat(resumed.claim().executionGeneration()).isZero();
            assertCode(() -> f.preparations().seed(first, f.configuration.preparations(), AT), IoError.WORKLOAD_CLAIM_FENCED);
            f.configuration.preparations().complete(f.preparations().seed(resumed, f.configuration.preparations(), AT));
        });
    }

    @Test void expiredReservationCannotCommitANextTenureThatIsAlreadyAtTheForbiddenBoundary() {
        withFixture(f -> {
            var grant = f.issue(); f.preparations().reserveFirst(grant, TTL);
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("claimGeneration", Long.MAX_VALUE - 2)
                    .append("leaseUntil", new Date(0))));
            var before = f.snapshot();
            assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void anotherLiveOwnerCannotTakeReservationButMayTakeItsExpiredNextTenure() {
        withFixture(f -> {
            var grant = f.issue(); var first = f.preparations().reserveFirst(grant, TTL);
            var other = f.node(new WorkloadOwner("node-b", "boot-b"));
            var before = f.snapshot();
            assertCode(() -> f.preparations().resume(grant.preparation(), other, TTL), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("leaseUntil", new Date(0))));
            var resumed = f.preparations().resume(grant.preparation(), other, TTL);
            assertThat(resumed.claim().owner()).isEqualTo(other.owner());
            assertThat(resumed.claim().claimGeneration()).isEqualTo(first.claim().claimGeneration() + 1);
        });
    }

    @Test void preparedQueryIsReadOnlyAndDoesNotRenewOrTakeOverEvenWhenExpired() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            f.preparations().seed(original, f.configuration.preparations(), AT);
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("leaseUntil", new Date(0))));
            var before = f.snapshot();
            var found = f.preparations().resume(grant.preparation(), f.node, TTL);
            assertThat(found.phase()).isEqualTo(PipelineCachePreparations.Phase.PREPARED);
            var receipt = f.preparations().seed(found, f.configuration.preparations(), AT.plusSeconds(1));
            assertThat(f.snapshot()).isEqualTo(before);
            f.configuration.preparations().complete(receipt);
        });
    }

    @Test void lossOfEitherOriginalFactOrBothNeverReseedsOrRestartsCounters() {
        for (int damage = 0; damage < 3; damage++) {
            int selected = damage;
            withFixture(f -> {
                var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
                var receipt = f.preparations().seed(original, f.configuration.preparations(), AT);
                f.configuration.preparations().complete(receipt);
                if (selected != 1) f.claims().deleteOne(pipelineFilter(f.scope));
                if (selected != 0) SystemCollections.CACHE_PIPELINE_STATE.on(f.cacheDatabase).deleteMany(new Document());
                var before = f.snapshot();
                // Query the retained ISSUED identity, as a lost-response operation would do.
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
                assertThat(f.configuration.preparations().issueFirstCreate(f.scope, f.node)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
                assertThat(f.snapshot()).isEqualTo(before);
            });
        }
    }

    @Test void ordinaryPortsCannotTurnAReservedPreparationIntoBusinessAuthority() {
        withFixture(f -> {
            var grant = f.issue(); var reservation = f.preparations().reserveFirst(grant, TTL);
            // A partial or out-of-protocol state row must never turn RESERVED into a prepared claim.
            SystemCollections.CACHE_PIPELINE_STATE.on(f.cacheDatabase).insertOne(new Document("_id", "flow")
                    .append("scope", MongoPipelinePreparationFacts.scope(f.scope)).append("stateJson", "NEW")
                    .append("epoch", 0L).append("runtimeWriteFence", 1L).append("touchMillis", AT.toEpochMilli()));
            var claims = f.cacheDomain.workloadClaims(f.business());
            var held = new MongoWorkloadClaimStore(f.claims()).read(reservation.claim().key()).orElseThrow().claim();
            var state = f.cacheDomain.pipelineStates(f.scope, reservation.claim(), ACTIVE);
            var before = f.snapshot();
            for (org.assertj.core.api.ThrowableAssert.ThrowingCallable action : List.<org.assertj.core.api.ThrowableAssert.ThrowingCallable>of(
                    () -> claims.readPipeline(f.scope), () -> claims.read(held.key()), () -> claims.readAll(List.of(held.key())),
                    () -> claims.acquirePipeline(f.scope, OWNER, 1, TTL), () -> claims.renew(held, TTL), () -> claims.release(held),
                    () -> claims.advanceExecution(held, 1, java.util.Set.of("node-a")), () -> claims.recordExecutionFailure(held, false),
                    () -> state.read("flow"), () -> state.compareAndSwap("flow", 0, "RUNNING", AT))) {
                assertCode(action, IoError.WORKLOAD_CLAIM_FENCED);
                assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }

    @Test void ordinaryPortsCannotRepairOrIgnoreLostPreparationMarkers() {
        withFixture(f -> {
            var grant = f.issue(); var reservation = f.preparations().reserveFirst(grant, TTL);
            f.preparations().seed(reservation, f.configuration.preparations(), AT);
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$unset", new Document("pipelinePreparation", "")));
            var before = f.snapshot();
            assertCode(() -> f.cacheDomain.workloadClaims(f.business()).acquirePipeline(f.scope, OWNER, 1, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertCode(() -> f.cacheDomain.pipelineStates(f.scope).read("flow"), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void damagedNonceScopePhaseAndCheckpointCannotBeRepairedOrTurnedIntoReceipt() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            f.preparations().seed(original, f.configuration.preparations(), AT);
            Document saved = f.claim();
            for (var mutation : List.of(new Document("pipelinePreparation.preparationId", UUID.randomUUID().toString()),
                    new Document("pipelinePreparation.phase", "secret-sentinel"), new Document("pipelinePreparation.extra", true),
                    new Document("pipelineScope.stateGeneration", 1))) {
                f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", mutation));
                var before = f.snapshot();
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.snapshot()).isEqualTo(before);
                f.claims().replaceOne(pipelineFilter(f.scope), saved);
            }
            var states = SystemCollections.CACHE_PIPELINE_STATE.on(f.cacheDatabase); Document state = f.state();
            for (var mutation : List.of(new Document("epoch", 0), new Document("runtimeWriteFence", 2L),
                    new Document("stateJson", "RUNNING"), new Document("scope.stateGeneration", 2L))) {
                states.updateOne(new Document(), new Document("$set", mutation));
                var before = f.snapshot();
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.snapshot()).isEqualTo(before);
                states.replaceOne(new Document(), state);
            }
        });
    }

    @Test void freshConfigCheckRefusesSeedAfterMappingRetirement() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            f.configDatabase.getCollection("operator_generations").updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("phase", "REBUILDING").append("operationId", "rebuild")));
            var before = f.snapshot();
            assertCode(() -> f.preparations().seed(original, f.configuration.preparations(), AT), IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void nodeRevocationBeforeItsPreparationWritePreventsTheReservationCommit() {
        withFixture(f -> {
            var grant = f.issue(); var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (updates(e, "fencedPreparationWrites") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = cacheClient(listener); var executor = Executors.newSingleThreadExecutor()) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                var result = executor.submit(() -> store.reserveFirst(grant, TTL));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(f.node.key())),
                            new Document("$set", new Document("claimGeneration", 2L).append("ownerBootId", "replacement")));
                } finally { proceed.countDown(); }
                assertFutureCode(() -> result.get(15, TimeUnit.SECONDS), IoError.WORKLOAD_CLAIM_FENCED);
                assertThat(f.claim()).isNull(); assertThat(f.state()).isNull();
                assertThat(SystemCollections.CACHE_IDENTITY.on(f.cacheDatabase).find().first().getLong("claimAdmissionFence")).isEqualTo(1L);
            }
        });
    }

    @Test void reservationCommittedBeforeNodeRevocationKeepsOnlyItsOriginalEvidence() {
        withFixture(f -> {
            var grant = f.issue(); var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getCommandName().equals("insert") && e.getCommand().getString("insert").getValue().equals("workload_claims")
                            && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = cacheClient(listener); var executor = Executors.newFixedThreadPool(2)) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                var result = executor.submit(() -> store.reserveFirst(grant, TTL));
                assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                var revocation = executor.submit(() -> f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(f.node.key())),
                        new Document("$set", new Document("claimGeneration", 2L).append("ownerBootId", "replacement"))));
                proceed.countDown();
                var reservation = result.get(15, TimeUnit.SECONDS); revocation.get(15, TimeUnit.SECONDS);
                assertThat(reservation.claim().owner()).isEqualTo(OWNER);
                assertThat(f.claim()).isNotNull(); assertThat(f.state()).isNull();
                assertCode(() -> store.reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            } finally { proceed.countDown(); }
        });
    }

    @Test void aLateOriginalSeedCannotInsertAfterItsReservationHistoryIsLost() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (updates(e, "pipelinePreparation") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = cacheClient(listener); var executor = Executors.newSingleThreadExecutor()) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                var result = executor.submit(() -> store.seed(original, f.configuration.preparations(), AT));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    f.claims().deleteOne(pipelineFilter(f.scope));
                } finally { proceed.countDown(); }
                assertFutureCode(() -> result.get(15, TimeUnit.SECONDS), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.claim()).isNull(); assertThat(f.state()).isNull();
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            }
        });
    }

    @Test void transientReservationRetryKeepsTheConsumedGrantAndFixedNonce() {
        withFixture(f -> {
            var grant = f.issue(); var failures = new AtomicInteger(); var nodeWrites = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) { if (updates(e, "fencedPreparationWrites")) nodeWrites.incrementAndGet(); }
                @Override public void commandFailed(CommandFailedEvent e) { if (e.getCommandName().equals("update")) failures.incrementAndGet(); }
            };
            try (var observed = cacheClient(listener)) {
                failCommand(f.cacheClient, "update", new Document("errorCode", 112).append("errorLabels", List.of("TransientTransactionError")));
                try {
                    var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                    store.reserveFirst(grant, TTL);
                } finally { clearFailpoint(f.cacheClient); }
                assertThat(failures).hasValue(1); assertThat(nodeWrites).hasValue(1);
                assertThat(f.claim()).containsEntry("claimGeneration", 1L).containsEntry("pipelinePreparation", marker(grant.preparation(), "RESERVED"));
                assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            }
        });
    }

    @Test void ambiguousCommitRetriesOnlyCommitAndNeverReentersFirstInsert() {
        withFixture(f -> {
            var grant = f.issue(); var inserts = new AtomicInteger(); var commits = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getCommandName().equals("insert")) inserts.incrementAndGet();
                    if (e.getCommandName().equals("commitTransaction")) commits.incrementAndGet();
                }
            };
            try (var observed = cacheClient(listener)) {
                failCommand(f.cacheClient, "commitTransaction", new Document("writeConcernError", new Document("code", 91).append("errmsg", "test commit reply lost"))
                        .append("errorLabels", List.of("UnknownTransactionCommitResult")));
                try {
                    var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                    store.reserveFirst(grant, TTL);
                } finally { clearFailpoint(f.cacheClient); }
                assertThat(inserts).hasValue(1); assertThat(commits.get()).isGreaterThanOrEqualTo(2);
                assertThat(f.claim()).containsEntry("claimGeneration", 1L);
            }
        });
    }

    @Test void aClosedOriginalCacheDomainConsumesButNeverExecutesTheGrant() {
        withFixture(f -> {
            var grant = f.issue(); var captured = f.preparations();
            f.cacheDomain.close();
            var before = f.snapshot();
            assertCode(() -> captured.reserveFirst(grant, TTL), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> captured.reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void originalGuardRevokedAfterStateInsertRollsBackStateAndPreparedMarker() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            var active = new AtomicBoolean(true);
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getCommandName().equals("insert") && e.getCommand().getString("insert").getValue().equals("pipeline_state")) active.set(false);
                }
            };
            var before = f.snapshot();
            try (var observed = cacheClient(listener)) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(),
                        () -> { if (!active.get()) throw MongoCachePipelineAccess.fenced(); });
                assertCode(() -> store.seed(original, f.configuration.preparations(), AT), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void lossAfterTheActualPrivateOffsetInsertRollsBackTheWholeInitialClosure() {
        withFixture(f -> {
            var source = new SourceResource("input",null,"mongodb",Map.of("uri","mongodb://source.invalid/source"),io.tapstate.core.model.SourceMode.CDC,
                    List.of(io.tapstate.core.model.TableRef.literal("orders")),null,null);
            var pipeline = new io.tapstate.core.model.PipelineResource("flow",null,List.of(io.tapstate.core.model.SourceRef.spec("input",false)),null,null,null,
                    new io.tapstate.core.model.Settings(null,null,null,null,io.tapstate.core.model.ReadMode.CDC_ONLY,"latest"),null);
            f.configuration.artifacts().saveAll(List.of(source,pipeline));
            var inputs = io.tapstate.spi.store.PipelinePreparationInputs.of(f.scope,pipeline,Map.of("input",source),Map.of("input",java.util.Set.of("orders")));
            var grant = ((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(f.scope,f.node,inputs)).creation();
            var original = f.preparations().reserveFirst(grant,TTL); var before = f.snapshot();
            try (var witness = new MongoCommandWitness(CACHE.getReplicaSetUrl(),f.cacheDatabase.getName(),"insert","direct_channel_offsets")) {
                var store = new MongoCachePipelinePreparations(witness.client,witness.database,f.business(),witness::refuseAfterCommand);
                assertCode(() -> store.seed(original,f.configuration.preparations(),AT),IoError.CACHE_BUSINESS_FENCED);
                witness.requireCommand();
            }
            assertThat(f.snapshot()).isEqualTo(before); assertThat(SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.cacheDatabase).countDocuments()).isZero();
            assertThat(SystemCollections.DIRECT_BATCHES.on(f.cacheDatabase).countDocuments()).isZero();
            assertThat(f.claim().get("pipelinePreparation",Document.class).getString("phase")).isEqualTo("RESERVED");
            var receipt = f.preparations().seed(original,f.configuration.preparations(),AT);
            assertThat(receipt.preparationId()).isEqualTo(original.preparationId());
            assertThat(SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.cacheDatabase).countDocuments()).isOne();
        });
    }

    @Test void missingOrDamagedInventoryGenesisCannotBeRecreatedBySeedReplayOrResume() {
        withFixture(f -> {
            var grant = f.issue(); var reserved = f.preparations().reserveFirst(grant,TTL);
            f.preparations().seed(reserved,f.configuration.preparations(),AT);
            var table = SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.cacheDatabase); Document saved = table.find().first();
            table.deleteMany(new Document()); var before = f.snapshot();
            assertCode(() -> f.preparations().resume(grant.preparation(),f.node,TTL),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertCode(() -> f.preparations().seed(reserved,f.configuration.preparations(),AT),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertThat(f.snapshot()).isEqualTo(before); table.insertOne(saved);
            for (var mutation : List.of(new Document("preparationId",UUID.randomUUID().toString()),new Document("revision",1L),
                    new Document("revision",0),new Document("formatVersion",1L),new Document("shapes",List.of("private-sentinel")))) {
                table.updateOne(new Document(),new Document("$set",mutation)); before = f.snapshot();
                assertCode(() -> f.preparations().resume(grant.preparation(),f.node,TTL),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertCode(() -> f.preparations().seed(reserved,f.configuration.preparations(),AT),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.snapshot()).isEqualTo(before); table.replaceOne(new Document(),saved);
            }
        });
    }

    @Test void retainedInventoryWithoutTheOriginalCheckpointAndClaimCannotBeAdoptedAsFirstPreparation() {
        withFixture(f -> {
            var grant = f.issue();
            try (var session = f.cacheClient.startSession()) { session.withTransaction(() -> {
                MongoOperatorInventory.prepareInitial(session,f.cacheDatabase,f.scope,grant.preparation().preparationId()); return null;
            }); }
            var before = f.snapshot();
            assertCode(() -> f.preparations().reserveFirst(grant,TTL),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void losingTheOriginalGuardAfterInventoryInsertRollsBackTheCheckpointAndPreparedClaim() {
        withFixture(f -> {
            var grant = f.issue(); var reserved = f.preparations().reserveFirst(grant,TTL); var active = new AtomicBoolean(true);
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent event) {
                if (event.getCommandName().equals("insert") && event.getCommand().getString("insert").getValue().equals("operator_cleanup")) active.set(false);
            }};
            var before = f.snapshot();
            try (var observed = cacheClient(listener)) {
                var store = new MongoCachePipelinePreparations(observed,observed.getDatabase(f.cacheDatabase.getName()),f.business(),
                        () -> { if (!active.get()) throw MongoCachePipelineAccess.fenced(); });
                assertCode(() -> store.seed(reserved,f.configuration.preparations(),AT),IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(active).isFalse(); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    private static Document marker(PipelinePreparation preparation, String phase) { return new Document("preparationId", preparation.preparationId()).append("phase", phase); }
    private static Document pipelineFilter(PipelineIntentScope scope) { return new Document("_id", MongoWorkloadClaimStore.id(new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId()))); }
    private static MongoClient cacheClient(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(CACHE.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static boolean updates(CommandStartedEvent e, String field) { return e.getCommandName().equals("update") && e.getCommand().getArray("updates").toString().contains(field); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(code));
    }
    private static void assertFutureCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).hasCauseInstanceOf(TapstateException.class).satisfies(error -> assertThat(((TapstateException) error.getCause()).code()).isEqualTo(code));
    }
    private static void failCommand(MongoClient client, String command, Document data) {
        data.append("failCommands", List.of(command));
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1)).append("data", data));
    }
    private static void clearFailpoint(MongoClient client) { client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
    @Test void rebuildBeginFreezesOneSuccessorButLeavesCacheHistoryUntouched() {
        withFixture(f -> {
            var request = rebuildRequest(f); long revision = f.version(); var before = f.snapshot();
            var previous = SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).find(new Document("_id","flow")).first().get("cachePreparation");
            var pending = f.configuration.rebuilds().begin(request);
            assertThat(pending.request()).isEqualTo(request); assertThat(pending.confirmationHash()).matches("[0-9a-f]{64}");
            assertThat(pending.request().targetScope().stateGeneration()).isEqualTo(2);
            var mapping = SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).find(new Document("_id","flow")).first();
            assertThat(mapping).containsEntry("stateGeneration",1L).containsEntry("phase","REBUILDING").containsEntry("operationId","rebuild-operation")
                    .containsEntry("cachePreparation",previous);
            assertThat(mapping.toJson()).doesNotContain("leaseUntil","password","uri");
            assertThat(f.version()).isEqualTo(revision+1); assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.configuration.intents().loadForActuation("flow")).isEmpty();
            f.configuration.rebuilds().requirePending(pending);
            assertCode(() -> f.configuration.preparations().issueFirstCreate(f.scope,f.node),IoError.PIPELINE_INTENT_FENCED);
        });
    }
    @Test void rebuildBeginCanReopenTheOriginalOperationWithoutAllocatingAnotherPreparation() {
        withFixture(f -> {
            var request = rebuildRequest(f); var original = f.configuration.rebuilds().begin(request); long revision = f.version();
            try (var reopened = MongoConfigurationDomain.open(f.settings(),"cluster-a")) {
                var resumed = reopened.rebuilds().begin(request);
                assertThat(resumed.preparationId()).isEqualTo(original.preparationId());
                assertThat(resumed.previousPreparationId()).isEqualTo(original.previousPreparationId());
                assertThat(resumed.confirmationHash()).isEqualTo(original.confirmationHash());
                reopened.rebuilds().requirePending(resumed);
                assertCode(() -> reopened.rebuilds().requirePending(original),IoError.PIPELINE_INTENT_FENCED);
            }
            assertThat(f.version()).isEqualTo(revision);
        });
    }
    @Test void rebuildBeginConcurrencyPublishesOneOriginalAnnexAndOneRevision() {
        withFixture(f -> {
            var request = rebuildRequest(f); long revision = f.version(); var begin = new CountDownLatch(1);
            try (var reopened = MongoConfigurationDomain.open(f.settings(),"cluster-a"); var executor = Executors.newFixedThreadPool(2)) {
                var a = executor.submit(() -> { begin.await(); return f.configuration.rebuilds().begin(request); });
                var b = executor.submit(() -> { begin.await(); return reopened.rebuilds().begin(request); });
                begin.countDown(); var first = a.get(20,TimeUnit.SECONDS); var second = b.get(20,TimeUnit.SECONDS);
                assertThat(first.preparationId()).isEqualTo(second.preparationId());
                assertThat(first.confirmationHash()).isEqualTo(second.confirmationHash());
            }
            assertThat(f.version()).isEqualTo(revision+1);
        });
    }
    @Test void rebuildPendingNeverAdoptsAnotherOperationEpochClaimOrInput() {
        withFixture(f -> {
            var original = rebuildRequest(f); var pending = f.configuration.rebuilds().begin(original); long revision = f.version();
            var changedEpoch = new io.tapstate.spi.store.PipelineRebuildRequest(original.operationId(),original.intent(),original.originalClaim(),1,original.inputs());
            var changedOperation = new io.tapstate.spi.store.PipelineRebuildRequest("different-operation",original.intent(),original.originalClaim(),0,original.inputs());
            var claim = original.originalClaim();
            var changedClaim = new io.tapstate.spi.store.PipelineRebuildRequest(original.operationId(),original.intent(),
                    new WorkloadClaimFence(claim.key(),claim.owner(),claim.claimGeneration()+1,claim.executionGeneration(),claim.topologyRevision()),0,original.inputs());
            for (var changed : List.of(changedEpoch,changedOperation,changedClaim))
                assertCode(() -> f.configuration.rebuilds().begin(changed),IoError.PIPELINE_INTENT_FENCED);
            f.configuration.rebuilds().requirePending(pending); assertThat(f.version()).isEqualTo(revision);
        });
    }
    @Test void rebuildRefusesAnUnpreparedPipelineAndNeverChangesTheFirstCreationFacts() {
        withFixture(f -> {
            var request = rawRebuildRequest(f,new WorkloadClaimFence(new WorkloadClaimKey("cluster-a",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),OWNER,1,0,1));
            long revision = f.version(); var before = f.snapshot();
            assertCode(() -> f.configuration.rebuilds().begin(request),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.version()).isEqualTo(revision); assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
        });
    }
    @Test void rebuildMissingAnnexAfterThePhaseCommitIsNeverRecreated() {
        withFixture(f -> {
            var request = rebuildRequest(f); f.configuration.rebuilds().begin(request); long revision = f.version();
            SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).updateOne(new Document("_id","flow"),new Document("$unset",new Document("stateRebuild",1)));
            assertCode(() -> f.configuration.rebuilds().begin(request),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.version()).isEqualTo(revision);
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).find(new Document("_id","flow")).first()).doesNotContainKey("stateRebuild");
        });
    }
    @Test void rebuildRefusesAnIntentChangedSinceTheConfirmation() {
        withFixture(f -> {
            var request = rebuildRequest(f); var current = request.intent();
            assertThat(f.configuration.intents().compareAndSet(f.scope,current.intentRevision(),
                    new DesiredState("flow",PipelineState.STOPPED,current.desired().revision(),false))).isTrue();
            long revision = f.version(); assertCode(() -> f.configuration.rebuilds().begin(request),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.version()).isEqualTo(revision);
            assertThat(f.configuration.pipelineStates().read("flow").orElseThrow().phase()).isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.ACTIVE);
        });
    }
    @Test void rebuildChecksActualSourceDefinitionsAndPersistsNoConnectionSecrets() {
        withFixture(f -> {
            var request = sourceRebuildRequest(f); var pending = f.configuration.rebuilds().begin(request);
            f.configuration.rebuilds().requirePending(pending);
            var mapping = SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).find(new Document("_id","flow")).first();
            assertThat(mapping.toJson()).doesNotContain("source.invalid","source-secret-sentinel","mongodb://");
            assertThat(request.inputs().sourceHashes()).containsOnlyKeys("input");
        });
    }
    @Test void rebuildDoesNotRefreshSourceInputsAfterTheConfirmation() {
        withFixture(f -> {
            var request = sourceRebuildRequest(f);
            var source = (SourceResource) f.configuration.artifacts().get("input").orElseThrow();
            f.configuration.artifacts().save(new SourceResource("input",source.metadata(),source.connector(),Map.of("uri","mongodb://changed.invalid/source"),
                    source.mode(),source.tables(),source.srs(),source.execution(),source.experimental()));
            long revision = f.version(); assertCode(() -> f.configuration.rebuilds().begin(request),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.version()).isEqualTo(revision);
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).find(new Document("_id","flow")).first()).doesNotContainKey("stateRebuild");
        });
    }
    @Test void rebuildGuardLossAfterTheActualMappingWriteRollsBackPhaseAnnexAndRevision() {
        withFixture(f -> {
            var request = rebuildRequest(f); long revision = f.version();
            var before = SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).find(new Document("_id","flow")).first();
            try (var witness = new MongoCommandWitness(CONFIG.getReplicaSetUrl(),f.configDatabase.getName(),"update","operator_generations")) {
                var material = SystemCollections.CLUSTER_KEYS.on(f.configDatabase).find().first().getString("material");
                var cipher = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                var bindings = new MongoStorageBindingStore(witness.client,witness.database,cipher);
                var artifacts = new MongoConfigurationArtifactStore(witness.client,witness.database,cipher);
                var store = new MongoPipelineStateRebuilds(witness.client,witness.database,"cluster-a",bindings,artifacts,witness::refuseAfterCommand);
                assertCode(() -> store.begin(request),IoError.CACHE_BUSINESS_FENCED); witness.requireCommand();
            }
            assertThat(f.version()).isEqualTo(revision);
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.configDatabase).find(new Document("_id","flow")).first()).isEqualTo(before);
        });
    }
    @Test void rebuildPendingRefusesForeignProofsWithoutCallingTheirAccessors() {
        withFixture(f -> {
            var request = rebuildRequest(f); var pending = f.configuration.rebuilds().begin(request); var calls = new AtomicInteger();
            io.tapstate.spi.store.PipelineRebuildPreparation forged = new io.tapstate.spi.store.PipelineRebuildPreparation() {
                @Override public io.tapstate.spi.store.PipelineRebuildRequest request() { calls.incrementAndGet(); return request; }
                @Override public String preparationId() { calls.incrementAndGet(); return pending.preparationId(); }
                @Override public String previousPreparationId() { calls.incrementAndGet(); return pending.previousPreparationId(); }
                @Override public String confirmationHash() { calls.incrementAndGet(); return pending.confirmationHash(); }
            };
            assertCode(() -> f.configuration.rebuilds().requirePending(forged),IoError.PIPELINE_INTENT_FENCED);
            assertCode(() -> MongoPipelineStateRebuilds.requireOriginalPending(forged),IoError.PIPELINE_INTENT_FENCED);
            assertThat(calls.get()).isZero();
        });
    }
    @Test void rebuildPhaseFencesOrdinaryNotesButAllowsOnlyItsOriginalRetirementContext() {
        withFixture(f -> {
            var oldRequest = sourceRebuildRequest(f); var notes = f.configuration.requiredNotes();
            var owner = new io.tapstate.spi.store.RequiredNoteOwner(f.scope.bindingVersion(),"pdk.state.flow.capture.input",
                    io.tapstate.spi.store.RequiredNoteOwner.Kind.PIPELINE,"flow",1);
            var old = notes.open(owner);
            var origin = new io.tapstate.spi.store.RequiredNoteOrigin("mongodb","b".repeat(64),new byte[]{1,2,3});
            notes.prepareOrigin(old,origin); notes.compareAndSet(old,"slot",0,new byte[]{4,5},false);
            var source = (SourceResource) f.configuration.artifacts().get("input").orElseThrow();
            var changed = new SourceResource("input",source.metadata(),source.connector(),Map.of("uri","mongodb://new.invalid/source"),
                    source.mode(),source.tables(),source.srs(),source.execution(),source.experimental());
            f.configuration.artifacts().save(changed);
            var pipeline = (io.tapstate.core.model.PipelineResource) f.configuration.artifacts().get("flow").orElseThrow();
            var inputs = io.tapstate.spi.store.PipelinePreparationInputs.of(oldRequest.targetScope(),pipeline,Map.of("input",changed),Map.of("input",java.util.Set.of("orders")));
            var request = new io.tapstate.spi.store.PipelineRebuildRequest(oldRequest.operationId(),oldRequest.intent(),oldRequest.originalClaim(),oldRequest.originalStateEpoch(),inputs);
            f.configuration.rebuilds().begin(request);
            assertCode(() -> notes.read(old,"slot"),IoError.NOTE_SCOPE_UNAVAILABLE);
            var retirement = notes.retirement(old,request.operationId());
            assertThat(retirement.origin()).contains(origin);
            assertThat(retirement.notes().read(old,"slot").bytes()).containsExactly(new byte[]{4,5});
            assertThat(f.configuration.pipelineStates().read("flow").orElseThrow().stateGeneration()).isEqualTo(1);
            assertCode(() -> notes.retirement(old,"foreign-operation"),IoError.NOTE_SCOPE_UNAVAILABLE);
        });
    }

    private static io.tapstate.spi.store.PipelineRebuildRequest sourceRebuildRequest(Fixture f) {
        var source = new SourceResource("input",null,"mongodb",Map.of("uri","mongodb://source.invalid/source","password","source-secret-sentinel"),io.tapstate.core.model.SourceMode.CDC,
                List.of(io.tapstate.core.model.TableRef.literal("orders")),null,null);
        var pipeline = new io.tapstate.core.model.PipelineResource("flow",null,List.of(io.tapstate.core.model.SourceRef.spec("input",false)),null,null,null,
                new io.tapstate.core.model.Settings(null,null,null,null,io.tapstate.core.model.ReadMode.SNAPSHOT_ONLY,null),null);
        f.configuration.artifacts().saveAll(List.of(source,pipeline));
        var intent = f.configuration.intents().load("flow").orElseThrow();
        assertThat(f.configuration.intents().compareAndSet(f.scope,intent.intentRevision(),new DesiredState("flow",PipelineState.RUNNING,CanonicalHash.of(pipeline),false))).isTrue();
        var initial = f.issue(); var reservation = f.preparations().reserveFirst(initial,TTL);
        f.configuration.preparations().complete(f.preparations().seed(reservation,f.configuration.preparations(),AT));
        var scope = new PipelineIntentScope(f.scope.clusterId(),f.scope.bindingVersion(),f.scope.cacheIncarnation(),"flow",2);
        var inputs = io.tapstate.spi.store.PipelinePreparationInputs.of(scope,pipeline,Map.of("input",source),Map.of("input",java.util.Set.of("orders")));
        return new io.tapstate.spi.store.PipelineRebuildRequest("rebuild-operation",f.configuration.intents().load("flow").orElseThrow(),reservation.claim(),0,inputs);
    }

    private static io.tapstate.spi.store.PipelineRebuildRequest rebuildRequest(Fixture f) {
        var initial = f.issue(); var reservation = f.preparations().reserveFirst(initial,TTL);
        f.configuration.preparations().complete(f.preparations().seed(reservation,f.configuration.preparations(),AT));
        return rawRebuildRequest(f,reservation.claim());
    }
    private static io.tapstate.spi.store.PipelineRebuildRequest rawRebuildRequest(Fixture f,WorkloadClaimFence claim) {
        var current = f.configuration.intents().load("flow").orElseThrow();
        var scope = new PipelineIntentScope(f.scope.clusterId(),f.scope.bindingVersion(),f.scope.cacheIncarnation(),"flow",2);
        var pipeline = (io.tapstate.core.model.PipelineResource) f.configuration.artifacts().get("flow").orElseThrow();
        var inputs = io.tapstate.spi.store.PipelinePreparationInputs.of(scope,pipeline,Map.of(),Map.of());
        return new io.tapstate.spi.store.PipelineRebuildRequest("rebuild-operation",current,claim,0,inputs);
    }

    private static void withFixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configName = "first_cfg_" + suffix; String cacheName = "first_cache_" + suffix;
        try (var configClient = MongoClients.create(CONFIG.getReplicaSetUrl()); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var cfg = configClient.getDatabase(configName); var cache = cacheClient.getDatabase(cacheName);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheName), cacheName, null);
            try (var configuration = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1);
                    var domain = MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "first-preparation-test")) {
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                configuration.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cacheConnection, master);
                var business = configuration.bindings().cachePreparation().orElseThrow().businessScope();
                domain.prepareAuth(new CacheAuthScope(business.clusterId(), business.bindingVersion(), StorageBinding.Mode.OP, business.cacheIncarnation(), 1), "auth-a", ACTIVE);
                try (var session = cacheClient.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION, SystemCollections.CACHE_OPERATOR_CLEANUP, SystemCollections.DIRECT_CHANNEL_OFFSETS, SystemCollections.DIRECT_BATCHES)) {
                        table.prepareCollectionOn(cache, session); for (var index : table.indexes()) IndexEnsure.createInTransaction(session,table.on(cache),index);
                    }
                }
                SystemCollections.CACHE_IDENTITY.on(cache).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(cache).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY")
                        .append("epoch", 1L).append("operationId", "business-a"));
                var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");
                configuration.artifacts().create(pipeline);
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), "flow", 1);
                configuration.intents().compareAndSet(scope, 0, new DesiredState("flow", PipelineState.RUNNING, CanonicalHash.of(pipeline), false));
                var nodeClaim = domain.workloadClaims(business).acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, OWNER.nodeId()), OWNER, 1, TTL).claim();
                scenario.run(new Fixture(configuration, cfg, cacheClient, cache, domain, scope, WorkloadClaimFence.from(nodeClaim)));
            } finally { cfg.drop(); cache.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoConfigurationDomain configuration, MongoDatabase configDatabase, MongoClient cacheClient,
            MongoDatabase cacheDatabase, MongoCacheDomain cacheDomain, PipelineIntentScope scope, WorkloadClaimFence node) {
        CacheBusinessScope business() { return new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()); }
        MongoConnectionSettings settings() { return new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configDatabase.getName()), null, Duration.ofSeconds(5)); }
        PipelineCachePreparations preparations() { return cacheDomain.pipelinePreparations(business(), ACTIVE); }
        FirstPipelineCreation issue() { return ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(scope, node)).creation(); }
        long version() { return configDatabase.getCollection("configuration_version").find().first().getLong("revision"); }
        MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cacheDatabase); }
        WorkloadClaimFence node(WorkloadOwner owner) { return WorkloadClaimFence.from(cacheDomain.workloadClaims(business())
                .acquire(new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.NODE_SESSION, owner.nodeId()), owner, 1, TTL).claim()); }
        Document claim() { return claims().find(pipelineFilter(scope)).first(); }
        Document state() { return SystemCollections.CACHE_PIPELINE_STATE.on(cacheDatabase).find(new Document("_id", scope.pipelineId())).first(); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_OPERATOR_CLEANUP))
                out.put(table.collectionName(), table.on(cacheDatabase).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
