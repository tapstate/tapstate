package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteCarry;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
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

/** Real configuration-domain witnesses for note deletion, clear generations and fixed-context CAS. */
@RequiresDocker
class MongoRequiredNoteStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test
    void keyDeletionRetainsTheRevisionAndNoOldPayloadOrEventArchive() {
        withNotes(f -> {
            RequiredNoteScope scope = f.notes.initialize(global());
            assertThat(f.notes.read(scope, "key").revision()).isZero();
            assertThat(f.notes.compareAndSet(scope, "key", 0, bytes("private-note-sentinel"), false)).isTrue();
            var stored = SystemCollections.PDK_NOTES.on(f.database).find().first();
            assertThat(stored.toJson()).doesNotContain("private-note-sentinel");
            assertThat(f.notes.read(scope, "key").bytes()).containsExactly(bytes("private-note-sentinel"));
            assertThat(f.notes.compareAndSet(scope, "key", 1, null, false)).isTrue();
            var deleted = f.notes.read(scope, "key");
            assertThat(deleted.revision()).isEqualTo(2);
            assertThat(deleted.bytes()).isNull();
            Document tombstone = SystemCollections.PDK_NOTES.on(f.database).find().first();
            assertThat(tombstone).containsEntry("deleted", true).containsEntry("revision", 2L).doesNotContainKey("value");
            assertThat(f.notes.compareAndSet(scope, "key", 1, bytes("late"), false)).isFalse();
            assertThat(f.notes.compareAndSet(scope, "key", 0, bytes("late-absence"), false)).isFalse();
            assertThat(f.notes.compareAndSet(scope, "key", 2, bytes("new"), false)).isTrue();
            assertThat(f.notes.read(scope, "key").revision()).isEqualTo(3);
            assertThat(SystemCollections.PDK_NOTES.on(f.database).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.database).find().first().getLong("revision")).isZero();
            assertThat(f.database.listCollectionNames().into(new ArrayList<>()))
                    .doesNotContain("operator_state", "workload_claims", "srs_meta", "sessions");
        });
    }

    @Test
    void deletingANeverWrittenKeyStillBlocksAPreviouslyCapturedAbsentRevision() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            assertThat(f.notes.compareAndSet(scope, "absent", 0, null, false)).isTrue();
            assertThat(f.notes.read(scope, "absent").revision()).isEqualTo(1);
            assertThat(f.notes.compareAndSet(scope, "absent", 0, bytes("stale"), false)).isFalse();
        });
    }

    @Test
    void clearRefusesOldTokensAndReclamationCannotDeleteAnotherScopeOrNewGeneration() {
        withNotes(f -> {
            var old = f.notes.initialize(global());
            var neighbor = f.notes.initialize(chain("neighbor"));
            f.notes.compareAndSet(old, "key", 0, bytes("old"), false);
            f.notes.compareAndSet(neighbor, "key", 0, bytes("neighbor"), false);
            var current = f.notes.clear(old, false);
            assertThat(current.generation()).isEqualTo(2);
            assertThatThrownBy(() -> f.notes.read(old, "key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.compareAndSet(old, "late", 0, bytes("late"), false))
                    .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.clear(old, false)).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(f.notes.read(current, "key").revision()).isZero();
            f.notes.compareAndSet(current, "key", 0, bytes("current"), false);
            assertThat(f.notes.reclaimBefore(current)).isEqualTo(1);
            assertThat(f.notes.reclaimBefore(current)).isZero();
            assertThat(f.notes.read(current, "key").bytes()).containsExactly(bytes("current"));
            assertThat(f.notes.read(neighbor, "key").bytes()).containsExactly(bytes("neighbor"));
        });
    }

    @Test
    void aDelayedCleanupUsesItsCapturedUpperBoundAfterANewerClear() {
        withNotes(f -> {
            var one = f.notes.initialize(global());
            f.notes.compareAndSet(one, "key", 0, bytes("one"), false);
            var two = f.notes.clear(one, false);
            f.notes.compareAndSet(two, "key", 0, bytes("two"), false);
            var three = f.notes.clear(two, false);
            f.notes.compareAndSet(three, "key", 0, bytes("three"), false);
            assertThat(f.notes.reclaimBefore(two)).isEqualTo(1);
            assertThat(SystemCollections.PDK_NOTES.on(f.database).countDocuments()).isEqualTo(2);
            assertThat(f.notes.reclaimBefore(three)).isEqualTo(1);
            assertThat(f.notes.read(three, "key").bytes()).containsExactly(bytes("three"));
        });
    }

    @Test
    void sharedRemoveAndClearKeepPrivateCarryBarriersAfterContentReclamation() {
        withNotes(f -> {
            var owner = pipeline("p1", 1);
            f.pipelineGeneration("p1", 1);
            var privateScope = f.notes.initialize(owner);
            f.notes.compareAndSet(privateScope, "slot", 0, bytes("private"), false);
            var shared = f.notes.initialize(chain("physical"));
            assertThat(f.notes.carryBlocked(shared, "slot")).isFalse();
            f.notes.compareAndSet(shared, "slot", 0, bytes("shared"), false);
            assertThat(f.notes.compareAndSet(shared, "slot", 1, null, true)).isTrue();
            assertThat(f.notes.carryBlocked(shared, "slot")).isTrue();
            assertThat(f.notes.compareAndSet(shared, "slot", 0, bytes("stale-private-copy"), false)).isFalse();
            var cleared = f.notes.clear(shared, true);
            assertThat(f.notes.carryBlocked(cleared, "other")).isTrue();
            assertThat(f.notes.reclaimBefore(cleared)).isEqualTo(1);
            assertThat(f.notes.read(privateScope, "slot").bytes()).containsExactly(bytes("private"));
            assertThat(SystemCollections.PDK_NOTE_MIGRATIONS.on(f.database).countDocuments()).isEqualTo(2);
            assertThat(f.notes.read(cleared, "slot").bytes()).isNull();
        });
    }

    @Test
    void twoClientsRacingTheSameRevisionHaveOneWinner() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            try (MongoClient otherClient = MongoClients.create(MONGO.getReplicaSetUrl());
                    var executor = Executors.newFixedThreadPool(2)) {
                var other = new MongoRequiredNoteStore(otherClient, otherClient.getDatabase(f.database.getName()), f.cipher());
                for (int round = 0; round < 6; round++) {
                    String key = "race_" + round;
                    CountDownLatch start = new CountDownLatch(1);
                    var first = executor.submit(() -> {
                        if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                        return f.notes.compareAndSet(scope, key, 0, bytes("first"), false);
                    });
                    var second = executor.submit(() -> {
                        if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                        return other.compareAndSet(scope, key, 0, bytes("second"), false);
                    });
                    start.countDown();
                    assertThat(List.of(first.get(20, TimeUnit.SECONDS), second.get(20, TimeUnit.SECONDS)))
                            .containsExactlyInAnyOrder(true, false);
                    assertThat(f.notes.read(scope, key).revision()).isEqualTo(1);
                }
            }
        });
    }

    @Test
    void aPausedCommandCannotAdoptTheScopeCreatedByClear() {
        withNotes(f -> {
            var original = f.notes.initialize(global());
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var paused = f.paused(entered, release);
                var old = executor.submit(() -> paused.compareAndSet(original, "key", 0, bytes("old"), false));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    var current = f.notes.clear(original, false);
                    f.notes.compareAndSet(current, "key", 0, bytes("current"), false);
                    release.countDown();
                    assertThatThrownBy(() -> old.get(20, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(TapstateException.class)
                            .satisfies(error -> scopeUnavailable(error.getCause()));
                    assertThat(f.notes.read(current, "key").bytes()).containsExactly(bytes("current"));
                } finally {
                    release.countDown();
                }
            }
        });
    }

    @Test
    void aPausedCommandCannotRefreshItsExpectedRevisionAfterADelete() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            f.notes.compareAndSet(scope, "key", 0, bytes("original"), false);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var paused = f.paused(entered, release);
                var old = executor.submit(() -> paused.compareAndSet(scope, "key", 1, bytes("stale"), false));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    f.notes.compareAndSet(scope, "key", 1, null, false);
                    release.countDown();
                    assertThat(old.get(20, TimeUnit.SECONDS)).isFalse();
                    assertThat(f.notes.read(scope, "key").revision()).isEqualTo(2);
                    assertThat(f.notes.read(scope, "key").bytes()).isNull();
                } finally {
                    release.countDown();
                }
            }
        });
    }

    @Test
    void pipelineOwnerReplacementFencesOldCommandsWithoutLosingNecessaryNotes() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var firstOwner = pipeline("p1", 1);
            var old = f.notes.initialize(firstOwner);
            f.notes.compareAndSet(old, "identity", 0, bytes("external-id"), false);
            f.pipelineGeneration("p1", 2);
            assertThatThrownBy(() -> f.notes.compareAndSet(old, "identity", 1, bytes("old"), false))
                    .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.read(old, "identity")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            var current = f.notes.open(pipeline("p1", 2));
            assertThat(current.generation()).isEqualTo(old.generation());
            assertThat(f.notes.read(current, "identity").bytes()).containsExactly(bytes("external-id"));
            assertThat(f.notes.compareAndSet(current, "identity", 1, bytes("current"), false)).isTrue();
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void ordinaryOpenDoesNotCreateMissingScopesOrAcceptAnotherBinding() {
        withNotes(f -> {
            assertThatThrownBy(() -> f.notes.open(global())).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).countDocuments()).isZero();
            assertThatThrownBy(() -> f.notes.initialize(new RequiredNoteOwner(2, "pdk.global-state",
                    RequiredNoteOwner.Kind.GLOBAL, "cluster-a", 0))).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.initialize(new RequiredNoteOwner(1, "pdk.global-state",
                    RequiredNoteOwner.Kind.GLOBAL, "cluster-b", 0))).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.initialize(pipeline("missing", 1)))
                    .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).countDocuments()).isZero();
        });
    }

    @Test
    void missingKeyRefusesNoteMutationAndClearWithoutMintingAnotherKey() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            f.notes.compareAndSet(scope, "identity", 0, bytes("retained"), false);
            SystemCollections.CLUSTER_KEYS.on(f.database).deleteMany(new Document());
            assertThatThrownBy(() -> f.notes.compareAndSet(scope, "identity", 1, null, false))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
            assertThatThrownBy(() -> f.notes.clear(scope, true)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).find().first().getLong("generation")).isEqualTo(1);
            assertThat(SystemCollections.PDK_NOTES.on(f.database).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.CLUSTER_KEYS.on(f.database).countDocuments()).isZero();
        });
    }

    @Test
    void ciphertextCannotBeTransplantedToAnotherKeyAndErrorsExposeNoPayload() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            f.notes.compareAndSet(scope, "one", 0, bytes("secret-note-sentinel"), false);
            f.notes.compareAndSet(scope, "two", 0, bytes("second"), false);
            var records = SystemCollections.PDK_NOTES.on(f.database);
            Document first = records.find(new Document("_id.key", "one")).first();
            records.updateOne(new Document("_id.key", "two"),
                    new Document("$set", new Document("value", first.getString("value"))));
            assertThatThrownBy(() -> f.notes.read(scope, "two"))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(error.args()).containsExactlyEntriesOf(Map.of("id", "required-note", "field", "value"));
                        assertThat(error.getCause()).isNull();
                    });
        });
    }

    @Test
    void cleanupFailureLeavesThePublishedGenerationUsableAndCanBeRetriedExactly() {
        withNotes(f -> {
            var old = f.notes.initialize(global());
            f.notes.compareAndSet(old, "key", 0, bytes("old"), false);
            var current = f.notes.clear(old, true);
            f.notes.compareAndSet(current, "key", 0, bytes("current"), false);
            f.admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("delete")).append("errorCode", 13)));
            try {
                assertThatThrownBy(() -> f.notes.reclaimBefore(current)).isInstanceOf(TapstateException.class);
            } finally {
                f.admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
            assertThat(f.notes.read(current, "key").bytes()).containsExactly(bytes("current"));
            assertThat(f.notes.carryBlocked(current, "other")).isTrue();
            assertThat(f.notes.reclaimBefore(current)).isEqualTo(1);
            assertThat(f.notes.read(current, "key").bytes()).containsExactly(bytes("current"));
        });
    }

    @Test
    void anUnboundConfigurationCannotPrepareFormalNoteScopes() {
        withNotes(f -> {
            SystemCollections.CONFIG_CLUSTER_IDENTITY.on(f.database).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("bindingVersion", 0L)));
            assertThatThrownBy(() -> f.notes.initialize(global())).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> new RequiredNoteOwner(0, "pdk.global-state", RequiredNoteOwner.Kind.GLOBAL,
                    "cluster-a", 0)).isInstanceOf(IllegalArgumentException.class);
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).countDocuments()).isZero();
        });
    }

    @Test
    void closingADriveDuringAnInFlightWriteAbortsTheConfigurationTransaction() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean active = new AtomicBoolean(true);
            var guarded = f.paused(entered, release).guardedBy(() -> {
                if (!active.get()) throw scopeFailure();
            });
            try (var executor = Executors.newSingleThreadExecutor()) {
                var pending = executor.submit(() -> guarded.compareAndSet(scope, "key", 0, bytes("late"), false));
                try {
                    assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                    active.set(false);
                    release.countDown();
                    assertThatThrownBy(() -> pending.get(20, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(TapstateException.class).satisfies(error -> scopeUnavailable(error.getCause()));
                    assertThat(f.notes.read(scope, "key").revision()).isZero();
                    assertThat(SystemCollections.PDK_NOTES.on(f.database).countDocuments()).isZero();
                } finally {
                    release.countDown();
                }
            }
        });
    }

    @Test
    void aDriverTransactionRetryRechecksTheOriginalExecutionGuard() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            AtomicInteger checks = new AtomicInteger();
            var guarded = f.notes.guardedBy(() -> {
                if (checks.incrementAndGet() > 1) throw scopeFailure();
            });
            f.admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("update")).append("errorCode", 112)
                            .append("errorLabels", List.of("TransientTransactionError"))));
            try {
                assertThatThrownBy(() -> guarded.compareAndSet(scope, "key", 0, bytes("stale-retry"), false))
                        .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            } finally {
                f.admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
            assertThat(checks.get()).isEqualTo(2);
            assertThat(f.notes.read(scope, "key").revision()).isZero();
            assertThat(SystemCollections.PDK_NOTES.on(f.database).countDocuments()).isZero();
        });
    }

    @Test
    void addingAGuardCannotRemoveAnEarlierLifecycleCheck() {
        withNotes(f -> {
            var scope = f.notes.initialize(global());
            var closed = f.notes.guardedBy(() -> { throw scopeFailure(); });
            var additional = closed.guardedBy(() -> {});
            assertThatThrownBy(() -> additional.read(scope, "key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
        });
    }

    @Test
    void aRetiringPipelineCannotUseItsOldOrdinaryNoteHandle() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var owner = new RequiredNoteOwner(1, "pdk.state.p1.src", RequiredNoteOwner.Kind.PIPELINE, "p1", 1);
            var scope = f.notes.initialize(owner);
            assertThat(f.notes.compareAndSet(scope, "key", 0, bytes("retained"), false)).isTrue();
            SystemCollections.OPERATOR_GENERATIONS.on(f.database).updateOne(new Document("_id", "p1"),
                    new Document("$set", new Document("phase", "RETIRING").append("operationId", "delete-a")));
            assertThatThrownBy(() -> f.notes.read(scope, "key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.compareAndSet(scope, "key", 1, bytes("late"), false))
                    .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(SystemCollections.PDK_NOTES.on(f.database).countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void anOrdinaryNoteWriteCannotRepairAMissingMappingFence() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var scope = f.notes.initialize(new RequiredNoteOwner(1, "pdk.state.p1.src", RequiredNoteOwner.Kind.PIPELINE, "p1", 1));
            assertThat(f.notes.compareAndSet(scope, "key", 0, bytes("retained"), false)).isTrue();
            SystemCollections.OPERATOR_GENERATIONS.on(f.database).updateOne(new Document("_id", "p1"),
                    new Document("$unset", new Document("noteWriteFence", "")));
            Document damaged = SystemCollections.OPERATOR_GENERATIONS.on(f.database).find().first();
            Document retained = SystemCollections.PDK_NOTES.on(f.database).find().first();
            assertThatThrownBy(() -> f.notes.compareAndSet(scope, "key", 1, bytes("late"), false))
                    .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.database).find().first()).isEqualTo(damaged);
            assertThat(SystemCollections.PDK_NOTES.on(f.database).find().first()).isEqualTo(retained);
        });
    }

    @Test
    void malformedActiveMappingsCannotAuthorizeOrdinaryNoteReadsOrWrites() {
        for (Document change : List.of(new Document("phase", List.of("ACTIVE")), new Document("stateGeneration", 1),
                new Document("operationId", 7), new Document("noteWriteFence", 2), new Document("noteWriteFence", -1L))) {
            withNotes(f -> {
                f.pipelineGeneration("p1", 1);
                var scope = f.notes.initialize(new RequiredNoteOwner(1, "pdk.state.p1.src", RequiredNoteOwner.Kind.PIPELINE, "p1", 1));
                assertThat(f.notes.compareAndSet(scope, "key", 0, bytes("retained"), false)).isTrue();
                SystemCollections.OPERATOR_GENERATIONS.on(f.database).updateOne(new Document("_id", "p1"),
                        new Document("$set", change));
                Document damaged = SystemCollections.OPERATOR_GENERATIONS.on(f.database).find().first();
                Document retained = SystemCollections.PDK_NOTES.on(f.database).find().first();
                assertThatThrownBy(() -> f.notes.read(scope, "key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
                assertThatThrownBy(() -> f.notes.compareAndSet(scope, "key", 1, bytes("late"), false))
                        .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
                assertThat(SystemCollections.OPERATOR_GENERATIONS.on(f.database).find().first()).isEqualTo(damaged);
                assertThat(SystemCollections.PDK_NOTES.on(f.database).find().first()).isEqualTo(retained);
            });
        }
    }

    @Test
    void aPreparedSharedCarryCanReadWhileThePrivatePipelineRebuildsOrRetires() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            f.notes.compareAndSet(source, "late-key", 0, bytes("original"), false);
            var shared = f.notes.initialize(chain("c1"));
            var carry = new RequiredNoteCarry(shared, source);
            assertThatThrownBy(() -> f.notes.readCarried(carry, "late-key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(f.notes.prepareCarry(carry)).isEqualTo(carry);
            assertThat(f.notes.prepareCarry(carry)).isEqualTo(carry);
            assertThat(SystemCollections.PDK_NOTE_CARRIES.on(f.database).countDocuments()).isEqualTo(1);
            assertThat(f.notes.carries(shared)).containsExactly(carry);
            f.pipelineGeneration("p1", 2);
            assertThatThrownBy(() -> f.notes.read(source, "late-key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(f.notes.readCarried(carry, "late-key").bytes()).containsExactly(bytes("original"));
            SystemCollections.OPERATOR_GENERATIONS.on(f.database).updateOne(new Document("_id", "p1"),
                    new Document("$set", new Document("phase", "RETIRING").append("operationId", "delete-a")));
            assertThatThrownBy(() -> f.notes.read(source, "late-key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.compareAndSet(source, "late-key", 1, bytes("stale"), false))
                    .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(f.notes.readCarried(carry, "late-key").bytes()).containsExactly(bytes("original"));
            assertThat(SystemCollections.PDK_NOTES.on(f.database).countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void aDependencyProtectsPrivateOverwriteRemovalClearAndRetainsBothCounts() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            f.notes.compareAndSet(source, "slot", 0, bytes("retained"), false);
            var shared = f.notes.initialize(chain("c1"));
            f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
            assertThatThrownBy(() -> f.notes.compareAndSet(source, "slot", 1, bytes("overwritten"), false))
                    .satisfies(MongoRequiredNoteStoreIT::referenced);
            assertThatThrownBy(() -> f.notes.compareAndSet(source, "slot", 1, null, false))
                    .satisfies(MongoRequiredNoteStoreIT::referenced);
            assertThatThrownBy(() -> f.notes.clear(source, false)).satisfies(MongoRequiredNoteStoreIT::referenced);
            assertThat(f.notes.read(source, "slot").bytes()).containsExactly(bytes("retained"));
            assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 1L);
            assertThat(scopeMetadata(f, shared)).containsEntry("outgoingCarries", 1L);
        });
    }

    @Test
    void sharedClearKeepsDependencyUntilActualSharedRetirementAndOldTokensCannotRetireAgain() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            f.notes.compareAndSet(source, "slot", 0, bytes("retained"), false);
            var shared = f.notes.initialize(chain("c1"));
            var carry = f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
            var cleared = f.notes.clear(shared, true);
            assertThat(f.notes.allCarryBlocked(cleared)).isTrue();
            assertThat(f.notes.carries(cleared)).isEmpty();
            assertThat(SystemCollections.PDK_NOTE_CARRIES.on(f.database).countDocuments()).isEqualTo(1);
            assertThatThrownBy(() -> f.notes.readCarried(carry, "slot")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThatThrownBy(() -> f.notes.clear(source, false)).satisfies(MongoRequiredNoteStoreIT::referenced);
            var retired = f.notes.retireShared(cleared);
            assertThat(retired.generation()).isEqualTo(3);
            assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 0L);
            assertThat(scopeMetadata(f, retired)).containsEntry("outgoingCarries", 0L);
            assertThat(SystemCollections.PDK_NOTE_CARRIES.on(f.database).countDocuments()).isZero();
            f.notes.compareAndSet(retired, "new", 0, bytes("new-generation"), false);
            assertThatThrownBy(() -> f.notes.retireShared(cleared)).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(f.notes.read(retired, "new").bytes()).containsExactly(bytes("new-generation"));
            var privateCleared = f.notes.clear(source, false);
            assertThat(f.notes.reclaimBefore(privateCleared)).isEqualTo(1);
        });
    }

    @Test
    void retiringOneSharedCaptureDoesNotReleaseAnotherCapturesPrivateDependency() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            var one = f.notes.initialize(chain("one"));
            var two = f.notes.initialize(chain("two"));
            f.notes.prepareCarry(new RequiredNoteCarry(one, source));
            f.notes.prepareCarry(new RequiredNoteCarry(two, source));
            assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 2L);
            f.notes.retireShared(one);
            assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 1L);
            assertThatThrownBy(() -> f.notes.clear(source, false)).satisfies(MongoRequiredNoteStoreIT::referenced);
            assertThat(f.notes.carries(two)).hasSize(1);
            f.notes.retireShared(two);
            assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 0L);
            assertThat(f.notes.clear(source, false).generation()).isEqualTo(2);
        });
    }

    @Test
    void privateCarryReadsHonorTheSharedPerKeyRemovalBarrier() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            f.notes.compareAndSet(source, "slot", 0, bytes("old-slot"), false);
            var shared = f.notes.initialize(chain("c1"));
            var carry = f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
            f.notes.compareAndSet(shared, "slot", 0, null, true);
            assertThat(f.notes.readCarried(carry, "slot").bytes()).isNull();
            assertThat(f.notes.read(source, "slot").bytes()).containsExactly(bytes("old-slot"));
        });
    }

    @Test
    void missingDependencyRowsNeverBecomePermissionToClearOrRepair() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            f.notes.compareAndSet(source, "slot", 0, bytes("retained"), false);
            var shared = f.notes.initialize(chain("c1"));
            var carry = f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
            SystemCollections.PDK_NOTE_CARRIES.on(f.database).deleteMany(new Document());
            assertThatThrownBy(() -> f.notes.carries(shared)).satisfies(MongoRequiredNoteStoreIT::unreadable);
            assertThatThrownBy(() -> f.notes.prepareCarry(carry)).satisfies(MongoRequiredNoteStoreIT::unreadable);
            assertThatThrownBy(() -> f.notes.compareAndSet(source, "slot", 1, null, false)).satisfies(MongoRequiredNoteStoreIT::unreadable);
            assertThatThrownBy(() -> f.notes.clear(source, false)).satisfies(MongoRequiredNoteStoreIT::unreadable);
            assertThat(f.notes.read(source, "slot").bytes()).containsExactly(bytes("retained"));
            assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 1L);
            assertThat(scopeMetadata(f, shared)).containsEntry("outgoingCarries", 1L);
        });
    }

    @Test
    void malformedDependencyCountsAndRecordsRefuseWithoutRepair() {
        for (String damage : List.of("count", "source-generation")) {
            withNotes(f -> {
                f.pipelineGeneration("p1", 1);
                var source = f.notes.initialize(pipeline("p1", 1));
                var shared = f.notes.initialize(chain("c1"));
                f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
                if (damage.equals("count")) SystemCollections.PDK_NOTE_SCOPES.on(f.database).updateOne(
                        new Document("_id", new Document("bindingVersion", 1L).append("namespace", shared.owner().namespace())),
                        new Document("$set", new Document("outgoingCarries", 1)));
                else SystemCollections.PDK_NOTE_CARRIES.on(f.database).updateOne(new Document(),
                        new Document("$set", new Document("sourceStateGeneration", 1)));
                Document damaged = SystemCollections.PDK_NOTE_CARRIES.on(f.database).find().first();
                assertThatThrownBy(() -> f.notes.carries(shared)).satisfies(MongoRequiredNoteStoreIT::unreadable);
                assertThatThrownBy(() -> f.notes.retireShared(shared)).satisfies(MongoRequiredNoteStoreIT::unreadable);
                assertThat(SystemCollections.PDK_NOTE_CARRIES.on(f.database).find().first()).isEqualTo(damaged);
                assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 1L);
            });
        }
    }

    @Test
    void aGuardFailureRollsBackBothReferenceCountsAndTheRelationship() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            var shared = f.notes.initialize(chain("c1"));
            AtomicInteger checks = new AtomicInteger();
            var guarded = f.notes.guardedBy(() -> { if (checks.incrementAndGet() == 3) throw scopeFailure(); });
            assertThatThrownBy(() -> guarded.prepareCarry(new RequiredNoteCarry(shared, source)))
                    .satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(checks.get()).isEqualTo(3);
            assertThat(f.notes.carries(shared)).isEmpty();
            assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 0L);
            assertThat(scopeMetadata(f, shared)).containsEntry("outgoingCarries", 0L);
        });
    }

    @Test
    void aSharedGuardClosureRefusesCarryReadsWithoutChangingThePrivateOwner() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            var shared = f.notes.initialize(chain("c1"));
            var carry = f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
            var closed = f.notes.guardedBy(() -> { throw scopeFailure(); });
            assertThatThrownBy(() -> closed.readCarried(carry, "key")).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(f.notes.carries(shared)).containsExactly(carry);
        });
    }

    @Test
    void twoClientsPreparingOneDependencyDoNotDuplicateEitherCount() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            var shared = f.notes.initialize(chain("c1"));
            var carry = new RequiredNoteCarry(shared, source);
            try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl());
                    var executor = Executors.newFixedThreadPool(2)) {
                var other = new MongoRequiredNoteStore(client, client.getDatabase(f.database.getName()), f.cipher());
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                var one = executor.submit(() -> { ready.countDown(); if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start"); return f.notes.prepareCarry(carry); });
                var two = executor.submit(() -> { ready.countDown(); if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start"); return other.prepareCarry(carry); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(one.get(20, TimeUnit.SECONDS)).isEqualTo(carry);
                assertThat(two.get(20, TimeUnit.SECONDS)).isEqualTo(carry);
                assertThat(scopeMetadata(f, source)).containsEntry("incomingCarries", 1L);
                assertThat(scopeMetadata(f, shared)).containsEntry("outgoingCarries", 1L);
            }
        });
    }

    @Test
    void dependencyPreparationAndPrivateClearCannotBothWinTheirOriginalTokens() {
        withNotes(f -> {
            try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var other = new MongoRequiredNoteStore(client, client.getDatabase(f.database.getName()), f.cipher());
                for (int round = 0; round < 6; round++) {
                    String pipeline = "p" + round;
                    f.pipelineGeneration(pipeline, 1);
                    var source = f.notes.initialize(pipeline(pipeline, 1));
                    var shared = f.notes.initialize(chain("c" + round));
                    var carry = new RequiredNoteCarry(shared, source);
                    CountDownLatch ready = new CountDownLatch(2);
                    CountDownLatch start = new CountDownLatch(1);
                    var add = executor.submit(() -> { ready.countDown(); if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start"); try { f.notes.prepareCarry(carry); return true; } catch (TapstateException refused) { scopeUnavailable(refused); return false; } });
                    var clear = executor.submit(() -> { ready.countDown(); if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start"); try { other.clear(source, false); return true; } catch (TapstateException refused) { referenced(refused); return false; } });
                    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                    assertThat(List.of(add.get(20, TimeUnit.SECONDS), clear.get(20, TimeUnit.SECONDS)))
                            .containsExactlyInAnyOrder(true, false);
                }
            }
        });
    }

    @Test
    void missingCarryIndexesRefuseReadOnlyOpenAndDoNotRecreateThem() {
        withNotes(f -> {
            var shared = f.notes.initialize(chain("c1"));
            SystemCollections.PDK_NOTE_CARRIES.on(f.database).dropIndex("pdk_note_carry_target");
            assertThatThrownBy(() -> f.notes.carries(shared)).satisfies(MongoRequiredNoteStoreIT::unreadable);
            assertThat(IndexEnsure.existing(SystemCollections.PDK_NOTE_CARRIES.on(f.database),
                    SystemCollections.PDK_NOTE_CARRIES.indexes().getFirst())).isNull();
        });
    }

    @Test
    void loadingCarryDependenciesValidatesThePrivateScopeBeforeAPluginCanOpen() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            var shared = f.notes.initialize(chain("c1"));
            f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
            SystemCollections.PDK_NOTE_SCOPES.on(f.database).deleteOne(new Document("_id",
                    new Document("bindingVersion", 1L).append("namespace", source.owner().namespace())));
            assertThatThrownBy(() -> f.notes.carries(shared)).satisfies(MongoRequiredNoteStoreIT::scopeUnavailable);
            assertThat(SystemCollections.PDK_NOTE_CARRIES.on(f.database).countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void loadingCarryDependenciesRefusesMissingOrMalformedPrivateScopeFencesWithoutRepair() {
        withNotes(f -> {
            f.pipelineGeneration("p1", 1);
            var source = f.notes.initialize(pipeline("p1", 1));
            var shared = f.notes.initialize(chain("c1"));
            var carry = f.notes.prepareCarry(new RequiredNoteCarry(shared, source));
            var scopes = SystemCollections.PDK_NOTE_SCOPES.on(f.database);
            Document filter = new Document("_id", scopeMetadata(f, source).get("_id"));
            for (Object invalid : new Object[]{null, "0", 0, -1L, Long.MAX_VALUE}) {
                scopes.updateOne(filter, invalid == null
                        ? new Document("$unset", new Document("writeFence", ""))
                        : new Document("$set", new Document("writeFence", invalid)));
                assertThatThrownBy(() -> f.notes.carries(shared)).satisfies(MongoRequiredNoteStoreIT::unreadable);
                assertThatThrownBy(() -> f.notes.readCarried(carry, "key")).satisfies(MongoRequiredNoteStoreIT::unreadable);
                assertThatThrownBy(() -> f.notes.open(source.owner())).satisfies(MongoRequiredNoteStoreIT::unreadable);
                assertThat(scopeMetadata(f, source).get("writeFence")).isEqualTo(invalid);
                assertThat(SystemCollections.PDK_NOTE_CARRIES.on(f.database).countDocuments()).isEqualTo(1);
            }
            scopes.updateOne(filter, new Document("$set", new Document("writeFence", 0L)));
            assertThat(f.notes.carries(shared)).containsExactly(carry);
        });
    }

    private static Document scopeMetadata(Fixture f, RequiredNoteScope scope) {
        return SystemCollections.PDK_NOTE_SCOPES.on(f.database).find(new Document("_id",
                new Document("bindingVersion", scope.owner().bindingVersion()).append("namespace", scope.owner().namespace()))).first();
    }

    private static void referenced(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.NOTE_REFERENCED));
    }

    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    private static TapstateException scopeFailure() {
        return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
    }

    private static RequiredNoteOwner global() {
        return new RequiredNoteOwner(1, "pdk.global-state", RequiredNoteOwner.Kind.GLOBAL, "cluster-a", 0);
    }

    private static RequiredNoteOwner chain(String id) {
        return new RequiredNoteOwner(1, "pdk.chain." + id, RequiredNoteOwner.Kind.CHAIN, id, 0);
    }

    private static RequiredNoteOwner pipeline(String id, long generation) {
        return new RequiredNoteOwner(1, "pdk.state." + id + ".source", RequiredNoteOwner.Kind.PIPELINE, id, generation);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void scopeUnavailable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.NOTE_SCOPE_UNAVAILABLE));
    }

    private static void withNotes(Scenario scenario) {
        try (MongoClient admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            String name = "required_notes_" + UUID.randomUUID().toString().replace("-", "");
            MongoDatabase database = admin.getDatabase(name);
            MongoConnectionSettings settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(name), null, Duration.ofSeconds(5));
            try (MongoConfigurationDomain domain = MongoConfigurationBootstrap.prepare(settings,
                    "cluster-a", "installation-a", "setup-a", 1)) {
                SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).updateOne(new Document("_id", "cluster"),
                        new Document("$set", new Document("bindingVersion", 1L)));
                scenario.run(new Fixture(admin, database, domain.requiredNotes()));
            } catch (Exception failure) {
                throw new AssertionError(failure);
            } finally {
                database.drop();
            }
        }
    }

    private record Fixture(MongoClient admin, MongoDatabase database, RequiredNoteStore notes) {
        void pipelineGeneration(String id, long generation) {
            SystemCollections.OPERATOR_GENERATIONS.on(database).replaceOne(new Document("_id", id),
                    new Document("_id", id).append("stateGeneration", generation).append("phase", "ACTIVE")
                            .append("noteWriteFence", 0L),
                    new com.mongodb.client.model.ReplaceOptions().upsert(true));
        }

        SourceConfigCipher cipher() {
            String material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            return new SourceConfigCipher(Base64.getUrlDecoder().decode(material));
        }

        RequiredNoteStore paused(CountDownLatch entered, CountDownLatch release) {
            SourceConfigCipher selected = cipher();
            SourceConfigCipherProvider provider = new SourceConfigCipherProvider() {
                @Override public SourceConfigCipher current() { return selected; }
                @Override public SourceConfigCipher refresh() { return selected; }
                @Override public SourceConfigCipher currentForWrite() {
                    entered.countDown();
                    try {
                        if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("write not released");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                    return selected;
                }
            };
            return new MongoRequiredNoteStore(admin, database, provider);
        }
    }

    @FunctionalInterface
    private interface Scenario { void run(Fixture fixture) throws Exception; }
}
