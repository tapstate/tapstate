package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Latest-intent CAS and mapping/version atomicity, without opening a cache client. */
@RequiresDocker
class MongoPipelineIntentStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final String HASH = CanonicalHash.of(pipeline());

    private static io.tapstate.spi.store.PipelineKeyMoveWindows.Range point(io.tapstate.core.event.SourceOrder order) {
        return new io.tapstate.spi.store.PipelineKeyMoveWindows.Range(order.epoch(), order.seq(), order.seq());
    }

    @Test
    void replayStopsBeforeReuseAndBothDurableFloorsMustCoverTheExactInterval() {
        withConfiguration((domain, database, scope) -> {
            var windows = domain.keyMoveWindows();
            var first = new io.tapstate.core.event.SourceOrder(3, 7);
            var last = new io.tapstate.core.event.SourceOrder(3, 12);
            var other = new io.tapstate.core.event.SourceOrder(1, 2);
            var orders = new io.tapstate.spi.store.PipelineKeyMoveWindows.Stream("chain", "source", "orders", null);
            var items = new io.tapstate.spi.store.PipelineKeyMoveWindows.Stream("channel", "source2", "items", "capture");
            windows.requireClear("flow");
            assertThatThrownBy(() -> windows.arm(scope, 40, "expired", orders, point(first), null,
                    () -> { throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }))
                    .isInstanceOf(TapstateException.class);
            windows.requireClear("flow");
            var initial = windows.arm(scope, 41, "activation", orders, point(first), null, () -> {});
            windows.arm(scope, 41, "activation", orders, point(last), initial, () -> {});
            var current = windows.arm(scope, 41, "activation", items, point(other), initial, () -> {});
            assertThat(current.ranges().get(orders)).isEqualTo(new io.tapstate.spi.store.PipelineKeyMoveWindows.Range(3, 7, 12));
            var covered = Map.of(orders, last, items, other);
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                var takeover = reopened.keyMoveWindows();
                assertThatThrownBy(() -> takeover.requireClear("flow")).isInstanceOfSatisfying(
                        TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.KEY_MOVE_RECOVERY_UNCERTAIN));
                assertThatThrownBy(() -> takeover.arm(scope, 42, "replacement", orders, point(last), null, () -> {})).isInstanceOf(TapstateException.class);
                assertThat(takeover.read("flow")).contains(current);
            }
            assertThat(windows.retire(initial, covered, covered)).isFalse();
            assertThat(windows.retire(current, Map.of(orders, last), covered)).isFalse();
            assertThat(windows.retire(current, covered, Map.of(orders, first, items, other))).isFalse();
            assertThat(windows.retire(current, covered, covered)).isTrue();
            var next = windows.arm(scope, 42, "replacement", orders, point(first), null, () -> {});
            assertThat(windows.retire(current, covered, covered)).isFalse();
            assertThat(windows.read("flow")).contains(next);
        });
    }

    @Test
    void disappearanceOfTheWindowCollectionRefusesTheNextAdmission() {
        withConfiguration((domain, database, scope) -> {
            var windows = domain.keyMoveWindows();
            var stream = new io.tapstate.spi.store.PipelineKeyMoveWindows.Stream("chain", "input", "orders", null);
            var changes = point(new io.tapstate.core.event.SourceOrder(1, 2));
            var last = windows.arm(scope, 42, "activation", stream, changes, null, () -> {});
            SystemCollections.PIPELINE_KEY_MOVE_WINDOWS.on(database).drop();
            assertThatThrownBy(() -> windows.requireClear(scope.pipelineId())).isInstanceOfSatisfying(
                    TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.KEY_MOVE_RECOVERY_UNCERTAIN));
            assertThatThrownBy(() -> windows.arm(scope, 42, "activation", stream, changes, last, () -> {}))
                    .isInstanceOf(TapstateException.class);
            assertThat(database.listCollectionNames()).doesNotContain(SystemCollections.PIPELINE_KEY_MOVE_WINDOWS.collectionName());
        });
    }

    @Test
    void droppingTheCollectionDuringFirstAdmissionCannotRecreateIt() {
        withConfiguration((domain, database, scope) -> {
            var table = SystemCollections.PIPELINE_KEY_MOVE_WINDOWS;
            var removed = new java.util.concurrent.atomic.AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("insert")
                            && table.collectionName().equals(event.getCommand().getString("insert").getValue())
                            && removed.compareAndSet(false, true)) {
                        table.on(database).drop();
                    }
                }
            };
            var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build();
            var orders = new io.tapstate.spi.store.PipelineKeyMoveWindows.Stream("chain", "source", "orders", null);
            var change = point(new io.tapstate.core.event.SourceOrder(1, 9));
            try (var client = MongoClients.create(settings)) {
                var windows = new MongoKeyMoveWindowStore(client.getDatabase(database.getName()), () -> {});
                assertThatThrownBy(() -> windows.arm(scope, 42, "activation", orders, change, null, () -> {}))
                        .isInstanceOfSatisfying(TapstateException.class,
                                e -> assertThat(e.code()).isEqualTo(IoError.KEY_MOVE_RECOVERY_UNCERTAIN));
                assertThat(removed).isTrue();
            }
            try (var retained = MongoConfigurationDomain.openExisting(settings(database))) {
                assertThatThrownBy(() -> retained.keyMoveWindows().requireClear(scope.pipelineId()))
                        .isInstanceOfSatisfying(TapstateException.class,
                                e -> assertThat(e.code()).isEqualTo(IoError.KEY_MOVE_RECOVERY_UNCERTAIN));
            }
            assertThat(database.listCollectionNames()).doesNotContain(table.collectionName());
        });
    }

    @Test
    void aRefusedExtensionRestoresOnlyTheRangeItActuallyExtended() {
        withConfiguration((domain, database, scope) -> {
            var windows = domain.keyMoveWindows();
            var stream = new io.tapstate.spi.store.PipelineKeyMoveWindows.Stream("chain", "input", "orders", null);
            var first = point(new io.tapstate.core.event.SourceOrder(1, 2));
            var last = point(new io.tapstate.core.event.SourceOrder(1, 6));
            var admitted = windows.arm(scope, 42, "activation", stream, first, null, () -> {});
            Runnable lost = () -> { throw new IllegalStateException("execution stopped"); };
            assertThatThrownBy(() -> windows.arm(scope, 42, "activation", stream, last, admitted, lost))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(windows.read(scope.pipelineId())).contains(admitted);
            var later = point(new io.tapstate.core.event.SourceOrder(1, 9));
            assertThatThrownBy(() -> windows.arm(scope, 42, "activation", stream, last, admitted, () -> {
                windows.arm(scope, 42, "activation", stream, later, null, () -> {});
                lost.run();
            })).isInstanceOf(IllegalStateException.class);
            assertThat(windows.read(scope.pipelineId()).orElseThrow().ranges().get(stream).last()).isEqualTo(9);
        });
    }

    @Test
    void aPreparedArtifactHandoffPreservesTheOriginalBindingWhenItsStartIsSuperseded() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.STOPPED, false, false, null))).isTrue();
            assertThat(store.compareAndSetExplicitStart(scope, 1, intent(PipelineState.RUNNING, false, true, 0L))).isTrue();
            var original = store.load("flow").orElseThrow();
            var bindings = store.connectorBindings();
            var oldNote = new RequiredNoteScope(RequiredNoteOwner.forNode(scope.bindingVersion(),
                    new PipelineNode("flow", "target"), PipelineNode.Purpose.SYNC, scope.stateGeneration()), 1);
            var old = new ConnectorRunBindings.Binding("target", PipelineNode.Purpose.SYNC, "mongodb", "1.0", "a".repeat(64), oldNote, null, null);
            var candidate = new ConnectorRunBindings.Binding("target", PipelineNode.Purpose.SYNC, "mongodb", "2.0", "b".repeat(64), oldNote, null, null);
            bindings.seed(original, List.of(old));
            assertThat(bindings.prepare(original, List.of(candidate)).handoff().phase()).isEqualTo(ConnectorRunBindings.Phase.PREPARED);
            assertThat(bindings.prepare(original, List.of(candidate)).effective()).containsExactly(old);
            assertThat(bindings.loadAll()).containsOnlyKeys("flow").containsEntry("flow", bindings.load(scope).orElseThrow());
            assertThat(domain.retainedConnectorArtifacts()).containsExactlyInAnyOrder(old.sha256(), candidate.sha256());
            assertThatThrownBy(() -> bindings.prepare(original, List.of(old))).satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(store.compareAndSet(scope, 2, intent(PipelineState.PAUSED, false, false, null))).isTrue();
            assertThatThrownBy(() -> bindings.publish(original)).satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(bindings.load(scope).orElseThrow().effective()).containsExactly(old);
            assertThatThrownBy(() -> store.compareAndSetExplicitStart(scope, 3, intent(PipelineState.RUNNING, false, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            bindings.abort(scope, 2);
            assertThat(bindings.load(scope).orElseThrow().effective()).containsExactly(old);
            assertThat(bindings.load(scope).orElseThrow().handoff()).isNull();
            assertThat(domain.retainedConnectorArtifacts()).containsExactly(old.sha256());
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                assertThat(reopened.intents().connectorBindings().load(scope)).isEqualTo(bindings.load(scope));
            }
        });
    }

    @Test
    void publishedBindingCleanupRetainsItsOriginalRevisionAcrossLaterIntents() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            var bindings = store.connectorBindings();
            assertThat(store.compareAndSetExplicitStart(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            var original = store.load("flow").orElseThrow();
            bindings.prepare(original, List.of());
            var published = bindings.publish(original);
            assertThat(published.effectiveIntentRevision()).isEqualTo(1);
            assertThat(published.handoff().phase()).isEqualTo(ConnectorRunBindings.Phase.PUBLISHED);
            assertThatThrownBy(() -> store.compareAndSetExplicitStart(scope, 1,
                    intent(PipelineState.RUNNING, false, false, null))).satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(store.compareAndSet(scope, 1, intent(PipelineState.PAUSED, false, false, null))).isTrue();
            assertThatThrownBy(() -> bindings.abort(scope, 1)).satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThatThrownBy(() -> bindings.complete(scope, 2)).satisfies(MongoPipelineIntentStoreIT::fenced);
            bindings.complete(scope, 1);
            bindings.complete(scope, 1);
            assertThat(bindings.load(scope).orElseThrow().handoff()).isNull();
            assertThat(bindings.load(scope).orElseThrow().effectiveIntentRevision()).isEqualTo(1);
        });
    }

    @Test
    void latestIntentAndCoordinatesSurviveReopenWithoutAnyCacheResource() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            var desired = intent(PipelineState.RUNNING, true, true, 7L);
            long before = revision(database);
            assertThat(store.compareAndSet(scope, 0, desired)).isTrue();
            assertThat(store.load("flow")).contains(new PipelineIntentSnapshot(scope, 1, desired));
            assertThat(revision(database)).isEqualTo(before + 1);
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                assertThat(reopened.intents().load("flow")).isEqualTo(store.load("flow"));
            }
            assertThat(database.listCollectionNames().into(new ArrayList<>()))
                    .contains("pipeline_intents").doesNotContain("pipeline_desired", "pipeline_state", "cache_identity", "workload_claims", "srs_meta");
        });
    }

    @Test
    void deferredRejectionsExposeOnlyCompleteBoundedDocumentIdentifiers() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            var desired = intent(PipelineState.RUNNING, false, false, null);
            var inputs = new io.tapstate.spi.store.PipelinePreparationInputs(HASH, Map.of(), Map.of(), List.of());
            var valid = Map.<String, Object>of("id", "orders v2/archive:2026", "field", "source[0].tables[1].name");
            assertThat(store.compareAndSetStart(scope, 0, desired, inputs)).isTrue();
            store.failStart(store.pendingStart("flow").orElseThrow(), IoError.DOCUMENT_UNREADABLE, valid);
            assertThat(domain.pipelineDisplayHeaders(List.of("flow"), true).get("flow").startFailureParams())
                    .isEqualTo(valid);
            assertThat(domain.pipelineDisplayHeaders(List.of("flow"), false).get("flow").startFailure()).isEmpty();
            assertThat(domain.pipelineDisplayHeaders(List.of(), true)).isEmpty();

            var rows = SystemCollections.PIPELINE_INTENTS.on(database);
            var filter = new Document("_id", "flow");
            var original = rows.find(filter).first();
            rows.updateOne(filter, new Document("$set", new Document("startFailureParams", new Document("id", "incomplete"))));
            assertThatThrownBy(() -> store.load("flow")).satisfies(MongoPipelineIntentStoreIT::unreadable);
            var malformed = domain.pipelineDisplayHeaders(List.of("flow"), true).get("flow");
            assertThat(malformed.startFailure()).isEmpty();
            assertThat(malformed.intentFailure()).hasValueSatisfying(MongoPipelineIntentStoreIT::unreadable);
            rows.replaceOne(filter, original);

            var mappings = SystemCollections.OPERATOR_GENERATIONS.on(database);
            var currentMapping = mappings.find(filter).first();
            mappings.updateOne(filter, new Document("$set", new Document("stateGeneration", 2L))
                    .append("$unset", new Document("connectorBindings", "")));
            assertThat(domain.pipelineDisplayHeaders(List.of("flow"), true).get("flow").startFailure()).isEmpty();
            mappings.replaceOne(filter, currentMapping);

            assertThat(store.compareAndSetStart(scope, 1, desired, inputs)).isTrue();
            store.failStart(store.pendingStart("flow").orElseThrow(), IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", "mongodb://credential.invalid/private", "field", "revision"));
            var rejection = domain.pipelineDisplayHeaders(List.of("flow"), true).get("flow");
            assertThat(rejection.startFailure()).contains(IoError.DOCUMENT_UNREADABLE);
            assertThat(rejection.startFailureParams()).isEmpty();
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first())
                    .doesNotContainKey("startFailureParams");
        });
    }

    @Test
    void rejectedDiagnosticValuesKeepTheGenericFailure() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            var desired = intent(PipelineState.RUNNING, false, false, null);
            var inputs = new io.tapstate.spi.store.PipelinePreparationInputs(HASH, Map.of(), Map.of(), List.of());
            long revision = 0;
            for (var params : List.<Map<String, Object>>of(
                    Map.of("id", " ", "field", "revision"),
                    Map.of("id", "a".repeat(257), "field", "revision"),
                    Map.of("id", "\u00e9".repeat(129), "field", "revision"),
                    Map.of("id", "bad\nline", "field", "revision"),
                    Map.of("id", "bad\u2028line", "field", "revision"),
                    Map.of("id", "bad\u2029line", "field", "revision"),
                    Map.of("id", "document", "field", "body:raw=value"),
                    Map.of("id", "document", "field", 1))) {
                assertThat(store.compareAndSetStart(scope, revision++, desired, inputs)).isTrue();
                var pending = store.pendingStart("flow").orElseThrow();
                assertThatThrownBy(() -> store.compareAndSetStart(scope, pending.intent().intentRevision(), desired, inputs))
                        .satisfies(MongoPipelineIntentStoreIT::fenced);
                store.failStart(pending, IoError.DOCUMENT_UNREADABLE, params);
                var rejection = domain.pipelineDisplayHeaders(List.of("flow"), true).get("flow");
                assertThat(rejection.startFailure()).contains(IoError.DOCUMENT_UNREADABLE);
                assertThat(rejection.startFailureParams()).isEmpty();
            }
            assertThat(store.remove(scope, revision)).isTrue();
            assertThat(domain.pipelineDisplayHeaders(List.of("flow"), true).get("flow").startFailure()).isEmpty();
        });
    }

    @Test
    void stopThenStartCoalescesIntoOneLatestValueWithTheOriginalOneTimeStamp() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.STOPPED, true, false, null))).isTrue();
            assertThat(store.compareAndSet(scope, 1, intent(PipelineState.RUNNING, true, true, 12L))).isTrue();
            assertThat(store.load("flow")).get().satisfies(snapshot -> {
                assertThat(snapshot.intentRevision()).isEqualTo(2);
                assertThat(snapshot.scope()).isEqualTo(scope);
                assertThat(snapshot.desired().revision()).isEqualTo(HASH);
                assertThat(snapshot.desired().purgeState()).isTrue();
                assertThat(snapshot.desired().reassemble()).isTrue();
                assertThat(snapshot.desired().rebuiltAtStateEpoch()).isEqualTo(12L);
            });
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).countDocuments()).isOne();
        });
    }

    @Test
    void staleRevisionOrArtifactHashCannotOverwriteTheLatestIntentOrAdvanceVersions() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            var desired = intent(PipelineState.RUNNING, false, false, null);
            assertThat(store.compareAndSet(scope, 0, desired)).isTrue();
            long before = revision(database); Document mapping = mapping(database);
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.STOPPED, true, false, null))).isFalse();
            var staleHash = new DesiredState("flow", PipelineState.STOPPED, "old-artifact-hash", true);
            assertThat(store.compareAndSet(scope, 1, staleHash)).isFalse();
            assertThat(store.load("flow")).contains(new PipelineIntentSnapshot(scope, 1, desired));
            assertThat(revision(database)).isEqualTo(before);
            assertThat(mapping(database)).isEqualTo(mapping);
        });
    }

    @Test
    void twoIndependentClientsCanPublishOnlyOneSuccessorOfAnObservedRevision() {
        withConfiguration((domain, database, scope) -> {
            assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            long before = revision(database);
            try (var other = MongoConfigurationDomain.open(settings(database), "cluster-a"); var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    return domain.intents().compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)); });
                var b = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    return other.intents().compareAndSet(scope, 1, intent(PipelineState.PAUSED, false, false, null)); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            }
            assertThat(domain.intents().load("flow")).get().extracting(PipelineIntentSnapshot::intentRevision).isEqualTo(2L);
            assertThat(revision(database)).isEqualTo(before + 1);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).countDocuments()).isOne();
        });
    }

    @Test
    void removingAnAbsentIntentAlsoPreventsADelayedFirstWriterFromRevivingIt() {
        withConfiguration((domain, database, scope) -> {
            assertThat(domain.intents().remove(scope, 0)).isTrue();
            assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .as("an earlier never-written observation cannot revive a removed intent").isFalse();
            assertThat(domain.intents().load("flow")).contains(new PipelineIntentSnapshot(scope, 1, null));
            assertThat(domain.intents().pipelineIds()).isEmpty();
        });
    }

    @Test
    void deletionRetainsASmallRevisionMarkerAndRejectsEarlierWritersAfterRecreation() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            assertThat(store.remove(scope, 1)).isTrue();
            var deleted = store.load("flow").orElseThrow();
            assertThat(deleted.deleted()).isTrue(); assertThat(deleted.intentRevision()).isEqualTo(2);
            assertThat(store.pipelineIds()).isEmpty();
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first())
                    .doesNotContainKeys("targetState", "revision", "assemblyRevision", "purgeState", "reassemble", "rebuiltAtStateEpoch");
            long before = revision(database);
            assertThat(store.remove(scope, 2)).isTrue(); assertThat(revision(database)).isEqualTo(before);
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.STOPPED, true, false, null))).isFalse();
            assertThat(store.compareAndSet(scope, 2, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            assertThat(store.compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null))).isFalse();
            assertThat(store.load("flow")).get().extracting(PipelineIntentSnapshot::intentRevision).isEqualTo(3L);
        });
    }

    @Test
    void callerCannotChooseADifferentIncarnationForTheCurrentBinding() {
        withConfiguration((domain, database, scope) -> {
            var wrong = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), UUID.randomUUID().toString(),
                    scope.pipelineId(), scope.stateGeneration());
            var originalMapping = mapping(database); long before = revision(database);
            assertThatThrownBy(() -> domain.intents().compareAndSet(wrong, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).countDocuments()).isZero();
            assertThat(mapping(database)).isEqualTo(originalMapping);
            assertThat(revision(database)).isEqualTo(before);
        });
    }

    @Test
    void aStoredDifferentIncarnationCannotBeReadRetargetedOrRemovedByAnOrdinaryCommand() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            SystemCollections.PIPELINE_INTENTS.on(database).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("cacheIncarnation", UUID.randomUUID().toString())));
            var stored = SystemCollections.PIPELINE_INTENTS.on(database).find().first();
            var originalMapping = mapping(database); long before = revision(database);
            assertThat(store.pipelineIds()).containsExactly("flow");
            assertThatThrownBy(() -> store.load("flow")).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.remove(scope, 1)).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first()).isEqualTo(stored);
            assertThat(mapping(database)).isEqualTo(originalMapping); assertThat(revision(database)).isEqualTo(before);
        });
    }

    @Test
    void anOrdinaryCommandCannotAdoptAnotherStoredGenerationAtTheSameRevision() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("stateGeneration", 2L)));
            var next = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), "flow", 2);
            var before = SystemCollections.PIPELINE_INTENTS.on(database).find().first();
            long version = revision(database); var originalMapping = mapping(database);
            assertThatThrownBy(() -> store.compareAndSet(next, 1, intent(PipelineState.STOPPED, true, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThatThrownBy(() -> store.remove(next, 1)).satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first()).isEqualTo(before);
            assertThat(mapping(database)).isEqualTo(originalMapping); assertThat(revision(database)).isEqualTo(version);
        });
    }

    @Test
    void missingRecoveryAuthorityCannotBeReconstructedFromAnExistingIntentOrCallerScope() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            SystemCollections.STORAGE_RECOVERY.on(database).deleteMany(new Document());
            var before = SystemCollections.PIPELINE_INTENTS.on(database).find().first();
            var originalMapping = mapping(database); long version = revision(database);
            assertThatThrownBy(() -> store.load("flow")).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.remove(scope, 1)).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThat(SystemCollections.STORAGE_RECOVERY.on(database).countDocuments()).isZero();
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first()).isEqualTo(before);
            assertThat(mapping(database)).isEqualTo(originalMapping); assertThat(revision(database)).isEqualTo(version);
        });
    }

    @Test
    void staleBindingGenerationAndNonActiveMappingAreNeverSilentlyRetargeted() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            for (var wrong : List.of(new PipelineIntentScope("cluster-b", 1, scope.cacheIncarnation(), "flow", 1),
                    new PipelineIntentScope("cluster-a", 2, scope.cacheIncarnation(), "flow", 1),
                    new PipelineIntentScope("cluster-a", 1, scope.cacheIncarnation(), "flow", 2))) {
                assertThatThrownBy(() -> store.compareAndSet(wrong, 0, intent(PipelineState.RUNNING, false, false, null)))
                        .satisfies(MongoPipelineIntentStoreIT::fenced);
            }
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("phase", "REBUILDING").append("operationId", "recovery-a")));
            assertThatThrownBy(() -> store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(store.load("flow")).isEmpty();
        });
    }

    @Test
    void postWriteLossOfOriginalAuthorityRollsBackIntentMappingAndGlobalVersion() {
        withConfiguration((domain, database, scope) -> {
            var checks = new AtomicInteger(); long before = revision(database); Document mapping = mapping(database);
            var guarded = domain.intents().guardedBy(() -> {
                if (checks.incrementAndGet() == 2) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(domain.intents().load("flow")).isEmpty();
            assertThat(revision(database)).isEqualTo(before); assertThat(mapping(database)).isEqualTo(mapping);
        });
    }

    @Test
    void aRealTransactionRetryRechecksTheOriginalGuardInsteadOfCreatingANewContext() {
        for (boolean injectFailure : List.of(false, true)) {
            withConfiguration((domain, database, scope) -> {
                var failedUpdates = new AtomicInteger(); var startedUpdates = new AtomicInteger(); var checks = new AtomicInteger();
                var witness = new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("update")) startedUpdates.incrementAndGet();
                    }
                    @Override public void commandFailed(CommandFailedEvent event) {
                        if (event.getCommandName().equals("update")) failedUpdates.incrementAndGet();
                    }
                };
                try (var admin = MongoClients.create(MONGO.getReplicaSetUrl());
                        var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                                .addCommandListener(witness).build())) {
                    admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", injectFailure ? new Document("times", 1) : "off")
                            .append("data", new Document("failCommands", List.of("update")).append("errorCode", 112)
                                    .append("errorLabels", List.of("TransientTransactionError"))));
                    try {
                        long before = revision(database);
                        String material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
                        var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                        var observedDatabase = observed.getDatabase(database.getName());
                        var originalBindings = new MongoStorageBindingStore(observed, observedDatabase, ciphers);
                        var guarded = new MongoPipelineIntentStore(observed, observedDatabase, "cluster-a", originalBindings).guardedBy(() -> {
                            checks.incrementAndGet();
                            if (failedUpdates.get() > 0) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
                        });
                        if (injectFailure) {
                            assertThatThrownBy(() -> guarded.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                                    .satisfies(MongoPipelineIntentStoreIT::fenced);
                            assertThat(failedUpdates.get()).isOne(); assertThat(startedUpdates.get()).isOne();
                            assertThat(checks.get()).isGreaterThanOrEqualTo(2); assertThat(revision(database)).isEqualTo(before);
                            assertThat(domain.intents().load("flow")).isEmpty();
                        } else {
                            assertThat(guarded.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
                            assertThat(failedUpdates.get()).isZero(); assertThat(startedUpdates.get()).isEqualTo(2);
                            assertThat(checks.get()).isGreaterThanOrEqualTo(2); assertThat(revision(database)).isEqualTo(before + 1);
                            assertThat(domain.intents().load("flow")).isPresent();
                        }
                    } finally {
                        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                    }
                }
            });
        }
    }

    @Test
    void corruptIntentBodiesRemainInTheIdProjectionButAreNeverDefaultedOrOverwritten() {
        for (String field : List.of("reassemble", "rebuiltAtStateEpoch", "cacheIncarnation", "intentRevision")) {
            withConfiguration((domain, database, scope) -> {
                assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
                SystemCollections.PIPELINE_INTENTS.on(database).updateOne(new Document(), new Document("$set", new Document(field, field.equals("cacheIncarnation") ? 7L : "raw-secret-sentinel")));
                long before = revision(database);
                assertThat(domain.intents().pipelineIds()).containsExactly("flow");
                assertThatThrownBy(() -> domain.intents().load("flow")).satisfies(MongoPipelineIntentStoreIT::unreadable);
                assertThatThrownBy(() -> domain.intents().compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)))
                        .satisfies(MongoPipelineIntentStoreIT::unreadable);
                assertThat(revision(database)).isEqualTo(before);
            });
        }
    }

    @Test
    void missingMappingOrConfigurationVersionRefusesWithoutCreatingReplacementFacts() {
        for (var missing : List.of(SystemCollections.OPERATOR_GENERATIONS, SystemCollections.CONFIGURATION_VERSION)) {
            withConfiguration((domain, database, scope) -> {
                missing.on(database).deleteMany(new Document());
                assertThatThrownBy(() -> domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                        .satisfies(MongoPipelineIntentStoreIT::unreadable);
                assertThat(domain.intents().load("flow")).isEmpty(); assertThat(missing.on(database).countDocuments()).isZero();
            });
        }
    }

    private static DesiredState intent(PipelineState target, boolean purge, boolean reassemble, Long epoch) {
        return new DesiredState("flow", target, HASH, purge, "assembly-a", reassemble, epoch);
    }
    private static io.tapstate.core.model.Resource pipeline() { return new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n"); }
    private static long revision(MongoDatabase database) { return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision"); }
    private static Document mapping(MongoDatabase database) { return SystemCollections.OPERATOR_GENERATIONS.on(database).find(new Document("_id", "flow")).first(); }
    private static void fenced(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
            error -> assertThat(error.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED)); }
    private static void unreadable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
        assertThat(error.getMessage()).doesNotContain("raw-secret-sentinel");
    }); }
    private static MongoConnectionSettings settings(MongoDatabase database) { return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5)); }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("config_intent_" + UUID.randomUUID().toString().replace("-", ""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(database), "cluster-a", "installation-a", "setup-a", 1)) {
                var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache.invalid/cache", "cache", null);
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb",
                        Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                domain.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cache, master);
                domain.artifacts().create(pipeline());
                var cacheScope = domain.bindings().cachePreparation().orElseThrow().businessScope();
                var scope = new PipelineIntentScope(cacheScope.clusterId(), cacheScope.bindingVersion(), cacheScope.cacheIncarnation(), "flow", 1);
                scenario.run(domain, database, scope);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, MongoDatabase database, PipelineIntentScope scope) throws Exception; }
}
