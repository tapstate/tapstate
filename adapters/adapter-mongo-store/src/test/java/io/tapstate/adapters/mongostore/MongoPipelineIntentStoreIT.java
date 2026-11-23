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

    @Test
    void schedulingHintsUseOneProjectionAndTrackEveryIntentRevisionAndPhysicalScope() {
        withConfiguration((domain, database, scope) -> {
            assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.STOPPED, false, false, null))).isTrue();
            String material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
            try (var reads = new MongoCommandWitness(MONGO.getReplicaSetUrl(), database.getName(), "find",
                    SystemCollections.PIPELINE_INTENTS.collectionName())) {
                var observed = new MongoPipelineIntentStore(reads.client, reads.database, "cluster-a",
                        new MongoStorageBindingStore(reads.client, reads.database, ciphers));
                assertThat(observed.scanCandidates()).containsExactly(new io.tapstate.spi.store.PipelineIntentStore.ScanCandidate(
                        "flow", 0, scope, 1, PipelineState.STOPPED));
                assertThat(reads.confirmedCommands()).isOne();
                assertThat(domain.intents().compareAndSet(scope, 1, intent(PipelineState.RUNNING, false, false, null))).isTrue();
                assertThat(observed.scanCandidates()).containsExactly(new io.tapstate.spi.store.PipelineIntentStore.ScanCandidate(
                        "flow", 2, scope, 2, PipelineState.RUNNING));
                assertThat(reads.confirmedCommands()).isEqualTo(2);
                var next = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), "flow", scope.stateGeneration()+1);
                SystemCollections.PIPELINE_INTENTS.on(database).updateOne(new Document("_id", "flow"),
                        new Document("$set", new Document("stateGeneration", next.stateGeneration())));
                assertThat(observed.scanCandidates()).containsExactly(new io.tapstate.spi.store.PipelineIntentStore.ScanCandidate(
                        "flow", 2, next, 2, PipelineState.RUNNING));
                assertThat(reads.confirmedCommands()).isEqualTo(3);
            }
        });
    }

    @Test
    void anAlertMayReuseOnlyTheMatchingCacheIdentityAlreadyAuditedInItsTransaction() {
        withConfiguration((domain, database, scope) -> {
            var desired = readyForActuation(domain, database, scope);
            var material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            var cipher = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
            var commands = new ArrayList<String>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommandName()); }
            };
            try (var client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(
                    new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build());
                 var session = client.startSession()) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                var db = client.getDatabase(database.getName());
                var bindings = new MongoStorageBindingStore(client, db, cipher);
                var store = new MongoPipelineIntentStore(client, db, scope.clusterId(), bindings);
                var previous = session.withTransaction(() -> {
                    var audited = bindings.cachePreparationForTransaction(session).orElseThrow();
                    commands.clear();
                    var expected = store.loadInTransaction(session, scope.pipelineId());
                    assertThat(expected).contains(new PipelineIntentSnapshot(scope, 1, desired));
                    int unshared = commands.size();
                    commands.clear();
                    assertThat(store.loadInTransaction(session, scope.pipelineId(), audited)).isEqualTo(expected);
                    assertThat(commands).containsExactly("find");
                    assertThat(unshared).isGreaterThan(commands.size());
                    var auth = audited.authScope();
                    for (var foreign : List.of(
                            new io.tapstate.spi.store.CacheAuthScope("foreign", auth.bindingVersion(), auth.mode(), auth.cacheIncarnation(), auth.authGeneration()),
                            new io.tapstate.spi.store.CacheAuthScope(auth.clusterId(), auth.bindingVersion()+1, auth.mode(), auth.cacheIncarnation(), auth.authGeneration()),
                            new io.tapstate.spi.store.CacheAuthScope(auth.clusterId(), auth.bindingVersion(), auth.mode(), "other", auth.authGeneration()))) {
                        var mismatch = new io.tapstate.spi.store.CachePreparation(audited.operationId(), foreign);
                        assertThatThrownBy(() -> store.loadInTransaction(session, scope.pipelineId(), mismatch))
                                .satisfies(MongoPipelineIntentStoreIT::unreadable);
                        assertThat(store.loadInTransaction(session, "absent", mismatch)).isEmpty();
                    }
                    System.out.printf("ALERT_INTENT_READ unshared=%d audited=1 originalScope=true%n", unshared);
                    return audited;
                });
                assertThatThrownBy(() -> store.loadInTransaction(session, scope.pipelineId(), previous))
                        .isInstanceOf(IllegalStateException.class);
            }
        });
    }

    @Test
    void actuationUsesOneSnapshotCommandAndReloadsTheLatestIntent() {
        withConfiguration((domain, database, scope) -> {
            var node = new io.tapstate.spi.store.WorkloadClaimFence(
                    new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(),
                            io.tapstate.spi.store.WorkloadClaimType.NODE_SESSION, "node-a"),
                    new io.tapstate.spi.store.WorkloadOwner("node-a", "boot-a"), 1, 0, 1);
            var preparation = new io.tapstate.spi.store.PipelinePreparation(scope.pipelineId(), scope.stateGeneration(),
                    io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE, scope, UUID.randomUUID().toString(), node);
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", scope.pipelineId()),
                    new Document("$set", new Document("cachePreparation", MongoPipelinePreparationFacts.encode(preparation))));
            var desired = intent(PipelineState.RUNNING, false, false, null);
            assertThat(domain.intents().compareAndSet(scope, 0, desired)).isTrue();
            var commands = new ArrayList<org.bson.BsonDocument>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommand().clone()); }
            };
            var material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
            try (var client = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                var db = client.getDatabase(database.getName());
                var store = new MongoPipelineIntentStore(client, db, scope.clusterId(), new MongoStorageBindingStore(client, db, ciphers));
                commands.clear();
                assertThat(store.loadForActuation(scope.pipelineId())).contains(new PipelineIntentSnapshot(scope, 1, desired));
                System.out.println("ACTUATION_READ commands=" + commands.stream().map(command -> command.getFirstKey()).toList());
                assertThat(commands).hasSize(1);
                assertThat(commands.getFirst().getString("aggregate").getValue()).isEqualTo("pipeline_intents");
                assertThat(commands.getFirst().getDocument("readConcern").getString("level").getValue()).isEqualTo("snapshot");
                assertThat(commands.getFirst().containsKey("txnNumber")).isFalse();
                assertThat(commands.stream().map(command -> command.getFirstKey()).toList()).containsExactly("aggregate");
                var stopped = intent(PipelineState.STOPPED, false, false, null);
                assertThat(domain.intents().compareAndSet(scope, 1, stopped)).isTrue();
                commands.clear();
                assertThat(store.loadForActuation(scope.pipelineId())).contains(new PipelineIntentSnapshot(scope, 2, stopped));
                assertThat(commands).hasSize(1);
            }
        });
    }

    @Test
    void actuationRetainsForeignCollectionCollationAndErrorPriority() {
        withConfiguration((domain, database, scope) -> {
            readyForActuation(domain, database, scope);
            var name = SystemCollections.TRANSFORM_DEFINITIONS.collectionName();
            SystemCollections.TRANSFORM_DEFINITIONS.on(database).drop();
            database.createCollection(name, new com.mongodb.client.model.CreateCollectionOptions().collation(
                    com.mongodb.client.model.Collation.builder().locale("en")
                            .collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build()));
            var source = StorageBinding.defaultSourceId(StorageBinding.Mode.OP);
            SystemCollections.TRANSFORM_DEFINITIONS.on(database).insertOne(new Document("_id", source.toUpperCase(java.util.Locale.ROOT)));
            assertThat(domain.intents().loadForActuation(scope.pipelineId())).isPresent();
            assertThatThrownBy(() -> domain.intents().load(scope.pipelineId()))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(failure.args()).containsEntry("id", "storage-binding");
                    });
            SystemCollections.PIPELINE_INTENTS.on(database).updateOne(new Document("_id", scope.pipelineId()),
                    new Document("$set", new Document("intentRevision", 1)));
            assertThatThrownBy(() -> domain.intents().loadForActuation(scope.pipelineId()))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(failure.args()).containsEntry("id", "pipeline-intent");
                    });
        });
    }

    @Test
    void actuationRetainsSourceCollationWhenTheSnapshotRootIsCaseInsensitive() {
        withConfiguration((domain, database, scope) -> {
            var desired = readyForActuation(domain, database, scope);
            var root = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database);
            var identity = root.find().first();
            root.drop();
            database.createCollection(SystemCollections.CONFIG_CLUSTER_IDENTITY.collectionName(),
                    new com.mongodb.client.model.CreateCollectionOptions().collation(
                            com.mongodb.client.model.Collation.builder().locale("en")
                                    .collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build()));
            root.insertOne(identity);
            SystemCollections.SOURCES.on(database).insertOne(new Document("_id",
                    StorageBinding.defaultSourceId(StorageBinding.Mode.OP).toUpperCase(java.util.Locale.ROOT)));
            assertThat(domain.intents().loadForActuation(scope.pipelineId())).contains(new PipelineIntentSnapshot(scope, 1, desired));
        });
    }

    @Test
    void actuationFallsBackForLargeRowsAndRefusesForeignCacheAndMappingScopes() {
        withConfiguration((domain, database, scope) -> {
            readyForActuation(domain, database, scope);
            var intents = SystemCollections.PIPELINE_INTENTS.on(database);
            var filter = new Document("_id", scope.pipelineId());
            var original = intents.find(filter).first();
            String revision = "a".repeat(70 * 1024);
            intents.updateOne(filter, new Document("$set", new Document("revision", revision)));
            assertThat(domain.intents().loadForActuation(scope.pipelineId())).get()
                    .satisfies(snapshot -> assertThat(snapshot.desired().revision()).isEqualTo(revision));
            intents.replaceOne(filter, original);
            intents.updateOne(filter, new Document("$set", new Document("cacheIncarnation", UUID.randomUUID().toString())));
            assertThatThrownBy(() -> domain.intents().loadForActuation(scope.pipelineId())).satisfies(MongoPipelineIntentStoreIT::unreadable);
            intents.replaceOne(filter, original);
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(filter,
                    new Document("$set", new Document("stateGeneration", 2L)));
            assertThatThrownBy(() -> domain.intents().loadForActuation(scope.pipelineId())).satisfies(MongoPipelineIntentStoreIT::fenced);
        });
    }

    @Test
    void actuationRebuildsAllCapturedFactsAfterATransientTransactionFailure() {
        withConfiguration((domain, database, scope) -> {
            readyForActuation(domain, database, scope);
            var stopped = intent(PipelineState.STOPPED, false, false, null);
            var commands = new ArrayList<String>();
            var changed = new java.util.concurrent.atomic.AtomicBoolean();
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
            try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var listener = new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommandName()); }
                    @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                        if (event.getCommandName().equals("find") && armed.compareAndSet(false, true)) {
                            admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                                    .append("mode", new Document("times", 1)).append("data", new Document("failCommands", List.of("find"))
                                            .append("errorCode", 112).append("errorLabels", List.of("TransientTransactionError"))));
                        }
                    }
                    @Override public void commandFailed(CommandFailedEvent event) {
                        if (event.getCommandName().equals("find") && changed.compareAndSet(false, true)) {
                            assertThat(domain.intents().compareAndSet(scope, 1, stopped)).isTrue();
                        }
                    }
                };
                try (var client = MongoClients.create(MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
                    client.getDatabase("admin").runCommand(new Document("ping", 1));
                    var db = client.getDatabase(database.getName());
                    // Exercise the transaction fallback with a real session and server failpoint.
                    var fallback = org.mockito.Mockito.mock(com.mongodb.client.MongoClient.class, org.mockito.AdditionalAnswers.delegatesTo(client));
                    org.mockito.Mockito.doReturn(org.mockito.Mockito.mock(com.mongodb.connection.ClusterDescription.class))
                            .when(fallback).getClusterDescription();
                    var store = new MongoPipelineIntentStore(fallback, db, scope.clusterId(), new MongoStorageBindingStore(fallback, db, ciphers));
                    commands.clear();
                    try {
                        assertThat(store.loadForActuation(scope.pipelineId())).contains(new PipelineIntentSnapshot(scope, 2, stopped));
                        assertThat(changed).isTrue();
                        assertThat(commands).containsSubsequence("find", "abortTransaction", "find", "commitTransaction");
                        assertThat(java.util.Collections.frequency(commands, "commitTransaction")).isOne();
                    } finally {
                        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                    }
                }
            }
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
            mappings.updateOne(filter, new Document("$set", new Document("stateGeneration", 2L)));
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
                store.failStart(store.pendingStart("flow").orElseThrow(), IoError.DOCUMENT_UNREADABLE, params);
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
    void aRetainedStartChecksTheBindingOnceInsideTheIntentWrite() {
        withConfiguration((domain, database, scope) -> {
            assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.STOPPED, false, false, null))).isTrue();
            String material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
            var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
            try (var reads = new MongoCommandWitness(MONGO.getReplicaSetUrl(), database.getName(), "find",
                    SystemCollections.STORAGE_BINDING.collectionName())) {
                var observed = new MongoPipelineIntentStore(reads.client, reads.database, "cluster-a",
                        new MongoStorageBindingStore(reads.client, reads.database, ciphers));
                assertThat(observed.compareAndSet(scope, 1, intent(PipelineState.RUNNING, false, false, null))).isTrue();
                assertThat(reads.confirmedCommands()).as("one authoritative binding snapshot for the intent CAS").isOne();
            }
        });
    }

    @Test
    void retainedStartReadsAndCommitsInFiveCommandsAndRefusesTheRacingRevision() {
        withConfiguration((domain, database, scope) -> {
            var node = new io.tapstate.spi.store.WorkloadClaimFence(
                    new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(),
                            io.tapstate.spi.store.WorkloadClaimType.NODE_SESSION, "node-a"),
                    new io.tapstate.spi.store.WorkloadOwner("node-a", "boot-a"), 1, 0, 1);
            var preparation = new io.tapstate.spi.store.PipelinePreparation(scope.pipelineId(), scope.stateGeneration(),
                    io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE, scope, UUID.randomUUID().toString(), node);
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", scope.pipelineId()),
                    new Document("$set", new Document("cachePreparation", MongoPipelinePreparationFacts.encode(preparation))));
            var old = intent(PipelineState.STOPPED, false, false, null);
            assertThat(domain.intents().compareAndSet(scope, 0, old)).isTrue();
            var next = intent(PipelineState.RUNNING, false, true, 3L);
            var inputs = io.tapstate.spi.store.PipelinePreparationInputs.of(scope,
                    (io.tapstate.core.model.PipelineResource) pipeline(), Map.of(), Map.of());
            var commands = new ArrayList<String>();
            var witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommandName()); }
            };
            var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder()
                    .decode(SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material"))));
            try (var client = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(witness).build())) {
                var db = client.getDatabase(database.getName());
                var bindings = new MongoStorageBindingStore(client, db, ciphers);
                var store = new MongoPipelineIntentStore(client, db, scope.clusterId(), bindings);
                var artifacts = new MongoConfigurationArtifactStore(client, db, ciphers);
                assertThat(store.commitRetainedStart(scope, 1, next, inputs, artifacts))
                        .isEqualTo(MongoConfigurationDomain.RetainedStart.SAVED);
                assertThat(commands).containsExactly("aggregate", "update", "update", "update", "commitTransaction");
                long written = revision(database);
                assertThatThrownBy(() -> store.commitRetainedStart(scope, 1, next, inputs, artifacts))
                        .satisfies(MongoPipelineIntentStoreIT::fenced);
                assertThat(revision(database)).isEqualTo(written);
                assertThat(domain.intents().load(scope.pipelineId())).contains(new PipelineIntentSnapshot(scope, 2, next));
            }
        });
    }

    @Test
    void startSnapshotCannotReadAValidPipelineFromTheWrongCollection() {
        withConfiguration((domain, database, scope) -> {
            var row = SystemCollections.PIPELINES.on(database).findOneAndDelete(new Document("_id", scope.pipelineId()));
            SystemCollections.SOURCES.on(database).insertOne(row);
            assertThatThrownBy(() -> domain.startSnapshot(scope.pipelineId())).satisfies(MongoPipelineIntentStoreIT::unreadable);
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
    private static DesiredState readyForActuation(MongoConfigurationDomain domain, MongoDatabase database, PipelineIntentScope scope) {
        var node = new io.tapstate.spi.store.WorkloadClaimFence(
                new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), io.tapstate.spi.store.WorkloadClaimType.NODE_SESSION, "node-a"),
                new io.tapstate.spi.store.WorkloadOwner("node-a", "boot-a"), 1, 0, 1);
        var preparation = new io.tapstate.spi.store.PipelinePreparation(scope.pipelineId(), scope.stateGeneration(),
                io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE, scope, UUID.randomUUID().toString(), node);
        SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", scope.pipelineId()),
                new Document("$set", new Document("cachePreparation", MongoPipelinePreparationFacts.encode(preparation))));
        var desired = intent(PipelineState.RUNNING, false, false, null);
        assertThat(domain.intents().compareAndSet(scope, 0, desired)).isTrue();
        return desired;
    }

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
