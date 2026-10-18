package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.FirstPipelineCreation;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelinePreparationReceipt;
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

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** CONFIG issuance and refusal tests; real completion is covered with independent CONFIG/CACHE clients. */
@RequiresDocker
class MongoPipelineStatePreparationsIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadClaimFence NODE = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a"), OWNER, 1, 0, 0);

    @Test void authoringCreatesOnlyUnassignedFactsAndReadsNeverMintOrWrite() {
        withConfiguration(f -> {
            long version = f.version();
            var status = f.domain.preparations().read("flow").orElseThrow();
            assertThat(status).isEqualTo(new PipelinePreparation("flow", 1, PipelinePreparation.Phase.UNASSIGNED, null, null, null));
            assertThat(f.mapping().get("cachePreparation")).isEqualTo(new Document("phase", "UNASSIGNED"));
            assertThat(f.domain.preparations().read("unknown")).isEmpty();
            assertThat(f.domain.preparations().read("flow")).contains(status);
            assertThat(f.version()).isEqualTo(version);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>()))
                    .doesNotContain("workload_claims", "pipeline_state", "cache_identity");
        });
    }

    @Test void independentClientsIncludingTheSameBootIssueExactlyOneGrant() {
        withConfiguration(f -> {
            try (var other = MongoConfigurationDomain.open(f.settings(), "cluster-a"); var executor = Executors.newFixedThreadPool(2)) {
                CountDownLatch ready = new CountDownLatch(2); CountDownLatch start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); start.await(); return f.domain.preparations().issueFirstCreate(f.scope, NODE); });
                var b = executor.submit(() -> { ready.countDown(); start.await(); return other.preparations().issueFirstCreate(f.scope, NODE); });
                assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue(); start.countDown();
                var results = List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
                assertThat(results.stream().filter(FirstPipelineCreationResult.Issued.class::isInstance)).hasSize(1);
                assertThat(results.stream().filter(FirstPipelineCreationResult.AlreadyIssued.class::isInstance)).hasSize(1);
                assertThat(results.stream().map(MongoPipelineStatePreparationsIT::facts).map(PipelinePreparation::preparationId).distinct()).hasSize(1);
                assertThat(f.domain.preparations().read("flow")).contains(facts(results.getFirst()));
                assertThat(f.database.getCollection("operator_generations").countDocuments()).isOne();
            }
        });
    }

    @Test void aLostIssueResponseOrReopenCanOnlyReturnAlreadyIssuedStatus() {
        withConfiguration(f -> {
            var first = issued(f.domain.preparations().issueFirstCreate(f.scope, NODE));
            Document saved = f.mapping();
            try (var reopened = MongoConfigurationDomain.open(f.settings(), "cluster-a")) {
                var repeated = reopened.preparations().issueFirstCreate(f.scope, NODE);
                assertThat(repeated).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
                assertThat(facts(repeated)).isEqualTo(first.preparation());
                assertThat(reopened.preparations().read("flow")).contains(first.preparation());
            }
            assertThat(f.mapping()).isEqualTo(saved);
        });
    }

    @Test void aGrantIsConsumedOnceBeforeCallbacksAndCannotBeSerialized() {
        withConfiguration(f -> {
            AtomicReference<FirstPipelineCreation> reference = new AtomicReference<>();
            AtomicBoolean reenter = new AtomicBoolean(true);
            var guarded = f.domain.preparations().guardedBy(() -> {
                if (reference.get() != null && reenter.compareAndSet(true, false)) {
                    assertThatThrownBy(reference.get()::consume).satisfies(MongoPipelineStatePreparationsIT::fenced);
                }
            });
            var grant = issued(guarded.issueFirstCreate(f.scope, NODE)); reference.set(grant);
            assertThatThrownBy(() -> {
                try (var out = new ObjectOutputStream(new ByteArrayOutputStream())) { out.writeObject(grant); }
            }).isInstanceOf(NotSerializableException.class);
            grant.consume();
            assertThatThrownBy(grant::consume).satisfies(MongoPipelineStatePreparationsIT::fenced);
        });
    }

    @Test void twoThreadsCannotUseTheSameGrantTwice() {
        withConfiguration(f -> {
            var grant = issued(f.domain.preparations().issueFirstCreate(f.scope, NODE));
            try (var executor = Executors.newFixedThreadPool(2)) {
                CountDownLatch start = new CountDownLatch(1);
                var one = executor.submit(() -> { start.await(); return consume(grant); });
                var two = executor.submit(() -> { start.await(); return consume(grant); });
                start.countDown();
                assertThat(List.of(one.get(5, TimeUnit.SECONDS), two.get(5, TimeUnit.SECONDS)))
                        .containsExactlyInAnyOrder(true, false);
            }
        });
    }

    @Test void aFailedInvocationGuardDoesNotRestoreTheCreationAuthority() {
        withConfiguration(f -> {
            AtomicBoolean active = new AtomicBoolean(true);
            var grant = issued(f.domain.preparations().guardedBy(() -> { if (!active.get()) throw failure(); }).issueFirstCreate(f.scope, NODE));
            active.set(false);
            assertThatThrownBy(grant::consume).satisfies(MongoPipelineStatePreparationsIT::fenced);
            assertThatThrownBy(grant::requireActive).satisfies(MongoPipelineStatePreparationsIT::fenced);
            active.set(true);
            assertThatThrownBy(grant::consume).satisfies(MongoPipelineStatePreparationsIT::fenced);
            assertThat(f.domain.preparations().issueFirstCreate(f.scope, NODE)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
        });
    }

    @Test void aStatusDtoCannotManufactureACompletionReceipt() {
        withConfiguration(f -> {
            assertThat(f.domain.intents().load("flow")).isPresent();
            long version = f.version();
            var grant = issued(f.domain.preparations().issueFirstCreate(f.scope, NODE));
            Document before = f.mapping();
            assertThatThrownBy(() -> f.domain.preparations().complete(receipt(grant.preparation())))
                    .satisfies(MongoPipelineStatePreparationsIT::fenced);
            assertThat(f.domain.intents().loadForActuation("flow")).isEmpty();
            assertThat(f.mapping()).isEqualTo(before);
            assertThat(f.version()).isEqualTo(version);
        });
    }

    @Test void capturedIntentViewsRefuseAfterTheirOriginalDomainCloses() {
        withConfiguration(f -> {
            var intents = f.domain.intents();
            f.domain.close();
            assertThatThrownBy(() -> intents.loadForActuation("flow")).satisfies(MongoPipelineStatePreparationsIT::fenced);
        });
    }

    @Test void closeBetweenTheEntryGuardAndSessionCreationIsStillCoded() {
        withConfiguration(f -> {
            var preparations = f.domain.preparations().guardedBy(f.domain::close);
            assertThatThrownBy(() -> preparations.read("flow")).satisfies(MongoPipelineStatePreparationsIT::fenced);
        });
    }

    @Test void aReceiptCannotCompleteAnotherOperationOrScope() {
        withConfiguration(f -> {
            var grant = issued(f.domain.preparations().issueFirstCreate(f.scope, NODE));
            Document saved = f.mapping();
            assertThatThrownBy(() -> f.domain.preparations().complete(new Receipt(f.scope, UUID.randomUUID().toString())))
                    .satisfies(MongoPipelineStatePreparationsIT::fenced);
            var wrong = new PipelineIntentScope("cluster-a", 1, UUID.randomUUID().toString(), "flow", 1);
            assertThatThrownBy(() -> f.domain.preparations().complete(new Receipt(wrong, grant.preparation().preparationId())))
                    .satisfies(MongoPipelineStatePreparationsIT::fenced);
            assertThat(f.mapping()).isEqualTo(saved);
            assertThat(f.domain.intents().loadForActuation("flow")).isEmpty();
        });
    }

    @Test void missingPreparationFactsAreNeverDefaultedToAnUnassignedGrant() {
        withConfiguration(f -> {
            f.database.getCollection("operator_generations").updateOne(new Document("_id", "flow"),
                    new Document("$unset", new Document("cachePreparation", "")));
            Document saved = f.mapping(); long version = f.version();
            assertThatThrownBy(() -> f.domain.preparations().read("flow")).satisfies(MongoPipelineStatePreparationsIT::unreadable);
            assertThatThrownBy(() -> f.domain.preparations().issueFirstCreate(f.scope, NODE)).satisfies(MongoPipelineStatePreparationsIT::unreadable);
            assertThatThrownBy(() -> f.domain.intents().loadForActuation("flow")).satisfies(MongoPipelineStatePreparationsIT::unreadable);
            assertThat(f.mapping()).isEqualTo(saved); assertThat(f.version()).isEqualTo(version);
        });
    }

    @Test void malformedPreparationScopeNodeAndUnknownFieldsAreNotRepairedOrEchoed() {
        Map<String, Object> damages = Map.of(
                "cachePreparation.phase", "secret-sentinel",
                "cachePreparation.preparationId", "secret-sentinel",
                "cachePreparation.scope.bindingVersion", 1,
                "cachePreparation.issuerNode.claimGeneration", 1,
                "cachePreparation.issuerNode.bootId", 12L,
                "cachePreparation.issuerNode.leaseUntil", "secret-sentinel");
        for (var damage : damages.entrySet()) {
            withConfiguration(f -> {
                issued(f.domain.preparations().issueFirstCreate(f.scope, NODE));
                f.database.getCollection("operator_generations").updateOne(new Document("_id", "flow"),
                        new Document("$set", new Document(damage.getKey(), damage.getValue())));
                Document saved = f.mapping();
                assertThatThrownBy(() -> f.domain.preparations().read("flow")).satisfies(MongoPipelineStatePreparationsIT::unreadable);
                assertThatThrownBy(() -> f.domain.preparations().issueFirstCreate(f.scope, NODE)).satisfies(MongoPipelineStatePreparationsIT::unreadable);
                assertThatThrownBy(() -> f.domain.intents().loadForActuation("flow")).satisfies(MongoPipelineStatePreparationsIT::unreadable);
                assertThat(f.mapping()).isEqualTo(saved);
            });
        }
    }

    @Test void foreignInputsCannotIssueAndARebuildingMappingIsNotActuatable() {
        withConfiguration(f -> {
            var wrong = new PipelineIntentScope("cluster-a", 1, UUID.randomUUID().toString(), "flow", 1);
            assertThatThrownBy(() -> f.domain.preparations().issueFirstCreate(wrong, NODE)).satisfies(MongoPipelineStatePreparationsIT::fenced);
            var capture = new WorkloadClaimFence(new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "source"), OWNER, 1, 0, 0);
            assertThatThrownBy(() -> f.domain.preparations().issueFirstCreate(f.scope, capture)).satisfies(MongoPipelineStatePreparationsIT::fenced);
            var grant = issued(f.domain.preparations().issueFirstCreate(f.scope, NODE));
            f.database.getCollection("operator_generations").updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("phase", "REBUILDING").append("operationId", "rebuild")));
            assertThat(f.domain.preparations().read("flow")).contains(grant.preparation());
            assertThat(f.domain.intents().loadForActuation("flow")).isEmpty();
            assertThatThrownBy(() -> f.domain.preparations().complete(receipt(grant.preparation()))).satisfies(MongoPipelineStatePreparationsIT::fenced);
        });
    }

    @Test void closingTheOriginalDomainRevokesItsUnusedGrantEvenAfterReopen() {
        withConfiguration(f -> {
            var store = f.domain.preparations();
            var grant = issued(store.issueFirstCreate(f.scope, NODE));
            f.domain.close();
            assertThatThrownBy(grant::consume).satisfies(MongoPipelineStatePreparationsIT::fenced);
            assertThatThrownBy(grant::requireActive).satisfies(MongoPipelineStatePreparationsIT::fenced);
            assertThatThrownBy(() -> store.read("flow")).satisfies(MongoPipelineStatePreparationsIT::fenced);
            try (var reopened = MongoConfigurationDomain.open(f.settings(), "cluster-a")) {
                assertThat(reopened.preparations().issueFirstCreate(f.scope, NODE)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
                assertThatThrownBy(grant::consume).satisfies(MongoPipelineStatePreparationsIT::fenced);
            }
        });
    }

    @Test void losingAuthorityAfterTheIssueCommitDoesNotLeakOrRemintAGrant() {
        withConfiguration(f -> {
            AtomicInteger checks = new AtomicInteger(); AtomicBoolean active = new AtomicBoolean(true);
            var guarded = f.domain.preparations().guardedBy(() -> {
                if (checks.incrementAndGet() == 4) active.set(false);
                if (!active.get()) throw failure();
            });
            assertThatThrownBy(() -> guarded.issueFirstCreate(f.scope, NODE)).satisfies(MongoPipelineStatePreparationsIT::fenced);
            assertThat(checks).hasValue(4);
            assertThat(f.domain.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.ISSUED);
            assertThat(f.domain.preparations().issueFirstCreate(f.scope, NODE)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
        });
    }

    @Test void transactionRetryKeepsOneNonceAndRechecksOriginalAuthority() {
        for (boolean revoke : List.of(false, true)) {
            withConfiguration(f -> {
                AtomicInteger failed = new AtomicInteger(); List<String> nonces = new java.util.concurrent.CopyOnWriteArrayList<>();
                var witness = new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("update")) {
                            var update = event.getCommand().getArray("updates").getFirst().asDocument().getDocument("u");
                            if (update.containsKey("$set") && update.getDocument("$set").containsKey("cachePreparation")) {
                                nonces.add(update.getDocument("$set").getDocument("cachePreparation").getString("preparationId").getValue());
                            }
                        }
                    }
                    @Override public void commandFailed(CommandFailedEvent event) { if (event.getCommandName().equals("update")) failed.incrementAndGet(); }
                };
                try (var admin = MongoClients.create(MONGO.getReplicaSetUrl());
                        var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                                .addCommandListener(witness).build())) {
                    String material = f.database.getCollection("cluster_keys").find().first().getString("material");
                    var cipher = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                    var database = observed.getDatabase(f.database.getName());
                    var bindings = new MongoStorageBindingStore(observed, database, cipher);
                    var store = new MongoPipelineStatePreparations(observed, database, "cluster-a", bindings,
                            () -> { if (revoke && failed.get() > 0) throw failure(); });
                    admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                            .append("data", new Document("failCommands", List.of("update")).append("errorCode", 112)
                                    .append("errorLabels", List.of("TransientTransactionError"))));
                    try {
                        if (revoke) {
                            assertThatThrownBy(() -> store.issueFirstCreate(f.scope, NODE)).satisfies(MongoPipelineStatePreparationsIT::fenced);
                            assertThat(f.domain.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                            assertThat(nonces).hasSize(1);
                        } else {
                            var grant = issued(store.issueFirstCreate(f.scope, NODE));
                            assertThat(nonces).hasSize(2).containsOnly(grant.preparation().preparationId());
                        }
                        assertThat(failed).hasValue(1);
                    } finally {
                        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                    }
                }
            });
        }
    }

    private static boolean consume(FirstPipelineCreation creation) {
        try { creation.consume(); return true; } catch (TapstateException refused) { assertThat(refused.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED); return false; }
    }
    private static PipelinePreparation facts(FirstPipelineCreationResult result) {
        return result instanceof FirstPipelineCreationResult.Issued issued ? issued.creation().preparation()
                : ((FirstPipelineCreationResult.AlreadyIssued) result).preparation();
    }
    @Test void anAmbiguousIssueCommitRetriesCommitWithoutRepeatingIssuanceOrReminting() {
        withConfiguration(f -> {
            AtomicInteger writes = new AtomicInteger(); AtomicInteger commits = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update")) writes.incrementAndGet();
                    if (event.getCommandName().equals("commitTransaction")) commits.incrementAndGet();
                }
            };
            try (var admin = MongoClients.create(MONGO.getReplicaSetUrl()); var observed = MongoClients.create(
                    MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
                var database = observed.getDatabase(f.database.getName());
                String material = SystemCollections.CLUSTER_KEYS.on(f.database).find().first().getString("material");
                var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                var bindings = new MongoStorageBindingStore(observed, database, ciphers);
                var store = new MongoPipelineStatePreparations(observed, database, "cluster-a", bindings, () -> {});
                admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("commitTransaction"))
                                .append("writeConcernError", new Document("code", 91).append("errmsg", "test commit reply lost"))
                                .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
                FirstPipelineCreation grant;
                try { grant = issued(store.issueFirstCreate(f.scope, NODE)); }
                finally { admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
                assertThat(writes).hasValue(1); assertThat(commits.get()).isGreaterThanOrEqualTo(2);
                assertThat(store.read("flow")).contains(grant.preparation());
                assertThat(store.issueFirstCreate(f.scope, NODE)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
            }
        });
    }

    private static FirstPipelineCreation issued(FirstPipelineCreationResult result) {
        assertThat(result).isInstanceOf(FirstPipelineCreationResult.Issued.class);
        return ((FirstPipelineCreationResult.Issued) result).creation();
    }
    private record Receipt(PipelineIntentScope scope, String preparationId) implements PipelinePreparationReceipt {}
    private static PipelinePreparationReceipt receipt(PipelinePreparation preparation) { return new Receipt(preparation.scope(), preparation.preparationId()); }
    private static TapstateException failure() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
    private static void fenced(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> assertThat(coded.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED)); }
    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
            assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(coded.getCause()).isNull();
            assertThat(coded.toString()).doesNotContain("secret-sentinel");
        });
    }
    private record Fixture(MongoConfigurationDomain domain, MongoDatabase database, PipelineIntentScope scope) {
        MongoConnectionSettings settings() { return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5)); }
        Document mapping() { return database.getCollection("operator_generations").find(new Document("_id", "flow")).first(); }
        long version() { return database.getCollection("configuration_version").find().first().getLong("revision"); }
    }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("config_prep_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1)) {
                var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache.invalid/cache", "cache", null);
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                domain.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cache, master);
                var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");
                domain.artifacts().create(pipeline);
                var business = domain.bindings().cachePreparation().orElseThrow().businessScope();
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), "flow", 1);
                assertThat(domain.intents().compareAndSet(scope, 0, new DesiredState("flow", PipelineState.RUNNING, CanonicalHash.of(pipeline)))).isTrue();
                scenario.run(new Fixture(domain, database, scope));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
