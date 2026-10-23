package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.SinkCapability;
import io.tapstate.core.model.WriteMode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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

/** Scoped small metadata stores over explicit business facts, separate from production root assembly. */
@RequiresDocker
class MongoCacheMetadataStoresIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final CacheAuthScope AUTH = new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1);
    private static final Runnable ACTIVE = () -> {};
    private static final byte[] SPEC = " {\n\"name\":\"example\"}\n".getBytes(StandardCharsets.UTF_8);
    private static final String HASH = ContentHash.of(SPEC);
    private static ConnectorCatalogEntry entry(String id) {
        return CatalogEntryReader.read("""
                {"id":"%s","name":"Example","displayName":"Example","icon":null,"group":"database",
                 "modes":["snapshot"],"discovery":"catalog","sink":{"capable":false,"writeSemantics":[]},"pushOut":false,"config":[],
                 "provenance":{"specPath":"spec.json","specContentHash":"%s","pdkApiVersion":"1.0.0","requiredLevel":null,"modeSource":{"snapshot":"derived"}}}
                """.formatted(id, HASH));
    }
    private static ConnectionTestResult result(long at) { return new ConnectionTestResult("source-a", "example", ConnectionTestResult.Outcome.PASSED,
            List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.PASSED, null, null, null, null)), at); }

    @Test void smallResourcesRemainSeparateAndPreserveCanonicalBodiesAndByteExactSpecs() {
        withFixture(f -> {
            var catalog = f.domain.connectorCatalog(SCOPE, ACTIVE); var specs = f.domain.connectorSpecs(SCOPE, ACTIVE);
            var results = f.domain.connectionTestResults(SCOPE, ACTIVE);
            specs.put(HASH, SPEC); catalog.upsert(entry("example")); results.save(result(1234L));
            var before = f.snapshot();
            assertThat(specs.get(HASH)).contains(SPEC); assertThat(specs.has(HASH)).isTrue();
            assertThat(catalog.get("example")).contains(entry("example")); assertThat(catalog.list()).containsExactly(entry("example"));
            assertThat(results.find("source-a")).contains(result(1234L));
            assertThat(f.snapshot()).isEqualTo(before); assertThat(f.admissions()).isEqualTo(3);
            assertThat(SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.CACHE_CONNECTOR_SPECS.on(f.database).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.CACHE_CONNECTION_TEST_RESULTS.on(f.database).countDocuments()).isEqualTo(1);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("connections", "users", "cluster_keys", "configuration_version");
        });
    }

    private static ConnectorCatalogEntry declaredDb2Sink() {
        var base = entry("db2");
        return new ConnectorCatalogEntry(base.id(), base.name(), base.displayName(), base.icon(), base.group(), base.modes(), base.discovery(),
                new SinkCapability(true, List.of(WriteMode.UPSERT, WriteMode.APPEND)), base.pushOut(), base.config(), base.provenance());
    }

    @Test void aFreshDeclaredDb2SinkIsStoredAndReadUnderTheRunningSupportPolicy() {
        withFixture(f -> {
            var catalog = f.domain.connectorCatalog(SCOPE, ACTIVE);
            catalog.upsert(declaredDb2Sink());
            assertThat(catalog.get("db2").orElseThrow().sink().capable()).isFalse();
            assertThat(catalog.list().getFirst().sink().writeSemantics()).isEmpty();
            var stored = SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database).find().first().get("sink", Document.class);
            assertThat(stored).containsEntry("capable", false).containsEntry("writeSemantics", List.of());
        });
    }

    @Test void aStoredCanonicalCapabilityMayBeSafelyNarrowedWithoutBecomingUnreadableOrRewritten() {
        withFixture(f -> {
            Document row = MongoConnectorCatalogStore.toDocument(declaredDb2Sink()).append("cacheScope", new Document("clusterId", SCOPE.clusterId())
                    .append("bindingVersion", SCOPE.bindingVersion()).append("cacheIncarnation", SCOPE.cacheIncarnation()));
            SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database).insertOne(row);
            var before = f.snapshot(); var catalog = f.domain.connectorCatalog(SCOPE, ACTIVE);
            assertThat(catalog.get("db2").orElseThrow().sink().capable()).isFalse();
            assertThat(catalog.list().getFirst().sink().writeSemantics()).isEmpty();
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void inputMutationCannotChangeACommandCapturedBeforeItsOriginalGuardRuns() {
        withFixture(f -> {
            byte[] source = SPEC.clone(); var mutateOnce = new AtomicBoolean();
            var access = new MongoCacheMetadataAccess(f.client, f.database, SCOPE, SystemCollections.CACHE_CONNECTOR_SPECS, () -> {
                if (mutateOnce.compareAndSet(false, true)) source[0] = '!';
            });
            new MongoCacheConnectorSpecStore(access).put(HASH, source);
            assertThat(f.domain.connectorSpecs(SCOPE, ACTIVE).get(HASH)).contains(SPEC);
            assertThat(source).isNotEqualTo(SPEC);
        });
    }

    @Test void invalidInputHashAndCorruptStoredSpecNeverBecomeDifferentContentUnderTheSameHash() {
        withFixture(f -> {
            var specs = f.domain.connectorSpecs(SCOPE, ACTIVE); var before = f.snapshot();
            assertCode(() -> specs.put("wrong", SPEC), IoError.DOCUMENT_INVALID);
            assertThat(f.snapshot()).isEqualTo(before);
            specs.put(HASH, SPEC);
            SystemCollections.CACHE_CONNECTOR_SPECS.on(f.database).updateOne(new Document("_id", HASH),
                    new Document("$set", new Document("spec", new Binary("private-sentinel".getBytes(StandardCharsets.UTF_8)))));
            var damaged = f.snapshot(); assertUnreadableWithoutCause(() -> specs.get(HASH));
            assertThat(f.snapshot()).isEqualTo(damaged);
        });
    }

    @Test void anIndependentClientConcurrentReregistrationPreservesOneExactSpec() {
        withFixture(f -> {
            try (var second = MongoCacheDomain.connect(f.binding, Duration.ofSeconds(5), "metadata-second"); var workers = Executors.newFixedThreadPool(2)) {
                var one = f.domain.connectorSpecs(SCOPE, ACTIVE); var two = second.connectorSpecs(SCOPE, ACTIVE); var start = new CountDownLatch(1);
                var a = workers.submit(() -> { await(start); one.put(HASH, SPEC); });
                var b = workers.submit(() -> { await(start); two.put(HASH, SPEC); });
                start.countDown(); a.get(15, TimeUnit.SECONDS); b.get(15, TimeUnit.SECONDS);
                assertThat(one.get(HASH)).contains(SPEC); assertThat(f.admissions()).isEqualTo(2);
                assertThat(SystemCollections.CACHE_CONNECTOR_SPECS.on(f.database).countDocuments()).isEqualTo(1);
            }
        });
    }

    @Test void aFacetDoesNotDependOnUnrelatedMetadataCollectionsAndNeverRepairsItsOwnMissingOne() {
        withFixture(f -> {
            var catalog = f.domain.connectorCatalog(SCOPE, ACTIVE); catalog.upsert(entry("example"));
            SystemCollections.CACHE_SOURCE_SCHEMAS.on(f.database).drop(); SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(f.database).drop();
            assertThat(catalog.get("example")).contains(entry("example"));
            assertThat(f.domain.connectorCatalog(SCOPE, ACTIVE).list()).containsExactly(entry("example"));
            SystemCollections.CACHE_CONNECTOR_SPECS.on(f.database).drop(); var before = f.snapshot();
            assertCode(() -> f.domain.connectorSpecs(SCOPE, ACTIVE), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("connector_specs");
        });
    }

    @Test void otherIncarnationsAreAbsentUntilAnExplicitCurrentScopeWriteRebuildsTheResource() {
        withFixture(f -> {
            var catalog = f.domain.connectorCatalog(SCOPE, ACTIVE); catalog.upsert(entry("example"));
            SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database).updateOne(new Document("_id", "example"),
                    new Document("$set", new Document("cacheScope.cacheIncarnation", "previous-cache")));
            var before = f.snapshot(); assertThat(catalog.get("example")).isEmpty(); assertThat(catalog.list()).isEmpty();
            assertThat(f.snapshot()).isEqualTo(before);
            catalog.upsert(entry("example")); assertThat(catalog.get("example")).contains(entry("example"));
        });
    }

    @Test void malformedScopeAndPayloadAreDiagnosedWithoutCoercionOrRawDecoderCauses() {
        withFixture(f -> {
            var catalog = f.domain.connectorCatalog(SCOPE, ACTIVE); var results = f.domain.connectionTestResults(SCOPE, ACTIVE);
            catalog.upsert(entry("example")); results.save(result(1234L));
            var rows = SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database); Document saved = rows.find().first();
            for (var mutation : List.of(new Document("cacheScope.bindingVersion", 1), new Document("cacheScope.extra", "private-sentinel"),
                    new Document("sink", "private-sentinel"))) {
                rows.updateOne(new Document(), new Document("$set", mutation));
                var before = f.snapshot(); assertUnreadableWithoutCause(() -> catalog.get("example")); assertUnreadableWithoutCause(catalog::list);
                assertThat(f.snapshot()).isEqualTo(before); rows.replaceOne(new Document(), saved);
            }
            SystemCollections.CACHE_CONNECTION_TEST_RESULTS.on(f.database).updateOne(new Document(), new Document("$set", new Document("testedAt", 1234.5d)));
            var before = f.snapshot(); assertUnreadableWithoutCause(() -> results.find("source-a"));
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void authenticationOnlyResetLeavesTheOriginalBusinessMetadataViewUsable() {
        withFixture(f -> {
            var specs = f.domain.connectorSpecs(SCOPE, ACTIVE); specs.put(HASH, SPEC);
            f.domain.advanceAuthGeneration(AUTH, 2, "auth-reset", ACTIVE);
            var before = f.snapshot(); assertThat(specs.get(HASH)).contains(SPEC); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void retiredAndClosedDomainsRefuseCapturedReadAndWriteViews() {
        withFixture(f -> {
            var catalog = f.domain.connectorCatalog(SCOPE, ACTIVE); var specs = f.domain.connectorSpecs(SCOPE, ACTIVE);
            var results = f.domain.connectionTestResults(SCOPE, ACTIVE);
            f.domain.retireBusiness(SCOPE, "retire-metadata", ACTIVE); var before = f.snapshot();
            assertCode(() -> catalog.upsert(entry("example")), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> catalog.get("example"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> specs.put(HASH, SPEC), IoError.CACHE_BUSINESS_FENCED); assertCode(() -> specs.has(HASH), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> results.save(result(1)), IoError.CACHE_BUSINESS_FENCED); assertCode(() -> results.find("source-a"), IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            f.domain.close(); assertCode(catalog::list, IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void retirementCommittedBeforeTheOriginalAdmissionWritePreventsLateMetadataPublication() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (updates(e, "cache_identity") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var store = new MongoCacheConnectorCatalogStore(new MongoCacheMetadataAccess(observed, observed.getDatabase(f.database.getName()),
                        SCOPE, SystemCollections.CACHE_CONNECTOR_CATALOG, ACTIVE));
                var result = worker.submit(() -> store.upsert(entry("example")));
                try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.domain.retireBusiness(SCOPE, "retire-metadata", ACTIVE); }
                finally { proceed.countDown(); }
                assertFutureCode(() -> result.get(15, TimeUnit.SECONDS), IoError.CACHE_BUSINESS_FENCED);
                assertThat(SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database).countDocuments()).isZero();
                assertThat(f.admissions()).isZero();
            }
        });
    }

    @Test void anAdmittedWriteCommitsBeforeTheRetirementThatThenFencesItsView() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (updates(e, "connector_catalog") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = client(listener); var workers = Executors.newFixedThreadPool(2)) {
                var store = new MongoCacheConnectorCatalogStore(new MongoCacheMetadataAccess(observed, observed.getDatabase(f.database.getName()),
                        SCOPE, SystemCollections.CACHE_CONNECTOR_CATALOG, ACTIVE));
                var write = workers.submit(() -> store.upsert(entry("example")));
                assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                var retired = workers.submit(() -> f.domain.retireBusiness(SCOPE, "retire-metadata", ACTIVE));
                proceed.countDown(); write.get(15, TimeUnit.SECONDS); retired.get(15, TimeUnit.SECONDS);
                assertThat(SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database).countDocuments()).isEqualTo(1);
                assertCode(store::list, IoError.CACHE_BUSINESS_FENCED);
            } finally { proceed.countDown(); }
        });
    }

    @Test void originalGuardLossAfterMetadataWriteRollsBackTheRootAndDataTogether() {
        withFixture(f -> {
            var active = new AtomicBoolean(true);
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) { if (updates(e, "connector_catalog")) active.set(false); }
            };
            var before = f.snapshot();
            try (var observed = client(listener)) {
                var store = new MongoCacheConnectorCatalogStore(new MongoCacheMetadataAccess(observed, observed.getDatabase(f.database.getName()),
                        SCOPE, SystemCollections.CACHE_CONNECTOR_CATALOG, () -> { if (!active.get()) throw MongoCacheBusinessIdentity.businessFenced(); }));
                assertCode(() -> store.upsert(entry("example")), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aTransientRetryRechecksTheOriginalGuardBeforeAnyNewAdmissionOrDataWrite() {
        withFixture(f -> {
            var active = new AtomicBoolean(true); var failed = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandFailed(CommandFailedEvent e) { if (e.getCommandName().equals("update")) { failed.incrementAndGet(); active.set(false); } }
            };
            var before = f.snapshot();
            try (var observed = client(listener)) {
                var store = new MongoCacheConnectorCatalogStore(new MongoCacheMetadataAccess(observed, observed.getDatabase(f.database.getName()),
                        SCOPE, SystemCollections.CACHE_CONNECTOR_CATALOG, () -> { if (!active.get()) throw MongoCacheBusinessIdentity.businessFenced(); }));
                f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("update")).append("errorCode", 112).append("errorLabels", List.of("TransientTransactionError"))));
                try { assertCode(() -> store.upsert(entry("example")), IoError.CACHE_BUSINESS_FENCED); }
                finally { f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
                assertThat(failed).hasValue(1);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void exhaustedAdmissionHistoryCannotOverflowOrPublishMetadata() {
        withFixture(f -> {
            SystemCollections.CACHE_IDENTITY.on(f.database).updateOne(new Document(), new Document("$set", new Document("claimAdmissionFence", Long.MAX_VALUE - 1)));
            var before = f.snapshot();
            assertCode(() -> f.domain.connectorCatalog(SCOPE, ACTIVE).upsert(entry("example")), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> f.domain.connectorSpecs(SCOPE, ACTIVE).put(HASH, SPEC), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> f.domain.connectionTestResults(SCOPE, ACTIVE).save(result(1)), IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            f.domain.retireBusiness(SCOPE, "retire-exhausted", ACTIVE);
        });
    }

    private static MongoClient client(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static boolean updates(CommandStartedEvent event, String table) { return event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals(table); }
    private static void await(CountDownLatch latch) { try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); } }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(code)); }
    private static void assertFutureCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) { assertThatThrownBy(action)
            .hasCauseInstanceOf(TapstateException.class).satisfies(failure -> assertThat(((TapstateException) failure.getCause()).code()).isEqualTo(code)); }
    private static void assertUnreadableWithoutCause(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, failure -> { assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(failure.getCause()).isNull(); assertThat(failure.toString()).doesNotContain("private-sentinel"); }); }
    private static void withFixture(Scenario scenario) {
        String name = "metadata_stores_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "metadata-test")) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(AUTH, "auth-a", ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareInTransaction(session, ACTIVE);
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY")
                        .append("epoch", 1L).append("operationId", "business-a"));
                scenario.run(new Fixture(client, database, domain, binding));
            } finally { database.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding) {
        long admissions() { return SystemCollections.CACHE_IDENTITY.on(database).find().first().getLong("claimAdmissionFence"); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>(); var names = database.listCollectionNames().into(new ArrayList<>());
            for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_CONNECTOR_CATALOG,
                    SystemCollections.CACHE_CONNECTOR_SPECS, SystemCollections.CACHE_CONNECTION_TEST_RESULTS))
                if (names.contains(table.collectionName())) out.put(table.collectionName(), table.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
