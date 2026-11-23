package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Per-step models use the original prepared Pipeline state and an actual live claim write. */
@RequiresDocker
class MongoCacheDerivedSchemaStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};
    private static Map<String, String> columns(String type) {
        var out = new LinkedHashMap<String, String>(); out.put("order.id", "bigint"); out.put("note", type); return out;
    }
    private static void record(DerivedSchemaStore store, String type) { store.record("orders", "source.orders", columns(type), "statement", "sources", "derivation"); }

    @Test void aStartupBatchPinsTheVersionsItRecords() {
        withFixture(f -> {
            var store = f.writer();
            record(store, "text");
            store.recordAndPinMany("orders", List.of(
                    new DerivedSchemaStore.Change("source.orders", new io.tapstate.spi.store.DerivedSchema(0, columns("bigint"), "statement", "sources", "derivation")),
                    new DerivedSchemaStore.Change("source.customers", new io.tapstate.spi.store.DerivedSchema(0, columns("text"), "statement", "sources", "derivation"))));
            assertThat(store.pinned("orders", "source.orders")).hasValueSatisfying(pin -> assertThat(pin.version()).isEqualTo(1));
            assertThat(store.pinned("orders", "source.customers")).hasValueSatisfying(pin -> assertThat(pin.version()).isZero());
            record(store, "text");
            assertThat(store.pinned("orders", "source.orders")).hasValueSatisfying(pin -> assertThat(pin.schema()).isEqualTo(columns("bigint")));
        });
    }

    private static DerivedSchemaStore.Recording recording(String step, String type) {
        return new DerivedSchemaStore.Recording(step, columns(type), "statement", "sources", "derivation");
    }

    @Test void batchRecordsShareOneOriginalTransaction() {
        withFixture(f -> {
            var transactions = new java.util.HashSet<String>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getDatabaseName().equals(f.database.getName()) && event.getCommandName().equals("update")
                            && event.getCommand().getString("update").getValue().equals(SystemCollections.CACHE_DERIVED_SCHEMAS.collectionName())) {
                        transactions.add(event.getCommand().get("lsid") + ":" + event.getCommand().get("txnNumber"));
                    }
                }
            };
            try (var client = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
                f.writer(client, ACTIVE).recordBatch("orders", List.of(recording("a", "text"), recording("b", "text"), recording("c", "text")));
                assertThat(transactions).as("all model writes use one guarded transaction").hasSize(1);
                assertThat(f.rows().countDocuments()).isEqualTo(3);
            }
        });
    }

    @Test void aMalformedLaterModelRollsBackEarlierBatchWrites() {
        withFixture(f -> {
            f.writer().record("orders", "bad", columns("text"), "statement", "sources", "derivation");
            f.rows().updateOne(new Document("_id", "orders.bad"), new Document("$set", new Document("versions", "broken")));
            var before = f.snapshot();
            assertUnreadable(() -> f.writer().recordBatch("orders", List.of(recording("first", "text"), recording("bad", "text"))));
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void batchWritesStillRefuseACommandLostAfterAnActualWrite() {
        withFixture(f -> {
            var before = f.snapshot();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(), f.database.getName(), "update",
                    SystemCollections.CACHE_DERIVED_SCHEMAS.collectionName())) {
                var writer = f.writer(witness.client, witness::refuseAfterCommand);
                assertCode(() -> writer.recordBatch("orders", List.of(recording("a", "text"), recording("b", "text"))), IoError.CACHE_BUSINESS_FENCED);
                witness.requireCommand();
                assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }

    @Test void aConflictedBatchRetriesAgainstFreshHistoryWithoutDuplicatingVersions() {
        withFixture(f -> {
            f.writer().record("orders", "a", columns("initial"), "statement", "sources", "derivation");
            var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (updates(event, "derived_schemas") && once.compareAndSet(false, true)) {
                        f.rows().updateOne(new Document("_id", "orders.a"),
                                new Document("$set", new Document("versions.0.statement", "concurrent")));
                    }
                }
            };
            try (var observed = client(listener)) {
                f.writer(observed, ACTIVE).recordBatch("orders", List.of(recording("a", "changed"), recording("b", "text")));
            }
            assertThat(once).isTrue();
            var row = f.rows().find(new Document("_id", "orders.a")).first();
            var history = row.getList("versions", Document.class);
            assertThat(history).hasSize(2);
            assertThat(history.getFirst().getString("statement")).isEqualTo("concurrent");
            assertThat(f.writer().latest("orders", "a").orElseThrow().version()).isEqualTo(1);
            assertThat(f.writer().latest("orders", "b").orElseThrow().version()).isZero();
        });
    }

    @Test void batchRefreshRetainsHistoryAndPins() {
        withFixture(f -> {
            var writer = f.writer(); record(writer, "text"); writer.pin("orders", "source.orders", 0);
            writer.recordBatch("orders", List.of(new DerivedSchemaStore.Recording("source.orders", columns("text"), "edited", "new", "v2")));
            assertThat(writer.latest("orders", "source.orders").orElseThrow().version()).isZero();
            assertThat(writer.pinned("orders", "source.orders").orElseThrow().statement()).isEqualTo("edited");
            writer.recordBatch("orders", List.of(recording("source.orders", "varchar")));
            assertThat(writer.latest("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
            assertThat(writer.pinned("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("text"));
        });
    }

    @Test void aWideModelReadUsesOneOriginalMetadataTransactionAndOneExactKeyQuery() {
        withFixture(f -> {
            var steps = java.util.stream.IntStream.range(0,12).mapToObj(i -> "source.table-"+i).collect(java.util.stream.Collectors.toSet());
            for (String step : steps) f.writer().record("orders",step,columns("text"),step,"sources","derivation");
            var requested = new java.util.HashSet<>(steps); requested.add("missing");
            var transactions = new java.util.HashSet<String>(); var queries = new java.util.concurrent.atomic.AtomicInteger();
            var guardCalls = new java.util.concurrent.atomic.AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getDatabaseName().equals(f.database.getName()) && event.getCommandName().equals("find")
                            && event.getCommand().getString("find").getValue().equals(SystemCollections.CACHE_DERIVED_SCHEMAS.collectionName())) {
                        queries.incrementAndGet(); transactions.add(event.getCommand().get("lsid")+":"+event.getCommand().get("txnNumber"));
                    }
                }
            };
            try (var client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build())) {
                var reader = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(client,client.getDatabase(f.database.getName()),SCOPE,null,guardCalls::incrementAndGet));
                var found = reader.latestMany("orders",requested);
                assertThat(found).containsOnlyKeys(steps).allSatisfy((step, model) -> {
                    assertThat(model.statement()).isEqualTo(step); assertThat(model.schema().keySet()).containsExactly("order.id","note");
                });
                assertThat(transactions).hasSize(1); assertThat(queries).hasValue(1); assertThat(guardCalls.get()).isLessThanOrEqualTo(5);
            }
        });
    }

    @Test void batchReadsCaptureTheRequestedKeysBeforeTheOriginalGuardRuns() {
        withFixture(f -> {
            record(f.writer(),"text"); var requested = new java.util.HashSet<>(Set.of("source.orders")); var once = new AtomicBoolean();
            var reader = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(f.client,f.database,SCOPE,null,() -> {
                if (once.compareAndSet(false,true)) { requested.clear(); requested.add("different"); }
            }));
            assertThat(reader.latestMany("orders",requested)).containsOnlyKeys("source.orders");
            assertThat(requested).containsExactly("different");
        });
    }

    @Test void oneMalformedBatchMemberRefusesTheWholeReadAndOtherGenerationsRemainMissing() {
        withFixture(f -> {
            record(f.writer(),"text"); f.writer().record("orders","other",columns("text"),"other","sources","v1");
            var reader = f.domain.pipelineDerivedSchemas(SCOPE); var filter = new Document("_id","orders.other");
            var otherGeneration = new PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),SCOPE.pipelineId(),2);
            f.rows().updateOne(filter,new Document("$set",new Document("scope",MongoPipelinePreparationFacts.scope(otherGeneration))));
            assertThat(reader.latestMany("orders",Set.of("source.orders","other","missing"))).containsOnlyKeys("source.orders");
            var otherPipeline = new PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),"foreign",1);
            f.rows().updateOne(filter,new Document("$set",new Document("scope",MongoPipelinePreparationFacts.scope(otherPipeline))));
            var before = f.snapshot();
            assertUnreadable(() -> reader.latestMany("orders",Set.of("source.orders","other")));
            assertThat(f.snapshot()).isEqualTo(before);
            assertCode(() -> reader.latestMany("foreign",Set.of("source.orders")),IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void aBatchCannotEscapeAnExpiredCommandAfterItsActualQueryOrUseTheWrongPreparationPhase() {
        withFixture(f -> {
            record(f.writer(),"text"); var before = f.snapshot();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database.getName(),"find",SystemCollections.CACHE_DERIVED_SCHEMAS.collectionName())) {
                var reader = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(witness.client,witness.database,SCOPE,null,witness::refuseAfterCommand));
                assertCode(() -> reader.latestMany("orders",Set.of("source.orders","missing")),IoError.CACHE_BUSINESS_FENCED);
                witness.requireCommand();
            }
            var authoring = new MongoCacheDerivedSchemaStore(MongoCachePipelineAccess.unpreparedModels(f.client,f.database,SCOPE,null,ACTIVE,MongoRateHistoryStore.DEFAULT_RETENTION));
            assertCode(() -> authoring.latestMany("orders",Set.of("source.orders")),IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void historyAppendsOnlyOnShapeChangeAndPreservesAPinDuringProvenanceRefresh() {
        withFixture(f -> {
            var writer = f.writer(); var reader = f.domain.pipelineDerivedSchemas(SCOPE);
            assertThat(reader.latest("orders", "source.orders")).isEmpty(); record(writer, "text"); writer.pin("orders", "source.orders", 0);
            writer.record("orders", "source.orders", columns("text"), "edited", "new-source", "v2");
            assertThat(reader.latest("orders", "source.orders").orElseThrow().version()).isZero();
            assertThat(reader.pinned("orders", "source.orders").orElseThrow().derivedBy()).isEqualTo("v2");
            record(writer, "varchar"); assertThat(reader.latest("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
            assertThat(reader.pinned("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("text"));
            assertThat(reader.latest("orders", "source.orders").orElseThrow().schema().keySet()).containsExactly("order.id", "note");
            writer.pin("orders", "source.orders", 1); assertThat(reader.pinned("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
            assertThat(f.rows().countDocuments()).isEqualTo(1); assertThat(f.claimWrites()).isEqualTo(5); assertThat(f.admissions()).isEqualTo(1);
        });
    }

    @Test void publicationBatchKeepsHistoryAndPinsWithOneHistoryReadAndOneWrite() {
        withFixture(f -> {
            var writer = f.writer();
            record(writer, "text");
            writer.pin("orders", "source.orders", 0);
            var catalogCommands = new java.util.concurrent.atomic.AtomicInteger();
            var historyReads = new java.util.concurrent.atomic.AtomicInteger();
            var historyWrites = new java.util.concurrent.atomic.AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (!event.getDatabaseName().equals(f.database.getName())) return;
                    if (event.getCommandName().equals("listCollections") || event.getCommandName().equals("listIndexes"))
                        catalogCommands.incrementAndGet();
                    var table = event.getCommand().get(event.getCommandName());
                    if (table == null || !table.isString()
                            || !table.asString().getValue().equals(SystemCollections.CACHE_DERIVED_SCHEMAS.collectionName())) return;
                    if (event.getCommandName().equals("find")) historyReads.incrementAndGet();
                    if (event.getCommandName().equals("update")) historyWrites.incrementAndGet();
                }
            };
            try (var client = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
                var batch = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(client,
                        client.getDatabase(f.database.getName()), SCOPE, WorkloadClaimFence.from(f.claim), ACTIVE));
                batch.recordMany("orders", List.of(
                        new DerivedSchemaStore.Change("source.orders", new io.tapstate.spi.store.DerivedSchema(
                                0, columns("text"), "edited", "source", "v2")),
                        new DerivedSchemaStore.Change("source.orders", new io.tapstate.spi.store.DerivedSchema(
                                0, columns("varchar"), "changed", "source", "v3")),
                        new DerivedSchemaStore.Change("source.customers", new io.tapstate.spi.store.DerivedSchema(
                                0, columns("text"), "new", "source", "v1"))));
            }
            assertThat(catalogCommands).hasValue(0);
            assertThat(historyReads).hasValue(1);
            assertThat(historyWrites).hasValue(1);
            assertThat(writer.pinned("orders", "source.orders").orElseThrow().derivedBy()).isEqualTo("v2");
            assertThat(writer.latest("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
            assertThat(writer.latest("orders", "source.customers").orElseThrow().version()).isZero();
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"first-checkout", "after-claim-write", "before-commit", "twice", "ordinary-record"})
    void poolClearanceRecoveryIsBoundedToTheOriginalUncommittedPublication(String stage) {
        withFixture(f -> {
            record(f.writer(), "old");
            f.writer().pin("orders", "source.orders", 0);
            var commands = new java.util.concurrent.CopyOnWriteArrayList<String>();
            var resume = new java.util.concurrent.atomic.AtomicReference<Runnable>();
            var pause = new java.util.concurrent.atomic.AtomicReference<Runnable>();
            var cleared = new java.util.concurrent.atomic.AtomicBoolean();
            var failures = new java.util.concurrent.atomic.AtomicInteger();
            var claimWrite = new java.util.concurrent.atomic.AtomicInteger();
            var modelWrite = new java.util.concurrent.atomic.AtomicInteger();
            var secondClear = new java.util.concurrent.atomic.AtomicBoolean();
            long beforeClaimWrites = f.claimWrites();
            var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .applyToConnectionPoolSettings(pool -> pool.addConnectionPoolListener(new com.mongodb.event.ConnectionPoolListener() {
                        @Override public void connectionCheckOutFailed(com.mongodb.event.ConnectionCheckOutFailedEvent event) {
                            failures.incrementAndGet();
                            var restore = resume.get();
                            if (restore != null) restore.run();
                        }
                    })).addCommandListener(new CommandListener() {
                        @Override public void commandStarted(CommandStartedEvent event) {
                            commands.add(event.getCommandName());
                            if (updates(event, "workload_claims")) claimWrite.set(event.getRequestId());
                            if (updates(event, "derived_schemas")) modelWrite.set(event.getRequestId());
                        }
                        @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                            if ((stage.equals("after-claim-write") && event.getRequestId() == claimWrite.get()
                                    || stage.equals("before-commit") && event.getRequestId() == modelWrite.get())
                                    && cleared.compareAndSet(false, true)) pause.get().run();
                            if (stage.equals("twice") && failures.get() == 1 && event.getCommandName().equals("aggregate")
                                    && secondClear.compareAndSet(false, true)) pause.get().run();
                        }
                    }).build();
            try (var client = MongoClients.create(settings)) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                var pool = publicationPool(client);
                pause.set(() -> poolMethod(pool, "invalidate", new Class<?>[]{Throwable.class}, new Object[]{null}));
                resume.set(() -> poolMethod(pool, "ready", new Class<?>[0], new Object[0]));
                var writer = f.writer(client, ACTIVE);
                commands.clear();
                if (stage.equals("first-checkout") || stage.equals("twice") || stage.equals("ordinary-record")) {
                    cleared.set(true); pause.get().run();
                }
                var changes = List.of(new DerivedSchemaStore.Change("source.orders", new io.tapstate.spi.store.DerivedSchema(
                        0, columns("wide"), "first", "source", "v1")),
                        new DerivedSchemaStore.Change("source.orders", new io.tapstate.spi.store.DerivedSchema(
                                0, columns("newest"), "second", "source", "v2")));
                var failure = org.assertj.core.api.Assertions.catchThrowable(() -> {
                    if (stage.equals("ordinary-record")) record(writer, "ordinary");
                    else writer.recordAndPinMany("orders", changes);
                });
                assertThat(failures).hasValue(stage.equals("twice") ? 2 : 1);
                var publicationCommands = List.copyOf(commands);
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                System.out.printf("DERIVED_POOL stage=%s failure=%s wire=%s connectivity=restored%n", stage,
                        failure instanceof TapstateException coded ? coded.getMessage() : failure, publicationCommands);
                if (stage.equals("twice") || stage.equals("ordinary-record")) {
                    assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
                        assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                        assertThat(coded.args()).containsEntry("detail", "MongoConnectionPoolClearedException");
                    });
                    assertThat(f.claimWrites()).isEqualTo(beforeClaimWrites);
                    assertThat(writer.pinned("orders", "source.orders").orElseThrow().version()).isZero();
                    return;
                }
                assertThat(failure).isNull();
                assertThat(f.claimWrites()).isEqualTo(beforeClaimWrites + 1);
                if (stage.equals("before-commit")) assertThat(publicationCommands.stream().filter("aggregate"::equals).count()).isEqualTo(1);
                assertThat(writer.latest("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("newest"));
                assertThat(writer.pinned("orders", "source.orders").orElseThrow().version()).isEqualTo(2);
                var stored = f.rows().find(new Document("_id", "orders.source.orders")).first();
                assertThat(stored.getList("versions", Document.class)).hasSize(3);
            }
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"owner", "retirement", "guard"})
    void publicationCheckoutRecoveryRechecksTheOriginalAuthority(String lost) {
        withFixture(f -> {
            record(f.writer(), "old"); f.writer().pin("orders", "source.orders", 0);
            var live = new AtomicBoolean(true);
            var resume = new java.util.concurrent.atomic.AtomicReference<Runnable>();
            var failures = new java.util.concurrent.atomic.AtomicInteger();
            var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .applyToConnectionPoolSettings(pool -> pool.addConnectionPoolListener(new com.mongodb.event.ConnectionPoolListener() {
                        @Override public void connectionCheckOutFailed(com.mongodb.event.ConnectionCheckOutFailedEvent event) {
                            resume.get().run();
                            if (failures.incrementAndGet() != 1) return;
                            switch (lost) {
                                case "owner" -> {
                                    var claims = f.domain.workloadClaims(BUSINESS);
                                    assertThat(claims.release(f.claim)).isTrue();
                                    assertThat(claims.acquirePipeline(SCOPE, new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).acquired()).isTrue();
                                }
                                case "retirement" -> f.domain.retireBusiness(BUSINESS, "retire-after-checkout", ACTIVE);
                                case "guard" -> live.set(false);
                                default -> throw new AssertionError(lost);
                            }
                        }
                    })).build();
            try (var client = MongoClients.create(settings)) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                var pool = publicationPool(client);
                resume.set(() -> poolMethod(pool, "ready", new Class<?>[0], new Object[0]));
                poolMethod(pool, "invalidate", new Class<?>[]{Throwable.class}, new Object[]{null});
                var writer = f.writer(client, () -> { if (!live.get()) throw MongoCacheBusinessIdentity.businessFenced(); });
                assertCode(() -> writer.recordAndPinMany("orders", publicationChanges()),
                        lost.equals("owner") ? IoError.WORKLOAD_CLAIM_FENCED : IoError.CACHE_BUSINESS_FENCED);
                assertThat(failures).hasValue(1);
                assertThat(f.rows().find().first().getList("versions", Document.class)).hasSize(1);
                System.out.printf("DERIVED_FENCE lost=%s retry=rejected original_history=true%n", lost);
            }
        });
    }

    @Test void publicationCheckoutRecoveryRetainsTheSameClientTimeoutWindow() {
        withFixture(f -> {
            var resume = new java.util.concurrent.atomic.AtomicReference<Runnable>();
            var failures = new java.util.concurrent.atomic.AtomicInteger();
            var delayed = new AtomicBoolean();
            var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .timeout(1000, TimeUnit.MILLISECONDS)
                    .applyToConnectionPoolSettings(pool -> pool.addConnectionPoolListener(new com.mongodb.event.ConnectionPoolListener() {
                        @Override public void connectionCheckOutFailed(com.mongodb.event.ConnectionCheckOutFailedEvent event) {
                            failures.incrementAndGet(); resume.get().run();
                            java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(600));
                        }
                    })).addCommandListener(new CommandListener() {
                        @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                            if (event.getCommandName().equals("aggregate") && delayed.compareAndSet(false, true))
                                java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500));
                        }
                    }).build();
            try (var client = MongoClients.create(settings)) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                var pool = publicationPool(client);
                resume.set(() -> poolMethod(pool, "ready", new Class<?>[0], new Object[0]));
                poolMethod(pool, "invalidate", new Class<?>[]{Throwable.class}, new Object[]{null});
                long began = System.nanoTime();
                assertThatThrownBy(() -> f.writer(client, ACTIVE).recordAndPinMany("orders", publicationChanges()))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                            assertThat(failure.args().get("detail").toString()).startsWith("MongoOperationTimeoutException");
                        });
                long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began);
                assertThat(millis).isLessThan(1800);
                assertThat(failures).hasValue(1);
                assertThat(f.rows().countDocuments()).isZero();
                System.out.printf("DERIVED_DEADLINE original_ms=1000 elapsed_ms=%d rolled_back=true%n", millis);
            }
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void anUnknownPublicationCommitNeverReplaysTheHistoryCallback(boolean exhaustDeadline) {
        withFixture(f -> {
            record(f.writer(), "old"); f.writer().pin("orders", "source.orders", 0);
            String app = "derived-commit-" + UUID.randomUUID();
            var modelWrites = new java.util.concurrent.atomic.AtomicInteger();
            var commits = new java.util.concurrent.atomic.AtomicInteger();
            var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .applicationName(app).addCommandListener(new CommandListener() {
                        @Override public void commandStarted(CommandStartedEvent event) {
                            if (updates(event, "derived_schemas")) modelWrites.incrementAndGet();
                            if (event.getCommandName().equals("commitTransaction")) commits.incrementAndGet();
                        }
                    });
            if (exhaustDeadline) settings.timeout(750, TimeUnit.MILLISECONDS);
            try (var client = MongoClients.create(settings.build())) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                var data = new Document("failCommands", List.of("commitTransaction")).append("appName", app)
                        .append("writeConcernError", new Document("code", 64).append("errmsg", "test commit reply lost"));
                if (exhaustDeadline) data.append("blockConnection", true).append("blockTimeMS", 100);
                f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", exhaustDeadline ? "alwaysOn" : new Document("times", 1)).append("data", data));
                Throwable failure;
                try {
                    failure = org.assertj.core.api.Assertions.catchThrowable(() -> f.writer(client, ACTIVE)
                            .recordAndPinMany("orders", publicationChanges()));
                } finally {
                    f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
                if (exhaustDeadline) assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                        coded -> assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                else assertThat(failure).isNull();
                assertThat(modelWrites).hasValue(1);
                assertThat(commits.get()).isGreaterThan(1);
                var row = f.rows().find(new Document("_id", "orders.source.orders")).first();
                assertThat(row.getList("versions", Document.class)).hasSize(3);
                assertThat(row.getLong("pin")).isEqualTo(2);
                System.out.printf("DERIVED_UNKNOWN_COMMIT exhausted=%s failure_reported=%s callbacks=1 commits=%d versions=3 pin=2%n",
                        exhaustDeadline, failure != null, commits.get());
            }
        });
    }

    private static List<DerivedSchemaStore.Change> publicationChanges() {
        return List.of(new DerivedSchemaStore.Change("source.orders", new io.tapstate.spi.store.DerivedSchema(
                        0, columns("wide"), "first", "source", "v1")),
                new DerivedSchemaStore.Change("source.orders", new io.tapstate.spi.store.DerivedSchema(
                        0, columns("newest"), "second", "source", "v2")));
    }

    /** Pauses the actual pinned driver's pool while the real replica set stays available. */
    private static Object publicationPool(MongoClient client) {
        try {
            var delegate = driverField(client, "delegate");
            var cluster = driverField(delegate, "cluster");
            var server = ((java.util.concurrent.atomic.AtomicReference<?>) driverField(cluster, "server")).get();
            return driverField(server, "connectionPool");
        } catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
    }
    private static Object driverField(Object owner, String field) throws ReflectiveOperationException {
        var value = owner.getClass().getDeclaredField(field);
        value.setAccessible(true);
        return value.get(owner);
    }
    private static void poolMethod(Object pool, String method, Class<?>[] types, Object[] args) {
        try {
            var call = pool.getClass().getMethod(method, types);
            call.setAccessible(true);
            call.invoke(pool, args);
        } catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
    }

    @Test void readOnlyViewsAndUnknownPinsCannotMutateEitherTheClaimStateOrModels() {
        withFixture(f -> {
            var reader = f.domain.pipelineDerivedSchemas(SCOPE); var before = f.snapshot();
            assertCode(() -> record(reader, "text"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> reader.pin("orders", "source.orders", 0), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> reader.delete("orders"), IoError.CACHE_BUSINESS_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            assertCode(() -> f.writer().pin("orders", "source.orders", 0), IoError.DOCUMENT_INVALID); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void recordingCapturesColumnOrderAndValuesBeforeACommandGuardCanMutateTheCaller() {
        withFixture(f -> {
            var columns = new LinkedHashMap<>(columns("text")); var once = new AtomicBoolean();
            var writer = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(f.client, f.database, SCOPE, WorkloadClaimFence.from(f.claim), () -> {
                if (once.compareAndSet(false, true)) { columns.clear(); columns.put("changed", "bad"); }
            }));
            writer.record("orders", "source.orders", columns, "statement", "sources", "derivation");
            assertThat(f.writer().latest("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("text"));
            assertThat(columns).containsOnlyKeys("changed");
        });
    }

    @Test void unrepresentableInputIsRejectedBeforeAnyDatabaseOrGuardCallback() {
        withFixture(f -> {
            var invalid = new LinkedHashMap<String, String>(); invalid.put("field", null); var guarded = new AtomicBoolean(); var before = f.snapshot();
            var writer = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(f.client, f.database, SCOPE, WorkloadClaimFence.from(f.claim), () -> guarded.set(true)));
            assertCode(() -> writer.record("orders", "step", invalid, "statement", "sources", "derivation"), IoError.DOCUMENT_INVALID);
            assertThat(guarded).isFalse(); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void sameColumnsInADifferentInputOrderDoNotMutateTheSchemaOfAnExistingPinnedVersion() {
        withFixture(f -> {
            var writer = f.writer(); record(writer, "text"); writer.pin("orders", "source.orders", 0);
            var reversed = new LinkedHashMap<String, String>(); reversed.put("note", "text"); reversed.put("order.id", "bigint");
            writer.record("orders", "source.orders", reversed, "new-statement", "new-source", "new-derivation");
            var pinned = writer.pinned("orders", "source.orders").orElseThrow();
            assertThat(pinned.version()).isZero(); assertThat(pinned.schema().keySet()).containsExactly("order.id", "note");
            assertThat(pinned.statement()).isEqualTo("new-statement");
        });
    }

    @Test void aKnownMissingPinnedVersionCannotBeReallocatedToAnotherModelByAnOrdinaryRecord() {
        withFixture(f -> {
            var writer = f.writer(); record(writer, "old"); record(writer, "held"); writer.pin("orders", "source.orders", 1);
            var versions = f.rows().find().first().getList("versions", Document.class);
            f.rows().updateOne(new Document(), new Document("$set", new Document("versions", List.of(versions.getFirst()))));
            assertThat(writer.pinned("orders", "source.orders")).isEmpty(); var before = f.snapshot();
            assertUnreadable(() -> record(writer, "replacement")); assertThat(f.snapshot()).isEqualTo(before);
            assertUnreadable(() -> writer.pin("orders", "source.orders", 0)); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void anOldExecutionCannotBorrowTheActuallyAdvancedClaimToWriteAPinOrHistory() {
        withFixture(f -> {
            var old = f.writer(); record(old, "text");
            var advanced = f.domain.workloadClaims(BUSINESS).advanceExecution(f.claim, 1, Set.of(OWNER.nodeId())).orElseThrow(); var before = f.snapshot();
            assertCode(() -> record(old, "old-execution"), IoError.WORKLOAD_CLAIM_FENCED);
            assertCode(() -> old.pin("orders", "source.orders", 0), IoError.WORKLOAD_CLAIM_FENCED); assertCode(() -> old.delete("orders"), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            var actual = f.domain.pipelineDerivedSchemas(SCOPE, WorkloadClaimFence.from(advanced), ACTIVE); record(actual, "new-execution");
            assertThat(actual.latest("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
        });
    }

    @Test void anExpiredClaimCannotAppendButTheOriginalModelRemainsReadable() {
        withFixture(f -> {
            var writer = f.writer(); record(writer, "text"); f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                    new Document("$set", new Document("leaseUntil", new Date(0)))); var before = f.snapshot();
            assertCode(() -> record(writer, "late"), IoError.WORKLOAD_CLAIM_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.domain.pipelineDerivedSchemas(SCOPE).latest("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("text"));
        });
    }

    @Test void aReplacementGenerationDoesNotAdoptTheOldHistoryPinOrLateWriter() {
        withFixture(f -> {
            var old = f.writer(); record(old, "old"); old.pin("orders", "source.orders", 0);
            var next = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 2);
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("scope", MongoPipelinePreparationFacts.scope(next))));
            f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), new Document("$set", new Document("pipelineScope", MongoPipelinePreparationFacts.scope(next))
                    .append("pipelinePreparation", MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
            var current = f.domain.pipelineDerivedSchemas(next, WorkloadClaimFence.from(f.claim), ACTIVE);
            assertThat(current.latest("orders", "source.orders")).isEmpty(); assertThat(current.pinned("orders", "source.orders")).isEmpty(); record(current, "new");
            assertThat(current.latest("orders", "source.orders").orElseThrow().version()).isZero(); assertThat(current.pinned("orders", "source.orders")).isEmpty();
            var before = f.snapshot(); assertCode(() -> old.latest("orders", "source.orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> old.delete("orders"), IoError.CACHE_BUSINESS_FENCED); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void concurrentClientsSerializeDifferentShapesWithoutDroppingHistoryOrItsExistingPin() {
        withFixture(f -> {
            record(f.writer(), "initial"); f.writer().pin("orders", "source.orders", 0); var start = new CountDownLatch(1);
            try (var second = MongoCacheDomain.connect(f.binding, Duration.ofSeconds(5), "second"); var workers = Executors.newFixedThreadPool(2)) {
                var other = second.pipelineDerivedSchemas(SCOPE, WorkloadClaimFence.from(f.claim), ACTIVE);
                var a = workers.submit(() -> { await(start); record(f.writer(), "one"); });
                var b = workers.submit(() -> { await(start); record(other, "two"); }); start.countDown();
                a.get(15, TimeUnit.SECONDS); b.get(15, TimeUnit.SECONDS);
                assertThat(f.rows().find().first().getList("versions", Document.class)).hasSize(3);
                assertThat(f.writer().pinned("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("initial"));
                assertThat(f.writer().latest("orders", "source.orders").orElseThrow().version()).isEqualTo(2);
            }
        });
    }

    @Test void originalGuardLossAfterThePayloadWriteRollsBackClaimStateAndHistoryTogether() {
        withFixture(f -> {
            record(f.writer(), "old"); var before = f.snapshot(); var live = new AtomicBoolean(true);
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (updates(e, "derived_schemas")) live.set(false);
            } };
            try (var observed = client(listener)) {
                var writer = f.writer(observed, () -> { if (!live.get()) throw MongoCacheBusinessIdentity.businessFenced(); });
                assertCode(() -> record(writer, "lost"), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void committedRetirementBeforeTheActualClaimWriteRefusesLateModelPublication() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (updates(e, "workload_claims") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
            } };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var future = worker.submit(() -> record(f.writer(observed, ACTIVE), "late"));
                try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.domain.retireBusiness(BUSINESS, "retire-models", ACTIVE); }
                finally { proceed.countDown(); }
                assertThatThrownBy(() -> future.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
                assertThat(f.rows().countDocuments()).isZero(); assertThat(f.claimWrites()).isZero();
            }
        });
    }

    @Test void malformedHistoryAndScopeDoNotUseOldDefaultsTruncateVersionsOrLeakDecoderCauses() {
        withFixture(f -> {
            record(f.writer(), "old"); f.writer().pin("orders", "source.orders", 0); var saved = f.rows().find().first();
            for (var mutation : List.of(new Document("versions.0.version", 0.5d), new Document("versions.0.extra", "private-sentinel"),
                    new Document("versions.0.columns.0.type", null), new Document("scope.stateGeneration", 1), new Document("pin", 0))) {
                f.rows().updateOne(new Document(), new Document("$set", mutation)); assertUnreadable(() -> f.writer().latest("orders", "source.orders"));
                f.rows().replaceOne(new Document(), saved);
            }
            f.rows().updateOne(new Document(), new Document("$unset", new Document("versions", "")));
            assertUnreadable(() -> f.writer().pinned("orders", "source.orders"));
        });
    }

    @Test void anUnheldPinAnswersEmptyAndNeverFallsBackToTheLatest() {
        withFixture(f -> {
            record(f.writer(), "old"); f.rows().updateOne(new Document(), new Document("$set", new Document("pin", 10L)));
            assertThat(f.writer().pinned("orders", "source.orders")).isEmpty();
            assertThat(f.writer().latest("orders", "source.orders")).isPresent();
        });
    }

    @Test void deletionRemovesOnlyTheOriginalGenerationAndReadViewsNeverRepairMissingStructures() {
        withFixture(f -> {
            record(f.writer(), "old"); Document other = new Document(f.rows().find().first()).append("_id", "orders.other");
            other.get("scope", Document.class).put("stateGeneration", 2L); f.rows().insertOne(other);
            f.writer().delete("orders"); assertThat(f.rows().countDocuments()).isEqualTo(1);
            assertThat(f.rows().find().first().getString("_id")).isEqualTo("orders.other");
            f.rows().drop(); assertThat(f.writer().latest("orders", "source.orders")).isEmpty();
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("derived_schemas");
        });
    }

    private static boolean updates(CommandStartedEvent e, String table) { return e.getCommandName().equals("update") && e.getCommand().getString("update").getValue().equals(table); }
    private static MongoClient client(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static void await(CountDownLatch latch) { try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); } }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code)); }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> { assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(e.getCause()).isNull(); assertThat(e.args().toString()).doesNotContain("private-sentinel"); }); }
    private static void withFixture(Scenario scenario) {
        String name = "cache_derived_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "derived-test")) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1), "auth-a", ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareInTransaction(session, ACTIVE);
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session);
                    SystemCollections.CACHE_PIPELINE_STATE.prepareCollectionOn(database, session); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "orders").append("scope", MongoPipelinePreparationFacts.scope(SCOPE))
                        .append("stateJson", "NEW").append("epoch", 0L).append("touchMillis", 0L).append("runtimeWriteFence", 1L));
                var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var retained = raw.acquire(KEY, OWNER, 1, Duration.ofSeconds(30)).claim(); raw.release(retained);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                        new Document("$set", new Document("pipelineScope", MongoPipelinePreparationFacts.scope(SCOPE)).append("pipelinePreparation",
                                MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
                var claim = domain.workloadClaims(BUSINESS).acquirePipeline(SCOPE, OWNER, 1, Duration.ofSeconds(30)).claim();
                scenario.run(new Fixture(client, database, domain, binding, claim));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; }
        catch (Exception e) { throw new AssertionError(e); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding, WorkloadClaim claim) {
        DerivedSchemaStore writer() { return domain.pipelineDerivedSchemas(SCOPE, WorkloadClaimFence.from(claim), ACTIVE); }
        DerivedSchemaStore writer(MongoClient original, Runnable guard) { return new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(original,
                original.getDatabase(database.getName()), SCOPE, WorkloadClaimFence.from(claim), guard)); }
        com.mongodb.client.MongoCollection<Document> rows() { return SystemCollections.CACHE_DERIVED_SCHEMAS.on(database); }
        com.mongodb.client.MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }
        long claimWrites() { var row = claims().find().first(); return row.get("fencedPipelineWrites") == null ? 0 : row.getLong("fencedPipelineWrites"); }
        long admissions() { return SystemCollections.CACHE_IDENTITY.on(database).find().first().getLong("claimAdmissionFence"); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (var table : List.of(SystemCollections.CACHE_DERIVED_SCHEMAS, SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE))
                out.put(table.collectionName(), table.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
