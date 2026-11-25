package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.RequiredDeadLetterContext;
import io.tapstate.spi.store.RequiredDeadLetterFact;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
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

/** Configuration-only adapter witnesses; no cache ACK, running job or browser acceptance is implied. */
@RequiresDocker
class MongoRequiredDeadLetterStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final RequiredDeadLetterContext SCOPE = new RequiredDeadLetterContext("cluster-a", 1, "flow", "nest-a", "", 1, "nest.flow.nest-a.root");

    @Test
    void typedRowsPositionsDeletionAndRecentReplacementSurviveReopen() {
        withConfiguration((domain, database) -> {
            var store = domain.requiredDeadLetters(); long before = version(database);
            var row = fact("a.$:key", 10, Map.of("converted", new ConvertedValue("42", "NUMERIC"), "$date", Map.of("$numberLong", "field-value")));
            assertThat(store.record(SCOPE, 0, row)).isTrue();
            assertThat(store.record(SCOPE, 0, fact("other", 20, null))).isTrue();
            assertThat(store.read(SCOPE, 2)).extracting(snapshot -> snapshot.fact().record().element()).containsExactly("other", "a.$:key");
            assertThat(store.find(SCOPE, "other").orElseThrow().fact().record().deletion()).isTrue();
            assertThat(store.record(SCOPE, 1, fact("a.$:key", 30, row.record().row()))).isTrue();
            assertThat(store.read(SCOPE, 1).getFirst().revision()).isEqualTo(2);
            assertThat(store.read(SCOPE, 0)).isEmpty();
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                var saved = reopened.requiredDeadLetters().find(SCOPE, "a.$:key").orElseThrow();
                assertThat(saved.fact().positions()).containsEntry("chain-a", new ChainPosition(new SourceOrder(7, 3), "source-token"));
                assertThat(saved.fact().record().row()).isEqualTo(row.record().row());
            }
            assertThat(rows(database).countDocuments()).isEqualTo(2); assertThat(version(database)).isEqualTo(before);
        });
    }

    @Test
    void staleRevisionDoesNotOverwriteOrAdvanceTheMappingFence() {
        withConfiguration((domain, database) -> {
            var store = domain.requiredDeadLetters(); assertThat(store.record(SCOPE, 0, fact("a", 10, Map.of("id", 1)))).isTrue();
            Document before = mapping(database);
            assertThat(store.record(SCOPE, 0, fact("a", 99, Map.of("id", 2)))).isFalse();
            assertThat(store.find(SCOPE, "a").orElseThrow().fact().record().discardedAt()).isEqualTo(10);
            assertThat(mapping(database)).isEqualTo(before);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"row", "deletion", "token"})
    void oneSourceEventCannotChangeItsRowDeletionOrToken(String changed) {
        withConfiguration((domain, database) -> {
            var store = domain.requiredDeadLetters(); var original = fact("a", 10, Map.of("id", 1));
            assertThat(store.record(SCOPE, 0, original)).isTrue(); Document before = mapping(database);
            var replacement = fact("a", 20, changed.equals("deletion") ? null : Map.of("id", changed.equals("row") ? 2 : 1));
            if (changed.equals("token")) replacement = new RequiredDeadLetterFact("inc-a", Map.of("chain-a", new ChainPosition(new SourceOrder(7, 3), "different-token")), replacement.record());
            var captured = replacement;
            assertThatThrownBy(() -> store.record(SCOPE, 1, captured)).satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_CONFLICT));
            assertThat(mapping(database)).isEqualTo(before); assertThat(store.find(SCOPE, "a").orElseThrow().revision()).isOne();
        });
    }

    @Test
    void aDistinctEventMayReplaceTheElementAtTheExplicitExpectedRevision() {
        withConfiguration((domain, database) -> {
            var store = domain.requiredDeadLetters(); assertThat(store.record(SCOPE, 0, fact("a", 10, Map.of("id", 1)))).isTrue();
            var next = new RequiredDeadLetterFact("inc-a", Map.of("chain-a", new ChainPosition(new SourceOrder(7, 4), "next-token")),
                    new NestDeadLetterRecord(SCOPE.namespace(), "a", "chain-a", "7:4", 2, 20, Map.of("id", 2)));
            assertThat(store.record(SCOPE, 1, next)).isTrue(); assertThat(store.find(SCOPE, "a").orElseThrow().fact()).isEqualTo(next);
            assertThat(rows(database).countDocuments()).isOne();
        });
    }

    @Test
    void historicalGenerationRemainsReadableButOnlyTheOriginalRetirementCanClearItsExactScope() {
        withConfiguration((domain, database) -> {
            var store = domain.requiredDeadLetters(); assertThat(store.record(SCOPE, 0, fact("a", 10, Map.of()))).isTrue();
            assertThatThrownBy(() -> store.dropNamespace(SCOPE, "retire-a")).satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_FENCED));
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", "flow"), new Document("$set", new Document("stateGeneration", 2L)));
            assertThat(store.find(SCOPE, "a")).isPresent();
            assertThatThrownBy(() -> store.record(SCOPE, 1, fact("a", 20, Map.of()))).satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_FENCED));
            var current = new RequiredDeadLetterContext("cluster-a", 1, "flow", "nest-a", "", 2, SCOPE.namespace());
            assertThat(store.record(current, 0, fact("a", 30, Map.of()))).isTrue();
            var sibling = new RequiredDeadLetterContext("cluster-a", 1, "flow", "nest-b", "child", 2, "nest.flow.nest-b.child");
            var siblingFact = new RequiredDeadLetterFact("inc-a", Map.of(), new NestDeadLetterRecord(sibling.namespace(), "b", "", "8:0", 0, 40, Map.of()));
            assertThat(store.record(sibling, 0, siblingFact)).isTrue();
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", "flow"), new Document("$set", new Document("phase", "RETIRING").append("operationId", "retire-a")));
            assertThatThrownBy(() -> store.dropNamespace(current, "wrong-operation")).satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_FENCED));
            assertThatThrownBy(() -> store.dropNamespace(SCOPE, "retire-a")).satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_FENCED));
            store.dropNamespace(current, "retire-a"); store.dropNamespace(current, "retire-a");
            assertThat(store.find(current, "a")).isEmpty(); assertThat(store.find(SCOPE, "a")).isPresent(); assertThat(store.find(sibling, "b")).isPresent();
            assertThatThrownBy(() -> store.record(current, 0, fact("late", 50, null))).satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_FENCED));
        });
    }

    @Test
    void nestedCallerValuesAreCapturedBeforeTheFirstRetryableCallback() {
        withConfiguration((domain, database) -> {
            var nested = new HashMap<String, Object>(); nested.put("id", 1); var count = new AtomicInteger();
            var guarded = domain.requiredDeadLetters().guardedBy(() -> { if (count.incrementAndGet() == 1) nested.put("id", 2); });
            assertThat(guarded.record(SCOPE, 0, fact("a", 10, Map.of("nested", nested)))).isTrue();
            assertThat(domain.requiredDeadLetters().find(SCOPE, "a").orElseThrow().fact().record().row()).containsEntry("nested", Map.of("id", 1));
            assertThat(count.get()).isEqualTo(2);
        });
    }

    @Test
    void lostPostWriteGuardRollsBackBothTheFactAndMappingFence() {
        withConfiguration((domain, database) -> {
            Document before = mapping(database); var count = new AtomicInteger();
            var guarded = domain.requiredDeadLetters().guardedBy(() -> { if (count.incrementAndGet() == 2) throw new TapstateException(IoError.REQUIRED_DEAD_LETTER_FENCED, Map.of(), null); });
            assertThatThrownBy(() -> guarded.record(SCOPE, 0, fact("a", 10, Map.of()))).satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_FENCED));
            assertThat(domain.requiredDeadLetters().find(SCOPE, "a")).isEmpty(); assertThat(mapping(database)).isEqualTo(before);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"revision", "fact.heldForMillis", "fact.positions.0.order.epoch", "fact.positions.0.token", "fact.row", "fact.cacheIncarnation"})
    void corruptFactsAreNotDefaultedOrOverwritten(String field) {
        withConfiguration((domain, database) -> {
            var store = domain.requiredDeadLetters(); assertThat(store.record(SCOPE, 0, fact("a", 10, Map.of("id", 1)))).isTrue();
            Object broken = field.equals("fact.row") ? "raw-secret-sentinel" : 0.5D;
            rows(database).updateOne(new Document(), new Document("$set", new Document(field, broken)));
            assertThatThrownBy(() -> store.find(SCOPE, "a")).satisfies(error -> code(error, IoError.DOCUMENT_UNREADABLE));
            assertThatThrownBy(() -> store.record(SCOPE, 1, fact("a", 20, Map.of("id", 1)))).satisfies(error -> code(error, IoError.DOCUMENT_UNREADABLE));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"mapping", "pipeline", "fence"})
    void missingConfigurationFactsCannotBeRecreatedByARecord(String missing) {
        withConfiguration((domain, database) -> {
            if (missing.equals("mapping")) SystemCollections.OPERATOR_GENERATIONS.on(database).deleteMany(new Document());
            else if (missing.equals("pipeline")) SystemCollections.PIPELINES.on(database).deleteMany(new Document());
            else SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document(), new Document("$unset", new Document("noteWriteFence", "")));
            assertThatThrownBy(() -> domain.requiredDeadLetters().record(SCOPE, 0, fact("a", 10, null))).satisfies(error -> code(error, IoError.DOCUMENT_UNREADABLE));
            assertThat(rows(database).countDocuments()).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-index", "ttl"})
    void ordinaryAccessRefusesIndexLossOrExpiryWithoutRepair(String damage) {
        withConfiguration((domain, database) -> {
            var store = domain.requiredDeadLetters(); assertThat(store.record(SCOPE, 0, fact("a", 10, Map.of()))).isTrue();
            if (damage.equals("ttl")) rows(database).createIndex(new Document("expiredAt", 1), new IndexOptions().name("forbidden_ttl").expireAfter(1L, TimeUnit.SECONDS));
            else rows(database).dropIndex("required_dead_letters_recent");
            assertThatThrownBy(() -> store.read(SCOPE, 10)).satisfies(error -> code(error, IoError.DOCUMENT_UNREADABLE));
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(database), "cluster-a")).satisfies(error -> code(error, IoError.DOCUMENT_UNREADABLE));
            assertThat(rows(database).countDocuments()).isOne();
        });
    }

    @Test
    void unsupportedInputIsRejectedBeforeAnyConfigurationWrite() {
        withConfiguration((domain, database) -> {
            Document before = mapping(database);
            assertThatThrownBy(() -> domain.requiredDeadLetters().record(SCOPE, 0, fact("a", 10, Map.of("raw-secret-sentinel", new Object()))))
                    .satisfies(error -> code(error, IoError.DOCUMENT_INVALID));
            assertThat(mapping(database)).isEqualTo(before); assertThat(rows(database).countDocuments()).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aDriverRetryKeepsTheOriginalExpectedRevisionAndGuard(boolean loseGuard) {
        withConfiguration((domain, database) -> {
            var paused = new CountDownLatch(1); var release = new CountDownLatch(1); var once = new AtomicBoolean(); var checks = new AtomicInteger();
            var witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("operator_generations") && once.compareAndSet(false, true)) {
                        paused.countDown();
                        try { assertThat(release.await(10, TimeUnit.SECONDS)).isTrue(); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                    }
                }
            };
            try (var client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(witness).build());
                 var executor = Executors.newSingleThreadExecutor()) {
                try {
                    var original = new MongoRequiredDeadLetterStore(client, client.getDatabase(database.getName()), "cluster-a").guardedBy(() -> {
                        if (checks.incrementAndGet() > 1 && loseGuard) throw new TapstateException(IoError.REQUIRED_DEAD_LETTER_FENCED, Map.of(), null);
                    });
                    var old = executor.submit(() -> original.record(SCOPE, 0, fact("a", 10, Map.of("value", "old"))));
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(domain.requiredDeadLetters().record(SCOPE, 0, fact("a", 20, Map.of("value", "winner")))).isTrue();
                    release.countDown();
                    if (loseGuard) assertThatThrownBy(() -> old.get(20, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
                    else assertThat(old.get(20, TimeUnit.SECONDS)).isFalse();
                    assertThat(checks.get()).isGreaterThanOrEqualTo(2);
                    assertThat(domain.requiredDeadLetters().find(SCOPE, "a").orElseThrow().fact().record().row()).containsEntry("value", "winner");
                } finally { release.countDown(); }
            }
        });
    }

    @Test
    void aMarkerShapedBusinessMapRemainsAMapRatherThanBecomingATypedCarrier() {
        withConfiguration((domain, database) -> {
            var marker = Map.<String, Object>of("__tapstate_carried", "42", "__tapstate_originType", "NUMERIC");
            var value = fact("a", 10, Map.of("value", marker));
            assertThat(domain.requiredDeadLetters().record(SCOPE, 0, value)).isTrue();
            assertThat(domain.requiredDeadLetters().find(SCOPE, "a").orElseThrow().fact().record().row()).containsEntry("value", marker);
        });
    }

    @Test
    void changingAnOrdinaryMapToATypedCarrierCannotBypassSameEventConflict() {
        withConfiguration((domain, database) -> {
            var marker = Map.<String, Object>of("__tapstate_carried", "42", "__tapstate_originType", "NUMERIC");
            var store = domain.requiredDeadLetters(); assertThat(store.record(SCOPE, 0, fact("a", 10, Map.of("value", marker)))).isTrue();
            assertThatThrownBy(() -> store.record(SCOPE, 1, fact("a", 20, Map.of("value", new ConvertedValue("42", "NUMERIC")))))
                    .satisfies(error -> code(error, IoError.REQUIRED_DEAD_LETTER_CONFLICT));
        });
    }

    @Test
    void nestedEscapeMapsListsAndTaggedBytesStayDistinctFromOrdinaryValues() {
        withConfiguration((domain, database) -> {
            var escape = Map.<String, Object>of("__tapstate_dead_letter_value", "converted", "value", "ordinary", "originType", "TEXT");
            var typed = new ConvertedValue(Map.of("nested", escape), "DOCUMENT");
            var bytes = new io.tapstate.core.event.Bytes((byte) 7, new byte[]{1, 2, 3});
            var row = Map.<String, Object>of("escape", escape, "nested", List.of(escape, typed, bytes), "typed", typed, "bytes", bytes);
            assertThat(domain.requiredDeadLetters().record(SCOPE, 0, fact("a", 10, row))).isTrue();
            assertThat(domain.requiredDeadLetters().find(SCOPE, "a").orElseThrow().fact().record().row()).isEqualTo(row);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void plainBinaryRowsRemainByteArraysIncludingNestedSameEventReplay(boolean replay) {
        withConfiguration((domain, database) -> {
            var row = Map.<String, Object>of("binary", new byte[]{1, 2, 3}, "nested", List.of(Map.of("binary", new byte[]{4, 5})));
            var store = domain.requiredDeadLetters(); assertThat(store.record(SCOPE, 0, fact("a", 10, row))).isTrue();
            if (replay) {
                var next = Map.<String, Object>of("binary", new byte[]{1, 2, 3}, "nested", List.of(Map.of("binary", new byte[]{4, 5})));
                assertThat(store.record(SCOPE, 1, fact("a", 20, next))).isTrue();
            }
            var saved = store.find(SCOPE, "a").orElseThrow();
            assertThat((byte[]) saved.fact().record().row().get("binary")).containsExactly(1, 2, 3);
            var nested = (List<?>) saved.fact().record().row().get("nested");
            assertThat((byte[]) ((Map<?, ?>) nested.getFirst()).get("binary")).containsExactly(4, 5);
            assertThat(saved.revision()).isEqualTo(replay ? 2 : 1);
        });
    }

    @Test
    void anExhaustedMappingFenceCannotCommitAnUnreadableNextMapping() {
        withConfiguration((domain, database) -> {
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document(), new Document("$set", new Document("noteWriteFence", Long.MAX_VALUE - 1)));
            Document before = mapping(database);
            assertThatThrownBy(() -> domain.requiredDeadLetters().record(SCOPE, 0, fact("a", 10, Map.of())))
                    .satisfies(error -> code(error, IoError.DOCUMENT_UNREADABLE));
            assertThat(mapping(database)).isEqualTo(before); assertThat(rows(database).countDocuments()).isZero();
        });
    }

    private static RequiredDeadLetterFact fact(String element, long at, Map<String, Object> row) {
        return new RequiredDeadLetterFact("inc-a", Map.of("chain-a", new ChainPosition(new SourceOrder(7, 3), "source-token")),
                new NestDeadLetterRecord(SCOPE.namespace(), element, "chain-a", "7:3", at, at, row));
    }
    private static com.mongodb.client.MongoCollection<Document> rows(MongoDatabase database) { return SystemCollections.CONFIG_NEST_DEAD_LETTERS.on(database); }
    private static Document mapping(MongoDatabase database) { return SystemCollections.OPERATOR_GENERATIONS.on(database).find(new Document("_id", "flow")).first(); }
    private static long version(MongoDatabase database) { return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision"); }
    private static void code(Throwable failure, IoError expected) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(expected); assertThat(error.getCause()).isNull(); assertThat(error.getMessage()).doesNotContain("raw-secret-sentinel");
        });
    }
    private static MongoConnectionSettings settings(MongoDatabase database) { return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5)); }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("required_dead_letters_" + UUID.randomUUID().toString().replace("-", ""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(database), "cluster-a", "installation-a", "setup-a", 1)) {
                var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache.invalid/cache", "cache", null);
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                domain.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cache, master);
                domain.artifacts().create(new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n"));
                scenario.run(domain, database);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, MongoDatabase database) throws Exception; }
}
