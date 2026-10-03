package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Page;
import io.tapstate.spi.store.RateHistoryStore.Visibility;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Explains the store's real scoped keyset reads while invisible history grows around them. */
@RequiresDocker
class MongoRateHistoryScopedQueryCostIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final String PIPELINE = "orders";
    private static final Visibility CURRENT = new Visibility("current-incarnation", false);
    private static final ObservationStore.Scope RUN_ONE = new ObservationStore.Scope("current-incarnation", 41);
    private static final ObservationStore.Scope RUN_TWO = new ObservationStore.Scope("current-incarnation", 42);
    private static final long DOCUMENT_BUDGET = 8;
    private static final long KEY_BUDGET = 12;

    @Test
    void sparseCurrentIncarnationReadsStayBoundedAsInvisibleHistoryGrows() {
        String databaseName = "scoped_history_cost_" + UUID.randomUUID();
        FindTrace trace = new FindTrace(SystemCollections.PIPELINE_RATE_HISTORY.collectionName());
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(trace).build();
        try (MongoClient client = MongoClients.create(settings)) {
            MongoDatabase database = client.getDatabase(databaseName);
            MongoCollection<Document> collection = SystemCollections.PIPELINE_RATE_HISTORY.on(database);
            MongoRateHistoryStore history = new MongoRateHistoryStore(database, collection, Duration.ofDays(15));
            Instant from = Instant.now().truncatedTo(ChronoUnit.MILLIS).minus(Duration.ofHours(1));
            Instant to = from.plusSeconds(180);
            seedInvisible(collection, from, 2);
            history.appendScoped(sample(from.minusSeconds(60), 100, from), RUN_ONE);
            history.appendScoped(sample(from, 101, from), RUN_ONE);
            history.appendScoped(sample(from.plusSeconds(60), 102, from), RUN_TWO);
            history.appendScoped(sample(to, 103, from), RUN_TWO);

            List<ReadCost> small = readAndExplain(history, collection, trace, from, to);
            // Add data through the same codec, retaining all legacy and older-incarnation rows.
            seedInvisible(collection, from, 128);
            assertThat(collection.countDocuments()).isEqualTo(784);
            List<ReadCost> grown = readAndExplain(history, collection, trace, from, to);

            for (int index = 0; index < grown.size(); index++) {
                ReadCost before = small.get(index);
                ReadCost after = grown.get(index);
                assertThat(after.operation()).isEqualTo(before.operation());
                assertThat(after.documents()).as("%s reads only its bounded visible candidates", after.operation())
                        .isLessThanOrEqualTo(DOCUMENT_BUDGET);
                assertThat(after.keys()).as("%s does not traverse invisible history keys", after.operation())
                        .isLessThanOrEqualTo(KEY_BUDGET);
                assertThat(after.documents()).as("%s document work does not grow with invisible rows", after.operation())
                        .isLessThanOrEqualTo(before.documents() + 4);
                assertThat(after.keys()).as("%s key work does not grow with invisible rows", after.operation())
                        .isLessThanOrEqualTo(before.keys() + 4);
            }
        }
    }

    private static List<ReadCost> readAndExplain(MongoRateHistoryStore history, MongoCollection<Document> collection,
            FindTrace trace, Instant from, Instant to) {
        List<ReadCost> costs = new ArrayList<>();
        Captured<Page> first = capture(trace, () -> history.readPageVisible(PIPELINE, CURRENT, from, to, null, 1));
        assertThat(first.result().hasMore()).isTrue();
        assertThat(first.result().entries()).singleElement().satisfies(entry -> assertSample(entry, from, 101, RUN_ONE));
        costs.add(explain(collection, "first-page", first.command(), 2));

        Captured<Page> suffix = capture(trace, () -> history.readPageVisible(PIPELINE, CURRENT, from, to,
                first.result().lastKey().orElseThrow(), 1));
        assertThat(suffix.result().hasMore()).isFalse();
        assertThat(suffix.result().entries()).singleElement()
                .satisfies(entry -> assertSample(entry, from.plusSeconds(60), 102, RUN_TWO));
        assertThat(suffix.result().lastKey()).isNotEqualTo(first.result().lastKey());
        costs.add(explain(collection, "suffix-page", suffix.command(), 1));

        Captured<Optional<Entry>> predecessor = capture(trace, () -> history.predecessorVisible(PIPELINE, CURRENT, from));
        assertSample(predecessor.result().orElseThrow(), from.minusSeconds(60), 100, RUN_ONE);
        costs.add(explain(collection, "predecessor", predecessor.command(), 1));

        Captured<Optional<Entry>> successor = capture(trace,
                () -> history.successorVisible(PIPELINE, CURRENT, from.plusSeconds(120)));
        assertSample(successor.result().orElseThrow(), to, 103, RUN_TWO);
        costs.add(explain(collection, "successor", successor.command(), 1));

        Captured<Page> empty = capture(trace,
                () -> history.readPageVisible(PIPELINE, CURRENT, from.plusSeconds(120), to, null, 1));
        assertThat(empty.result().entries()).as("the visible successor at the upper bound is excluded").isEmpty();
        assertThat(empty.result().hasMore()).isFalse();
        costs.add(explain(collection, "empty-range", empty.command(), 0));
        return List.copyOf(costs);
    }

    private static void assertSample(Entry entry, Instant at, long records, ObservationStore.Scope scope) {
        assertThat(entry.sample().pipelineId()).isEqualTo(PIPELINE);
        assertThat(entry.sample().observedAt()).isEqualTo(at);
        assertThat(entry.key().observedAt()).isEqualTo(at);
        assertThat(entry.sample().counters()).containsEntry("records.out", records);
        assertThat(entry.scope()).contains(scope);
    }

    private static <T> Captured<T> capture(FindTrace trace, Supplier<T> read) {
        trace.start();
        T result;
        List<BsonDocument> commands;
        try {
            result = read.get();
        } finally {
            commands = trace.stop();
        }
        assertThat(commands).as("one real find per bounded history read").hasSize(1);
        return new Captured<>(result, commands.getFirst());
    }

    private static ReadCost explain(MongoCollection<Document> collection, String operation,
            BsonDocument command, long returned) {
        assertThat(command.getString("find").getValue()).isEqualTo(collection.getNamespace().getCollectionName());
        assertThat(command.containsKey("hint")).as("the production read chose its own plan").isFalse();
        BsonDocument filter = command.getDocument("filter");
        BsonDocument sort = command.getDocument("sort");
        int limit = command.getNumber("limit").intValue();
        assertThat(limit).isBetween(1, 2);
        assertThat(filter.toJson()).contains("pipelineId", PIPELINE, "pipelineIncarnationId", "current-incarnation");
        Document stats = collection.find(filter).sort(sort).limit(limit)
                .explain(ExplainVerbosity.EXECUTION_STATS).get("executionStats", Document.class);
        assertThat(((Number) stats.get("nReturned")).longValue()).as("the replay returned the same bounded candidates")
                .isEqualTo(returned);
        ReadCost cost = new ReadCost(operation, ((Number) stats.get("totalDocsExamined")).longValue(),
                ((Number) stats.get("totalKeysExamined")).longValue());
        System.out.printf("scoped-history-query-cost operation=%s returned=%d limit=%d docs=%d keys=%d%n",
                operation, returned, limit, cost.documents(), cost.keys());
        return cost;
    }

    private static void seedInvisible(MongoCollection<Document> collection, Instant from, int countPerBand) {
        List<Document> rows = new ArrayList<>();
        for (int index = 0; index < countPerBand; index++) {
            for (long second : List.of(-59L + index % 59, 1L + index % 59, 121L + index % 59)) {
                Instant at = from.plusSeconds(second);
                rows.add(MongoRateHistoryStore.toDocument(sample(at, 900 + index, from)));
                rows.add(MongoRateHistoryStore.toDocument(sample(at, 1900 + index, from))
                        .append(MongoRateHistoryStore.PIPELINE_INCARNATION_ID, "old-incarnation")
                        .append(MongoRateHistoryStore.EXECUTION_GENERATION, 40L));
            }
        }
        collection.insertMany(rows);
    }

    private static RateSample sample(Instant at, long records, Instant from) {
        return new RateSample(PIPELINE, at, Map.of("records.out", records), Map.of(), from.minusSeconds(3600));
    }

    private record Captured<T>(T result, BsonDocument command) { }
    private record ReadCost(String operation, long documents, long keys) { }

    private static final class FindTrace implements CommandListener {
        private final String collection;
        private final List<BsonDocument> commands = new ArrayList<>();
        private boolean active;

        private FindTrace(String collection) { this.collection = collection; }

        synchronized void start() { commands.clear(); active = true; }
        synchronized List<BsonDocument> stop() { active = false; return List.copyOf(commands); }

        @Override
        public synchronized void commandStarted(CommandStartedEvent event) {
            if (active && "find".equals(event.getCommandName())
                    && collection.equals(event.getCommand().getString("find").getValue())) {
                // The callback owns this buffer only while it runs; the replay retains an independent copy.
                commands.add(BsonDocument.parse(event.getCommand().toJson()));
            }
        }
    }
}
