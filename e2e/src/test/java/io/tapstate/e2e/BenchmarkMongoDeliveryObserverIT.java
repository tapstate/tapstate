package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The observer uses Mongo's real replica-set change stream, without a product process. */
class BenchmarkMongoDeliveryObserverIT {

    private static final Duration BOUND = Duration.ofSeconds(15);
    private static final List<MongoClient> CLIENTS = new ArrayList<>();

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @AfterAll
    static void closeClients() {
        CLIENTS.forEach(MongoClient::close);
        CLIENTS.clear();
    }

    @Test
    void insertAndRepeatedKeyUpdateHaveOneDurationEachAtBothTargetLocations() {
        String external = SharedMongo.replicaSetUrl("benchmark_delivery_external");
        String views = SharedMongo.replicaSetUrl("views");
        for (BenchmarkWorkloadDefinitions.TargetLocation location
                : BenchmarkWorkloadDefinitions.TargetLocation.values()) {
            String table = location == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                    ? "benchmark_delivery_view_test" : "benchmark_delivery_external_test";
            String uri = location == BenchmarkWorkloadDefinitions.TargetLocation.MANAGED_VIEW
                    ? views : external;
            MongoCollection<Document> target = freshCollection(uri, table);
            BenchmarkWorkloadDefinitions.TargetExpectation expectation = target(location, table);
            List<BenchmarkMongoDeliveryObserver.Delivery> delivered;

            try (BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                    expectation, external, views, row -> String.valueOf(row.get("id")))) {
                long insertIssued = System.nanoTime();
                observer.expectBatch(insertIssued, List.of(
                        new BenchmarkMongoDeliveryObserver.ExpectedChange(
                                "1", BenchmarkMongoDeliveryObserver.Kind.INSERT)));
                assertThat(target.updateOne(Filters.eq("id", 1L),
                        Updates.combine(Updates.setOnInsert("id", 1L), Updates.set("value", "first")),
                        new UpdateOptions().upsert(true)).getUpsertedId()).isNotNull();

                long updateIssued = System.nanoTime();
                observer.expectBatch(updateIssued, List.of(
                        new BenchmarkMongoDeliveryObserver.ExpectedChange(
                                "1", BenchmarkMongoDeliveryObserver.Kind.UPDATE)));
                assertThat(target.updateOne(Filters.eq("id", 1L), Updates.set("value", "second"))
                        .getMatchedCount()).isEqualTo(1);

                long secondInsertIssued = System.nanoTime();
                observer.expectBatch(secondInsertIssued, List.of(
                        new BenchmarkMongoDeliveryObserver.ExpectedChange(
                                "2", BenchmarkMongoDeliveryObserver.Kind.INSERT)));
                target.insertOne(new Document("id", 2L).append("value", "third"));

                // The acknowledged target writes above are the authoritative completion point here.
                delivered = observer.finish(BOUND);
            }

            assertThat(delivered).extracting(BenchmarkMongoDeliveryObserver.Delivery::key)
                    .containsExactly("1", "1", "2");
            assertThat(delivered).extracting(BenchmarkMongoDeliveryObserver.Delivery::kind)
                    .containsExactly(BenchmarkMongoDeliveryObserver.Kind.INSERT,
                            BenchmarkMongoDeliveryObserver.Kind.UPDATE,
                            BenchmarkMongoDeliveryObserver.Kind.INSERT);
            assertThat(delivered).allSatisfy(sample -> {
                assertThat(sample.durationNanos()).isNotNegative();
                assertThat(sample.observedAtNanos()).isGreaterThanOrEqualTo(sample.issuedAtNanos());
            });
            assertThat(target.countDocuments()).isEqualTo(2);
        }
    }

    @Test
    void barrierRejectsMissingDeliveryWithoutAWaitGuess() {
        String external = SharedMongo.replicaSetUrl("benchmark_delivery_missing");
        String views = SharedMongo.replicaSetUrl("views");
        String table = "benchmark_delivery_missing_test";
        freshCollection(external, table);

        try (BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                target(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO, table),
                external, views, row -> String.valueOf(row.get("id")))) {
            observer.expectBatch(System.nanoTime(), List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "absent", BenchmarkMongoDeliveryObserver.Kind.INSERT)));
            assertThatThrownBy(() -> observer.finish(BOUND))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("1 missing deliveries");
        }
    }

    @Test
    void unregisteredAndDuplicateTargetChangesFailClosed() {
        String external = SharedMongo.replicaSetUrl("benchmark_delivery_extra");
        String views = SharedMongo.replicaSetUrl("views");
        String unknownTable = "benchmark_delivery_unknown_test";
        MongoCollection<Document> unknownTarget = freshCollection(external, unknownTable);

        try (BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                target(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO, unknownTable),
                external, views, row -> String.valueOf(row.get("id")))) {
            observer.expectBatch(System.nanoTime(), List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "1", BenchmarkMongoDeliveryObserver.Kind.INSERT)));
            unknownTarget.insertOne(new Document("id", 1L));
            unknownTarget.insertOne(new Document("id", 2L));
            assertThatThrownBy(() -> observer.finish(BOUND))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("unmatched target change for key 2");
        }

        String duplicateTable = "benchmark_delivery_duplicate_test";
        MongoCollection<Document> duplicateTarget = freshCollection(external, duplicateTable);
        try (BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                target(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO, duplicateTable),
                external, views, row -> String.valueOf(row.get("id")))) {
            observer.expectBatch(System.nanoTime(), List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "1", BenchmarkMongoDeliveryObserver.Kind.INSERT)));
            duplicateTarget.insertOne(new Document("id", 1L).append("value", 1));
            duplicateTarget.updateOne(Filters.eq("id", 1L), Updates.set("value", 2));
            assertThatThrownBy(() -> observer.finish(BOUND))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("extra or duplicate target change for key 1");
        }
    }

    @Test
    void wrongOperationTypeCannotPassAsDelivery() {
        String external = SharedMongo.replicaSetUrl("benchmark_delivery_wrong_operation");
        String views = SharedMongo.replicaSetUrl("views");
        String table = "benchmark_delivery_wrong_operation_test";
        MongoCollection<Document> target = freshCollection(external, table);

        try (BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                target(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO, table),
                external, views, row -> String.valueOf(row.get("id")))) {
            observer.expectBatch(System.nanoTime(), List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "1", BenchmarkMongoDeliveryObserver.Kind.UPDATE)));
            target.insertOne(new Document("id", 1L));
            assertThatThrownBy(() -> observer.finish(BOUND))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("target operation mismatch for key 1");
        }
    }

    @Test
    void checkpointsKeepOneStreamOpenAndExcludeTerminalWritesFromMeasuredLatency() {
        String external = SharedMongo.replicaSetUrl("benchmark_delivery_phases");
        String views = SharedMongo.replicaSetUrl("views");
        String table = "benchmark_delivery_phases_test";
        MongoCollection<Document> target = freshCollection(external, table);
        target.insertOne(new Document("id", 1L).append("value", "seed"));

        try (BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                target(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO, table),
                external, views, row -> String.valueOf(row.get("id")))) {
            observer.expectBatch("cold-read", System.nanoTime(), List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "1", BenchmarkMongoDeliveryObserver.Kind.UPDATE)));
            target.updateOne(Filters.eq("id", 1L), Updates.set("value", "cold"));
            assertThat(observer.checkpoint("cold-read", BOUND)).singleElement()
                    .extracting(BenchmarkMongoDeliveryObserver.Delivery::key).isEqualTo("1");
            assertThat(observer.checkpoint("join-idle", BOUND)).isEmpty();

            observer.expectBatch("cdc-update", System.nanoTime(), List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "1", BenchmarkMongoDeliveryObserver.Kind.UPDATE)));
            target.updateOne(Filters.eq("id", 1L), Updates.set("value", "cdc"));
            assertThat(observer.checkpoint("cdc-update", BOUND)).singleElement()
                    .extracting(BenchmarkMongoDeliveryObserver.Delivery::key).isEqualTo("1");

            observer.expectUnmeasured("terminal", List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "2", BenchmarkMongoDeliveryObserver.Kind.INSERT)));
            target.insertOne(new Document("id", 2L).append("value", "terminal"));
            assertThat(observer.checkpoint("terminal", BOUND)).isEmpty();
            String targetId = "observer_test/" + table;
            assertThat(observer.observedCoverage()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    new BenchmarkMongoDeliveryObserver.ObservedKey(
                            "cold-read", targetId, "1", BenchmarkMongoDeliveryObserver.Kind.UPDATE), 1L,
                    new BenchmarkMongoDeliveryObserver.ObservedKey(
                            "cdc-update", targetId, "1", BenchmarkMongoDeliveryObserver.Kind.UPDATE), 1L,
                    new BenchmarkMongoDeliveryObserver.ObservedKey(
                            "terminal", targetId, "2", BenchmarkMongoDeliveryObserver.Kind.INSERT), 1L));
        }
    }

    @Test
    void aLateDuplicateAfterTheFirstCheckpointFailsBeforeTheNextBarrier() {
        String external = SharedMongo.replicaSetUrl("benchmark_delivery_late_duplicate");
        String views = SharedMongo.replicaSetUrl("views");
        String table = "benchmark_delivery_late_duplicate_test";
        MongoCollection<Document> target = freshCollection(external, table);
        target.insertOne(new Document("id", 1L).append("value", "seed"));

        try (BenchmarkMongoDeliveryObserver observer = BenchmarkMongoDeliveryObserver.open(
                target(BenchmarkWorkloadDefinitions.TargetLocation.EXTERNAL_MONGO, table),
                external, views, row -> String.valueOf(row.get("id")))) {
            observer.expectBatch("cold-read", System.nanoTime(), List.of(
                    new BenchmarkMongoDeliveryObserver.ExpectedChange(
                            "1", BenchmarkMongoDeliveryObserver.Kind.UPDATE)));
            target.updateOne(Filters.eq("id", 1L), Updates.set("value", "first"));
            assertThat(observer.checkpoint("cold-read", BOUND)).hasSize(1);

            target.updateOne(Filters.eq("id", 1L), Updates.set("value", "late-duplicate"));
            assertThatThrownBy(() -> observer.checkpoint("cdc-update", BOUND))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("extra or duplicate target change for key 1");
        }
    }

    private static BenchmarkWorkloadDefinitions.TargetExpectation target(
            BenchmarkWorkloadDefinitions.TargetLocation location, String table) {
        return new BenchmarkWorkloadDefinitions.TargetExpectation(
                "observer_test", location, table, BenchmarkWorkloadDefinitions.Projection.COPY, 0, "unused");
    }

    private static MongoCollection<Document> freshCollection(String uri, String table) {
        ConnectionString address = new ConnectionString(uri);
        MongoClient client = MongoClients.create(address);
        CLIENTS.add(client);
        MongoCollection<Document> collection = client.getDatabase(address.getDatabase()).getCollection(table);
        collection.drop();
        return collection;
    }
}
