package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.OperationType;
import io.tapstate.adapters.mongostore.MongoSrsLogStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Qualifies complete append evidence after trim and distinguishes an incomplete replacement delta. */
@RequiresDocker
class BenchmarkTableLogAppendWitnessIT {
    @Test
    void aTokenlessAppendSurvivesTrimWhileAReplacementCannotSupplyACompleteRecord() {
        String databaseName = "benchmark_table_log_append_witness";
        String ring = "srs.exact-chain.orders";
        try (var client = MongoClients.create(SharedMongo.replicaSetUrl(databaseName))) {
            var collection = client.getDatabase(databaseName).getCollection(MongoStorePort.SRS_LOG);
            collection.drop();
            collection.insertOne(new Document("_id", "setup"));
            var store = new MongoSrsLogStore(collection);
            try (var changes = collection.watch(List.of(Aggregates.match(Filters.and(
                    Filters.in("operationType", "insert", "replace", "update"),
                    Filters.eq("documentKey._id.ring", ring)))))
                    .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                SrsLogRecord tokenless = new SrsLogRecord(null, Op.UPDATE, 123L,
                        Map.of("id", 12_000L, "qty", 1L), Map.of("id", 12_000L, "qty", 2L), 0L, 7L);
                SrsLogRecord tokenBearing = new SrsLogRecord("batch-ending-on-another-row", Op.UPDATE, 124L,
                        Map.of("id", 12_001L, "qty", 1L), Map.of("id", 12_001L, "qty", 2L), 0L, 7L);
                store.storeAll(ring, 41L, List.of(tokenless, tokenBearing));
                store.store(ring, 41L, new SrsLogRecord(null, Op.UPDATE, 125L,
                        Map.of("id", 12_000L, "qty", 2L), Map.of("id", 12_000L, "qty", 3L), 0L, 7L));
                store.trim(ring, 42L);
                assertThat(store.load(ring, 41L)).isEmpty();

                List<Document> captured = new ArrayList<>();
                List<String> operations = new ArrayList<>();
                long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (captured.size() < 3 && System.nanoTime() < deadline) {
                    var change = changes.tryNext();
                    if (change == null) { continue; }
                    operations.add(change.getOperationTypeString());
                    var key = change.getDocumentKey().getDocument("_id");
                    assertThat(key.getString("ring").getValue()).isEqualTo(ring);
                    Document event;
                    if (change.getOperationType() == OperationType.UPDATE) {
                        assertThat(change.getUpdateDescription()).isNotNull();
                        event = Document.parse(change.getUpdateDescription().getUpdatedFields().toJson());
                    } else {
                        assertThat(change.getOperationType()).isIn(OperationType.INSERT, OperationType.REPLACE);
                        event = change.getFullDocument();
                        assertThat(event).isNotNull();
                    }
                    captured.add(new Document("seq", key.getNumber("seq").longValue())
                            .append("epoch", event.get("epoch"))
                            .append("after", event.get("after"))
                            .append("srcToken", event.get("srcToken")));
                }
                assertThat(captured).hasSize(3);
                System.out.println("table-log-append-witness operations=" + operations + " records=" + captured);
                assertThat(captured).extracting(document -> document.getLong("seq")).containsExactly(41L, 42L, 41L);
                assertThat(operations).containsExactly("insert", "insert", "update");
                assertThat(captured.subList(0, 2))
                        .extracting(document -> ((Number) document.get("epoch")).longValue())
                        .containsOnly(7L);
                assertThat(captured.getFirst().get("srcToken")).isNull();
                assertThat(captured.getFirst().get("after", Document.class)).containsEntry("id", 12_000L)
                        .containsEntry("qty", 2L);
                assertThat(captured.get(1).getString("srcToken")).isEqualTo("batch-ending-on-another-row");
                // Mongo reports only the changed nested value; missing identity fields are not inferred.
                assertThat(captured.get(2)).containsEntry("epoch", null).containsEntry("after", null);
            }
        }
    }
}
