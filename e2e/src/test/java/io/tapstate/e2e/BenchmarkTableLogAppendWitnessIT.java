package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.OperationType;
import io.tapstate.adapters.mongostore.MongoSrsLogStore;
import io.tapstate.adapters.mongostore.MongoSrsMetaStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsConsumerId;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Qualifies complete append evidence after trim and distinguishes an incomplete replacement delta. */
@RequiresDocker
class BenchmarkTableLogAppendWitnessIT {
    @Test
    void theRealStoreRequiresEveryWriterEvenAfterItsReadCursorPassesTheMarker() {
        String database = "benchmark_table_ack_actual_store_witness";
        String chain = "exact-ack-chain";
        String consumer = SrsConsumerId.of("pipeline", "source").value();
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var db = client.getDatabase(database); db.drop();
            var roots = db.getCollection(MongoStorePort.SRS_META);
            var cursors = db.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS);
            var meta = new MongoSrsMetaStore(client, roots, cursors);
            meta.create(chain, null);
            meta.configureSinkWriters(chain, consumer, Map.of("orders", List.of("first", "second")),
                    ConsumerProgressKind.SRS);
            Document lookup = new Document("miningChainId", chain).append("pipelineId", consumer);
            var binding = BenchmarkTableAckGate.bind(chain, "pipeline", "source", "orders", cursors.find(lookup).first());
            var marker = new BenchmarkTableTerminalObserver.Point("marker", "srs." + chain + ".orders", 7L, 41L, null);
            meta.advanceConsumerReadSeq(chain, consumer, "orders", 999L);
            meta.advanceSinkWriterAcked(chain, consumer, "first", "orders", new ChainPosition(new SourceOrder(7L, 41L), null));
            assertThat(BenchmarkTableAckGate.covers(binding, marker, cursors.find(lookup).first())).isFalse();
            meta.advanceSinkWriterAcked(chain, consumer, "second", "orders", new ChainPosition(new SourceOrder(7L, 40L), null));
            assertThat(BenchmarkTableAckGate.covers(binding, marker, cursors.find(lookup).first())).isFalse();
            meta.advanceSinkWriterAcked(chain, consumer, "second", "orders", new ChainPosition(new SourceOrder(7L, 41L), null));
            assertThat(BenchmarkTableAckGate.covers(binding, marker, cursors.find(lookup).first())).isTrue();
        }
    }

    @Test
    void aFloatingCaptureEpochIsRejectedRatherThanTruncated() {
        String database = "benchmark_table_terminal_fractional_epoch_witness";
        String ring = "srs.fractional-observer-chain.orders";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var collection = client.getDatabase(database).getCollection(MongoStorePort.SRS_LOG);
            collection.drop(); collection.insertOne(new Document("_id", "setup"));
            var marker = new BenchmarkTableTerminalObserver.Marker("orders/cdc-end", ring, "u", 12_000L, "qty", 2L);
            var observer = BenchmarkTableTerminalObserver.open(uri, database, List.of(marker));
            try {
                collection.insertOne(new Document("_id", new Document("ring", ring).append("seq", 41L))
                        .append("epoch", 7.5d).append("op", "u")
                        .append("after", new Document("id", 12_000L).append("qty", 2L)));
                assertThatThrownBy(() -> observer.await(marker.id(), Duration.ofSeconds(10)))
                        .isInstanceOf(AssertionError.class).hasStackTraceContaining("no proven original capture order");
            } finally {
                assertThatThrownBy(observer::close).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("no proven original capture order");
            }
        }
    }

    @Test
    void droppingTheObservedLogCannotLeaveACapturedMarkerQualified() throws Exception {
        String database = "benchmark_table_terminal_invalidation_witness";
        String ring = "srs.invalidated-observer-chain.orders";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var collection = client.getDatabase(database).getCollection(MongoStorePort.SRS_LOG);
            collection.drop(); collection.insertOne(new Document("_id", "setup"));
            var marker = new BenchmarkTableTerminalObserver.Marker("orders/cdc-end", ring, "u", 12_000L, "qty", 2L);
            var observer = BenchmarkTableTerminalObserver.open(uri, database, List.of(marker));
            try {
                new MongoSrsLogStore(collection).store(ring, 41L, new SrsLogRecord(null, Op.UPDATE, 123L,
                        Map.of("id", 12_000L, "qty", 1L), Map.of("id", 12_000L, "qty", 2L), 0L, 7L));
                observer.await(marker.id(), Duration.ofSeconds(10));
                collection.drop();
                Await.until("table log invalidation rejected", Duration.ofSeconds(10), () -> {
                    try { observer.check(); return false; }
                    catch (AssertionError expected) { return true; }
                }, () -> "the captured marker's collection was dropped");
                assertThatThrownBy(observer::check).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("stream was invalidated");
            } finally {
                assertThatThrownBy(observer::close).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("stream was invalidated");
                assertThatThrownBy(observer::close).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("stream was invalidated");
            }
        }
    }

    @Test
    void replacingARetainedMarkerKeyWithAnotherRowCannotHideTheRewrite() throws Exception {
        String database = "benchmark_table_terminal_replacement_witness";
        String ring = "srs.replacement-observer-chain.orders";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var collection = client.getDatabase(database).getCollection(MongoStorePort.SRS_LOG);
            collection.drop(); collection.insertOne(new Document("_id", "setup"));
            var marker = new BenchmarkTableTerminalObserver.Marker("orders/cdc-end", ring, "u", 12_000L, "qty", 2L);
            var observer = BenchmarkTableTerminalObserver.open(uri, database, List.of(marker));
            try {
                new MongoSrsLogStore(collection).store(ring, 41L, new SrsLogRecord(null, Op.UPDATE, 123L,
                        Map.of("id", 12_000L, "qty", 1L), Map.of("id", 12_000L, "qty", 2L), 0L, 7L));
                observer.await(marker.id(), Duration.ofSeconds(10));
                Document key = new Document("ring", ring).append("seq", 41L);
                collection.replaceOne(Filters.eq("_id", key), new Document("_id", key)
                        .append("op", "u").append("epoch", 7L)
                        .append("after", new Document("id", 99L).append("qty", 3L)));
                Await.until("retained marker rewrite rejected", Duration.ofSeconds(2), () -> {
                    try { observer.check(); return false; }
                    catch (AssertionError expected) { return true; }
                }, () -> "a replacement changed the retained marker's row identity");
                assertThatThrownBy(observer::check).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("retained marker log key was rewritten");
            } finally {
                try { observer.close(); }
                catch (AssertionError expectedObserverFailure) {
                    assertThat(expectedObserverFailure).hasStackTraceContaining("retained marker log key was rewritten");
                }
            }
        }
    }

    @Test
    void aSecondExactMarkerIsRejectedEvenWhenItUsesAnotherLogSequence() throws Exception {
        String database = "benchmark_table_terminal_duplicate_witness";
        String ring = "srs.duplicate-observer-chain.orders";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var collection = client.getDatabase(database).getCollection(MongoStorePort.SRS_LOG);
            collection.drop(); collection.insertOne(new Document("_id", "setup"));
            var marker = new BenchmarkTableTerminalObserver.Marker("orders/cdc-end", ring, "u", 12_000L, "qty", 2L);
            var observer = BenchmarkTableTerminalObserver.open(uri, database, List.of(marker));
            var store = new MongoSrsLogStore(collection);
            try {
                var row = new SrsLogRecord(null, Op.UPDATE, 123L,
                        Map.of("id", 12_000L, "qty", 1L), Map.of("id", 12_000L, "qty", 2L), 0L, 7L);
                store.store(ring, 41L, row);
                observer.await(marker.id(), Duration.ofSeconds(10));
                store.store(ring, 42L, row);
                Await.until("duplicate exact log marker rejected", Duration.ofSeconds(10), () -> {
                    try { observer.check(); return false; }
                    catch (AssertionError expected) { return true; }
                }, () -> "the duplicate append has not reached the observer");
                assertThatThrownBy(observer::check).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("duplicate table marker");
            } finally {
                assertThatThrownBy(observer::close).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("duplicate table marker");
            }
        }
    }

    @Test
    void aMarkerWithoutAnOriginalCaptureEpochCannotBeQualified() {
        String database = "benchmark_table_terminal_unknown_epoch_witness";
        String ring = "srs.unknown-observer-chain.orders";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var collection = client.getDatabase(database).getCollection(MongoStorePort.SRS_LOG);
            collection.drop(); collection.insertOne(new Document("_id", "setup"));
            var marker = new BenchmarkTableTerminalObserver.Marker("orders/cdc-end", ring, "u", 12_000L, "qty", 2L);
            var observer = BenchmarkTableTerminalObserver.open(uri, database, List.of(marker));
            try {
                new MongoSrsLogStore(collection).store(ring, 41L, new SrsLogRecord(null, Op.UPDATE, 123L,
                        Map.of("id", 12_000L, "qty", 1L), Map.of("id", 12_000L, "qty", 2L), 0L));
                assertThatThrownBy(() -> observer.await(marker.id(), Duration.ofSeconds(10)))
                        .isInstanceOf(AssertionError.class).hasStackTraceContaining("no proven original capture order");
            } finally {
                assertThatThrownBy(observer::close).isInstanceOf(AssertionError.class)
                        .hasStackTraceContaining("no proven original capture order");
            }
        }
    }

    @Test
    void aBoundedObserverRetainsAnExactTokenlessMarkerAcrossImmediateTrim() throws Exception {
        String database = "benchmark_table_terminal_observer_witness";
        String ring = "srs.exact-observer-chain.orders";
        String uri = SharedMongo.replicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            var collection = client.getDatabase(database).getCollection(MongoStorePort.SRS_LOG);
            collection.drop();
            collection.insertOne(new Document("_id", "setup"));
            var marker = new BenchmarkTableTerminalObserver.Marker("orders/cdc-end", ring,
                    "u", 12_000L, "qty", 2L);
            try (var observer = BenchmarkTableTerminalObserver.open(uri, database, List.of(marker))) {
                var store = new MongoSrsLogStore(collection);
                store.store(ring, 41L, new SrsLogRecord(null, Op.UPDATE, 123L,
                        Map.of("id", 12_000L, "qty", 1L), Map.of("id", 12_000L, "qty", 2L), 0L, 7L));
                store.trim(ring, 41L);
                assertThat(observer.await(marker.id(), Duration.ofSeconds(10)))
                        .isEqualTo(new BenchmarkTableTerminalObserver.Point(marker.id(), ring, 7L, 41L, null));
                assertThat(store.load(ring, 41L)).isEmpty();
            }
        }
    }

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
