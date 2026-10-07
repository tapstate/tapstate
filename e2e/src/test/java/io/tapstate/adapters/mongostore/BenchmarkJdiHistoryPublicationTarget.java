package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.HistoryRollupStore;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Calls the current event/rollup production stores without loading their types in reference targets. */
public final class BenchmarkJdiHistoryPublicationTarget {
    private BenchmarkJdiHistoryPublicationTarget() { }
    public static void main(String[] args) {
        try { run(args[0]); }
        catch (Throwable failure) { BenchmarkJdiEncoderTarget.reportFailure(failure); System.exit(2); }
    }

    private static void run(String mode) throws Exception {
        boolean eventMode = mode.startsWith("store-event");
        BenchmarkJdiEncoderTarget.origin(DocumentCodec.class);
        BenchmarkJdiEncoderTarget.origin(eventMode ? MongoPipelineEventStore.class : MongoHistoryRollupStore.class);
        BenchmarkJdiEncoderTarget.origin(eventMode ? PipelineEvent.class : HistoryRollupStore.Bucket.class);
        String uri = System.getenv("TAPSTATE_JDI_WITNESS_MONGO_URI");
        if (uri == null || uri.isBlank()) { throw new AssertionError("history store witness has no private connection input"); }
        try (var client = MongoClients.create(uri)) {
            var database = client.getDatabase("jdi_cost_witness");
            String collectionName = eventMode ? "pipeline_events" : "pipeline_history_rollups";
            var collection = database.getCollection(collectionName); collection.drop();
            Instant at = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
            if (eventMode) {
                var store = new MongoPipelineEventStore(database, collection, Duration.ofDays(15));
                var event = new PipelineEvent("cost-event", "proof", "cost-inc", 7L,
                        PipelineEvent.Kind.STATE_CHANGED, at, PipelineState.NEW, PipelineState.RUNNING,
                        null, null, null);
                BenchmarkJdiEncoderTarget.ready();
                store.append(event);
                extra(mode);
                BenchmarkJdiEncoderTarget.done();
                if (!MongoPipelineEventStore.toEvent(collection.find().first()).equals(event)) {
                    throw new AssertionError("event publication did not round trip");
                }
            } else {
                var store = new MongoHistoryRollupStore(database, collection, Duration.ofDays(15));
                Instant start = Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(), 300) * 300 - 300);
                var rate = new HistoryRollupStore.Rate(BigDecimal.valueOf(20), BigDecimal.ONE, BigDecimal.TEN);
                var fragment = new HistoryRollupStore.Fragment(0, HistoryRollupStore.StartReason.CONTINUATION,
                        start, start.plusSeconds(120), rate, rate,
                        List.of(new HistoryRollupStore.Lag("orders", start.plusSeconds(120), 3, 8)),
                        new HistoryRollupStore.CounterStats(BigDecimal.valueOf(20), 120_000_000_000L, BigDecimal.TEN),
                        new HistoryRollupStore.CounterStats(BigDecimal.valueOf(20), 120_000_000_000L, BigDecimal.TEN),
                        new io.tapstate.spi.store.RateHistoryStore.Key(start.plusSeconds(120), "source-key"), start.plusSeconds(120));
                var bucket = new HistoryRollupStore.Bucket(new HistoryRollupStore.Key("proof",
                        HistoryRollupStore.Scope.incarnation("cost-inc"), HistoryRollupStore.Resolution.PT5M, start),
                        at, at, at.plusSeconds(300), false, List.of(fragment), List.of(), 3);
                BenchmarkJdiEncoderTarget.ready();
                store.upsert(bucket);
                extra(mode);
                BenchmarkJdiEncoderTarget.done();
                if (!store.read(bucket.key()).orElseThrow().equals(bucket)) {
                    throw new AssertionError("nonempty rollup publication did not round trip");
                }
            }
        }
    }

    private static void extra(String mode) {
        if (mode.endsWith("-conversion")) {
            try (var writer = new org.bson.BsonDocumentWriter(new org.bson.BsonDocument())) {
                new DocumentCodec().encode(writer, new Document("redundant", 1L), EncoderContext.builder().build());
            }
        }
        if (!mode.endsWith("-extra")) { return; }
        try (BasicOutputBuffer bytes = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(bytes)) {
            new DocumentCodec().encode(writer, new Document("redundant", 1L), EncoderContext.builder().build());
            if (bytes.getSize() <= 0) { throw new AssertionError("redundant encoding did not produce bytes"); }
        }
    }
}
