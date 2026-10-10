package io.tapstate.e2e;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.cursor.TimeoutMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.bson.RawBsonDocument;

/** Three exact metadata reads share the observer's owned client after its clock refusal. */
final class BenchmarkNativeOperationWallLookup implements BenchmarkNativeOperationWallEvidence.Lookup {
    private final MongoClient client;
    private final String namespace;

    BenchmarkNativeOperationWallLookup(MongoClient client, String namespace) {
        this.client = Objects.requireNonNull(client);
        this.namespace = Objects.requireNonNull(namespace);
    }

    @Override
    public BenchmarkNativeOperationWallEvidence.ReadResult read(BenchmarkNativeOperationWallEvidence.Query query) {
        long started = System.nanoTime();
        List<BenchmarkNativeOperationWallEvidence.NativeDocument> documents = new ArrayList<>(2);
        try {
            if (!namespace.equals(query.namespace())) { throw new AssertionError("native query is outside the owned namespace"); }
            long remaining = Math.subtractExact(query.deadlineNanos(), started);
            long timeoutMillis = Math.min(query.maxTimeMillis(), TimeUnit.NANOSECONDS.toMillis(remaining));
            if (timeoutMillis <= 0) { throw new AssertionError("native query has no positive bounded timeout left"); }
            var collection = client.getDatabase("local").getCollection("oplog.rs", RawBsonDocument.class)
                    .withReadPreference(ReadPreference.primary()).withTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
            try (var cursor = collection.find(query.filter()).projection(query.projection())
                    .limit(query.limit()).batchSize(query.limit())
                    .maxTime(query.maxTimeMillis(), TimeUnit.MILLISECONDS)
                    .timeoutMode(TimeoutMode.CURSOR_LIFETIME).iterator()) {
                while (documents.size() < query.limit() && cursor.hasNext()) {
                    RawBsonDocument document = cursor.next();
                    var buffer = document.getByteBuffer();
                    byte[] bytes;
                    try {
                        if (buffer.remaining() > BenchmarkNativeOperationWallEvidence.MAX_RECORD_BYTES) {
                            throw new AssertionError("native projected BSON exceeds the record budget");
                        }
                        bytes = new byte[buffer.remaining()]; buffer.asNIO().get(bytes);
                    } finally { buffer.release(); }
                    documents.add(new BenchmarkNativeOperationWallEvidence.NativeDocument(document, bytes));
                }
            }
            return new BenchmarkNativeOperationWallEvidence.ReadResult(documents, started, System.nanoTime(), null);
        } catch (RuntimeException | AssertionError unavailable) {
            return new BenchmarkNativeOperationWallEvidence.ReadResult(documents, started, System.nanoTime(),
                    "NATIVE_METADATA_QUERY_FAILED:" + unavailable.getClass().getSimpleName());
        }
    }
}
