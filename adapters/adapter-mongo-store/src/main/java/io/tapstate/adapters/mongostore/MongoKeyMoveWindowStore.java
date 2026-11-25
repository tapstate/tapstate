package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineKeyMoveWindows;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Map;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.UUID;

/** One majority-durable interval per pipeline. A new revision is issued on every admission. */
final class MongoKeyMoveWindowStore implements PipelineKeyMoveWindows {
    private final MongoDatabase database;
    private final MongoCollection<Document> rows;
    private final Runnable active;

    MongoKeyMoveWindowStore(MongoDatabase database, Runnable active) {
        this.database = database;
        this.rows = SystemCollections.PIPELINE_KEY_MOVE_WINDOWS.on(database)
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.LINEARIZABLE)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.active = active;
    }

    @Override public Optional<Window> read(String pipelineId) {
        active.run();
        return StoreIo.call(() -> {
            var stored = rows.find(new Document("_id", pipelineId)).first();
            if (stored != null) return Optional.of(decode(stored));
            requireCollection(pipelineId);
            return Optional.empty();
        });
    }

    private BsonBinary requireCollection(String pipelineId) {
        var collection = database.withReadPreference(ReadPreference.primary()).listCollections(BsonDocument.class)
                .filter(new Document("name", rows.getNamespace().getCollectionName())).first();
        if (collection == null || !collection.isDocument("info") || !collection.getDocument("info").isBinary("uuid")) {
            throw PipelineKeyMoveWindows.unresolved(pipelineId);
        }
        return collection.getDocument("info").getBinary("uuid");
    }

    private boolean insertFirst(Window next) {
        String pipeline = next.scope().pipelineId();
        // Pin this insert to the namespace just inspected. An unqualified insert can recreate a
        // dropped collection and erase the evidence that active pipelines lost their intervals.
        var insert = new Document("insert", rows.getNamespace().getCollectionName())
                .append("collectionUUID", requireCollection(pipeline)).append("documents", java.util.List.of(encode(next)))
                .append("writeConcern", rows.getWriteConcern().asDocument());
        Document result;
        try {
            result = database.runCommand(insert, ReadPreference.primary());
        } catch (MongoException failure) {
            if (failure.getCode() == 361 || failure.getCode() == 26) throw PipelineKeyMoveWindows.unresolved(pipeline);
            throw failure;
        }
        if (result.containsKey("writeConcernError")) {
            throw new MongoException(64, "key move admission was not acknowledged by a majority");
        }
        // The command inserts one document, so the reply can carry only its write error.
        var writeErrors = result.getList("writeErrors", Document.class, java.util.List.of());
        if (!writeErrors.isEmpty()) {
            int code = writeErrors.getFirst().getInteger("code");
            if (code == 361 || code == 26) throw PipelineKeyMoveWindows.unresolved(pipeline);
            if (code == 11000) return false;
            throw new MongoException(code, "key move admission was refused");
        }
        if (!Integer.valueOf(1).equals(result.get("n"))) throw PipelineKeyMoveWindows.unresolved(pipeline);
        return true;
    }

    @Override public Window arm(PipelineIntentScope scope, long executionId, String activation, Stream stream, Range changes,
            Window lastAdmission, Runnable verifyAuthority) {
        return StoreIo.call(() -> {
            // A matching revision proves both the identity and all bounds used for this replacement.
            // Most batches extend their own last admission without a second round trip.
            var previous = Optional.ofNullable(lastAdmission);
            for (;;) {
                var ranges = new LinkedHashMap<Stream, Range>();
                if (previous.isPresent()) {
                    var open = previous.get();
                    if (!open.scope().equals(scope) || open.executionId() != executionId || !open.activation().equals(activation))
                        throw PipelineKeyMoveWindows.unresolved(scope.pipelineId());
                    ranges.putAll(open.ranges());
                }
                ranges.merge(stream, changes, (a,b) -> {
                    if (a.epoch() != b.epoch()) throw PipelineKeyMoveWindows.unresolved(scope.pipelineId());
                    return new Range(a.epoch(), Math.min(a.first(), b.first()), Math.max(a.last(), b.last()));
                });
                var next = new Window(scope, executionId, activation, UUID.randomUUID(), ranges);
                active.run();
                if (previous.isPresent()) {
                    if (rows.replaceOne(encode(previous.get()), encode(next)).getMatchedCount() == 1) {
                        try {
                            verifyAuthority.run();
                        } catch (RuntimeException lost) {
                            try {
                                rows.replaceOne(match(next), encode(previous.get()));
                            } catch (RuntimeException cleanup) {
                                lost.addSuppressed(cleanup);
                            }
                            throw lost;
                        }
                        return next;
                    }
                } else {
                    if (!insertFirst(next)) {
                        previous = read(scope.pipelineId());
                        continue;
                    }
                    try {
                        verifyAuthority.run();
                    } catch (RuntimeException lost) {
                        // This first admission has released no row. Another admission always changes its revision.
                        try {
                            rows.deleteOne(match(next));
                        } catch (RuntimeException cleanup) {
                            lost.addSuppressed(cleanup);
                        }
                        throw lost;
                    }
                    return next;
                }
                previous = read(scope.pipelineId());
            }
        });
    }

    @Override public boolean retire(Window observed, Map<Stream, SourceOrder> resumable, Map<Stream, SourceOrder> settled) {
        for (var entry : observed.ranges().entrySet()) {
            var range = entry.getValue();
            if (!range.coveredBy(resumable.get(entry.getKey())) || !range.coveredBy(settled.get(entry.getKey()))) return false;
        }
        active.run();
        // Match the complete observed interval, including its revision. Reopen and extension both change it.
        return StoreIo.call(() -> rows.deleteOne(encode(observed)).getDeletedCount() == 1);
    }

    private static Document match(Window window) {
        return new Document("_id", window.scope().pipelineId()).append("revision", window.revision().toString());
    }

    private static Document encode(Window window) {
        var scope = window.scope();
        var ranges = window.ranges().entrySet().stream().sorted(Map.Entry.comparingByKey(java.util.Comparator
                .comparing(Stream::chain).thenComparing(Stream::source).thenComparing(Stream::table)
                .thenComparing(Stream::privateCapture, java.util.Comparator.nullsFirst(String::compareTo)))).map(entry ->
                new Document("chain", entry.getKey().chain()).append("source", entry.getKey().source())
                        .append("table", entry.getKey().table()).append("privateCapture", entry.getKey().privateCapture()).append("epoch", entry.getValue().epoch())
                        .append("first", entry.getValue().first()).append("last", entry.getValue().last())).toList();
        return match(window).append("formatVersion", 1).append("clusterId", scope.clusterId())
                .append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation())
                .append("stateGeneration", scope.stateGeneration()).append("executionId", window.executionId())
                .append("activation", window.activation()).append("ranges", ranges);
    }

    private static Window decode(Document row) {
        try {
            if (row.getInteger("formatVersion") != 1) throw new IllegalArgumentException();
            var scope = new PipelineIntentScope(row.getString("clusterId"), row.getLong("bindingVersion"),
                    row.getString("cacheIncarnation"), row.getString("_id"), row.getLong("stateGeneration"));
            var ranges = new LinkedHashMap<Stream, Range>();
            for (var item : row.getList("ranges", Document.class)) {
                if (ranges.put(new Stream(item.getString("chain"), item.getString("source"),
                        item.getString("table"), item.getString("privateCapture")), new Range(item.getLong("epoch"), item.getLong("first"),
                        item.getLong("last"))) != null) throw new IllegalArgumentException();
            }
            return new Window(scope, row.getLong("executionId"), row.getString("activation"), UUID.fromString(row.getString("revision")), ranges);
        } catch (RuntimeException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", "pipeline-key-move-window", "field", "interval"), invalid);
        }
    }
}
