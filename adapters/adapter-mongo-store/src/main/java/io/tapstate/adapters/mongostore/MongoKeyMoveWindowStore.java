package io.tapstate.adapters.mongostore;

import com.mongodb.MongoWriteException;
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
import org.bson.Document;

import java.util.Map;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.UUID;

/** One majority-durable interval per pipeline. A new revision is issued on every admission. */
final class MongoKeyMoveWindowStore implements PipelineKeyMoveWindows {
    private final MongoCollection<Document> rows;
    private final Runnable active;

    MongoKeyMoveWindowStore(MongoDatabase database, Runnable active) {
        this.rows = SystemCollections.PIPELINE_KEY_MOVE_WINDOWS.on(database)
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.LINEARIZABLE)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.active = active;
    }

    @Override public Optional<Window> read(String pipelineId) {
        active.run();
        return StoreIo.call(() -> Optional.ofNullable(rows.find(new Document("_id", pipelineId)).first())
                .map(MongoKeyMoveWindowStore::decode));
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
                        verifyAuthority.run();
                        return next;
                    }
                } else {
                    try {
                        rows.insertOne(encode(next));
                    } catch (MongoWriteException raced) {
                        if (raced.getError().getCode() != 11000) throw raced;
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
