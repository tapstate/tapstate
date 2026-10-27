package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded history reads and claim-fenced samples for one original Pipeline state generation. */
final class MongoCacheRateHistoryStore implements RateHistoryStore {
    private final MongoCachePipelineAccess access;
    private final MongoCollection<Document> rows;
    private final Duration retention;

    MongoCacheRateHistoryStore(MongoCachePipelineAccess access) {
        this.access = Objects.requireNonNull(access, "access"); this.retention = access.rateRetention;
        this.rows = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(access.database);
    }

    @Override public void append(RateSample sample) {
        Objects.requireNonNull(sample, "sample"); access.requirePipeline(sample.pipelineId());
        // Identity and bytes belong to this invocation, including every transaction retry.
        Document captured = MongoRateHistoryStore.toDocument(sample).append("_id", new ObjectId())
                .append("scope", MongoPipelinePreparationFacts.scope(access.scope));
        decode(captured);
        transaction(true, session -> { rows.insertOne(session, new Document(captured)); return null; });
    }

    @Override public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId")); Objects.requireNonNull(from, "from"); Objects.requireNonNull(to, "to");
        if (!from.isBefore(to)) throw new IllegalArgumentException("a rate-history range is half-open and non-empty");
        if (limit < 1 || limit > MAX_PAGE_SIZE) throw new IllegalArgumentException("a rate-history page limit is bounded");
        if (after != null && !after.observedAt().isBefore(to)) throw new IllegalArgumentException("a continuation key precedes the upper bound");
        Document filter = filter().append("observedAt", new Document("$gte", Date.from(from)).append("$lt", Date.from(to)));
        if (after != null) filter.append("$or", List.of(new Document("observedAt", new Document("$gt", Date.from(after.observedAt()))),
                new Document("observedAt", Date.from(after.observedAt())).append("_id", new Document("$gt", id(after)))));
        return transaction(false, session -> {
            var found = new ArrayList<Entry>();
            try (var cursor = rows.find(session, filter).sort(order(1)).limit(limit + 1).iterator()) {
                while (cursor.hasNext()) found.add(decode(cursor.next()));
            }
            return new Page(found.size() > limit ? found.subList(0, limit) : found, found.size() > limit);
        });
    }
    @Override public Optional<Entry> read(String pipelineId, Key key) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId")); Objects.requireNonNull(key, "key");
        Document filter = filter().append("_id", id(key)).append("observedAt", Date.from(key.observedAt()));
        return transaction(false, session -> Optional.ofNullable(rows.find(session, filter).first()).map(this::decode));
    }
    @Override public Optional<Entry> predecessor(String pipelineId, Instant at) { return boundary(pipelineId, at, true); }
    @Override public Optional<Entry> successor(String pipelineId, Instant at) { return boundary(pipelineId, at, false); }
    private Optional<Entry> boundary(String pipelineId, Instant at, boolean before) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId")); Objects.requireNonNull(at, "at");
        Document filter = filter().append("observedAt", new Document(before ? "$lt" : "$gte", Date.from(at)));
        return transaction(false, session -> Optional.ofNullable(rows.find(session, filter).sort(order(before ? -1 : 1)).limit(1).first()).map(this::decode));
    }
    @Override public void deleteAll(String pipelineId) {
        access.requirePipeline(Objects.requireNonNull(pipelineId, "pipelineId"));
        transaction(true, session -> { rows.deleteMany(session, filter()); return null; });
    }
    @Override public Duration retention() { return retention; }

    private Document filter() {
        var scope = access.scope;
        return new Document("pipelineId", scope.pipelineId()).append("scope.clusterId", scope.clusterId())
                .append("scope.bindingVersion", scope.bindingVersion()).append("scope.cacheIncarnation", scope.cacheIncarnation())
                .append("scope.pipelineId", scope.pipelineId()).append("scope.stateGeneration", scope.stateGeneration());
    }
    private Entry decode(Document document) {
        try {
            if (!(document.get("_id") instanceof ObjectId id) || !access.scope.pipelineId().equals(document.get("pipelineId"))
                    || !(document.get("scope") instanceof Document scope) || !access.scope.equals(MongoPipelinePreparationFacts.scope(scope))) throw unreadable();
            Document payload = new Document(document); payload.remove("_id"); payload.remove("scope");
            RateSample sample = MongoRateHistoryStore.toSample(payload);
            if (!payload.equals(MongoRateHistoryStore.toDocument(sample))) throw unreadable();
            return new Entry(new Key(sample.observedAt(), id.toHexString()), sample);
        } catch (TapstateException | IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private static ObjectId id(Key key) { try { return new ObjectId(key.internalKey()); }
        catch (IllegalArgumentException malformed) { throw new IllegalArgumentException("a Mongo rate-history key is not an ObjectId"); } }
    private static Document order(int direction) { return new Document("observedAt", direction).append("_id", direction); }
    private TapstateException unreadable() { return access.unreadable("rate-history"); }
    private <T> T transaction(boolean mutation, java.util.function.Function<ClientSession, T> action) {
        return access.metadataTransaction(SystemCollections.CACHE_PIPELINE_RATE_HISTORY, mutation, action);
    }
}
