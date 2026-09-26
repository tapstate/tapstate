package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RateHistoryStore;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * The MongoDB history of movement samples: one document per sample, appended and never overwritten,
 * left to expire by the server. Samples leave by age and by nothing else: the collection carries an
 * expiring index on {@code observedAt}, so a document older than the retention is dropped by the server's
 * own sweep — which runs whether or not anything is still writing, and that is the property this store
 * exists for. A writer trimming on its way in would never trim the history of a pipeline that has stopped.
 *
 * <p><strong>The timestamp is a BSON date, never a string.</strong> An expiring index over a string
 * matches nothing and drops nothing, and the collection would grow forever with an index on it that
 * looks exactly right. The same date is what the range read sorts and bounds by.
 *
 * <p>The retention is configured, and this store writes it onto the index at construction: created when
 * absent, altered in place when the existing expiry differs, left alone when it matches. A refusal at
 * that step is a startup failure, on purpose — a store that carried on with the old expiry under a new
 * configuration would be expiring on a schedule nobody configured, and nothing watching the data would
 * say so. The server's sweep runs about once a minute, so a sample leaves some time after it falls due
 * rather than on the moment.
 */
public final class MongoRateHistoryStore implements RateHistoryStore {

    private static final long APPEND_DEADLINE_SECONDS = 5;

    /** How long a sample is kept when nothing says otherwise. */
    public static final Duration DEFAULT_RETENTION = Duration.ofDays(15);

    /** The field a sample names its pipeline under; published so a reader of the raw collection spells it once. */
    public static final String PIPELINE_ID = "pipelineId";
    /** The field a sample is dated by, and the one the expiring index is over. */
    public static final String OBSERVED_AT = "observedAt";
    static final String INTERNAL_ID = "_id";
    static final String COUNTERS = "counters";
    static final String LAG = "lag";
    static final String COUNTING_SINCE = "countingSince";

    private final MongoCollection<Document> collection;
    private final Duration retention;

    public MongoRateHistoryStore(MongoDatabase database, MongoCollection<Document> collection, Duration retention) {
        Objects.requireNonNull(database, "database");
        this.collection = Objects.requireNonNull(collection, "collection");
        this.retention = Objects.requireNonNull(retention, "retention");
        if (retention.isNegative() || retention.isZero()) {
            throw new IllegalArgumentException("a retention is a positive length of time: " + retention);
        }
        // The expiring index with the configured retention, and the read index; both idempotent, and the
        // first one altered in place when a configured retention differs from the one on the index.
        StoreIo.run(() -> {
            IndexEnsure.ensure(database, collection, new SystemCollections.IndexSpec(
                    List.of(OBSERVED_AT), false, retention.toSeconds()));
            IndexEnsure.ensure(database, collection, new SystemCollections.IndexSpec(
                    List.of(PIPELINE_ID, OBSERVED_AT, INTERNAL_ID), false));
        });
    }

    @Override
    public void append(RateSample sample) {
        Objects.requireNonNull(sample, "sample");
        StoreIo.run(() -> collection.withTimeout(APPEND_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .insertOne(toDocument(sample)));
    }

    @Override
    public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        requireRange(from, to);
        requireLimit(limit);
        if (after != null && !after.observedAt().isBefore(to)) {
            throw new IllegalArgumentException("a rate-history continuation key precedes the requested upper bound");
        }

        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq(PIPELINE_ID, pipelineId));
        filters.add(Filters.gte(OBSERVED_AT, Date.from(from)));
        filters.add(Filters.lt(OBSERVED_AT, Date.from(to)));
        if (after != null) {
            Date afterAt = Date.from(after.observedAt());
            ObjectId afterId = internalId(after);
            filters.add(Filters.or(
                    Filters.gt(OBSERVED_AT, afterAt),
                    Filters.and(Filters.eq(OBSERVED_AT, afterAt), Filters.gt(INTERNAL_ID, afterId))));
        }

        List<Entry> candidates = StoreIo.call(() -> {
            List<Entry> found = new ArrayList<>();
            collection.find(Filters.and(filters))
                    .sort(Sorts.orderBy(Sorts.ascending(OBSERVED_AT), Sorts.ascending(INTERNAL_ID)))
                    .limit(limit + 1)
                    .forEach(document -> found.add(toEntry(document)));
            return found;
        });
        boolean hasMore = candidates.size() > limit;
        List<Entry> entries = hasMore ? candidates.subList(0, limit) : candidates;
        return new Page(entries, hasMore);
    }

    @Override
    public Optional<Entry> predecessor(String pipelineId, Instant at) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(at, "at");
        Document found = StoreIo.call(() -> collection.find(Filters.and(
                        Filters.eq(PIPELINE_ID, pipelineId),
                        Filters.lt(OBSERVED_AT, Date.from(at))))
                .sort(Sorts.orderBy(Sorts.descending(OBSERVED_AT), Sorts.descending(INTERNAL_ID)))
                .limit(1)
                .first());
        return Optional.ofNullable(found).map(MongoRateHistoryStore::toEntry);
    }

    @Override
    public Optional<Entry> read(String pipelineId, Key key) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(key, "key");
        Document found = StoreIo.call(() -> collection.find(Filters.and(
                        Filters.eq(PIPELINE_ID, pipelineId),
                        Filters.eq(OBSERVED_AT, Date.from(key.observedAt())),
                        Filters.eq(INTERNAL_ID, internalId(key))))
                .limit(1)
                .first());
        return Optional.ofNullable(found).map(MongoRateHistoryStore::toEntry);
    }

    @Override
    public Optional<Entry> successor(String pipelineId, Instant at) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(at, "at");
        Document found = StoreIo.call(() -> collection.find(Filters.and(
                        Filters.eq(PIPELINE_ID, pipelineId),
                        Filters.gte(OBSERVED_AT, Date.from(at))))
                .sort(Sorts.orderBy(Sorts.ascending(OBSERVED_AT), Sorts.ascending(INTERNAL_ID)))
                .limit(1)
                .first());
        return Optional.ofNullable(found).map(MongoRateHistoryStore::toEntry);
    }

    @Override
    public void deleteAll(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        StoreIo.run(() -> collection.deleteMany(Filters.eq(PIPELINE_ID, pipelineId)));
    }

    @Override
    public Duration retention() {
        return retention;
    }

    /**
     * Maps a sample to its stored document; the two instants are BSON dates. Published so a fixture can
     * lay down a history in the shape the product writes, rather than in one spelled beside it.
     */
    public static Document toDocument(RateSample sample) {
        Document counters = new Document();
        sample.counters().forEach(counters::append);
        Document lag = new Document();
        sample.lag().forEach(lag::append);
        Document document = new Document(PIPELINE_ID, sample.pipelineId())
                .append(OBSERVED_AT, Date.from(sample.observedAt()))
                .append(COUNTERS, counters)
                .append(LAG, lag);
        if (sample.countingSince() != null) {
            document.append(COUNTING_SINCE, Date.from(sample.countingSince()));
        }
        return document;
    }

    /** Reconstructs a sample; a missing or wrong-typed cell is corruption, surfaced coded rather than as a cast. */
    static RateSample toSample(Document document) {
        Object rawId = document.get(PIPELINE_ID);
        if (!(rawId instanceof String pipelineId)) {
            throw corrupt(String.valueOf(rawId), PIPELINE_ID);
        }
        Object rawAt = document.get(OBSERVED_AT);
        if (!(rawAt instanceof Date observedAt)) {
            throw corrupt(pipelineId, OBSERVED_AT);
        }
        Object rawSince = document.get(COUNTING_SINCE);
        if (rawSince != null && !(rawSince instanceof Date)) {
            throw corrupt(pipelineId, COUNTING_SINCE);
        }
        return new RateSample(pipelineId, observedAt.toInstant(),
                longs(document.get(COUNTERS), pipelineId, COUNTERS),
                longs(document.get(LAG), pipelineId, LAG),
                rawSince == null ? null : ((Date) rawSince).toInstant());
    }

    private static Entry toEntry(Document document) {
        Object rawId = document.get(INTERNAL_ID);
        if (!(rawId instanceof ObjectId id)) {
            Object pipelineId = document.get(PIPELINE_ID);
            throw corrupt(String.valueOf(pipelineId), INTERNAL_ID);
        }
        RateSample sample = toSample(document);
        return new Entry(new Key(sample.observedAt(), id.toHexString()), sample);
    }

    private static ObjectId internalId(Key key) {
        try {
            return new ObjectId(key.internalKey());
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("a Mongo rate-history key is not an ObjectId", malformed);
        }
    }

    private static void requireRange(Instant from, Instant to) {
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("a rate-history range is half-open and non-empty");
        }
    }

    private static void requireLimit(int limit) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("a rate-history page limit is between 1 and " + MAX_PAGE_SIZE);
        }
    }

    private static Map<String, Long> longs(Object raw, String pipelineId, String field) {
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Document cells)) {
            throw corrupt(pipelineId, field);
        }
        Map<String, Long> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : cells.entrySet()) {
            if (!(entry.getValue() instanceof Number number)) {
                throw corrupt(pipelineId, field);
            }
            out.put(entry.getKey(), number.longValue());
        }
        return out;
    }

    private static TapstateException corrupt(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(id), "field", field), null);
    }
}
