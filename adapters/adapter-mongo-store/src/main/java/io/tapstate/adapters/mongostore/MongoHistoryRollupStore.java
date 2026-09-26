package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** A bounded, replaceable Mongo cache of closed history buckets, separate from authoritative raw samples. */
public final class MongoHistoryRollupStore implements HistoryRollupStore {

    public static final Duration DEFAULT_RETENTION = Duration.ofDays(15);

    static final String ID = "_id";
    static final String PIPELINE_ID = "pipelineId";
    static final String SCOPE_KEY = "scopeKey";
    static final String INCARNATION_ID = "pipelineIncarnationId";
    static final String RESOLUTION = "resolution";
    static final String BUCKET_START = "bucketStart";
    static final String COMPUTED_AT = "computedAt";
    static final String INPUT_READ_STARTED_AT = "inputReadStartedAt";
    static final String VALID_UNTIL = "validUntil";
    static final String REQUIRES_FINER_RESOLUTION = "requiresFinerResolution";
    static final String FRAGMENTS = "fragments";
    static final String GAPS = "gaps";

    private static final String LEGACY_SCOPE = "legacy";
    private static final String INCARNATION_PREFIX = "incarnation:";
    private static final long WRITE_DEADLINE_SECONDS = 5;

    private final MongoCollection<Document> collection;
    private final Duration retention;

    public MongoHistoryRollupStore(MongoDatabase database, MongoCollection<Document> collection,
            Duration retention) {
        Objects.requireNonNull(database, "database");
        this.collection = Objects.requireNonNull(collection, "collection");
        this.retention = Objects.requireNonNull(retention, "retention");
        if (retention.isNegative() || retention.isZero() || retention.getNano() != 0) {
            throw new IllegalArgumentException("rollup retention is a positive whole number of seconds");
        }
        StoreIo.run(() -> {
            IndexEnsure.ensure(database, collection, new SystemCollections.IndexSpec(
                    List.of(BUCKET_START), false, retention.toSeconds()));
            IndexEnsure.ensure(database, collection, new SystemCollections.IndexSpec(
                    List.of(PIPELINE_ID, SCOPE_KEY, RESOLUTION, BUCKET_START), false));
        });
    }

    @Override
    public void upsert(Bucket bucket) {
        Objects.requireNonNull(bucket, "bucket");
        Document proposed = toDocument(bucket);
        StoreIo.run(bucket.key().pipelineId(), () -> collection.withTimeout(WRITE_DEADLINE_SECONDS,
                TimeUnit.SECONDS).replaceOne(Filters.eq(ID, proposed.get(ID)), proposed,
                        new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<Bucket> read(Key key) {
        Objects.requireNonNull(key, "key");
        return Optional.ofNullable(StoreIo.call(() -> collection.find(Filters.eq(ID, documentId(key))).first()))
                .map(MongoHistoryRollupStore::toBucket);
    }

    @Override
    public List<Bucket> readRange(String pipelineId, Scope scope, Resolution resolution,
            Instant from, Instant to, int limit) {
        nonBlank(pipelineId, "pipelineId");
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(resolution, "resolution");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        if (!from.isBefore(to) || limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("a rollup range is non-empty and its page is bounded");
        }
        return StoreIo.call(() -> {
            List<Bucket> found = new ArrayList<>();
            collection.find(Filters.and(Filters.eq(PIPELINE_ID, pipelineId),
                            Filters.eq(SCOPE_KEY, scopeKey(scope)),
                            Filters.eq(RESOLUTION, resolution.name()),
                            Filters.gte(BUCKET_START, Date.from(ceilMillis(from))),
                            Filters.lt(BUCKET_START, Date.from(ceilMillis(to)))))
                    .sort(Sorts.ascending(BUCKET_START))
                    .limit(limit)
                    .forEach(document -> found.add(toBucket(document)));
            return List.copyOf(found);
        });
    }

    @Override
    public void deleteIncarnation(String pipelineId, String incarnationId) {
        nonBlank(pipelineId, "pipelineId");
        nonBlank(incarnationId, "incarnationId");
        StoreIo.run(() -> collection.withTimeout(WRITE_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .deleteMany(Filters.and(Filters.eq(PIPELINE_ID, pipelineId),
                        Filters.eq(SCOPE_KEY, scopeKey(Scope.incarnation(incarnationId))),
                        Filters.eq(INCARNATION_ID, incarnationId))));
    }

    @Override
    public void deleteLegacy(String pipelineId) {
        nonBlank(pipelineId, "pipelineId");
        StoreIo.run(() -> collection.withTimeout(WRITE_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .deleteMany(Filters.and(Filters.eq(PIPELINE_ID, pipelineId),
                        Filters.eq(SCOPE_KEY, LEGACY_SCOPE), Filters.exists(INCARNATION_ID, false))));
    }

    @Override
    public Duration retention() {
        return retention;
    }

    static Document toDocument(Bucket bucket) {
        Key key = bucket.key();
        Document document = new Document(ID, documentId(key))
                .append(PIPELINE_ID, key.pipelineId())
                .append(SCOPE_KEY, scopeKey(key.scope()))
                .append(RESOLUTION, key.resolution().name())
                .append(BUCKET_START, Date.from(key.bucketStart()))
                .append(COMPUTED_AT, Date.from(bucket.computedAt()))
                .append(INPUT_READ_STARTED_AT, Date.from(bucket.inputReadStartedAt()))
                .append(VALID_UNTIL, Date.from(bucket.validUntil()))
                .append(REQUIRES_FINER_RESOLUTION, bucket.requiresFinerResolution())
                .append(FRAGMENTS, bucket.fragments().stream().map(MongoHistoryRollupStore::toDocument).toList())
                .append(GAPS, bucket.gaps().stream().map(MongoHistoryRollupStore::toDocument).toList());
        key.scope().incarnationId().ifPresent(id -> document.append(INCARNATION_ID, id));
        return document;
    }

    private static Document documentId(Key key) {
        return new Document(PIPELINE_ID, key.pipelineId())
                .append(SCOPE_KEY, scopeKey(key.scope()))
                .append(RESOLUTION, key.resolution().name())
                .append(BUCKET_START, Date.from(key.bucketStart()));
    }

    private static String scopeKey(Scope scope) {
        return scope.incarnationId().map(id -> INCARNATION_PREFIX + id).orElse(LEGACY_SCOPE);
    }

    private static Document toDocument(Fragment fragment) {
        Document document = new Document("segment", fragment.segment())
                .append("startReason", fragment.startReason().name())
                .append("intervalStart", Date.from(fragment.intervalStart()))
                .append("intervalEnd", Date.from(fragment.intervalEnd()))
                .append("lag", fragment.lag().stream().map(MongoHistoryRollupStore::toDocument).toList());
        if (fragment.recordsOut() != null) {
            document.append("recordsOut", toDocument(fragment.recordsOut()));
        }
        if (fragment.bytesOut() != null) {
            document.append("bytesOut", toDocument(fragment.bytesOut()));
        }
        return document;
    }

    private static Document toDocument(Rate rate) {
        return new Document("delta", rate.delta().toPlainString())
                .append("averageRate", rate.averageRate().toPlainString())
                .append("maxRate", rate.maxRate().toPlainString());
    }

    private static Document toDocument(Lag lag) {
        return new Document("table", lag.table())
                .append("observedAt", Date.from(lag.observedAt()))
                .append("last", lag.last())
                .append("max", lag.max());
    }

    private static Document toDocument(Gap gap) {
        return new Document("segment", gap.segment())
                .append("intervalStart", Date.from(gap.intervalStart()))
                .append("intervalEnd", Date.from(gap.intervalEnd()))
                .append("reason", gap.reason().name());
    }

    static Bucket toBucket(Document document) {
        String pipelineId = requiredString(document, PIPELINE_ID);
        try {
            String storedScopeKey = requiredString(document, SCOPE_KEY);
            Object rawIncarnation = document.get(INCARNATION_ID);
            Scope scope = rawIncarnation == null ? Scope.legacy()
                    : Scope.incarnation(requiredString(document, INCARNATION_ID));
            if (!scopeKey(scope).equals(storedScopeKey)) {
                throw corrupt(pipelineId, SCOPE_KEY);
            }
            Resolution resolution = Resolution.valueOf(requiredString(document, RESOLUTION));
            Key key = new Key(pipelineId, scope, resolution, requiredDate(document, BUCKET_START));
            if (!documentId(key).equals(document.get(ID))) {
                throw corrupt(pipelineId, ID);
            }
            Object rawMarker = document.get(REQUIRES_FINER_RESOLUTION);
            if (!(rawMarker instanceof Boolean marker)) {
                throw corrupt(pipelineId, REQUIRES_FINER_RESOLUTION);
            }
            return new Bucket(key, requiredDate(document, COMPUTED_AT),
                    requiredDate(document, INPUT_READ_STARTED_AT),
                    requiredDate(document, VALID_UNTIL), marker,
                    documents(document, FRAGMENTS, pipelineId).stream()
                            .map(fragment -> toFragment(fragment, pipelineId)).toList(),
                    documents(document, GAPS, pipelineId).stream()
                            .map(gap -> toGap(gap, pipelineId)).toList());
        } catch (IllegalArgumentException invalid) {
            throw corrupt(pipelineId, "bucket");
        }
    }

    private static Fragment toFragment(Document document, String pipelineId) {
        return new Fragment(requiredInt(document, "segment", pipelineId),
                StartReason.valueOf(requiredString(document, "startReason")),
                requiredDate(document, "intervalStart"), requiredDate(document, "intervalEnd"),
                optionalRate(document, "recordsOut", pipelineId), optionalRate(document, "bytesOut", pipelineId),
                documents(document, "lag", pipelineId).stream().map(lag -> toLag(lag, pipelineId)).toList());
    }

    private static Rate optionalRate(Document document, String field, String pipelineId) {
        Object raw = document.get(field);
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof Document rate)) {
            throw corrupt(pipelineId, field);
        }
        return new Rate(decimal(rate, "delta"), decimal(rate, "averageRate"), decimal(rate, "maxRate"));
    }

    private static Lag toLag(Document document, String pipelineId) {
        return new Lag(requiredString(document, "table"), requiredDate(document, "observedAt"),
                requiredLong(document, "last", pipelineId), requiredLong(document, "max", pipelineId));
    }

    private static Gap toGap(Document document, String pipelineId) {
        return new Gap(requiredInt(document, "segment", pipelineId),
                requiredDate(document, "intervalStart"), requiredDate(document, "intervalEnd"),
                GapReason.valueOf(requiredString(document, "reason")));
    }

    private static BigDecimal decimal(Document document, String field) {
        return new BigDecimal(requiredString(document, field));
    }

    private static List<Document> documents(Document document, String field, String pipelineId) {
        Object raw = document.get(field);
        if (!(raw instanceof List<?> list)) {
            throw corrupt(pipelineId, field);
        }
        List<Document> out = new ArrayList<>(list.size());
        for (Object item : list) {
            if (!(item instanceof Document entry)) {
                throw corrupt(pipelineId, field);
            }
            out.add(entry);
        }
        return out;
    }

    private static String requiredString(Document document, String field) {
        Object raw = document.get(field);
        if (!(raw instanceof String value) || value.isBlank()) {
            throw corrupt(String.valueOf(document.get(PIPELINE_ID)), field);
        }
        return value;
    }

    private static Instant requiredDate(Document document, String field) {
        Object raw = document.get(field);
        if (!(raw instanceof Date value)) {
            throw corrupt(String.valueOf(document.get(PIPELINE_ID)), field);
        }
        return value.toInstant();
    }

    private static int requiredInt(Document document, String field, String pipelineId) {
        Object raw = document.get(field);
        if (!(raw instanceof Integer || raw instanceof Long)
                || ((Number) raw).longValue() != ((Number) raw).intValue()) {
            throw corrupt(pipelineId, field);
        }
        return ((Number) raw).intValue();
    }

    private static long requiredLong(Document document, String field, String pipelineId) {
        Object raw = document.get(field);
        if (!(raw instanceof Integer || raw instanceof Long)) {
            throw corrupt(pipelineId, field);
        }
        return ((Number) raw).longValue();
    }

    private static void nonBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " is not blank");
        }
    }

    private static Instant ceilMillis(Instant value) {
        Instant millis = value.truncatedTo(ChronoUnit.MILLIS);
        return value.equals(millis) ? value : millis.plusMillis(1);
    }

    private static TapstateException corrupt(String pipelineId, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", pipelineId, "field", field), null);
    }
}
