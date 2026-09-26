package io.tapstate.adapters.mongostore;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineEventStore;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** One event per Mongo document, with an age bound and an incarnation-scoped keyset read. */
public final class MongoPipelineEventStore implements PipelineEventStore {

    public static final Duration DEFAULT_RETENTION = Duration.ofDays(15);

    static final String ID = "_id";
    static final String PIPELINE_ID = "pipelineId";
    static final String INCARNATION_ID = "pipelineIncarnationId";
    static final String GENERATION = "executionGeneration";
    static final String KIND = "kind";
    static final String OCCURRED_AT = "occurredAt";
    static final String BEFORE_STATE = "beforeState";
    static final String AFTER_STATE = "afterState";
    static final String FAILURE = "failure";
    static final String REASON = "reason";
    static final String GAP_FROM = "gapFrom";
    static final String GAP_TO = "gapTo";
    static final String GAP_REASONS = "gapReasons";

    private static final long WRITE_DEADLINE_SECONDS = 5;
    private static final int DUPLICATE_KEY = 11000;

    private final MongoCollection<Document> collection;
    private final Duration retention;

    public MongoPipelineEventStore(MongoDatabase database, MongoCollection<Document> collection, Duration retention) {
        Objects.requireNonNull(database, "database");
        this.collection = Objects.requireNonNull(collection, "collection");
        this.retention = Objects.requireNonNull(retention, "retention");
        if (retention.isNegative() || retention.isZero() || retention.getNano() != 0) {
            throw new IllegalArgumentException("event retention is a positive whole number of seconds");
        }
        StoreIo.run(() -> {
            IndexEnsure.ensure(database, collection, new SystemCollections.IndexSpec(
                    List.of(OCCURRED_AT), false, retention.toSeconds()));
            IndexEnsure.ensure(database, collection, new SystemCollections.IndexSpec(
                    List.of(PIPELINE_ID, INCARNATION_ID, OCCURRED_AT, ID), false));
        });
    }

    @Override
    public void append(PipelineEvent event) {
        Objects.requireNonNull(event, "event");
        Document proposed = toDocument(event);
        StoreIo.run(event.id(), () -> {
            MongoCollection<Document> bounded = collection.withTimeout(WRITE_DEADLINE_SECONDS, TimeUnit.SECONDS);
            try {
                bounded.insertOne(proposed);
            } catch (MongoWriteException duplicate) {
                if (duplicate.getError().getCode() != DUPLICATE_KEY) {
                    throw duplicate;
                }
                Document existing = bounded.find(Filters.eq(ID, event.id())).first();
                if (existing == null) {
                    throw duplicate;
                }
                if (!existing.equals(proposed)) {
                    throw new IllegalStateException("an event id was reused with conflicting content: " + event.id());
                }
            }
        });
    }

    @Override
    public Page readPage(String pipelineId, String incarnationId, Instant from, Instant to, Key after, int limit) {
        nonBlank(pipelineId, "pipelineId");
        nonBlank(incarnationId, "incarnationId");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("an event range is half-open and non-empty");
        }
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("an event page limit is between 1 and " + MAX_PAGE_SIZE);
        }
        if (after != null && !after.occurredAt().isBefore(to)) {
            throw new IllegalArgumentException("an event continuation key precedes the upper bound");
        }
        List<Bson> filters = new ArrayList<>(List.of(
                Filters.eq(PIPELINE_ID, pipelineId),
                Filters.eq(INCARNATION_ID, incarnationId),
                Filters.gte(OCCURRED_AT, Date.from(ceilMillis(from))),
                Filters.lt(OCCURRED_AT, Date.from(ceilMillis(to)))));
        if (after != null) {
            Date at = Date.from(after.occurredAt());
            filters.add(Filters.or(Filters.gt(OCCURRED_AT, at),
                    Filters.and(Filters.eq(OCCURRED_AT, at), Filters.gt(ID, after.id()))));
        }
        List<PipelineEvent> candidates = StoreIo.call(() -> {
            List<PipelineEvent> found = new ArrayList<>();
            collection.find(Filters.and(filters))
                    .sort(Sorts.orderBy(Sorts.ascending(OCCURRED_AT), Sorts.ascending(ID)))
                    .limit(limit + 1)
                    .forEach(document -> found.add(toEvent(document)));
            return found;
        });
        boolean hasMore = candidates.size() > limit;
        return new Page(hasMore ? candidates.subList(0, limit) : candidates, hasMore);
    }

    @Override
    public void deleteIncarnation(String pipelineId, String incarnationId) {
        nonBlank(pipelineId, "pipelineId");
        nonBlank(incarnationId, "incarnationId");
        StoreIo.run(() -> collection.withTimeout(WRITE_DEADLINE_SECONDS, TimeUnit.SECONDS).deleteMany(Filters.and(
                Filters.eq(PIPELINE_ID, pipelineId), Filters.eq(INCARNATION_ID, incarnationId))));
    }

    @Override
    public Duration retention() {
        return retention;
    }

    static Document toDocument(PipelineEvent event) {
        Document document = new Document(ID, event.id())
                .append(PIPELINE_ID, event.pipelineId())
                .append(INCARNATION_ID, event.pipelineIncarnationId())
                .append(GENERATION, event.executionGeneration())
                .append(KIND, event.kind().name())
                .append(OCCURRED_AT, Date.from(event.occurredAt()));
        if (event.beforeState() != null) {
            document.append(BEFORE_STATE, event.beforeState().name());
        }
        if (event.afterState() != null) {
            document.append(AFTER_STATE, event.afterState().name());
        }
        if (event.failure() != null) {
            document.append(FAILURE, new Document("code", event.failure().code())
                    .append("params", new Document(event.failure().params())));
        }
        if (event.reason() != null) {
            document.append(REASON, event.reason());
        }
        if (event.gap() != null) {
            document.append(GAP_FROM, Date.from(event.gap().from()))
                    .append(GAP_TO, Date.from(event.gap().to()))
                    .append(GAP_REASONS, event.gap().reasons().stream().map(Enum::name).toList());
        }
        return document;
    }

    static PipelineEvent toEvent(Document document) {
        String id = requiredString(document, ID);
        try {
            Object rawGeneration = document.get(GENERATION);
            if (!(rawGeneration instanceof Long || rawGeneration instanceof Integer)) {
                throw corrupt(id, GENERATION);
            }
            ObservationFailure failure = null;
            if (document.containsKey(FAILURE)) {
                Object rawFailure = document.get(FAILURE);
                if (!(rawFailure instanceof Document cells)) {
                    throw corrupt(id, FAILURE);
                }
                Object rawParams = cells.get("params");
                if (!(rawParams instanceof Document params)) {
                    throw corrupt(id, FAILURE);
                }
                Map<String, String> copied = new LinkedHashMap<>();
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    if (!(entry.getValue() instanceof String value)) {
                        throw corrupt(id, FAILURE);
                    }
                    copied.put(entry.getKey(), value);
                }
                failure = new ObservationFailure(requiredString(cells, "code"), copied);
            }
            PipelineEvent.Gap gap = null;
            if (document.containsKey(GAP_FROM) || document.containsKey(GAP_TO)
                    || document.containsKey(GAP_REASONS)) {
                Object rawReasons = document.get(GAP_REASONS);
                if (!(rawReasons instanceof List<?> reasons)) {
                    throw corrupt(id, GAP_REASONS);
                }
                List<PipelineEvent.GapReason> parsed = new ArrayList<>();
                for (Object reason : reasons) {
                    if (!(reason instanceof String name)) {
                        throw corrupt(id, GAP_REASONS);
                    }
                    parsed.add(PipelineEvent.GapReason.valueOf(name));
                }
                gap = new PipelineEvent.Gap(requiredDate(document, GAP_FROM),
                        requiredDate(document, GAP_TO), parsed);
            }
            return new PipelineEvent(id,
                    requiredString(document, PIPELINE_ID),
                    requiredString(document, INCARNATION_ID),
                    ((Number) rawGeneration).longValue(),
                    PipelineEvent.Kind.valueOf(requiredString(document, KIND)),
                    requiredDate(document, OCCURRED_AT),
                    optionalState(document, BEFORE_STATE),
                    optionalState(document, AFTER_STATE),
                    failure,
                    optionalString(document, REASON),
                    gap);
        } catch (IllegalArgumentException malformed) {
            throw corrupt(id, "event");
        }
    }

    private static PipelineState optionalState(Document document, String field) {
        String name = optionalString(document, field);
        return name == null ? null : PipelineState.valueOf(name);
    }

    private static String optionalString(Document document, String field) {
        if (!document.containsKey(field)) {
            return null;
        }
        return requiredString(document, field);
    }

    private static String requiredString(Document document, String field) {
        Object value = document.get(field);
        if (!(value instanceof String text) || text.isBlank()) {
            throw corrupt(String.valueOf(document.get(ID)), field);
        }
        return text;
    }

    private static Instant requiredDate(Document document, String field) {
        Object value = document.get(field);
        if (!(value instanceof Date date)) {
            throw corrupt(String.valueOf(document.get(ID)), field);
        }
        return date.toInstant();
    }

    private static String nonBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " is not blank");
        }
        return value;
    }

    /** Stored instants have millisecond precision, so a fractional boundary excludes the prior millisecond. */
    private static Instant ceilMillis(Instant instant) {
        Instant floor = instant.truncatedTo(ChronoUnit.MILLIS);
        return floor.equals(instant) ? floor : floor.plusMillis(1);
    }

    private static TapstateException corrupt(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }
}
