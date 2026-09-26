package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * The MongoDB per-pipeline observation store: one observation document per pipeline, keyed by the
 * pipeline id (as {@code _id}), carrying the lifecycle state and the metrics / per-table snapshot
 * progress / per-table position sub-documents. Scoped writes compare the execution owner and observation
 * time in the same Mongo operation; the legacy save method remains an unfenced upsert.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A stored document whose state is missing or unrecognized, or whose metric / snapshot / position
 * cells carry the wrong BSON type, is store corruption — surfaced as a coded io diagnostic, not a bare crash
 * while reconstructing. A document written before positions existed simply has no positions field and reads
 * back with empty positions; one written before the measured facts travelled reads back with none.
 *
 * <p><strong>The facts are stored as the fact type lays them out</strong> — one array element per metric,
 * each with its points, each point with its attributes, its instants as BSON dates and either a value or
 * the four parts of a distribution — and reconstructed through the fact type's own constructor. So a stored
 * fact this version would refuse to build (a distribution over bounds no longer registered, a closed
 * attribute carrying a value the set no longer holds) reads as corruption of the {@code facts} field, on
 * the same terms as an unrecognized state: the document is rewritten on the next publish, and until then a
 * reader is told the document is unreadable rather than handed a fact that means something else now.
 */
public final class MongoObservationStore implements ObservationStore {

    private static final long IO_DEADLINE_SECONDS = 5;

    private final MongoCollection<Document> collection;

    public MongoObservationStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(Observation observation) {
        Objects.requireNonNull(observation, "observation");
        // Upsert by the pipeline id (the document _id): a re-publish overwrites the latest projection in
        // place (last write wins) rather than accumulating documents. An observation is not fenced.
        StoreIo.run(() -> collection.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS).replaceOne(
                new Document("_id", observation.pipelineId()), toDocument(observation), new ReplaceOptions().upsert(true)));
    }

    @Override
    public boolean saveScoped(Observation observation, Scope scope) {
        Objects.requireNonNull(observation, "observation");
        Objects.requireNonNull(scope, "scope");
        Instant observedAt = Objects.requireNonNull(observation.observedAt(), "observedAt");
        if (observedAt.getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException("a scoped observation time must have millisecond precision");
        }
        String id = observation.pipelineId();
        Document legacy = new Document("pipelineIncarnationId", new Document("$exists", false))
                .append("executionGeneration", new Document("$exists", false));
        Document newerGeneration = new Document("executionGeneration",
                new Document("$lt", scope.executionGeneration()));
        Document newerTimeInSameExecution = new Document("pipelineIncarnationId", scope.pipelineIncarnationId())
                .append("executionGeneration", scope.executionGeneration())
                .append("observedAt", new Document("$lt", Date.from(observedAt)));
        Document filter = new Document("_id", id)
                .append("$or", List.of(legacy, newerGeneration, newerTimeInSameExecution));
        Document replacement = toDocument(observation)
                .append("pipelineIncarnationId", scope.pipelineIncarnationId())
                .append("executionGeneration", scope.executionGeneration());
        return StoreIo.call(id, () -> {
            try {
                UpdateResult result = collection.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                        .replaceOne(filter, replacement, new ReplaceOptions().upsert(true));
                return result.getModifiedCount() != 0 || result.getUpsertedId() != null;
            } catch (MongoException conflict) {
                // An existing _id whose scope or time did not match makes the upsert attempt collide.
                // A concurrent insert may also win first; the next observation may retry with fresh facts.
                if (duplicateKey(conflict)) {
                    return false;
                }
                throw conflict;
            }
        });
    }

    private static boolean duplicateKey(MongoException failure) {
        return failure instanceof com.mongodb.MongoWriteException write
                ? write.getError().getCode() == 11000 : failure.getCode() == 11000;
    }

    @Override
    public Optional<Observation> read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> collection.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", pipelineId)).first());
        return document == null ? Optional.empty() : Optional.of(toObservation(document));
    }

    @Override
    public Optional<Stored> readStored(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> collection.withTimeout(IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", pipelineId)).first());
        if (document == null) {
            return Optional.empty();
        }
        Object rawIncarnation = document.get("pipelineIncarnationId");
        Object rawGeneration = document.get("executionGeneration");
        Optional<Scope> scope;
        if (rawIncarnation == null && rawGeneration == null) {
            scope = Optional.empty();
        } else if (rawIncarnation instanceof String incarnation
                && (rawGeneration instanceof Long || rawGeneration instanceof Integer)
                && !incarnation.isBlank() && ((Number) rawGeneration).longValue() > 0) {
            scope = Optional.of(new Scope(incarnation, ((Number) rawGeneration).longValue()));
        } else {
            throw corrupt(pipelineId, "observation scope");
        }
        return Optional.of(new Stored(toObservation(document), scope));
    }

    @Override
    public void delete(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // deleteOne on a missing _id removes nothing and reports so without failing, which is the no-op
        // an unpublished observation is meant to be.
        StoreIo.run(() -> collection.deleteOne(new Document("_id", pipelineId)));
    }

    /**
     * Maps an observation to its stored document: pipeline id as {@code _id}, state / metrics / snapshot /
     * positions / facts as fields.
     */
    static Document toDocument(Observation observation) {
        Document metrics = new Document();
        observation.metrics().forEach(metrics::append);
        List<Document> facts = new ArrayList<>();
        observation.facts().forEach(fact -> facts.add(toDocument(fact)));
        Document snapshot = new Document();
        observation.snapshot().forEach((table, progress) -> {
            Document cell = new Document("rowsDone", progress.rowsDone());
            if (progress.rowsTotal() != null) {
                cell.append("rowsTotal", progress.rowsTotal());
            }
            if (progress.donePct() != null) {
                cell.append("donePct", progress.donePct());
            }
            snapshot.append(table, cell);
        });
        Document positions = new Document();
        observation.positions().forEach(positions::append);
        Document document = new Document("_id", observation.pipelineId())
                .append("state", observation.state().name())
                .append("metrics", metrics)
                .append("snapshot", snapshot)
                .append("positions", positions)
                .append("facts", facts);
        if (observation.observedAt() != null) {
            // Stored as a BSON date, not a string: the server can then order and expire on it, and a
            // lexicographic sort over an instant is not a chronological one. Absent when the publisher did
            // not record a time, so absence keeps meaning "not known" rather than the epoch.
            document.append("observedAt", Date.from(observation.observedAt()));
        }
        if (observation.failure() != null) {
            // Only a pipeline that actually died carries a failure: the field is absent while healthy rather
            // than present and empty, so absence keeps meaning "nothing went wrong".
            Document params = new Document();
            observation.failure().params().forEach(params::append);
            document.append("failure",
                    new Document("code", observation.failure().code()).append("params", params));
        }
        return document;
    }

    /** Reconstructs an observation from its stored document. */
    static Observation toObservation(Document document) {
        String id = document.getString("_id");
        String state = document.getString("state");
        if (state == null) {
            // A stored observation missing the state field this version requires is store corruption.
            throw corrupt(id);
        }
        return new Observation(id, parseState(state, id), readMetrics(document, id), readSnapshot(document, id),
                readPositions(document, id), readFailure(document, id), readObservedAt(document, id),
                readFacts(document, id));
    }

    /**
     * One metric as stored: its name, kind and unit, and its points. The kind is stored by its constant's
     * name, the way the state is, so the stored word and the type it names cannot drift apart.
     */
    private static Document toDocument(MetricFact fact) {
        List<Document> points = new ArrayList<>();
        for (MetricPoint point : fact.points()) {
            // Attributes sorted by key: a document that lists the same point's attributes in two orders on
            // two ticks is two documents that mean one thing, and the difference is noise to anything
            // reading the store directly.
            Document attributes = new Document();
            new TreeMap<>(point.attributes()).forEach(attributes::append);
            Document cell = new Document("attributes", attributes);
            if (point.startTime() != null) {
                cell.append("startTime", Date.from(point.startTime()));
            }
            // A BSON date, like the observation's own time and for the same reason; and like it, held to
            // milliseconds, which is what a date carries -- a start taken with finer precision is stored
            // truncated, the same way on every tick, so it still identifies one accumulation.
            cell.append("observedAt", Date.from(point.observedAt()));
            if (point.value() != null) {
                cell.append("value", point.value());
            } else {
                HistogramValue histogram = point.histogram();
                cell.append("count", histogram.count())
                        .append("sum", histogram.sum())
                        .append("bounds", histogram.bounds())
                        .append("bucketCounts", histogram.bucketCounts());
            }
            points.add(cell);
        }
        return new Document("name", fact.name())
                .append("type", fact.type().name())
                .append("unit", fact.unit())
                .append("points", points);
    }

    /**
     * Reads the facts array back through the fact type's own constructor; a missing field reads empty (the
     * document was written before facts travelled), and a cell of the wrong shape, or a fact this version
     * refuses to build, is corruption of this field.
     */
    private static List<MetricFact> readFacts(Document document, String id) {
        Object raw = document.get("facts");
        if (raw == null) {
            return List.of();
        }
        if (!(raw instanceof List<?> stored)) {
            throw corrupt(id, "facts");
        }
        List<MetricFact> facts = new ArrayList<>();
        for (Object element : stored) {
            if (!(element instanceof Document fact)) {
                throw corrupt(id, "facts");
            }
            facts.add(readFact(fact, id));
        }
        return facts;
    }

    private static MetricFact readFact(Document fact, String id) {
        String name = requireString(fact.get("name"), id);
        String type = requireString(fact.get("type"), id);
        String unit = requireString(fact.get("unit"), id);
        if (!(fact.get("points") instanceof List<?> stored)) {
            throw corrupt(id, "facts");
        }
        List<MetricPoint> points = new ArrayList<>();
        for (Object element : stored) {
            if (!(element instanceof Document point)) {
                throw corrupt(id, "facts");
            }
            points.add(readPoint(point, id));
        }
        try {
            return new MetricFact(name, MetricType.valueOf(type), unit, points);
        } catch (IllegalArgumentException e) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id), "field", "facts"), e);
        }
    }

    private static MetricPoint readPoint(Document point, String id) {
        Map<String, String> attributes = new LinkedHashMap<>();
        Object rawAttributes = point.get("attributes");
        if (rawAttributes != null) {
            if (!(rawAttributes instanceof Document stored)) {
                throw corrupt(id, "facts");
            }
            for (Map.Entry<String, Object> attribute : stored.entrySet()) {
                attributes.put(attribute.getKey(), requireString(attribute.getValue(), id));
            }
        }
        Instant startTime = optionalDate(point.get("startTime"), id);
        Object rawObservedAt = point.get("observedAt");
        if (!(rawObservedAt instanceof Date observedAt)) {
            throw corrupt(id, "facts");
        }
        Long value = optionalLong(point.get("value"), id);
        HistogramValue histogram = null;
        if (point.get("count") != null || point.get("bounds") != null) {
            histogram = readHistogram(point, id);
        }
        try {
            return new MetricPoint(attributes, startTime, observedAt.toInstant(), value, histogram);
        } catch (IllegalArgumentException e) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id), "field", "facts"), e);
        }
    }

    private static HistogramValue readHistogram(Document point, String id) {
        long count = requireLong(point.get("count"), id);
        if (!(point.get("sum") instanceof Number sum)) {
            throw corrupt(id, "facts");
        }
        List<Double> bounds = new ArrayList<>();
        if (!(point.get("bounds") instanceof List<?> storedBounds)) {
            throw corrupt(id, "facts");
        }
        for (Object bound : storedBounds) {
            if (!(bound instanceof Number number)) {
                throw corrupt(id, "facts");
            }
            bounds.add(number.doubleValue());
        }
        List<Long> bucketCounts = new ArrayList<>();
        if (!(point.get("bucketCounts") instanceof List<?> storedCounts)) {
            throw corrupt(id, "facts");
        }
        for (Object bucket : storedCounts) {
            bucketCounts.add(requireLong(bucket, id));
        }
        try {
            return new HistogramValue(count, sum.doubleValue(), bounds, bucketCounts);
        } catch (IllegalArgumentException e) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id), "field", "facts"), e);
        }
    }

    private static Instant optionalDate(Object value, String id) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Date date)) {
            throw corrupt(id, "facts");
        }
        return date.toInstant();
    }

    /**
     * Reads when the projection was taken; a missing field reads null — the document was written before this
     * version recorded it, or its publisher did not. Null stays null: a reader filling it in from its own
     * clock would report every stale projection as fresh, which is the one answer this field exists to stop.
     * A non-date cell is corruption, caught here rather than escaping as a ClassCastException later.
     */
    private static Instant readObservedAt(Document document, String id) {
        Object raw = document.get("observedAt");
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof Date observedAt)) {
            throw corrupt(id);
        }
        return observedAt.toInstant();
    }

    /**
     * Reads the failure sub-document as its code plus named arguments; a missing field reads null (the
     * pipeline is healthy, or the document was written before failures existed), a non-document field, a
     * missing code or a non-string cell is corruption.
     */
    private static ObservationFailure readFailure(Document document, String id) {
        Object raw = document.get("failure");
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof Document failure)) {
            throw corrupt(id);
        }
        // requireString covers both a missing code and a wrong-typed one: Document.getString is an
        // unchecked cast (get(key, String.class)), so a non-string code (store corruption written by a
        // future or foreign writer) would otherwise escape as a bare ClassCastException instead of this
        // same coded diagnostic every other corrupt cell in this class already gets.
        String code = requireString(failure.get("code"), id);
        Object rawParams = failure.get("params");
        if (rawParams == null) {
            return new ObservationFailure(code, Map.of());
        }
        if (!(rawParams instanceof Document params)) {
            throw corrupt(id);
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            out.put(entry.getKey(), requireString(entry.getValue(), id));
        }
        return new ObservationFailure(code, out);
    }

    /** A stored state this version does not recognize is corruption, not a bare enum-valueOf crash. */
    private static PipelineState parseState(String state, String id) {
        try {
            return PipelineState.valueOf(state);
        } catch (IllegalArgumentException e) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id), "field", "state"), e);
        }
    }

    /** Reads the metrics sub-document as name -> long; a non-number cell is corruption, not a class-cast crash. */
    private static Map<String, Long> readMetrics(Document document, String id) {
        Object raw = document.get("metrics");
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Document metrics)) {
            throw corrupt(id);
        }
        Map<String, Long> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            out.put(entry.getKey(), requireLong(entry.getValue(), id));
        }
        return out;
    }

    /** Reads the snapshot sub-document as table -> progress; a missing rows-done or wrong type is corruption. */
    private static Map<String, TableSnapshot> readSnapshot(Document document, String id) {
        Object raw = document.get("snapshot");
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Document snapshot)) {
            throw corrupt(id);
        }
        Map<String, TableSnapshot> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : snapshot.entrySet()) {
            if (!(entry.getValue() instanceof Document cell)) {
                throw corrupt(id);
            }
            long rowsDone = requireLong(cell.get("rowsDone"), id);
            Long rowsTotal = optionalLong(cell.get("rowsTotal"), id);
            Integer donePct = optionalInt(cell.get("donePct"), id);
            out.put(entry.getKey(), new TableSnapshot(rowsDone, rowsTotal, donePct));
        }
        return out;
    }

    /**
     * Reads the positions sub-document as table -> srcpos; a missing field reads empty (an observation
     * stored before positions existed), a non-document field or a non-string cell is corruption.
     */
    private static Map<String, String> readPositions(Document document, String id) {
        Object raw = document.get("positions");
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Document positions)) {
            throw corrupt(id);
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : positions.entrySet()) {
            out.put(entry.getKey(), requireString(entry.getValue(), id));
        }
        return out;
    }

    private static long requireLong(Object value, String id) {
        if (!(value instanceof Number number)) {
            throw corrupt(id);
        }
        return number.longValue();
    }

    private static String requireString(Object value, String id) {
        if (!(value instanceof String string)) {
            throw corrupt(id);
        }
        return string;
    }

    private static Long optionalLong(Object value, String id) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number number)) {
            throw corrupt(id);
        }
        return number.longValue();
    }

    private static Integer optionalInt(Object value, String id) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number number)) {
            throw corrupt(id);
        }
        return number.intValue();
    }

    private static TapstateException corrupt(String id) {
        return corrupt(id, "observation");
    }

    private static TapstateException corrupt(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(id), "field", field), null);
    }
}
