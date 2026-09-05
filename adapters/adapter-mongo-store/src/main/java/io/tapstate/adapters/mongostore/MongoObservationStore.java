package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB per-pipeline observation store: one observation document per pipeline, keyed by the
 * pipeline id (as {@code _id}), carrying the lifecycle state and the metrics / per-table snapshot
 * progress / per-table position sub-documents. An observation is the latest projection, not fenced: it is
 * a straight upsert by pipeline id (last write wins), not the epoch-fencing compare-and-swap the actual
 * state store uses.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A stored document whose state is missing or unrecognized, or whose metric / snapshot / position
 * cells carry the wrong BSON type, is store corruption — surfaced as a coded io diagnostic, not a bare crash
 * while reconstructing. A document written before positions existed simply has no positions field and reads
 * back with empty positions.
 */
public final class MongoObservationStore implements ObservationStore {

    private final MongoCollection<Document> collection;

    public MongoObservationStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(Observation observation) {
        Objects.requireNonNull(observation, "observation");
        // Upsert by the pipeline id (the document _id): a re-publish overwrites the latest projection in
        // place (last write wins) rather than accumulating documents. An observation is not fenced.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", observation.pipelineId()), toDocument(observation), new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<Observation> read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", pipelineId)).first());
        return document == null ? Optional.empty() : Optional.of(toObservation(document));
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
     * positions as fields.
     */
    static Document toDocument(Observation observation) {
        Document metrics = new Document();
        observation.metrics().forEach(metrics::append);
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
                .append("positions", positions);
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
                readPositions(document, id), readFailure(document, id));
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
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(id), "field", "observation"), null);
    }
}
