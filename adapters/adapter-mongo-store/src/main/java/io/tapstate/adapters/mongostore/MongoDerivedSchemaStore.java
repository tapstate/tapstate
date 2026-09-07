package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB side record of the columns a pipeline step works out for itself: one document per
 * pipeline, keyed by the pipeline id (as {@code _id}), carrying one entry per step and, within it, that
 * step's append-only version history.
 *
 * <p><b>Why one document per pipeline and not one per step.</b> Both questions this store is asked are
 * then answered by the {@code _id} alone - reading one step's latest, and dropping everything a removed
 * pipeline recorded - so it needs no index beyond the one every collection already has. A document per
 * step would key on the pair, which leaves the drop querying a field the {@code _id} index cannot serve,
 * and this store has no way to create an index of its own.
 *
 * <p><b>Why steps and columns are arrays rather than sub-document keys.</b> A step id and a column name
 * are author-chosen text, and BSON field names cannot hold a dot. Keying by them would work until the
 * first author wrote {@code SELECT o.id AS "order.id"}, and would then fail inside the driver rather
 * than anywhere a message could name the cause.
 *
 * <p><b>The read-modify-write is deliberate and its race is benign.</b> {@link #record} reads the
 * document to work out the next version, so two starts of one pipeline racing here could have one
 * overwrite the other. Both are deriving the same step from the same stored inputs, so they compute the
 * same schema; and a start that derived a <em>different</em> schema is refused before it reaches this
 * store, never recorded. What the race can lose is one provenance refresh, which the next start redoes.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A stored document whose entries carry the wrong BSON type, or lack a field this version
 * requires, is store corruption - surfaced as a coded io diagnostic, not a bare crash while
 * reconstructing.
 */
public final class MongoDerivedSchemaStore implements DerivedSchemaStore {

    private final MongoCollection<Document> collection;

    public MongoDerivedSchemaStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public Optional<DerivedSchema> latest(String pipelineId, String stepId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stepId, "stepId");
        List<DerivedSchema> versions = versionsOf(read(pipelineId), pipelineId, stepId);
        return versions.isEmpty() ? Optional.empty() : Optional.of(versions.get(versions.size() - 1));
    }

    @Override
    public void record(String pipelineId, String stepId, Map<String, String> schema, String statement,
            String derivedFrom, String derivedBy) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stepId, "stepId");
        Objects.requireNonNull(schema, "schema");
        Document document = read(pipelineId);
        List<DerivedSchema> versions = new ArrayList<>(versionsOf(document, pipelineId, stepId));
        DerivedSchema last = versions.isEmpty() ? null : versions.get(versions.size() - 1);
        if (last != null && last.schema().equals(schema)) {
            // Same shape: the provenance is refreshed in place so the next difference stays attributable,
            // and no version is spent on a schema that did not move.
            versions.set(versions.size() - 1,
                    new DerivedSchema(last.version(), last.schema(), statement, derivedFrom, derivedBy));
        } else {
            versions.add(new DerivedSchema(
                    last == null ? 0L : last.version() + 1, schema, statement, derivedFrom, derivedBy));
        }
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", pipelineId),
                withStep(document, pipelineId, stepId, versions),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public void delete(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // deleteOne on a missing _id removes nothing and reports so without failing, which is the no-op a
        // pipeline that recorded nothing is meant to be.
        StoreIo.run(() -> collection.deleteOne(new Document("_id", pipelineId)));
    }

    private Document read(String pipelineId) {
        return StoreIo.call(() -> collection.find(new Document("_id", pipelineId)).first());
    }

    /** The stored document with this step's history replaced; every other step is carried through. */
    private static Document withStep(Document document, String pipelineId, String stepId,
            List<DerivedSchema> versions) {
        List<Document> steps = new ArrayList<>();
        for (Document step : stepsOf(document, pipelineId)) {
            if (!stepId.equals(requireString(step.get("step"), pipelineId))) {
                steps.add(step);
            }
        }
        List<Document> stored = new ArrayList<>();
        for (DerivedSchema version : versions) {
            stored.add(toDocument(version));
        }
        steps.add(new Document("step", stepId).append("versions", stored));
        return new Document("_id", pipelineId).append("steps", steps);
    }

    static Document toDocument(DerivedSchema version) {
        List<Document> columns = new ArrayList<>();
        version.schema().forEach((name, type) ->
                columns.add(new Document("name", name).append("type", type)));
        return new Document("version", version.version())
                .append("columns", columns)
                .append("statement", version.statement())
                .append("derivedFrom", version.derivedFrom())
                .append("derivedBy", version.derivedBy());
    }

    /** The step entries of a stored document; empty for an absent document. */
    private static List<Document> stepsOf(Document document, String pipelineId) {
        if (document == null) {
            return List.of();
        }
        Object raw = document.get("steps");
        if (raw == null) {
            return List.of();
        }
        if (!(raw instanceof List<?> steps)) {
            throw corrupt(pipelineId);
        }
        List<Document> out = new ArrayList<>();
        for (Object entry : steps) {
            if (!(entry instanceof Document step)) {
                throw corrupt(pipelineId);
            }
            out.add(step);
        }
        return out;
    }

    /** One step's version history, oldest first; empty where the pipeline or the step has none. */
    private static List<DerivedSchema> versionsOf(Document document, String pipelineId, String stepId) {
        for (Document step : stepsOf(document, pipelineId)) {
            if (stepId.equals(requireString(step.get("step"), pipelineId))) {
                return toVersions(step.get("versions"), pipelineId);
            }
        }
        return List.of();
    }

    private static List<DerivedSchema> toVersions(Object raw, String pipelineId) {
        if (raw == null) {
            return List.of();
        }
        if (!(raw instanceof List<?> versions)) {
            throw corrupt(pipelineId);
        }
        List<DerivedSchema> out = new ArrayList<>();
        for (Object entry : versions) {
            if (!(entry instanceof Document version)) {
                throw corrupt(pipelineId);
            }
            out.add(toDerivedSchema(version, pipelineId));
        }
        return out;
    }

    static DerivedSchema toDerivedSchema(Document version, String pipelineId) {
        Object rawColumns = version.get("columns");
        if (!(rawColumns instanceof List<?> columns)) {
            throw corrupt(pipelineId);
        }
        Map<String, String> schema = new LinkedHashMap<>();
        for (Object entry : columns) {
            if (!(entry instanceof Document column)) {
                throw corrupt(pipelineId);
            }
            schema.put(requireString(column.get("name"), pipelineId),
                    requireString(column.get("type"), pipelineId));
        }
        try {
            return new DerivedSchema(requireLong(version.get("version"), pipelineId), schema,
                    requireString(version.get("statement"), pipelineId),
                    requireString(version.get("derivedFrom"), pipelineId),
                    requireString(version.get("derivedBy"), pipelineId));
        } catch (IllegalArgumentException e) {
            // A stored value the record itself rejects - a negative version, a blank provenance - is
            // store corruption, not a caller's bad argument.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId), e);
        }
    }

    private static long requireLong(Object value, String pipelineId) {
        if (!(value instanceof Number number)) {
            throw corrupt(pipelineId);
        }
        return number.longValue();
    }

    private static String requireString(Object value, String pipelineId) {
        if (!(value instanceof String string)) {
            throw corrupt(pipelineId);
        }
        return string;
    }

    private static TapstateException corrupt(String pipelineId) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(pipelineId)), null);
    }
}
