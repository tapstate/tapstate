package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
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
 * The MongoDB side record of the columns a pipeline step works out for itself: one document per step,
 * keyed by {@code <pipelineId>.<stepId>} (as {@code _id}), carrying that step's append-only version
 * history.
 *
 * <p><b>Why a step per document, and why the key is compound.</b> A pipeline's steps used to share one
 * document, which put a pipeline's entire recorded history - every step, every version, every column -
 * under MongoDB's 16MB per-document limit. Measured 2026-09-08, five steps of a 1000-column table
 * recorded over forty shape changes crossed it, at which point no further shape could be recorded for
 * any step of that pipeline and nothing shrank the document back. Splitting per step is what removes
 * that shared ceiling.
 *
 * <p>The key is compound rather than a plain step id because both questions this store is asked still
 * have to be served by the {@code _id} index, this module having no way to create an index of its own:
 * reading one step's latest is an exact {@code _id} lookup, and dropping everything a removed pipeline
 * recorded is a range over the {@code _id} prefix {@code <pipelineId>.}. A pipeline id cannot itself
 * contain a dot - the parser refuses one, the dot being the reserved addressing separator - so the
 * first dot always separates the two halves and no two (pipeline, step) pairs can collide on one key.
 * That invariant is load-bearing enough to be checked here rather than assumed.
 *
 * <p><b>Why steps and columns are arrays rather than sub-document keys.</b> A step id and a column name
 * are author-chosen text, and BSON field names cannot hold a dot. Keying by them would work until the
 * first author wrote {@code SELECT o.id AS "order.id"}, and would then fail inside the driver rather
 * than anywhere a message could name the cause.
 *
 * <p><b>The read-modify-write is deliberate and its race is benign.</b> {@link #record} reads the
 * step's document to work out the next version, so two starts of one pipeline racing here could have
 * one overwrite the other. Both are deriving the same step from the same stored inputs, so they compute the
 * same schema; and a start that derived a <em>different</em> schema is refused before it reaches this
 * store, never recorded. What the race can lose is one provenance refresh, which the next start redoes.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A stored document whose entries carry the wrong BSON type, or lack a field this version
 * requires, is store corruption - surfaced as a coded io diagnostic, not a bare crash while
 * reconstructing.
 */
public final class MongoDerivedSchemaStore implements DerivedSchemaStore {

    /**
     * The one character that separates a pipeline id from a step id inside a key. A pipeline id cannot
     * contain it, so the first occurrence always splits the two; a step id may (a source node's step id
     * is {@code <sourceId>.<table>}), which is why the split is by first occurrence and not by last.
     */
    private static final char SEPARATOR = '.';

    /** The field a step's document carries the version a run is holding, beside that step's history. */
    private static final String PIN = "pin";

    private final MongoCollection<Document> collection;

    public MongoDerivedSchemaStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public Optional<DerivedSchema> latest(String pipelineId, String stepId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stepId, "stepId");
        List<DerivedSchema> versions = stepVersions(pipelineId, stepId);
        return versions.isEmpty() ? Optional.empty() : Optional.of(versions.get(versions.size() - 1));
    }

    @Override
    public void record(String pipelineId, String stepId, Map<String, String> schema, String statement,
            String derivedFrom, String derivedBy) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stepId, "stepId");
        Objects.requireNonNull(schema, "schema");
        List<DerivedSchema> versions = new ArrayList<>(stepVersions(pipelineId, stepId));
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
        writeStep(pipelineId, stepId, versions);
    }

    @Override
    public void pin(String pipelineId, String stepId, long version) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stepId, "stepId");
        String key = key(pipelineId, stepId);
        // On the step's own document, so it is dropped by the same prefix range that drops the history
        // it points into. A pin outliving its history would name a version nothing holds, and the read
        // below answers empty for that rather than reaching for whatever is newest.
        StoreIo.run(key, () -> collection.updateOne(
                new Document("_id", key),
                new Document("$set", new Document(PIN, version)),
                new UpdateOptions().upsert(true)));
    }

    @Override
    public Optional<DerivedSchema> pinned(String pipelineId, String stepId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(stepId, "stepId");
        Document document = read(key(pipelineId, stepId));
        if (document == null || document.get(PIN) == null) {
            return Optional.empty();
        }
        long version = requireLong(document.get(PIN), pipelineId);
        for (DerivedSchema recorded : stepVersions(pipelineId, stepId)) {
            if (recorded.version() == version) {
                return Optional.of(recorded);
            }
        }
        return Optional.empty();
    }

    @Override
    public void delete(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // Every key this pipeline owns starts with "<pipelineId>." and none of another pipeline's does,
        // so the half-open range from that prefix up to the next character serves the drop off the _id
        // index. The pipeline's own id is deleted alongside it: that is where a record written before the
        // split still lives, and leaving it would strand the one document this drop exists to remove.
        String prefix = pipelineId + SEPARATOR;
        Document ownedKeys = new Document("$gte", prefix).append("$lt", pipelineId + (char) (SEPARATOR + 1));
        StoreIo.run(() -> collection.deleteMany(
                new Document("$or", List.of(
                        new Document("_id", ownedKeys),
                        new Document("_id", pipelineId)))));
    }

    /**
     * The key one step's document is stored under.
     *
     * <p>A pipeline id carrying the separator would make the key ambiguous - {@code ("a.b", "c")} and
     * {@code ("a", "b.c")} would name one document and each would read the other's history. The parser
     * refuses such an id, so reaching here with one is a defect in whatever built it rather than
     * anything an author can cause, and it crashes bare rather than being reported as a store failure.
     */
    private static String key(String pipelineId, String stepId) {
        if (pipelineId.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException(
                    "pipelineId must not contain '" + SEPARATOR + "': " + pipelineId);
        }
        return pipelineId + SEPARATOR + stepId;
    }

    /** One step's version history, oldest first; empty where the step has none. */
    private List<DerivedSchema> stepVersions(String pipelineId, String stepId) {
        Document document = read(key(pipelineId, stepId));
        if (document != null) {
            return toVersions(document.get("versions"), pipelineId);
        }
        return List.of();
    }

    private void writeStep(String pipelineId, String stepId, List<DerivedSchema> versions) {
        List<Document> stored = new ArrayList<>();
        for (DerivedSchema version : versions) {
            stored.add(toDocument(version));
        }
        String key = key(pipelineId, stepId);
        // Updated field by field rather than replaced whole: the pin sits on this same document, and a
        // replace would drop it on every record - so a run would lose what it was holding the moment
        // anybody recorded a shape, which is precisely the case the pin is read in.
        StoreIo.run(key, () -> collection.updateOne(
                new Document("_id", key),
                new Document("$set", new Document("versions", stored)),
                new UpdateOptions().upsert(true)));
    }

    private Document read(String id) {
        return StoreIo.call(() -> collection.find(new Document("_id", id)).first());
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
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId, "field", "versions"), e);
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
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(pipelineId), "field", "versions"), null);
    }
}
