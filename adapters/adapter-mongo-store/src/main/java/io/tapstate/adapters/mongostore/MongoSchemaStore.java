package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The MongoDB discovered-schema store: stores the discovery envelope for a connection as an envelope
 * document keyed by the connection's id — the connector id and discovery time it reports — plus one
 * document per table, keyed {@code <connectionId>.<generation>.<ordinal>}, carrying that table's
 * fields, primary key and indexes.
 *
 * <p><b>Why a table per document.</b> The whole envelope used to be one document, which put every
 * table a connection has under MongoDB's 16MB per-document limit. Measured 2026-09-08 the ceiling
 * falls at roughly 150,000 columns summed across the connection - 1500 tables of 120 columns does not
 * fit - and a connection over it cannot be discovered at all, which makes it unusable rather than
 * slow. Neither dimension has to be remarkable, so no author looking at their own tables would see it
 * coming.
 *
 * <p><b>Why a generation, and why the envelope is written last.</b> A re-discovery has to become
 * visible all at once: a reader that saw half of one discovery and half of the previous one would be
 * told a table has columns it does not have, and would refuse or accept an expression on that basis.
 * With the tables spread over many documents there is no single write that replaces them, so each
 * discovery writes its tables under a fresh generation - invisible, because nothing points at them -
 * and then names that generation in the envelope. That last step is a one-document write, so a reader
 * rechecks the envelope after loading its tables and retries if publication changed meanwhile.
 * Writers register a lease in the envelope before inserting tables. Publication and reclamation
 * compare the envelope's revision: a sweep revokes expired leases before deleting its fixed set of
 * unreachable generations, and a revoked writer cannot publish. A later sweep can therefore recover
 * after process death without deleting another writer's current or active unpublished tables.
 *
 * <p>The discovery payload is a fixed shape of plain scalars and lists, so it is mapped field by field rather
 * than through a generic value normalization; on read the driver's {@code Document} / list values are
 * reconstructed into the pure model, so no driver type escapes this module (rule R3). A stored document
 * whose shape cannot be reconstructed is surfaced as a coded {@code io.document-unreadable} diagnostic.
 */
public final class MongoSchemaStore implements SchemaStore {

    /**
     * The stamp every document this build writes carries, and the one thing that tells a discovery run
     * against the current model apart from one that predates it.
     *
     * <p>It has to be a stamp the writer puts on rather than something read out of the content: a
     * document from before the types were resolved is recognisable by its fields carrying no resolved
     * type, but so is a model of a connection that legitimately holds no fields at all - and reading the
     * second as the first would make an empty source undiscoverable no matter how often it is
     * discovered. The stamp is present on every write, so its absence says exactly one thing.
     */
    static final String MODEL_VERSION = "modelVersion";

    /**
     * The model this build writes and reads: source types resolved onto the tapstate namespace, with
     * the tables held as documents of their own rather than inline.
     *
     * <p>Bumped when the tables moved out of the envelope. A document written by the previous build
     * carries its tables inline and names no generation, so reading it through the current path would
     * find no tables and report an empty database - a wrong answer in the shape of a right one.
     * Startup migration moves those inline observations into table documents before this reader is
     * available. The stamp remains a read guard for callers that bypass verified startup.
     */
    static final int RESOLVED_TYPES = 2;

    /** Maximum complete read attempts under publication contention, not a wall-clock timeout. */
    static final int MAX_READ_ATTEMPTS = 8;

    private static final String WRITERS = "_writers";
    private static final String REVISION = "_revision";
    private static final int MAX_WRITE_ATTEMPTS = 8;
    // A stalled insertion may lose publication rights after five minutes. It must retry discovery
    // if a later sweep revokes its lease; elapsed time alone never authorizes deleting its tables.
    private static final long WRITE_LEASE_MILLIS = 300_000L;

    private final MongoCollection<Document> collection;

    public MongoSchemaStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    /** The field naming which discovery a stored table belongs to; absent on the envelope itself. */
    static final String GENERATION = "generation";

    /**
     * The one character separating the parts of a table key. A connection id cannot contain it, so the
     * connection's own documents are exactly the keys in {@code [<id>., <id>/)} and no other
     * connection's fall in that range. Table names are stored as values, so duplicate names and
     * separator characters cannot collide in document keys.
     */
    private static final char SEPARATOR = '.';

    @Override
    public void save(DiscoveredSourceModel discovered) {
        Objects.requireNonNull(discovered, "discovered");
        String connectionId = discovered.connectionId();
        String generation = UUID.randomUUID().toString();
        List<Document> tables = new ArrayList<>();
        List<SourceTable> model = discovered.model().tables();
        for (int order = 0; order < model.size(); order++) {
            SourceTable table = model.get(order);
            tables.add(tableDocument(table)
                    .append("_id", tableKey(connectionId, generation, order))
                    .append(GENERATION, generation)
                    // Discovery order is part of what was stored and read back, so it is carried
                    // explicitly: document key order need not match numeric discovery order.
                    .append("order", order));
        }
        // Mongo's clock starts the lease; no process-local registry or clock decides ownership.
        StoreIo.run(connectionId, () -> collection.updateOne(new Document("_id", connectionId),
                new Document("$currentDate", new Document(WRITERS + "." + generation, true))
                        .append("$inc", new Document(REVISION, 1L)), new UpdateOptions().upsert(true)));
        try {
            if (!tables.isEmpty()) {
                StoreIo.run(connectionId, () -> collection.insertMany(tables));
            }
            publish(discovered, generation);
        } catch (RuntimeException failure) {
            // An ordinary failure relinquishes the lease immediately, leaving cleanup to a later
            // discovery. Process death instead leaves a lease that a later sweep can expire.
            try {
                StoreIo.run(() -> collection.updateOne(new Document("_id", connectionId),
                        new Document("$unset", new Document(WRITERS + "." + generation, ""))
                                .append("$inc", new Document(REVISION, 1L))));
            } catch (RuntimeException releaseFailure) {
                failure.addSuppressed(releaseFailure);
            }
            throw failure;
        }
        reclaim(connectionId);
    }

    private void publish(DiscoveredSourceModel discovered, String generation) {
        String connectionId = discovered.connectionId();
        for (int attempt = 0; attempt < MAX_WRITE_ATTEMPTS; attempt++) {
            Document current = StoreIo.call(() -> collection.find(new Document("_id", connectionId)).first());
            Document writers = writers(current);
            if (!writers.containsKey(generation)) {
                break;
            }
            writers.remove(generation);
            Document replacement = envelope(discovered, generation)
                    .append(WRITERS, writers).append(REVISION, revision(current) + 1);
            Document displaced = StoreIo.call(connectionId, () -> collection.findOneAndReplace(
                    fence(current), replacement,
                    new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE)));
            if (displaced != null) {
                return;
            }
        }
        throw new TapstateException(IoError.SCHEMA_WRITE_CONTENTION,
                Map.of("connectionId", connectionId), null);
    }

    private void reclaim(String connectionId) {
        for (int attempt = 0; attempt < MAX_WRITE_ATTEMPTS; attempt++) {
            Document current = StoreIo.call(() -> collection.aggregate(List.of(
                    new Document("$match", new Document("_id", connectionId)),
                    new Document("$set", new Document("_reclaimBefore",
                            new Document("$subtract", List.of("$$NOW", WRITE_LEASE_MILLIS)))))).first());
            Document active = writers(current);
            Date cutoff = current.getDate("_reclaimBefore");
            active.entrySet().removeIf(entry -> ((Date) entry.getValue()).compareTo(cutoff) <= 0);
            Set<String> protectedGenerations = new HashSet<>(active.keySet());
            protectedGenerations.add(current.getString(GENERATION));
            Set<String> garbage = new HashSet<>();
            StoreIo.run(() -> collection.find(new Document("_id", ownedKeys(connectionId)))
                    .projection(new Document(GENERATION, 1))
                    .forEach(table -> {
                        String candidate = table.getString(GENERATION);
                        if (candidate != null && !protectedGenerations.contains(candidate)) {
                            garbage.add(candidate);
                        }
                    }));
            // Registration precedes insertion. This CAS proves no writer registered or published
            // while candidates were collected, and fences every expired writer before deletion.
            long matched = StoreIo.call(() -> collection.updateOne(fence(current),
                    new Document("$set", new Document(WRITERS, active))
                            .append("$inc", new Document(REVISION, 1L))).getMatchedCount());
            if (matched != 0) {
                if (!garbage.isEmpty()) {
                    StoreIo.run(() -> collection.deleteMany(new Document("_id", ownedKeys(connectionId))
                            .append(GENERATION, new Document("$in", new ArrayList<>(garbage)))));
                }
                return;
            }
        }
        // A busy connection can defer its sweep to the next discovery; its publication already won.
    }

    private static Document writers(Document envelope) {
        return new Document(envelope.get(WRITERS, Document.class));
    }

    private static long revision(Document envelope) {
        return envelope.getLong(REVISION);
    }

    private static Document fence(Document envelope) {
        return new Document("_id", envelope.getString("_id")).append(REVISION, revision(envelope));
    }

    @Override
    public Optional<DiscoveredSourceModel> get(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        for (int attempt = 0; attempt < MAX_READ_ATTEMPTS; attempt++) {
            Document document = StoreIo.call(() -> collection.find(new Document("_id", connectionId)).first());
            // Verified startup already moved legacy envelopes into the current shape.
            if (document == null || !carriesResolvedTypes(document)) {
                return Optional.empty();
            }
            List<Document> tables = storedTables(connectionId, document);
            Document current = StoreIo.call(() -> collection.find(new Document("_id", connectionId)).first());
            // The tables may have been reclaimed between these reads. A fresh UUID on every save
            // prevents an intervening publication from returning to the generation we first saw.
            if (current != null && Objects.equals(document.get(GENERATION), current.get(GENERATION))) {
                return Optional.of(toDiscovered(document, tables));
            }
        }
        // Continuous publication must not hold a caller indefinitely or masquerade as no discovery.
        throw new TapstateException(IoError.SCHEMA_READ_CONTENTION,
                Map.of("connectionId", connectionId), null);
    }

    /**
     * The stored table documents of a connection's current discovery, in the order they were
     * discovered. An envelope carrying the current stamp but naming no generation is corrupt: the two
     * are written together.
     */
    private List<Document> storedTables(String connectionId, Document envelope) {
        String generation = envelope.getString(GENERATION);
        if (generation == null) {
            throw unreadable(connectionId);
        }
        List<Document> tables = new ArrayList<>();
        StoreIo.run(() -> collection
                .find(new Document("_id", ownedKeys(connectionId)).append(GENERATION, generation))
                .forEach(tables::add));
        tables.sort(Comparator.comparingInt(table -> order(table, connectionId)));
        return tables;
    }

    /** Where a stored table sat in the discovery; an unreadable position is corruption, not zero. */
    private static int order(Document table, String connectionId) {
        if (!(table.get("order") instanceof Number number)) {
            throw unreadable(connectionId);
        }
        return number.intValue();
    }

    /** The half-open {@code _id} range holding every table document a connection owns. */
    private static Document ownedKeys(String connectionId) {
        if (connectionId.indexOf(SEPARATOR) >= 0) {
            // Such an id would make one connection's range overlap another's. The parser refuses one,
            // so arriving here with it is a defect in whatever built it rather than an author's doing.
            throw new IllegalArgumentException(
                    "connectionId must not contain '" + SEPARATOR + "': " + connectionId);
        }
        return new Document("$gte", connectionId + SEPARATOR)
                .append("$lt", connectionId + (char) (SEPARATOR + 1));
    }

    private static String tableKey(String connectionId, String generation, int order) {
        ownedKeys(connectionId);
        return connectionId + SEPARATOR + generation + SEPARATOR + order;
    }

    /**
     * Whether a stored document is a discovery of the model this build reads — that is, one whose
     * columns carry types resolved onto the tapstate namespace. Startup migrates legacy envelopes
     * before readers are exposed; direct callers still cannot mistake an old envelope for a new one.
     */
    static boolean carriesResolvedTypes(Document document) {
        return Integer.valueOf(RESOLVED_TYPES).equals(document.getInteger(MODEL_VERSION));
    }

    /**
     * Maps a discovery to its envelope document: the connection id as {@code _id}, the model version
     * stamp, the connector id and discovery time as scalars, and the generation whose table documents
     * make up this discovery. The tables themselves are documents of their own.
     */
    static Document envelope(DiscoveredSourceModel discovered, String generation) {
        return new Document("_id", discovered.connectionId())
                .append(MODEL_VERSION, RESOLVED_TYPES)
                .append("connectorId", discovered.connectorId())
                .append("discoveredAt", discovered.discoveredAt())
                .append(GENERATION, generation);
    }

    static Document tableDocument(SourceTable table) {
        List<Document> fields = new ArrayList<>();
        for (SourceField field : table.fields()) {
            // The document holds both type namespaces, so each key names the one it carries. The declared
            // type is null when discovery could not resolve it; stored as a null value, read back as null.
            fields.add(new Document("name", field.name())
                    .append("type", field.dataType())
                    .append("tapstateType", field.type().name())
                    .append("unknownBecause", field.unknownBecause()));
        }
        List<Document> indexes = new ArrayList<>();
        for (SourceIndex index : table.indexes()) {
            indexes.add(new Document("name", index.name())
                    .append("fields", List.copyOf(index.fields()))
                    .append("unique", index.unique()));
        }
        // A table nothing counted stores a null under the key rather than the key being left out; either
        // reads back as uncounted, and writing it keeps the shape of a stored table the same whether or
        // not the source could be counted.
        return new Document("name", table.name())
                .append("fields", fields)
                .append("primaryKey", List.copyOf(table.primaryKey()))
                .append("indexes", indexes)
                .append("approximateRowCount", table.approximateRowCount());
    }

    /**
     * The resolved type a stored field carries, or unknown when the document predates the resolution or
     * names a type this build does not know. An unreadable type is the absence of one, never a refusal of
     * the whole read: the model is a derived observation that re-discovery replaces.
     */
    /**
     * One stored column read back, with the reason it has no resolved type where it has none.
     *
     * <p><b>A reason written down at discovery wins over anything this could say.</b> The connector was
     * open then and is not now, so the cause it recorded - a shape with no member in the namespace, a
     * number described without a width - is not recoverable here; replacing it with a storage-side
     * remark would turn every such column into "the stored type did not resolve", which says only that
     * the record was read.
     *
     * <p>The three this side can attribute are its own: a record written before a resolved type was
     * kept, a spelling that is not a type in this build (a constant that went away), and a record that
     * stored the unknown without saying which one it was. They are different problems - the first is
     * ordinary and needs a re-discovery, the second is a compatibility break, the third is a record
     * from before this component existed.
     */
    private static SourceField field(String name, Document stored) {
        String declared = stored.getString("type");
        String spelling = stored.getString("tapstateType");
        if (spelling == null) {
            return new SourceField(name, declared, TapstateType.UNKNOWN,
                    "the stored record was written before a resolved type was kept");
        }
        TapstateType type = named(spelling);
        if (type == null) {
            return new SourceField(name, declared, TapstateType.UNKNOWN,
                    "the stored type '" + spelling + "' is not a tapstate type in this build");
        }
        if (type != TapstateType.UNKNOWN) {
            return new SourceField(name, declared, type);
        }
        String because = stored.getString("unknownBecause");
        return new SourceField(name, declared, TapstateType.UNKNOWN,
                because == null || because.isBlank()
                        ? "the stored record says the type is unknown and does not say which unknown"
                        : because);
    }

    /** The type a stored spelling names, or null where this build has no such type. */
    private static TapstateType named(String spelling) {
        for (TapstateType candidate : TapstateType.values()) {
            if (candidate.name().equals(spelling)) {
                return candidate;
            }
        }
        return null;
    }

    /** Reconstructs a discovery from its envelope and its table documents, or fails coded when unreadable. */
    static DiscoveredSourceModel toDiscovered(Document envelope, List<Document> storedTables) {
        String id = envelope.getString("_id");
        String connectorId = envelope.getString("connectorId");
        if (connectorId == null) {
            throw unreadable(id);
        }
        long discoveredAt = discoveredAt(envelope, id);
        List<SourceTable> tables = new ArrayList<>();
        for (Document table : storedTables) {
            tables.add(toTable(table, id));
        }
        return new DiscoveredSourceModel(id, connectorId, discoveredAt, new SourceModel(tables));
    }

    /** Reads the stored discovery time: an absent or non-numeric value is corrupt. */
    private static long discoveredAt(Document document, String id) {
        Object value = document.get("discoveredAt");
        if (!(value instanceof Number number)) {
            throw unreadable(id);
        }
        return number.longValue();
    }

    private static SourceTable toTable(Document table, String id) {
        String name = table.getString("name");
        if (name == null) {
            throw unreadable(id);
        }
        List<SourceField> fields = new ArrayList<>();
        for (Document field : documentList(table.get("fields"), id)) {
            String fieldName = field.getString("name");
            if (fieldName == null) {
                throw unreadable(id);
            }
            fields.add(field(fieldName, field));
        }
        List<SourceIndex> indexes = new ArrayList<>();
        for (Document index : documentList(table.get("indexes"), id)) {
            String indexName = index.getString("name");
            if (indexName == null) {
                throw unreadable(id);
            }
            indexes.add(new SourceIndex(indexName, stringList(index.get("fields"), id), unique(index, id)));
        }
        return new SourceTable(
                name, fields, stringList(table.get("primaryKey"), id), indexes, approximateRowCount(table));
    }

    /**
     * The row count a stored table carries, or null where it carries none — a document written before
     * counting existed, or a source that could not be counted. Absence stays absence rather than
     * becoming zero: zero says the table is empty, which is an answer a reader sizing state off it
     * would act on, and the two must not arrive as the same value. A non-numeric value is read as
     * absent for the same reason the resolved type is: the model is a derived observation that a
     * re-discovery replaces, so an unreadable measurement is one nobody took.
     */
    private static Long approximateRowCount(Document table) {
        return table.get("approximateRowCount") instanceof Number number ? number.longValue() : null;
    }

    /** Reads a stored array-of-documents field: an absent field is empty, a non-array or non-document element is corrupt. */
    private static List<Document> documentList(Object value, String id) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw unreadable(id);
        }
        List<Document> documents = new ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof Document document)) {
                throw unreadable(id);
            }
            documents.add(document);
        }
        return documents;
    }

    /** Reads a stored array-of-strings field: an absent field is empty, a non-array or non-string element is corrupt. */
    private static List<String> stringList(Object value, String id) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw unreadable(id);
        }
        List<String> strings = new ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof String string)) {
                throw unreadable(id);
            }
            strings.add(string);
        }
        return strings;
    }

    /** Reads an index's unique flag: an absent flag reads as false, a present non-boolean is corrupt. */
    private static boolean unique(Document index, String id) {
        Object value = index.get("unique");
        if (value == null) {
            return false;
        }
        if (!(value instanceof Boolean flag)) {
            throw unreadable(id);
        }
        return flag;
    }

    private static TapstateException unreadable(String id) {
        // A stored schema document whose shape cannot be reconstructed is store corruption, surfaced as a
        // coded io diagnostic rather than a bare crash while reconstructing.
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(id), "field", "schema"), null);
    }
}
