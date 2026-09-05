package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB discovered-schema store: stores the discovery envelope for a connection as one
 * structured document keyed by the connection's id — the connector id and discovery time it reports,
 * and the source model's tables (with their fields, primary key and indexes) as nested sub-documents.
 *
 * <p>The envelope is a fixed shape of plain scalars and lists, so it is mapped field by field rather
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

    /** The model this build writes and reads: source types resolved onto the tapstate namespace. */
    static final int RESOLVED_TYPES = 1;

    private final MongoCollection<Document> collection;

    public MongoSchemaStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(DiscoveredSourceModel discovered) {
        Objects.requireNonNull(discovered, "discovered");
        // Upsert by the connection id (the document _id): the stored form is a full replacement, so a
        // re-discovery of the same connection overwrites in place rather than accumulating documents.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", discovered.connectionId()),
                toDocument(discovered),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<DiscoveredSourceModel> get(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", connectionId)).first());
        // A model written before the types were resolved answers as no model at all. Its columns would
        // all read as having no resolved type, which is refused wherever a resolved type is needed - and
        // refused with a diagnostic about the columns, telling the author to change an expression that
        // is not wrong. Answering "not discovered" instead gives them the one action that fixes it, and
        // discovering is what makes it true.
        if (document == null || !carriesResolvedTypes(document)) {
            return Optional.empty();
        }
        return Optional.of(toDiscovered(document));
    }

    /**
     * Whether a stored document is a discovery of the model this build reads — that is, one whose
     * columns carry types resolved onto the tapstate namespace. A document without the stamp predates
     * that resolution and is answered as no discovery at all, so the author is asked to discover rather
     * than told that every column they read has no resolved type.
     */
    static boolean carriesResolvedTypes(Document document) {
        return Integer.valueOf(RESOLVED_TYPES).equals(document.getInteger(MODEL_VERSION));
    }

    /**
     * Maps a discovery envelope to its stored document: the connection id as {@code _id}, the model
     * version stamp, the connector id and discovery time as scalars, and the model's tables as a field.
     */
    static Document toDocument(DiscoveredSourceModel discovered) {
        List<Document> tables = new ArrayList<>();
        for (SourceTable table : discovered.model().tables()) {
            tables.add(tableDocument(table));
        }
        return new Document("_id", discovered.connectionId())
                .append(MODEL_VERSION, RESOLVED_TYPES)
                .append("connectorId", discovered.connectorId())
                .append("discoveredAt", discovered.discoveredAt())
                .append("tables", tables);
    }

    private static Document tableDocument(SourceTable table) {
        List<Document> fields = new ArrayList<>();
        for (SourceField field : table.fields()) {
            // The document holds both type namespaces, so each key names the one it carries. The declared
            // type is null when discovery could not resolve it; stored as a null value, read back as null.
            fields.add(new Document("name", field.name())
                    .append("type", field.dataType())
                    .append("tapstateType", field.type().name()));
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
    private static TapstateType tapstateType(Document field) {
        String name = field.getString("tapstateType");
        if (name == null) {
            return TapstateType.UNKNOWN;
        }
        for (TapstateType candidate : TapstateType.values()) {
            if (candidate.name().equals(name)) {
                return candidate;
            }
        }
        return TapstateType.UNKNOWN;
    }

    /** Reconstructs a discovery envelope from its stored document, or fails coded when the shape is unreadable. */
    static DiscoveredSourceModel toDiscovered(Document document) {
        String id = document.getString("_id");
        String connectorId = document.getString("connectorId");
        if (connectorId == null) {
            throw unreadable(id);
        }
        long discoveredAt = discoveredAt(document, id);
        List<SourceTable> tables = new ArrayList<>();
        for (Document table : documentList(document.get("tables"), id)) {
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
            fields.add(new SourceField(fieldName, field.getString("type"), tapstateType(field)));
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
