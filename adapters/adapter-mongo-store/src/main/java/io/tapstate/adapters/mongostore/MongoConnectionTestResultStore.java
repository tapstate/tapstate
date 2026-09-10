package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB connection-test-result store: stores the latest test result for a connection as one
 * document keyed by the connection's id, holding the overall outcome, the per-item checks (each with its
 * optional diagnostics) and the test time.
 *
 * <p>The result is a fixed shape of plain scalars and lists, so it is mapped field by field rather than
 * through a generic value normalization; on read the driver's {@code Document} / list values are
 * reconstructed into the pure model, so no driver type escapes this module (rule R3). The overall outcome
 * is stored and read as its own field, never derived from the items. A stored document whose shape cannot
 * be reconstructed is surfaced as a coded {@code io.document-unreadable} diagnostic.
 */
public final class MongoConnectionTestResultStore implements ConnectionTestResultStore {

    private final MongoCollection<Document> collection;

    public MongoConnectionTestResultStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(ConnectionTestResult result) {
        Objects.requireNonNull(result, "result");
        // Upsert by the connection id (the document _id): the stored form is a full replacement, so a
        // re-test of the same connection overwrites in place rather than accumulating documents.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", result.connectionId()),
                toDocument(result),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<ConnectionTestResult> find(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", connectionId)).first());
        return document == null ? Optional.empty() : Optional.of(toResult(document));
    }

    /** Maps a result to its stored document: the connection id as {@code _id}, outcome and items as fields. */
    static Document toDocument(ConnectionTestResult result) {
        List<Document> items = new ArrayList<>();
        for (ConnectionTestItem item : result.items()) {
            // Optional diagnostics are stored as null values when absent, read back as null.
            items.add(new Document("name", item.name())
                    .append("status", item.status().name())
                    .append("message", item.message())
                    .append("reason", item.reason())
                    .append("solution", item.solution())
                    .append("connectorErrorCode", item.connectorErrorCode()));
        }
        return new Document("_id", result.connectionId())
                .append("connectorId", result.connectorId())
                .append("outcome", result.outcome().name())
                .append("items", items)
                .append("testedAt", result.testedAt());
    }

    /** Reconstructs a result from its stored document, or fails coded when the shape is unreadable. */
    static ConnectionTestResult toResult(Document document) {
        String id = document.getString("_id");
        String connectorId = document.getString("connectorId");
        if (connectorId == null) {
            throw unreadable(id);
        }
        ConnectionTestResult.Outcome outcome = outcome(document, id);
        long testedAt = testedAt(document, id);
        List<ConnectionTestItem> items = new ArrayList<>();
        for (Document item : documentList(document.get("items"), id)) {
            items.add(toItem(item, id));
        }
        return new ConnectionTestResult(id, connectorId, outcome, items, testedAt);
    }

    private static ConnectionTestItem toItem(Document item, String id) {
        String name = item.getString("name");
        if (name == null) {
            throw unreadable(id);
        }
        return new ConnectionTestItem(
                name,
                status(item, id),
                item.getString("message"),
                item.getString("reason"),
                item.getString("solution"),
                item.getString("connectorErrorCode"));
    }

    /** Reads the stored overall outcome: an absent or unrecognized value is corrupt. */
    private static ConnectionTestResult.Outcome outcome(Document document, String id) {
        String value = document.getString("outcome");
        if (value == null) {
            throw unreadable(id);
        }
        try {
            return ConnectionTestResult.Outcome.valueOf(value);
        } catch (IllegalArgumentException unknown) {
            throw unreadable(id);
        }
    }

    /** Reads a stored item status: an absent or unrecognized value is corrupt. */
    private static ConnectionTestItem.Status status(Document item, String id) {
        String value = item.getString("status");
        if (value == null) {
            throw unreadable(id);
        }
        try {
            return ConnectionTestItem.Status.valueOf(value);
        } catch (IllegalArgumentException unknown) {
            throw unreadable(id);
        }
    }

    /** Reads the stored test time: an absent or non-numeric value is corrupt. */
    private static long testedAt(Document document, String id) {
        Object value = document.get("testedAt");
        if (!(value instanceof Number number)) {
            throw unreadable(id);
        }
        return number.longValue();
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

    private static TapstateException unreadable(String id) {
        // A stored result document whose shape cannot be reconstructed is store corruption, surfaced as a
        // coded io diagnostic rather than a bare crash while reconstructing.
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(id), "field", "result"), null);
    }
}
