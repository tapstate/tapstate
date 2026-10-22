package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import org.bson.Document;

import java.util.Objects;

/**
 * The MongoDB audit log: each audit record is stored as one append-only document. The document
 * carries the attempt time (as epoch millis, a plain long — no JSR-310 codec needed, matching the
 * state store), the subject, the operation id and the target. No {@code _id} is assigned, so every
 * record is a distinct append (the driver assigns a fresh ObjectId) — an audit log is never updated
 * or overwritten in place.
 *
 * <p>A driver IO failure during the append is translated into a coded io diagnostic through {@link
 * StoreIo}, so no driver type escapes the module (rule R3) and it surfaces loudly enough for the
 * control layer's audit gate to refuse the operation ("no audit, no execute").
 */
public final class MongoAuditStore implements AuditStore {

    private final MongoCollection<Document> collection;

    public MongoAuditStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void record(AuditRecord record) {
        Objects.requireNonNull(record, "record");
        StoreIo.run(() -> collection.insertOne(toDocument(record)));
    }

    /**
     * Maps an audit record to its append-only document; the timestamp as epoch millis, the rest as
     * fields. The declared version precondition and the detail are appended only when the operation had
     * them, so a record without them produces the same document shape every already-written record has.
     */
    static Document toDocument(AuditRecord record) {
        Document document = new Document("ts", record.timestamp().toEpochMilli())
                .append("principal", record.principal())
                .append("operationId", record.operationId())
                .append("resourceId", record.resourceId());
        if (record.expectedContentHash() != null) {
            document.append("expectedContentHash", record.expectedContentHash());
        }
        if (record.detail() != null) {
            document.append("detail", new Document(record.detail()));
        }
        return document;
    }
}
