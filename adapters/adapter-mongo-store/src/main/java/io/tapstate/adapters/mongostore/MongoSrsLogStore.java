package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogStore;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB durable change log: one document per change, keyed by the ring it was written to and the
 * sequence that ring assigned. The key is a compound {@code _id} -- {@code {ring, seq}} -- so a change
 * is looked up by exact key and cannot be written twice at the same position, both on the index Mongo
 * maintains for every collection.
 *
 * <p><strong>The same index answers the two questions that are not exact lookups</strong>, which is why
 * this store adds none of its own. BSON compares documents field by field in declaration order, so every
 * key of one ring forms a contiguous run of the {@code _id} index ordered by sequence, and a bounded
 * range over {@code _id} selects exactly that run: the largest sequence is its last entry, and a trim is
 * a range delete over its front. This holds only while every key is built with {@code ring} before
 * {@code seq}, so exactly one place builds one.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A stored document that cannot be read back into its model is coded
 * {@code io.document-unreadable}.
 */
public final class MongoSrsLogStore implements SrsLogStore {

    private static final String RING = "ring";
    private static final String SEQ = "seq";

    private final MongoCollection<Document> collection;

    public MongoSrsLogStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void store(String ring, long seq, SrsLogRecord record) {
        Objects.requireNonNull(ring, "ring");
        Objects.requireNonNull(record, "record");
        Document key = key(ring, seq);
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", key), toDocument(key, record), new ReplaceOptions().upsert(true)));
    }

    @Override
    public void storeAll(String ring, long firstSeq, List<SrsLogRecord> records) {
        Objects.requireNonNull(ring, "ring");
        Objects.requireNonNull(records, "records");
        if (records.isEmpty()) {
            return;
        }
        List<WriteModel<Document>> writes = new ArrayList<>(records.size());
        long seq = firstSeq;
        for (SrsLogRecord record : records) {
            Objects.requireNonNull(record, "record");
            Document key = key(ring, seq++);
            writes.add(new ReplaceOneModel<>(
                    new Document("_id", key), toDocument(key, record), new ReplaceOptions().upsert(true)));
        }
        // Ordered, so the run lands in the order the ring assigned it rather than in whatever order the
        // driver finds convenient. The run occupies consecutive sequences, and a reader that meets a gap
        // cannot tell a write still in flight from one that failed.
        StoreIo.run(() -> collection.bulkWrite(writes, new BulkWriteOptions().ordered(true)));
    }

    @Override
    public Optional<SrsLogRecord> load(String ring, long seq) {
        Objects.requireNonNull(ring, "ring");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", key(ring, seq))).first());
        return document == null ? Optional.empty() : Optional.of(toRecord(document));
    }

    @Override
    public long largestSequence(String ring) {
        Objects.requireNonNull(ring, "ring");
        Document last = StoreIo.call(() -> collection.find(ringRange(ring, Long.MAX_VALUE))
                .sort(new Document("_id", -1))
                .limit(1)
                .first());
        if (last == null) {
            return -1L;
        }
        Object id = last.get("_id");
        if (!(id instanceof Document key) || !(key.get(SEQ) instanceof Number seq)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id)), null);
        }
        return seq.longValue();
    }

    @Override
    public void trim(String ring, long throughSeq) {
        Objects.requireNonNull(ring, "ring");
        StoreIo.run(() -> collection.deleteMany(ringRange(ring, throughSeq)));
    }

    /**
     * The key of one change. Field order is load-bearing: {@code ring} first is what makes one ring a
     * contiguous run of the {@code _id} index, and {@code seq} second is what orders that run.
     */
    private static Document key(String ring, long seq) {
        return new Document(RING, ring).append(SEQ, seq);
    }

    /**
     * The keys of one ring up to and including {@code throughSeq}, as a bounded range over {@code _id}.
     * The lower bound carries the smallest sequence rather than being open, so the range cannot reach
     * into the ring that sorts before this one.
     */
    private static Document ringRange(String ring, long throughSeq) {
        return new Document("_id", new Document("$gte", key(ring, Long.MIN_VALUE))
                .append("$lte", key(ring, throughSeq)));
    }

    private static Document toDocument(Document key, SrsLogRecord record) {
        Document document = new Document("_id", key)
                .append("op", record.op().symbol())
                .append("ts", record.ts())
                .append("schemaVer", record.schemaVer());
        // The nullable fields are written only when present, never as explicit nulls -- the same shape the
        // meta store uses, so a reader tells "no position" from "a position that is the empty string".
        if (record.srcToken() != null) {
            document.append("srcToken", record.srcToken());
        }
        if (record.before() != null) {
            document.append("before", new Document(record.before()));
        }
        if (record.after() != null) {
            document.append("after", new Document(record.after()));
        }
        return document;
    }

    private static SrsLogRecord toRecord(Document document) {
        try {
            return new SrsLogRecord(
                    document.getString("srcToken"),
                    Op.fromSymbol(document.getString("op")),
                    document.getLong("ts"),
                    rowImage(document, "before"),
                    rowImage(document, "after"),
                    document.getLong("schemaVer"));
        } catch (RuntimeException e) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(document.get("_id"))), e);
        }
    }

    private static Map<String, Object> rowImage(Document document, String field) {
        Document image = document.get(field, Document.class);
        return image == null ? null : new LinkedHashMap<>(image);
    }
}
