package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB SRS meta store: one durable coordination document per mining chain — the offset, consumer
 * cursor and schema truth that outlives the in-memory change ring. The document is keyed by the mining
 * chain id (as {@code _id}); each facet is advanced by its own atomic update, so a consumer that sets
 * its own cursor never clobbers another consumer's concurrent set or the chain's offset advance.
 *
 * <p>The consumer cursors are stored as a sub-document keyed by pipeline id — a resource id, which the
 * grammar forbids from containing a dot, so the id is a safe update path and one consumer's cursor is
 * set at {@code consumerOffsets.<pipelineId>} independently. The schema history is an append-only array
 * advanced by {@code $push}. The nullable positions are stored only when present, never as explicit
 * nulls.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A re-seed of an existing chain (which would discard its accumulated truth) and a mutate of
 * an unseeded chain are caller ordering errors — surfaced bare (an {@code IllegalStateException}), not
 * laundered into an io code that would hide the defect. A stored document that cannot be read back into
 * its model is coded {@code io.document-unreadable}.
 */
public final class MongoSrsMetaStore implements SrsMetaStore {

    private final MongoCollection<Document> collection;

    public MongoSrsMetaStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public Optional<SrsMeta> read(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", miningChainId)).first());
        return document == null ? Optional.empty() : Optional.of(toMeta(document));
    }

    @Override
    public void create(String miningChainId, String retention) {
        // Insert-only: insertOne fails on a duplicate _id, so an existing chain's accumulated offset /
        // cursor / schema truth is never discarded by a re-seed.
        Document document = toDocument(new SrsMeta(miningChainId, null, List.of(), null, List.of(), retention));
        try {
            collection.insertOne(document);
        } catch (MongoException e) {
            throw classifyInsertFailure(e, miningChainId);
        }
    }

    @Override
    public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
        Objects.requireNonNull(sourceReadOffset, "sourceReadOffset");
        update(miningChainId, new Document("$set", new Document("sourceReadOffset", sourceReadOffset)));
    }

    @Override
    public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        Objects.requireNonNull(offset, "offset");
        // Keyed by the dot-free pipeline id, so one consumer's cursor is set independently of the others'.
        update(miningChainId, new Document("$set",
                new Document("consumerOffsets." + offset.pipelineId(), consumerToDocument(offset))));
    }

    @Override
    public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
        update(miningChainId, consumerReadSeqUpdate(pipelineId, table, lastReadSeq));
    }

    @Override
    public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
        update(miningChainId, sinkAckedUpdate(pipelineId, position));
    }

    /**
     * The path-scoped update advancing one consumer's read cursor for one table: it sets only
     * {@code consumerOffsets.<pipelineId>.perTableSeq.<table>}, so the consumer's sink-acked position is
     * left untouched by a read-cursor advance. Both keys are dot-free — the pipeline id is a resource id
     * the grammar forbids a dot in, and an L1 stream name is a bare identifier — so the dotted path
     * addresses exactly one field. A deep {@code $set} creates the intermediate objects, so a reader may
     * advance before the consumer has any other cursor state.
     */
    static Document consumerReadSeqUpdate(String pipelineId, String table, long lastReadSeq) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        return new Document("$set",
                new Document("consumerOffsets." + pipelineId + ".perTableSeq." + table, lastReadSeq));
    }

    /**
     * The path-scoped update that advances one consumer's durable sink-acked position: a {@code $set} on
     * {@code consumerOffsets.<pipelineId>.sinkAckedSrcpos} and the two fields carrying the order it sat
     * at, so the reader's per-table cursor is left untouched by a sink-ack advance. The pipeline id is a
     * resource id the grammar forbids a dot in, so each dotted path addresses exactly one field. A deep
     * {@code $set} creates the intermediate objects, so a sink may ack before the consumer has any other
     * cursor state.
     *
     * <p>The three fields move together in one update. A token stored without its order can no longer be
     * ranked against anything, and an order stored without its token is nothing a read can resume from;
     * either alone would be a record no later comparison can use.
     */
    static Document sinkAckedUpdate(String pipelineId, ChainPosition position) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        String path = "consumerOffsets." + pipelineId + ".";
        Document fields = new Document(path + "sinkAckedEpoch", position.order().epoch())
                .append(path + "sinkAckedSeq", position.order().seq());
        if (position.token() != null) {
            fields.append(path + "sinkAckedSrcpos", position.token());
        }
        return new Document("$set", fields);
    }

    @Override
    public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
        Objects.requireNonNull(cdcStartPosition, "cdcStartPosition");
        if (snapshotEpoch < 0) {
            throw new IllegalArgumentException("snapshotEpoch must not be negative, got " + snapshotEpoch);
        }
        // One update, both fields: a resumed snapshot reads them together, so a state where the seam
        // position is stored without the generation it belongs to must not be reachable.
        update(miningChainId, new Document("$set", new Document("cdcStartPosition", cdcStartPosition)
                .append("snapshotEpoch", snapshotEpoch)));
    }

    @Override
    public long openEpoch(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        // An atomic increment read back after the write: two members opening the same chain must take two
        // different generations, so the counter is advanced by the store rather than read, added to and
        // written back. It touches only epoch, leaving any pinned snapshot generation where it is.
        Document updated = StoreIo.call(() -> collection.findOneAndUpdate(
                new Document("_id", miningChainId),
                new Document("$inc", new Document("epoch", 1L)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
        if (updated == null) {
            throw new IllegalStateException("srs meta mutate on an unseeded mining chain: " + miningChainId
                    + " (create must seed it first)");
        }
        return readEpoch(updated, "epoch");
    }

    @Override
    public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
        Objects.requireNonNull(version, "version");
        update(miningChainId, new Document("$push", new Document("schemaHistory", schemaToDocument(version))));
    }

    @Override
    public void markSnapshotComplete(String miningChainId, String table) {
        update(miningChainId, snapshotCompleteUpdate(table));
    }

    /**
     * The update that marks one table's snapshot drained: an {@code $addToSet} on
     * {@code snapshotCompletedTables}. A set add, not a push — the mark answers "has this table drained?",
     * so re-marking a table (a replay, a re-run of the snapshot) must be a no-op rather than a duplicate
     * entry.
     */
    static Document snapshotCompleteUpdate(String table) {
        Objects.requireNonNull(table, "table");
        return new Document("$addToSet", new Document("snapshotCompletedTables", table));
    }

    /**
     * Applies an atomic update to a seeded chain. A zero matched count means no document carried the id:
     * the chain was never seeded, a caller ordering error surfaced bare (not laundered into an io code).
     */
    private void update(String miningChainId, Document update) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        UpdateResult result = StoreIo.call(() -> collection.updateOne(new Document("_id", miningChainId), update));
        if (result.getMatchedCount() == 0) {
            throw new IllegalStateException("srs meta mutate on an unseeded mining chain: " + miningChainId
                    + " (create must seed it first)");
        }
    }

    /**
     * Classifies a failed seed insert: a duplicate {@code _id} is a caller ordering error (the chain was
     * already seeded), surfaced bare like the unseeded-mutate ordering error — not laundered into an io
     * code that would hide it; any other driver failure is a coded io diagnostic.
     */
    static RuntimeException classifyInsertFailure(MongoException e, String miningChainId) {
        if (e instanceof MongoWriteException write && write.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
            return new IllegalStateException(
                    "create on an already-seeded mining chain: " + miningChainId + " (create is insert-only)", e);
        }
        return StoreIo.coded(e);
    }

    /** Maps a meta record to its stored document: the mining chain id as {@code _id}, the rest as fields. */
    static Document toDocument(SrsMeta meta) {
        Document consumers = new Document();
        for (ConsumerOffset offset : meta.consumerOffsets()) {
            consumers.append(offset.pipelineId(), consumerToDocument(offset));
        }
        List<Document> schemaHistory = new ArrayList<>();
        for (SchemaVersion version : meta.schemaHistory()) {
            schemaHistory.add(schemaToDocument(version));
        }
        // The structural fields are always present (empty when seeded); the nullable positions are
        // appended only when set, so a seed reads back as a seed rather than as corruption.
        Document document = new Document("_id", meta.miningChainId())
                .append("consumerOffsets", consumers)
                .append("schemaHistory", schemaHistory)
                .append("snapshotCompletedTables", List.copyOf(meta.snapshotCompletedTables()));
        if (meta.sourceReadOffset() != null) {
            document.append("sourceReadOffset", meta.sourceReadOffset());
        }
        if (meta.cdcStartPosition() != null) {
            document.append("cdcStartPosition", meta.cdcStartPosition());
        }
        if (meta.retention() != null) {
            document.append("retention", meta.retention());
        }
        // Zero means "no generation opened" and "no snapshot pinned", which is also what an absent field
        // reads back as, so a seed stays a seed rather than carrying two fields that say nothing.
        if (meta.epoch() != 0L) {
            document.append("epoch", meta.epoch());
        }
        if (meta.snapshotEpoch() != 0L) {
            document.append("snapshotEpoch", meta.snapshotEpoch());
        }
        return document;
    }

    /**
     * Reads one generation counter out of a stored document. Absent is zero rather than corruption: the
     * meta field set is append-only and these fields are newer than the collection, so a document an older
     * build wrote has no generation opened. A stored value of another type is corruption, surfaced as a
     * coded io diagnostic rather than a bare cast failure.
     */
    private static long readEpoch(Document document, String field) {
        Object raw = document.get(field);
        if (raw == null) {
            return 0L;
        }
        if (raw instanceof Number number) {
            return number.longValue();
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(document.get("_id"))), null);
    }

    /** Reconstructs a meta record from its stored document. */
    static SrsMeta toMeta(Document document) {
        String id = document.getString("_id");
        Object consumersRaw = document.get("consumerOffsets");
        Object schemaRaw = document.get("schemaHistory");
        if (id == null || !(consumersRaw instanceof Document consumersDoc) || !(schemaRaw instanceof List<?> entries)) {
            // A stored meta missing a field this version requires is store corruption, surfaced as a
            // coded io diagnostic rather than a bare cast / unboxing crash while reconstructing.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), null);
        }
        List<ConsumerOffset> consumers = new ArrayList<>();
        for (Map.Entry<String, Object> entry : consumersDoc.entrySet()) {
            consumers.add(consumerFromDocument(entry.getKey(), asDocument(entry.getValue(), id)));
        }
        List<SchemaVersion> schemaHistory = new ArrayList<>();
        for (Object entry : entries) {
            schemaHistory.add(schemaFromDocument(asDocument(entry, id), id));
        }
        // Absent snapshotCompletedTables is not corruption, unlike the two structural fields above: the
        // meta field set is append-only and this field is newer than the collection, so a document written
        // by an older build simply has no table marked.
        Object completedRaw = document.get("snapshotCompletedTables");
        List<String> snapshotCompletedTables = new ArrayList<>();
        if (completedRaw instanceof List<?> completedEntries) {
            for (Object entry : completedEntries) {
                snapshotCompletedTables.add(String.valueOf(entry));
            }
        }
        return new SrsMeta(id, document.getString("sourceReadOffset"), consumers,
                document.getString("cdcStartPosition"), schemaHistory, document.getString("retention"),
                snapshotCompletedTables, readEpoch(document, "epoch"), readEpoch(document, "snapshotEpoch"));
    }

    /** Reconstructs one consumer cursor from its stored sub-document, keyed by the pipeline id. */
    private static ConsumerOffset consumerFromDocument(String pipelineId, Document document) {
        Map<String, Long> perTableSeq = new LinkedHashMap<>();
        Object perTableRaw = document.get("perTableSeq");
        if (perTableRaw instanceof Document perTableDoc) {
            for (Map.Entry<String, Object> entry : perTableDoc.entrySet()) {
                perTableSeq.put(entry.getKey(), ((Number) entry.getValue()).longValue());
            }
        } else if (perTableRaw != null) {
            // Present but not a sub-document is store corruption. Absent is a valid sink-ack-only consumer:
            // the sink created the entry (a sinkAckedSrcpos-only $set) before the reader published any
            // per-table cursor, mirroring how an absent sinkAckedSrcpos reads back as null. It reads as an
            // empty cursor rather than as corruption.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId), null);
        }
        return new ConsumerOffset(pipelineId, perTableSeq, sinkAckedFrom(document));
    }

    /** Reconstructs one schema version from its stored sub-document. */
    private static SchemaVersion schemaFromDocument(Document document, String miningChainId) {
        Long version = document.getLong("version");
        Long ddlSeq = document.getLong("ddlSeq");
        Object schemaRaw = document.get("schema");
        if (version == null || ddlSeq == null || !(schemaRaw instanceof Document schemaDoc)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", miningChainId), null);
        }
        return new SchemaVersion(version, new LinkedHashMap<>(schemaDoc), ddlSeq);
    }

    /** Reads a nested value as a document, or surfaces store corruption when it is not one. */
    private static Document asDocument(Object value, String miningChainId) {
        if (value instanceof Document document) {
            return document;
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", miningChainId), null);
    }

    /**
     * The acked position a consumer document carries, or null when it has none. A record whose token was
     * written without the order it sat at cannot be ranked against the reader's position, and reads back as
     * nothing acked: that pins a source-read advance where it stands, which only ever costs re-mining
     * changes already read - the direction that keeps them re-minable at all.
     */
    private static ChainPosition sinkAckedFrom(Document document) {
        Object epoch = document.get("sinkAckedEpoch");
        Object seq = document.get("sinkAckedSeq");
        if (!(epoch instanceof Number) || !(seq instanceof Number)) {
            return null;
        }
        return new ChainPosition(
                new SourceOrder(((Number) epoch).longValue(), ((Number) seq).longValue()),
                document.getString("sinkAckedSrcpos"));
    }

    /** Maps one consumer cursor to its stored sub-document (the per-table read cursor plus the acked position). */
    private static Document consumerToDocument(ConsumerOffset offset) {
        Document perTable = new Document();
        for (Map.Entry<String, Long> entry : offset.perTableSeq().entrySet()) {
            perTable.append(entry.getKey(), entry.getValue());
        }
        Document document = new Document("perTableSeq", perTable);
        if (offset.sinkAcked() != null) {
            document.append("sinkAckedEpoch", offset.sinkAcked().order().epoch())
                    .append("sinkAckedSeq", offset.sinkAcked().order().seq());
            if (offset.sinkAcked().token() != null) {
                document.append("sinkAckedSrcpos", offset.sinkAcked().token());
            }
        }
        return document;
    }

    /** Maps one schema version to its stored sub-document. */
    private static Document schemaToDocument(SchemaVersion version) {
        return new Document("version", version.version())
                .append("schema", new Document(version.schema()))
                .append("ddlSeq", version.ddlSeq());
    }
}
