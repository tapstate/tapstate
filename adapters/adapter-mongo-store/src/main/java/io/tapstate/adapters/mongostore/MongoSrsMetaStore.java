package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
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
 * <p>Each consumer's own state is stored as a sub-document keyed by pipeline id — a resource id, which
 * the grammar forbids from containing a dot, so the id is a safe update path and one consumer is updated
 * at {@code consumerOffsets.<pipelineId>} independently. That sub-document holds everything belonging to
 * one pipeline rather than to the chain: its read cursor, its acked position, and the tables whose initial
 * load its sink has confirmed. The schema history is an append-only array advanced by {@code $push}. The
 * nullable positions are stored only when present, never as explicit nulls.
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

    /**
     * Fetches the consumer cursors alone, asking the endpoint for that field and no other.
     *
     * <p>This exists because of what it does not carry back. The record's schema history grows by one
     * entry per DDL and is never trimmed, and the cdc write path -- which reads this on every run of
     * changes -- never looks at it. Measured against a real endpoint on a chain with 500 DDLs behind it,
     * the whole record is 671 KB and reads at 6.4 ms, while this projection reads at 0.5 ms and does not
     * move as the history grows.
     *
     * <p>It cannot go through the shared reconstruction: that one requires the schema history to be
     * present and reports a document without it as corruption, which is the right reading there and the
     * wrong one here, where its absence was asked for.
     */
    @Override
    public List<ConsumerOffset> consumerOffsets(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("consumerOffsets"))
                .first());
        if (document == null) {
            return List.of();
        }
        String id = String.valueOf(document.get("_id"));
        Object consumersRaw = document.get("consumerOffsets");
        if (!(consumersRaw instanceof Document consumersDoc)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id), null);
        }
        List<ConsumerOffset> consumers = new ArrayList<>();
        for (Map.Entry<String, Object> entry : consumersDoc.entrySet()) {
            consumers.add(consumerFromDocument(entry.getKey(), asDocument(entry.getValue(), id)));
        }
        return List.copyOf(consumers);
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
    public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        // Two updates, and the split is the guard. The first carries the ordering condition in its own
        // filter, so the comparison and the write are one atomic act: a read-then-write would let a second
        // member land its advance in between and be overwritten by this one, which is the rewind this
        // exists to stop. It matches nothing when the recorded position already ranks at or after this one.
        long matched = StoreIo.call(() -> collection.updateOne(
                sourceReadAdvanceFilter(miningChainId, position.order()),
                new Document("$set", sourceReadFields(position))).getMatchedCount());
        if (matched > 0) {
            return;
        }
        // Nothing matched, which is either "the chain is not seeded" -- a caller ordering error the other
        // mutators raise too -- or "this position does not move the chain forward", which is ordinary and
        // silent. Only a second look tells them apart, and it runs on the path that changed nothing.
        requireSeeded(miningChainId);
    }

    /**
     * The filter that admits an advance: this chain, and a recorded position strictly before {@code order}
     * — no record yet, or a lower generation, or the same generation and a lower sequence. Positions are
     * ranked by generation first because a rebuilt ring numbers its sequences from zero again, so a
     * sequence alone is only meaningful within the ring that assigned it.
     */
    static Document sourceReadAdvanceFilter(String miningChainId, SourceOrder order) {
        return new Document("_id", miningChainId).append("$or", List.of(
                new Document("sourceReadEpoch", new Document("$exists", false)),
                new Document("sourceReadEpoch", new Document("$lt", order.epoch())),
                new Document("sourceReadEpoch", order.epoch())
                        .append("sourceReadSeq", new Document("$lt", order.seq()))));
    }

    /**
     * The fields one advance writes: the order it reached and, when the position carries one, the token.
     * They move together — a token stored without its order can no longer be ranked, and an order without
     * its token is nothing a read can resume from.
     */
    private static Document sourceReadFields(ChainPosition position) {
        Document fields = new Document("sourceReadEpoch", position.order().epoch())
                .append("sourceReadSeq", position.order().seq());
        if (position.token() != null) {
            fields.append("sourceReadOffset", position.token());
        }
        return fields;
    }

    /** Raises the caller ordering error the advancing mutators share when a chain has no record. */
    private void requireSeeded(String miningChainId) {
        Document existing = StoreIo.call(() -> collection.find(new Document("_id", miningChainId)).first());
        if (existing == null) {
            throw new IllegalStateException("srs meta mutate on an unseeded mining chain: " + miningChainId
                    + " (create must seed it first)");
        }
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
    public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
        update(miningChainId, snapshotCompleteUpdate(pipelineId, table));
    }

    /**
     * The update that marks one table's snapshot drained for one consumer: an {@code $addToSet} on
     * {@code consumerOffsets.<pipelineId>.snapshotCompletedTables}. A set add, not a push — the mark
     * answers "has this table landed in this pipeline's target?", so re-marking a table (a replay, a
     * re-run of the snapshot) must be a no-op rather than a duplicate entry.
     *
     * <p>Scoped under the consumer for the same reason its cursor is: the pipeline id is a resource id the
     * grammar forbids a dot in, so the dotted path addresses exactly one consumer's set and cannot reach a
     * neighbour's. Recording it against the chain instead is what let a pipeline new to a shared chain read
     * another pipeline's answer and skip a load it had never done. The path creates the consumer entry when
     * the pipeline has none yet, and touches nothing else in it.
     */
    static Document snapshotCompleteUpdate(String pipelineId, String table) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        return new Document("$addToSet",
                new Document("consumerOffsets." + pipelineId + ".snapshotCompletedTables", table));
    }

    @Override
    public List<String> miningChainIdsWithConsumer(String pipelineId) {
        // Asked of the chains, not of the consumer: a chain carries its consumers, so the presence of the
        // dot-free pipeline id under consumerOffsets is itself the membership test. Only the id is read,
        // never the record, so enumerating never reconstructs — and so never fails — on a corrupt document.
        Document filter = consumerPresenceFilter(pipelineId);
        return StoreIo.call(() -> collection.find(filter)
                .projection(Projections.include("_id"))
                .map(document -> document.getString("_id"))
                .into(new ArrayList<>()));
    }

    @Override
    public void dropChain(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        // deleteOne on a missing _id removes nothing and reports so without failing, which is the no-op
        // an absent chain is meant to be.
        StoreIo.run(() -> collection.deleteOne(new Document("_id", miningChainId)));
    }

    @Override
    public void detachConsumer(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        // Deliberately not routed through update(): a detach is idempotent, so an absent chain is the end
        // condition already met rather than the ordering error the advancing mutators treat it as.
        Document filter = new Document("_id", miningChainId);
        StoreIo.run(() -> collection.updateOne(filter, detachConsumerUpdate(pipelineId)));
    }

    /**
     * The membership test for one consumer: a chain matches when it carries a cursor at
     * {@code consumerOffsets.<pipelineId>}. The pipeline id is a resource id the grammar forbids a dot in,
     * so the dotted path addresses exactly one field and cannot reach into a neighbouring consumer's.
     */
    static Document consumerPresenceFilter(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return new Document("consumerOffsets." + pipelineId, new Document("$exists", true));
    }

    /**
     * The path-scoped update that removes one consumer from a chain: an {@code $unset} of
     * {@code consumerOffsets.<pipelineId>} alone, so every other consumer's cursor and the chain's own
     * offset, cdc start position and schema history survive it untouched. Removing the whole entry rather
     * than blanking its positions is what takes the departing consumer out of the two minimums that would
     * otherwise still fold it in.
     */
    static Document detachConsumerUpdate(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return new Document("$unset", new Document("consumerOffsets." + pipelineId, ""));
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
                .append("schemaHistory", schemaHistory);
        if (meta.sourceRead() != null) {
            document.putAll(sourceReadFields(meta.sourceRead()));
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
        return new SrsMeta(id, sourceReadFrom(document), consumers,
                document.getString("cdcStartPosition"), schemaHistory, document.getString("retention"),
                readEpoch(document, "epoch"), readEpoch(document, "snapshotEpoch"));
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
        return new ConsumerOffset(
                pipelineId, perTableSeq, sinkAckedFrom(document), snapshotCompletedFrom(document));
    }

    /**
     * The tables one consumer has finished loading, empty when it has finished none. Absent is not
     * corruption: a consumer entry is created by whichever of its three writers gets there first, and the
     * two position writers create it without this field.
     */
    private static List<String> snapshotCompletedFrom(Document document) {
        Object raw = document.get("snapshotCompletedTables");
        List<String> completed = new ArrayList<>();
        if (raw instanceof List<?> entries) {
            for (Object entry : entries) {
                completed.add(String.valueOf(entry));
            }
        }
        return completed;
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
    /**
     * How far the chain has read, or null when nothing has read it. A document written before this record
     * carried an order has a token and no order: it reads back as nothing read, which costs re-mining
     * changes already read rather than skipping changes that were not — the direction that loses nothing.
     */
    private static ChainPosition sourceReadFrom(Document document) {
        Object epoch = document.get("sourceReadEpoch");
        Object seq = document.get("sourceReadSeq");
        if (!(epoch instanceof Number) || !(seq instanceof Number)) {
            return null;
        }
        return new ChainPosition(
                new SourceOrder(((Number) epoch).longValue(), ((Number) seq).longValue()),
                document.getString("sourceReadOffset"));
    }

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

    /**
     * Maps one consumer's record to its stored sub-document: the per-table read cursor, the tables it has
     * finished loading (omitted while it has finished none, so a cursor-only consumer stays a cursor-only
     * consumer) and the acked position.
     */
    private static Document consumerToDocument(ConsumerOffset offset) {
        Document perTable = new Document();
        for (Map.Entry<String, Long> entry : offset.perTableSeq().entrySet()) {
            perTable.append(entry.getKey(), entry.getValue());
        }
        Document document = new Document("perTableSeq", perTable);
        if (!offset.snapshotCompletedTables().isEmpty()) {
            document.append("snapshotCompletedTables", List.copyOf(offset.snapshotCompletedTables()));
        }
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
