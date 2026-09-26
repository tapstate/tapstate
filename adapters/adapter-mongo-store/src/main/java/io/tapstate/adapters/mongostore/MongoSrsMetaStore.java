package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
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

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The MongoDB SRS meta store: one durable coordination document per mining chain — the offset and schema
 * truth that outlive the in-memory change ring — plus one cursor document per consumer pipeline. The chain
 * document is keyed by the mining chain id (as {@code _id}); a consumer document has a compound id naming
 * its chain and pipeline, so each facet advances independently and no consumer cursor can fill the chain
 * document until its own source position can no longer move.
 *
 * <p>A consumer document holds everything belonging to one pipeline rather than to the chain: its read
 * cursor, its acked position, the tables whose initial load its sink has confirmed, and the seam and
 * generation at which its load began. Records written before that split carried those documents under the
 * chain's {@code consumerOffsets} field. The first consumer write migrates them; a chain write that finds
 * an old record already at the endpoint ceiling does the same and retries. The copy is insert-only and the
 * embedded map is cleared only after every cursor has landed, so an interrupted migration loses nothing.
 * That read/copy/clear is one transaction: concurrent migrations serialize before a detach deletes its
 * cursor, so a copier holding an older snapshot cannot restore the departed consumer. The schema history
 * remains an append-only array on the chain document, advanced by an update that keeps the newest entries
 * inside a fixed byte budget. Nullable positions are stored only when present, never as explicit nulls.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A re-seed of an existing chain (which would discard its accumulated truth) and a mutate of
 * an unseeded chain are caller ordering errors — surfaced bare (an {@code IllegalStateException}), not
 * laundered into an io code that would hide the defect. A stored document that cannot be read back into
 * its model is coded {@code io.document-unreadable}.
 */
public final class MongoSrsMetaStore implements SrsMetaStore {

    /**
     * A root-local fence advanced with every split-cursor write. It has no model meaning: its write is
     * what makes the root-existence check conflict with a concurrent lifecycle delete.
     */
    private static final String CONSUMER_WRITE_REVISION = "consumerWriteRevision";

    /**
     * Per table, the ring sequence up to which this consumer has nothing left to receive -- the last change
     * its sink confirmed there, or where the ring stood when it arrived. What a run of it carries on from.
     * Raised with the chain's acked position and dropped with the rest of the consumer when its record is
     * rewritten, as a write-back that lets the acks go does.
     */
    static final String PER_TABLE_RING_DONE = "perTableRingDone";
    static final String SINK_ACKED_BY_TABLE = "sinkAckedByTable";

    /**
     * How much of a chain's schema history the record retains, in bytes of stored entries.
     *
     * <p>The record is one document, and the endpoint refuses a write whose result passes its 16 MiB
     * ceiling — while the history is the one facet that grows for the life of a chain, by an entry per
     * DDL it has ever seen. Left unbounded it arrives at a state where the only write that can record a
     * schema change is a write that cannot land, and the chain then cannot say that its source's schema
     * moved: not a slow read, a chain stuck.
     *
     * <p>Bytes rather than a count of entries, because bytes are what the ceiling counts. An entry is a
     * table's field schema, and a wide table's is a hundred times a narrow one's, so a count that holds
     * the record under the ceiling at one entry size does not at another. A sixteenth of the ceiling
     * leaves the rest of the chain record — its positions and structural fields — ample room, and is a
     * window hundreds of versions long at the entry size this product's records carry.
     */
    private static final long SCHEMA_HISTORY_BUDGET_BYTES = 1024L * 1024L;

    /**
     * What one history entry costs the array beyond its own bytes: the key, which is the element's index,
     * and the type byte. A margin rather than an accounting — the key widens by a digit every tenfold —
     * and stated as a bound, so an entry is never credited with fewer bytes than it occupies and what is
     * retained stays inside the budget rather than touching it.
     */
    private static final int HISTORY_ENTRY_OVERHEAD_BYTES = 12;

    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> consumers;
    private final MongoClient client;
    private final Clock clock;

    public MongoSrsMetaStore(MongoClient client, MongoCollection<Document> collection) {
        this(client, collection, collection, Clock.systemUTC());
    }

    /** The same store reading a given clock, for a caller that needs the recorded time to be decidable. */
    public MongoSrsMetaStore(MongoClient client, MongoCollection<Document> collection, Clock clock) {
        this(client, collection, collection, clock);
    }

    /** A store whose chain roots and per-consumer cursors live in their declared collections. */
    public MongoSrsMetaStore(
            MongoClient client, MongoCollection<Document> collection, MongoCollection<Document> consumers) {
        this(client, collection, consumers, Clock.systemUTC());
    }

    /** The same two-collection store reading a given clock. */
    MongoSrsMetaStore(MongoClient client, MongoCollection<Document> collection,
            MongoCollection<Document> consumers, Clock clock) {
        this.client = Objects.requireNonNull(client, "client");
        this.collection = Objects.requireNonNull(collection, "collection");
        this.consumers = Objects.requireNonNull(consumers, "consumers");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public Optional<SrsMeta> read(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", miningChainId)).first());
        if (document == null) {
            return Optional.empty();
        }
        SrsMeta chain = toMeta(document);
        return Optional.of(new SrsMeta(
                chain.miningChainId(),
                chain.sourceRead(),
                mergedConsumers(document),
                chain.schemaHistory(),
                chain.retention(),
                chain.epoch(),
                chain.sourceReadAt()));
    }

    /**
     * Fetches the consumer cursors alone, asking the chain document only for its legacy field and reading
     * the split cursor documents without carrying the schema history over the wire.
     *
     * <p>This exists because of what it does not carry back. The record's schema history grows by one
     * entry per DDL, up to the bound the record keeps it under, and the cdc write path -- which reads
     * this on every run of changes -- never looks at it. Measured against a real endpoint on a chain with
     * 500 DDLs behind it, the whole record is 671 KB and reads at 6.4 ms, while this projection reads at
     * 0.5 ms and does not move as the history grows.
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
        return mergedConsumers(document);
    }

    @Override
    public Optional<PhysicalSelection> physicalSelection(String miningChainId) {
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include(
                        "physicalCaptureEpoch", "physicalCaptureRevision", "physicalCaptureTables")).first());
        if (root == null || (!root.containsKey("physicalCaptureEpoch")
                && !root.containsKey("physicalCaptureTables"))) {
            return Optional.empty();
        }
        Object rawEpoch = root.get("physicalCaptureEpoch");
        Object rawTables = root.get("physicalCaptureTables");
        if (!(rawEpoch instanceof Number epoch) || !(rawTables instanceof List<?> tables)
                || tables.isEmpty() || tables.stream().anyMatch(table -> !(table instanceof String))) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", miningChainId, "field", "physicalCaptureTables"), null);
        }
        Object rawRevision = root.get("physicalCaptureRevision");
        if (rawRevision != null && !(rawRevision instanceof Number)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", miningChainId, "field", "physicalCaptureRevision"), null);
        }
        long revision = rawRevision == null ? 1L : ((Number) rawRevision).longValue();
        return Optional.of(new PhysicalSelection(epoch.longValue(), revision, tables.stream()
                .map(String.class::cast).toList()));
    }

    @Override
    public boolean publishPhysicalSelection(String miningChainId, PhysicalSelection selection) {
        Objects.requireNonNull(selection, "selection");
        if (selection.revision() != 1L) {
            throw new IllegalArgumentException("a newly opened ring starts at physical capture revision one");
        }
        List<String> tables = selection.tables().stream().distinct().sorted().toList();
        Document sameSelection = new Document("physicalCaptureEpoch", selection.epoch())
                .append("physicalCaptureTables", tables)
                .append("$or", List.of(
                        new Document("physicalCaptureRevision", new Document("$exists", false)),
                        new Document("physicalCaptureRevision", 1L)));
        Document filter = new Document("_id", miningChainId)
                .append("epoch", selection.epoch())
                .append("$expr", new Document("$setIsSubset", List.of(
                        new Document("$ifNull", List.of("$physicalCaptureRequested", List.of())), tables)))
                .append("$or", List.of(
                        new Document("physicalCaptureEpoch", new Document("$exists", false)),
                        new Document("physicalCaptureEpoch", new Document("$lt", selection.epoch())),
                        sameSelection));
        Document fields = new Document("physicalCaptureEpoch", selection.epoch())
                .append("physicalCaptureRevision", 1L)
                .append("physicalCaptureTables", tables);
        long matched = StoreIo.call(() -> collection.updateOne(filter,
                new Document("$set", fields)).getMatchedCount());
        if (matched == 0) {
            requireSeeded(miningChainId);
        }
        return matched == 1;
    }

    @Override
    public List<String> requestedPhysicalTables(String miningChainId) {
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("physicalCaptureRequested")).first());
        if (root == null || !root.containsKey("physicalCaptureRequested")) {
            return List.of();
        }
        Object raw = root.get("physicalCaptureRequested");
        if (!(raw instanceof List<?> tables) || tables.stream().anyMatch(table -> !(table instanceof String))) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", miningChainId, "field", "physicalCaptureRequested"), null);
        }
        return tables.stream().map(String.class::cast).distinct().sorted().toList();
    }

    @Override
    public boolean requestPhysicalTables(String miningChainId, long epoch, List<String> tables) {
        Objects.requireNonNull(tables, "tables");
        if (epoch < 1 || tables.isEmpty() || tables.stream().anyMatch(table -> table == null || table.isBlank())) {
            throw new IllegalArgumentException("physical capture expansion requires an open generation and tables");
        }
        long matched = StoreIo.call(() -> collection.updateOne(
                new Document("_id", miningChainId).append("epoch", epoch),
                new Document("$addToSet", new Document("physicalCaptureRequested",
                        new Document("$each", tables.stream().distinct().sorted().toList()))))
                .getMatchedCount());
        if (matched == 0) {
            requireSeeded(miningChainId);
        }
        return matched == 1;
    }

    @Override
    public boolean replacePhysicalSelection(
            String miningChainId, PhysicalSelection expected, PhysicalSelection replacement) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(replacement, "replacement");
        if (expected.epoch() != replacement.epoch()
                || replacement.revision() != expected.revision() + 1
                || !replacement.tables().containsAll(expected.tables())) {
            throw new IllegalArgumentException("a physical subscription replacement must expand the current selection");
        }
        Document filter = new Document("_id", miningChainId)
                .append("epoch", expected.epoch())
                .append("physicalCaptureEpoch", expected.epoch())
                .append("physicalCaptureTables", expected.tables())
                .append("$expr", new Document("$setIsSubset", List.of(
                        new Document("$ifNull", List.of("$physicalCaptureRequested", List.of())),
                        replacement.tables())));
        if (expected.revision() == 1L) {
            filter.append("$or", List.of(
                    new Document("physicalCaptureRevision", new Document("$exists", false)),
                    new Document("physicalCaptureRevision", 1L)));
        } else {
            filter.append("physicalCaptureRevision", expected.revision());
        }
        long matched = StoreIo.call(() -> collection.updateOne(filter,
                new Document("$set", new Document("physicalCaptureRevision", replacement.revision())
                        .append("physicalCaptureTables", replacement.tables()))).getMatchedCount());
        if (matched == 0) {
            requireSeeded(miningChainId);
        }
        return matched == 1;
    }

    @Override
    public void clearPhysicalRequests(String miningChainId, long epoch, List<String> tables) {
        if (tables.isEmpty()) {
            return;
        }
        long matched = StoreIo.call(() -> collection.updateOne(
                new Document("_id", miningChainId).append("epoch", epoch),
                new Document("$pullAll", new Document("physicalCaptureRequested", tables)))
                .getMatchedCount());
        if (matched == 0) {
            requireSeeded(miningChainId);
        }
    }

    @Override
    public void create(String miningChainId, String retention) {
        // Insert-only: insertOne fails on a duplicate _id, so an existing chain's accumulated offset /
        // cursor / schema truth is never discarded by a re-seed.
        Document document = toDocument(new SrsMeta(miningChainId, null, List.of(), List.of(), retention));
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
        long matched = writeChainWithConsumerMigration(miningChainId, () -> collection.updateOne(
                sourceReadAdvanceFilter(miningChainId, position.order()),
                new Document("$set", sourceReadFields(position, Instant.now(clock)))).getMatchedCount());
        if (matched > 0) {
            return;
        }
        // Nothing matched, which is either "the chain is not seeded" -- a caller ordering error the other
        // mutators raise too -- or "this position does not move the chain forward", which is ordinary and
        // silent. Only a second look tells them apart, and it runs on the path that changed nothing.
        requireSeeded(miningChainId);
    }

    @Override
    public boolean physicalPrefixTrusted(String miningChainId) {
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("physicalPrefixTrusted")).first());
        return root != null && Boolean.TRUE.equals(root.get("physicalPrefixTrusted"));
    }

    @Override
    public boolean establishPhysicalAnchor(String miningChainId, ChainPosition position) {
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        Objects.requireNonNull(position.token(), "physical source anchor token");
        Document fields = sourceReadFields(position, Instant.now(clock))
                .append("physicalPrefixTrusted", true);
        long matched = writeChainWithConsumerMigration(miningChainId, () -> collection.updateOne(
                new Document("_id", miningChainId)
                        .append("epoch", position.order().epoch())
                        .append("sourceReadOffset", new Document("$exists", false)),
                new Document("$set", fields)).getMatchedCount());
        if (matched == 1) {
            return true;
        }
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("epoch", "physicalPrefixTrusted"))
                .first());
        if (root == null) {
            throw unseededChain(miningChainId);
        }
        return readEpoch(root, "epoch") == position.order().epoch()
                && Boolean.TRUE.equals(root.get("physicalPrefixTrusted"));
    }

    @Override
    public void rewindSourceReadOffset(String miningChainId, String token) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(token, "token");
        // One unconditional update, and the unset is half of what it does. The order recorded beside the
        // token says where the engine observed that token in the ring, and this token was not observed
        // here at all -- leaving the old one in place would have the record claim the new position sits
        // exactly where the old one did, in the comparison that decides what is safe to forget.
        long matched = writeChainWithConsumerMigration(miningChainId, () -> collection.updateOne(
                        new Document("_id", miningChainId),
                        new Document("$set", new Document("sourceReadOffset", token)
                                .append("sourceReadAt", Instant.now(clock).toEpochMilli())
                                .append("physicalPrefixTrusted", true))
                                .append("$unset", new Document("sourceReadEpoch", "")
                                        .append("sourceReadSeq", "")))
                .getMatchedCount());
        if (matched == 0) {
            // The filter names the chain and nothing else, so nothing matching can only mean no record.
            requireSeeded(miningChainId);
        }
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
     * The fields a write to the read offset lays down: the order it reached, the token, and when it was
     * written. An advance carries both halves of the position — a token stored without its order can no
     * longer be ranked, and an order without its token is nothing a read can resume from — so each part
     * is written only when it is there, and after a rewind the order is the part that is not.
     */
    private static Document sourceReadFields(ChainPosition position, Instant at) {
        Document fields = new Document();
        if (position.order() != null) {
            fields.append("sourceReadEpoch", position.order().epoch())
                    .append("sourceReadSeq", position.order().seq());
        }
        if (position.token() != null) {
            fields.append("sourceReadOffset", position.token());
        }
        if (at != null) {
            fields.append("sourceReadAt", at.toEpochMilli());
        }
        return fields;
    }

    /** Raises the caller ordering error the advancing mutators share when a chain has no record. */
    private void requireSeeded(String miningChainId) {
        Document existing = StoreIo.call(() -> collection.find(new Document("_id", miningChainId)).first());
        if (existing == null) {
            throw unseededChain(miningChainId);
        }
    }

    @Override
    public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
        Objects.requireNonNull(offset, "offset");
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> consumers.replaceOne(session,
                consumerKey(miningChainId, offset.pipelineId()),
                consumerDocument(miningChainId, offset),
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
        updateConsumer(miningChainId, pipelineId, consumerReadSeqUpdate(pipelineId, table, lastReadSeq));
    }

    @Override
    public void selectConsumerTables(
            String miningChainId, String pipelineId, List<String> tables, long epoch,
            String cursorWriterToken) {
        Objects.requireNonNull(tables, "tables");
        if (cursorWriterToken == null || cursorWriterToken.isBlank()) {
            throw new IllegalArgumentException("consumer cursor writer token must be non-blank");
        }
        if (epoch < 1) {
            throw new IllegalArgumentException("consumer selection epoch must be positive");
        }
        List<String> selected = List.copyOf(tables);
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> {
            Document root = collection.find(session, new Document("_id", miningChainId))
                    .projection(Projections.include("epoch")).first();
            if (root == null || readEpoch(root, "epoch") != epoch) {
                throw new IllegalStateException("consumer selection must match the open ring generation");
            }
            Document key = consumerKey(miningChainId, pipelineId);
            Document prior = consumers.find(session, key)
                    .projection(Projections.include(
                            "selectedTables", "selectedTablesEpoch", "cursorWriterToken", "perTableSeq",
                            PER_TABLE_RING_DONE, SINK_ACKED_BY_TABLE))
                    .first();
            List<String> previous = prior == null ? null : selectedTablesFrom(prior, pipelineId);
            Long previousEpoch = prior == null ? null : selectedTablesEpochFrom(prior);
            String previousToken = prior == null ? null : cursorWriterTokenFrom(prior, pipelineId);
            if (selected.equals(previous) && Objects.equals(epoch, previousEpoch)
                    && cursorWriterToken.equals(previousToken)) {
                return;
            }
            Object rawCursors = prior == null ? null : prior.get("perTableSeq");
            if (rawCursors != null && !(rawCursors instanceof Document)) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", pipelineId, "field", "perTableSeq"), null);
            }
            Document priorCursors = (Document) rawCursors;
            // Only another source of this same prepared run may reuse read progress. A replacement job
            // may resume below the old reader's cursor, even while the ring generation stays the same.
            Document retained = new Document();
            if (previous != null && Objects.equals(epoch, previousEpoch)
                    && cursorWriterToken.equals(previousToken)
                    && priorCursors != null) {
                for (String table : selected) {
                    if (previous.contains(table) && priorCursors.containsKey(table)) {
                        retained.put(table, priorCursors.get(table));
                    }
                }
            }
            Document retainedDone = new Document();
            Document retainedAcks = new Document();
            if (prior != null && Objects.equals(epoch, previousEpoch)) {
                Object rawDone = prior.get(PER_TABLE_RING_DONE);
                if (rawDone != null && !(rawDone instanceof Document)) {
                    throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                            Map.of("id", pipelineId, "field", PER_TABLE_RING_DONE), null);
                }
                Document done = (Document) rawDone;
                Map<String, ChainPosition> acks = tableAcksFrom(prior, pipelineId);
                for (String table : selected) {
                    if (done != null && done.containsKey(table)) {
                        retainedDone.put(table, done.get(table));
                    }
                    ChainPosition ack = acks.get(table);
                    if (ack != null) {
                        retainedAcks.put(table, positionToDocument(ack));
                    }
                }
            }
            Document update = new Document("$set", new Document("selectedTables", selected)
                    .append("selectedTablesEpoch", epoch)
                    .append("cursorWriterToken", cursorWriterToken)
                    .append("perTableSeq", retained)
                    .append(PER_TABLE_RING_DONE, retainedDone)
                    .append(SINK_ACKED_BY_TABLE, retainedAcks))
                    .append("$setOnInsert", consumerIdentity(miningChainId, pipelineId));
            consumers.updateOne(session, key, update, new UpdateOptions().upsert(true));
        });
    }

    @Override
    public void advanceConsumerReadSeq(
            String miningChainId, String pipelineId, String table, long epoch,
            String cursorWriterToken, long lastReadSeq) {
        if (epoch < 1) {
            throw new IllegalArgumentException("consumer read epoch must be positive");
        }
        if (cursorWriterToken == null || cursorWriterToken.isBlank()) {
            throw new IllegalArgumentException("consumer cursor writer token must be non-blank");
        }
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> consumers.updateOne(session,
                new Document(consumerKey(miningChainId, pipelineId))
                        .append("selectedTablesEpoch", epoch)
                        .append("cursorWriterToken", cursorWriterToken)
                        .append("selectedTables", table),
                consumerReadSeqUpdate(pipelineId, table, lastReadSeq)));
    }

    @Override
    public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
        updateConsumer(miningChainId, pipelineId, sinkAckedUpdate(pipelineId, position));
    }

    @Override
    public void advanceSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        updateConsumer(miningChainId, pipelineId, sinkAckedUpdate(pipelineId, table, position));
    }

    @Override
    public void advanceTableSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> {
            Document key = consumerKey(miningChainId, pipelineId);
            Document current = consumers.find(session, key)
                    .projection(Projections.include(SINK_ACKED_BY_TABLE, "selectedTablesEpoch", "selectedTables"))
                    .first();
            if (current != null && current.get("selectedTablesEpoch") instanceof Number generation
                    && generation.longValue() != position.order().epoch()) {
                return;
            }
            if (current != null && current.get("selectedTables") instanceof List<?> selected
                    && !selected.contains(table)) {
                return;
            }
            ChainPosition prior = current == null ? null : tableAcksFrom(current, pipelineId).get(table);
            if (prior != null && prior.order().compareTo(position.order()) >= 0) {
                return;
            }
            Document fields = new Document(SINK_ACKED_BY_TABLE + "." + table,
                    positionToDocument(position));
            Document update = new Document("$set", fields)
                    .append("$setOnInsert", consumerIdentity(miningChainId, pipelineId));
            if (position.order().seq() >= 0) {
                update.append("$max", new Document(PER_TABLE_RING_DONE + "." + table,
                        position.order().seq()));
            }
            consumers.updateOne(session, key, update, new UpdateOptions().upsert(true));
        });
    }

    @Override
    public void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
        Objects.requireNonNull(table, "table");
        // Older callers have no epoch to prove. Their marker may only enter an unselected legacy cursor,
        // which the generation-scoped trimmer conservatively refuses to use.
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> consumers.updateOne(session,
                new Document(consumerKey(miningChainId, pipelineId))
                        .append("selectedTablesEpoch", new Document("$exists", false))
                        .append(PER_TABLE_RING_DONE + "." + table, new Document("$exists", false)),
                new Document("$set", new Document(PER_TABLE_RING_DONE + "." + table, seq))));
    }

    @Override
    public void startRingAfter(
            String miningChainId, String pipelineId, String table, long epoch, long seq) {
        Objects.requireNonNull(table, "table");
        if (epoch < 1 || seq < -1) {
            throw new IllegalArgumentException("ring arrival needs a positive epoch and valid sequence");
        }
        migrateLegacyConsumers(miningChainId, true);
        writeConsumer(miningChainId, session -> {
            Document root = collection.find(session, new Document("_id", miningChainId))
                    .projection(Projections.include("epoch")).first();
            if (root == null || readEpoch(root, "epoch") != epoch) {
                return;
            }
            Document filter = new Document(consumerKey(miningChainId, pipelineId))
                    .append("selectedTablesEpoch", epoch)
                    .append("selectedTables", table)
                    .append(PER_TABLE_RING_DONE + "." + table, new Document("$exists", false));
            consumers.updateOne(session, filter,
                    new Document("$set", new Document(PER_TABLE_RING_DONE + "." + table, seq)));
        });
    }

    @Override
    public Map<String, Long> ringDoneThrough(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document consumer = StoreIo.call(() -> consumers.find(consumerKey(miningChainId, pipelineId))
                .projection(Projections.include(PER_TABLE_RING_DONE)).first());
        return consumer == null ? Map.of() : ringDoneFrom(consumer, pipelineId);
    }

    /**
     * The path-scoped update advancing one consumer document's read cursor for one table. A seed at
     * {@code -1} and later reader reports use {@code $set}; a new ring generation may number below an
     * earlier cursor. The selection write clears a replaced reader's cursors before it starts reporting.
     * Neither form touches the sink-acked position. The L1 stream name is a bare identifier, so the
     * dotted path addresses one field.
     */
    static Document consumerReadSeqUpdate(String pipelineId, String table, long lastReadSeq) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        if (lastReadSeq < -1) {
            throw new IllegalArgumentException("a consumer read cursor cannot precede an unread ring");
        }
        return new Document("$set", new Document("perTableSeq." + table, lastReadSeq));
    }

    /**
     * The path-scoped update that advances one consumer document's durable sink-acked position: a
     * {@code $set} on the token and the two fields carrying the order it sat at, so the reader's per-table
     * cursor in that document is left untouched. An upsert lets a sink ack before the consumer has any
     * other cursor state.
     *
     * <p>The three fields move together in one update. A token stored without its order can no longer be
     * ranked against anything, and an order stored without its token is nothing a read can resume from;
     * either alone would be a record no later comparison can use.
     *
     * <p>A position carrying no token clears the stored one rather than leaving it, and that is the same
     * rule rather than an exception to it. A source names a position for a run of changes when it has one,
     * so an ack with none is an order that no token belongs to; leaving the token an earlier and lower
     * position stored pairs this order with it, and the pair is read back as one position. The source-read
     * advance both ranks and writes that pair down, so the chain's offset moves to this order carrying a
     * token from beneath it -- and a real token arriving in between is then refused as a rewind against an
     * order it never reached. Cleared, the position reads back as ordered and tokenless, which that advance
     * already declines to write down, leaving the offset where it stands.
     */
    static Document sinkAckedUpdate(String pipelineId, ChainPosition position) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(position.order(), "position order");
        Document fields = new Document("sinkAckedEpoch", position.order().epoch())
                .append("sinkAckedSeq", position.order().seq());
        Document update = new Document("$set", fields);
        if (position.token() != null) {
            fields.append("sinkAckedSrcpos", position.token());
        } else {
            update.append("$unset", new Document("sinkAckedSrcpos", ""));
        }
        return update;
    }

    /**
     * The same update, raising {@code perTableRingDone.<table>} to the ring sequence the order carries in
     * the same write, so the per-table record can never run ahead of the chain position it came with. A
     * {@code $max} rather than a set: two members confirming at once both write, and the record only ever
     * moves forward. A snapshot row sits at a reserved sequence below every change and is no place in any
     * ring, so it raises nothing.
     */
    static Document sinkAckedUpdate(String pipelineId, String table, ChainPosition position) {
        Objects.requireNonNull(table, "table");
        Document update = sinkAckedUpdate(pipelineId, position);
        if (position.order().seq() >= 0) {
            update.append("$max", new Document(PER_TABLE_RING_DONE + "." + table, position.order().seq()));
        }
        return update;
    }

    @Override
    public void setCdcStart(
            String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(cdcStartPosition, "cdcStartPosition");
        if (snapshotEpoch < 0) {
            throw new IllegalArgumentException("snapshotEpoch must not be negative, got " + snapshotEpoch);
        }
        // One update, both fields: a resumed snapshot reads them together, so a state where the seam
        // position is stored without the generation it belongs to must not be reachable.
        updateConsumer(miningChainId, pipelineId,
                new Document("$set", new Document("cdcStartPosition", cdcStartPosition)
                        .append("snapshotEpoch", snapshotEpoch)));
    }

    @Override
    public boolean setCdcStartIfCurrent(String miningChainId, String pipelineId,
            String cursorWriterToken, long selectedTablesEpoch,
            String cdcStartPosition, long snapshotEpoch) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(cdcStartPosition, "cdcStartPosition");
        if (cursorWriterToken == null || cursorWriterToken.isBlank()
                || selectedTablesEpoch < 1 || snapshotEpoch < 1) {
            throw new IllegalArgumentException("scoped snapshot seam needs a token and positive generations");
        }
        migrateLegacyConsumers(miningChainId, true);
        AtomicBoolean accepted = new AtomicBoolean();
        writeConsumer(miningChainId, session -> {
            Document filter = new Document(consumerKey(miningChainId, pipelineId))
                    .append("cursorWriterToken", cursorWriterToken)
                    .append("selectedTablesEpoch", selectedTablesEpoch);
            UpdateResult result = consumers.updateOne(session, filter,
                    new Document("$set", new Document("cdcStartPosition", cdcStartPosition)
                            .append("snapshotEpoch", snapshotEpoch)));
            // A transaction callback may be retried; only the last attempt is the answer.
            accepted.set(result.getMatchedCount() == 1);
        });
        return accepted.get();
    }

    @Override
    public long openEpoch(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        // An atomic increment read back after the write: two members opening the same chain must take two
        // different generations, so the counter is advanced by the store rather than read, added to and
        // written back. It touches only epoch, leaving every pinned pipeline snapshot generation where it is.
        Document updated = writeChainWithConsumerMigration(miningChainId, () -> collection.findOneAndUpdate(
                new Document("_id", miningChainId),
                new Document("$inc", new Document("epoch", 1L)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
        if (updated == null) {
            throw unseededChain(miningChainId);
        }
        return readEpoch(updated, "epoch");
    }

    /**
     * Appends a version to the chain's schema history and cuts the retained history back to its budget,
     * in one atomic update.
     *
     * <p>The cut is what keeps the append landable however far the history has already grown, and it has
     * to be part of the same write. A trim on its own would be a second write, and between the two the
     * record would still carry an array the endpoint refuses to grow, so a schema change arriving then
     * would be the one that cannot be recorded. Cutting as part of the append means the entry being
     * appended is written or the update does not happen at all.
     *
     * <p>What it drops is the oldest entries, whole. An entry is never rewritten to make room: versions
     * are what a consumer resolves the schema in force at a change against, and a version rewritten to
     * something smaller would resolve to a schema its table never had, which is worse than a version
     * missing.
     */
    @Override
    public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
        Objects.requireNonNull(version, "version");
        updatePipeline(miningChainId, List.of(new Document("$set",
                new Document("schemaHistory", historyWithinBudget(schemaToDocument(version))))));
    }

    /**
     * The expression that appends one stored entry to the stored history and cuts the array back to the
     * newest entries the budget holds. Runs inside the update, so what it reads and what it writes are one
     * atomic act and two appends racing on one chain cannot lose each other's entry.
     *
     * <p>The cut walks the array backwards, from the entry just appended to the oldest one, counting how
     * many of the newest entries the budget holds; what it writes back is that many entries off the end.
     * The entry just appended is counted whatever it weighs — the write exists to record it — and the first
     * entry that does not fit closes the window, so what is retained is a suffix of the history: the newest
     * versions, contiguous, rather than whichever older entries happened to be small enough to squeeze in.
     *
     * <p>Only the count travels through the walk, never the entries. An accumulator that carried the kept
     * entries would copy that array on every step and so cost the square of what it retains, and what it
     * retains is a byte budget rather than a count: a narrow table's entries are tens of bytes, so
     * thousands of them fit, and the cost of a single append was measured at hundreds of milliseconds
     * there. A tail slice of a counted length is also already oldest-first, which is the order the record
     * stores.
     */
    private static Document historyWithinBudget(Document newEntry) {
        // A missing history reads as an empty one: this is the only write that can record a schema change,
        // so a record it refused to grow would be a chain that can no longer say its schema moved.
        Document appended = new Document("$concatArrays", List.of(
                new Document("$ifNull", List.of("$schemaHistory", List.of())),
                List.of(new Document("$literal", newEntry))));
        Document cut = new Document("$reduce",
                new Document("input", new Document("$reverseArray", "$$all"))
                        .append("initialValue", new Document("count", 0)
                                .append("used", 0L)
                                .append("closed", false))
                        .append("in", countOfNewestWithinBudget()));
        // The walk reads `$$all`, so it is bound around it, and the count it arrives at is taken off the
        // end of that same array — the newest entries, in the order they arrived.
        return new Document("$let", new Document("vars", new Document("all", appended))
                .append("in", new Document("$let", new Document("vars", new Document("cut", cut))
                        .append("in", new Document("$slice", List.of("$$all",
                                new Document("$subtract", List.of(0, "$$cut.count"))))))));
    }

    /**
     * One step of that walk: the accumulator carries how many of the newest entries are kept, what they
     * weigh between them, and whether the window has closed. Once it has closed the step reads nothing
     * further — the entries the walk has already decided to drop are never sized.
     *
     * <p>An element that is not a document is charged more than the whole budget, which closes the window
     * on it and on everything older. It is not a version — no consumer could resolve a schema against it —
     * and it must not be sized either: asking for the size of a string aborts the update, and asking for
     * the size of a null answers null, which compares as under any budget and would leave the history
     * growing unbounded again, silently, which is the state this bound exists to end.
     */
    private static Document countOfNewestWithinBudget() {
        Document isDocument = new Document("$eq", List.of(new Document("$type", "$$this"), "object"));
        // Sized as itself where it is a document and as an empty one where it is not, so that what reaches
        // the size operator is a document whether or not the branch that uses the answer is taken.
        Document sizeable = new Document("$cond",
                List.of(isDocument, "$$this", new Document("$literal", new Document())));
        Document bytes = new Document("$cond", List.of(isDocument,
                new Document("$add",
                        List.of(new Document("$bsonSize", "$$sizeable"), HISTORY_ENTRY_OVERHEAD_BYTES)),
                SCHEMA_HISTORY_BUDGET_BYTES + 1));
        // Dropping one drops every older entry with it, which is what makes the survivor a suffix.
        Document dropped = new Document("count", "$$value.count")
                .append("used", "$$value.used")
                .append("closed", true);
        Document keep = new Document("$cond", List.of(
                new Document("$or", List.of(
                        new Document("$eq", List.of("$$value.count", 0)),
                        new Document("$and", List.of(
                                new Document("$eq", List.of("$$value.closed", false)),
                                new Document("$lte", List.of(
                                        new Document("$add", List.of("$$value.used", "$$bytes")),
                                        SCHEMA_HISTORY_BUDGET_BYTES)))))),
                new Document("count", new Document("$add", List.of("$$value.count", 1)))
                        .append("used", new Document("$add", List.of("$$value.used", "$$bytes")))
                        .append("closed", false),
                dropped));
        return new Document("$cond", List.of("$$value.closed", dropped,
                new Document("$let", new Document("vars", new Document("sizeable", sizeable))
                        .append("in", new Document("$let", new Document("vars", new Document("bytes", bytes))
                                .append("in", keep))))));
    }

    @Override
    public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
        updateConsumer(miningChainId, pipelineId, snapshotCompleteUpdate(pipelineId, table));
    }

    /**
     * The update that marks one table's snapshot drained in one consumer document: an {@code $addToSet}
     * on {@code snapshotCompletedTables}. A set add, not a push — the mark answers "has this table landed
     * in this pipeline's target?", so re-marking a table must be a no-op rather than a duplicate entry.
     *
     * <p>The containing document scopes the mark to the pipeline. Recording it against the chain instead
     * is what let a pipeline new to a shared chain read another pipeline's answer and skip a load it had
     * never done. An upsert creates the consumer entry when the pipeline has none and touches nothing else.
     */
    static Document snapshotCompleteUpdate(String pipelineId, String table) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(table, "table");
        return new Document("$addToSet", new Document("snapshotCompletedTables", table));
    }

    @Override
    public List<String> miningChainIdsWithConsumer(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // Include both shapes during the lazy migration window. Only ids are read, so enumeration never
        // reconstructs a cursor and a corrupt cursor cannot prevent a departing pipeline from detaching.
        LinkedHashSet<String> chains = new LinkedHashSet<>();
        StoreIo.call(() -> collection.find(consumerPresenceFilter(pipelineId))
                .projection(Projections.include("_id"))
                .map(document -> document.getString("_id"))
                .into(chains));
        StoreIo.call(() -> consumers.find(new Document("pipelineId", pipelineId))
                .projection(Projections.include("miningChainId"))
                .map(document -> document.getString("miningChainId"))
                .into(chains));
        return List.copyOf(chains);
    }

    @Override
    public void dropChain(String miningChainId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        // The root and its split cursors are one lifecycle fact, even though they occupy two collections.
        // Deleting both in one transaction leaves no point at which the id can be seeded again while the
        // old cleanup can still reach its new cursors. The driver may retry the body; both deletes are
        // idempotent, so repeating them preserves the same end state.
        StoreIo.run(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    collection.deleteOne(session, new Document("_id", miningChainId));
                    consumers.deleteMany(session, consumersOfChain(miningChainId));
                    return null;
                });
            }
        });
    }

    @Override
    public void detachConsumer(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        // A detach is idempotent, so an absent chain is already the requested end state. Migration still
        // runs when the chain exists, preserving every other legacy cursor before this one is removed.
        // Its transaction also serializes any older migration snapshot before the delete, so no durable
        // marker has to remain merely to keep a stale copier from recreating this cursor.
        migrateLegacyConsumers(miningChainId, false);
        StoreIo.run(() -> consumers.deleteOne(consumerKey(miningChainId, pipelineId)));
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
     * Applies an update written as a pipeline — the form an update takes when what it writes is a function
     * of what the record already holds, which no single update operator expresses. Still one atomic act on
     * one chain document.
     */
    private void updatePipeline(String miningChainId, List<Document> pipeline) {
        applyToSeeded(miningChainId,
                () -> collection.updateOne(new Document("_id", miningChainId), pipeline));
    }

    /**
     * Checks the result of a chain update. A zero matched count means no document carried the id — the
     * chain was never seeded, a caller ordering error surfaced bare (not laundered into an io code). The
     * chain id is handed to the translation so that a size refusal names the record it was for.
     */
    private void applyToSeeded(String miningChainId, Supplier<UpdateResult> updateOne) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        UpdateResult result = writeChainWithConsumerMigration(miningChainId, updateOne);
        if (result.getMatchedCount() == 0) {
            throw unseededChain(miningChainId);
        }
    }

    /**
     * Writes one split consumer document after moving any embedded predecessors out of the chain record.
     * The identity fields are insert-only so a partial update can create the cursor without replacing a
     * different facet written concurrently by the same pipeline.
     */
    private void updateConsumer(String miningChainId, String pipelineId, Document update) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        migrateLegacyConsumers(miningChainId, true);
        update.append("$setOnInsert", consumerIdentity(miningChainId, pipelineId));
        writeConsumer(miningChainId, session -> consumers.updateOne(session,
                consumerKey(miningChainId, pipelineId), update, new UpdateOptions().upsert(true)));
    }

    /**
     * Checks the root and writes one split cursor as a single lifecycle operation. The revision increment
     * deliberately writes the root rather than merely reading it: a concurrent {@link #dropChain(String)}
     * then conflicts on that document, so MongoDB serializes the two transactions. If the drop wins, a
     * retry finds no root and refuses the mutation; if this write wins, the later drop removes its cursor.
     */
    private void writeConsumer(String miningChainId, ConsumerWrite write) {
        StoreIo.run(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    UpdateResult fenced = collection.updateOne(session,
                            new Document("_id", miningChainId),
                            new Document("$inc", new Document(CONSUMER_WRITE_REVISION, 1L)));
                    if (fenced.getMatchedCount() == 0) {
                        throw unseededChain(miningChainId);
                    }
                    write.apply(session);
                    return null;
                });
            }
        });
    }

    @FunctionalInterface
    private interface ConsumerWrite {
        void apply(ClientSession session);
    }

    /**
     * Retries a chain-document write after losslessly splitting legacy consumer cursors out of a record
     * that they have already filled. A size failure with no embedded cursors is unchanged: migration has
     * no truthful bytes to reclaim from that document.
     */
    private <T> T writeChainWithConsumerMigration(String miningChainId, Supplier<T> write) {
        try {
            return StoreIo.call(miningChainId, write);
        } catch (TapstateException e) {
            if (e.code() != IoError.DOCUMENT_TOO_LARGE) {
                throw e;
            }
            // Retry even when this call finds the map already empty: another writer may have completed
            // the migration after this write was refused, and rethrowing the stale refusal would turn that
            // successful concurrent repair into a false failure.
            migrateLegacyConsumers(miningChainId, true);
            return StoreIo.call(miningChainId, write);
        }
    }

    /**
     * Copies every cursor from the legacy embedded map into its own document, then clears that map. The
     * copy uses {@code $setOnInsert}: if a previous attempt landed a cursor and stopped before the clear,
     * retrying cannot replace that cursor with the older embedded value. The empty map remains on the root
     * because it is a structural field older readers and the stored-record decoder require.
     */
    private void migrateLegacyConsumers(String miningChainId, boolean requireSeeded) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Document root = StoreIo.call(() -> collection.find(new Document("_id", miningChainId))
                .projection(Projections.include("consumerOffsets"))
                .first());
        Document embedded = embeddedConsumers(root, miningChainId, requireSeeded);
        if (embedded == null || embedded.isEmpty()) {
            return;
        }
        StoreIo.run(miningChainId, () -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    migrateLegacyConsumers(session, miningChainId, requireSeeded);
                    return null;
                });
            }
        });
    }

    /** The transactional body of legacy migration, kept separate because transaction callbacks may retry. */
    private void migrateLegacyConsumers(
            ClientSession session, String miningChainId, boolean requireSeeded) {
        Document root = collection.find(session, new Document("_id", miningChainId))
                .projection(Projections.include("consumerOffsets"))
                .first();
        Document embedded = embeddedConsumers(root, miningChainId, requireSeeded);
        if (embedded == null || embedded.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : embedded.entrySet()) {
            String pipelineId = entry.getKey();
            Document identityAndCursor = consumerIdentity(miningChainId, pipelineId);
            identityAndCursor.putAll(asDocument(entry.getValue(), miningChainId));
            insertLegacyConsumer(session, miningChainId, pipelineId, identityAndCursor);
        }
        UpdateResult cleared = collection.updateOne(session,
                new Document("_id", miningChainId),
                new Document("$set", new Document("consumerOffsets", new Document())));
        if (cleared.getMatchedCount() == 0 && requireSeeded) {
            throw unseededChain(miningChainId);
        }
    }

    /** Reads and validates the legacy cursor map, or answers absent for an allowed missing chain. */
    private static Document embeddedConsumers(
            Document root, String miningChainId, boolean requireSeeded) {
        if (root == null) {
            if (requireSeeded) {
                throw unseededChain(miningChainId);
            }
            return null;
        }
        Object raw = root.get("consumerOffsets");
        if (raw instanceof Document embedded) {
            return embedded;
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", miningChainId, "field", "consumerOffsets"), null);
    }

    /**
     * Lands one legacy cursor unless an earlier attempt landed it first. This runs in the same transaction
     * as the root clear, so a competing migration either precedes this one or makes its callback retry.
     */
    private void insertLegacyConsumer(ClientSession session, String miningChainId,
            String pipelineId, Document identityAndCursor) {
        consumers.updateOne(session,
                consumerKey(miningChainId, pipelineId),
                new Document("$setOnInsert", identityAndCursor),
                new UpdateOptions().upsert(true));
    }

    /** Reads legacy and split cursors, with the split document winning during an interrupted migration. */
    private List<ConsumerOffset> mergedConsumers(Document root) {
        String miningChainId = root.getString("_id");
        Object raw = root.get("consumerOffsets");
        if (miningChainId == null || !(raw instanceof Document embedded)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(miningChainId),
                            "field", miningChainId == null ? "_id" : "consumerOffsets"), null);
        }
        Map<String, ConsumerOffset> merged = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : embedded.entrySet()) {
            merged.put(entry.getKey(),
                    consumerFromDocument(entry.getKey(), asDocument(entry.getValue(), miningChainId)));
        }
        List<Document> split = StoreIo.call(() -> consumers.find(consumersOfChain(miningChainId))
                .into(new ArrayList<>()));
        for (Document document : split) {
            String pipelineId = document.getString("pipelineId");
            if (pipelineId == null) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", String.valueOf(document.get("_id")), "field", "pipelineId"), null);
            }
            merged.put(pipelineId, consumerFromDocument(pipelineId, document));
        }
        return List.copyOf(merged.values());
    }

    /** The collision-free id of one cursor document; chain roots keep scalar string ids. */
    private static Document consumerKey(String miningChainId, String pipelineId) {
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        return new Document("_id", new Document("chain", miningChainId).append("pipeline", pipelineId));
    }

    /** Fields every split cursor carries so both lookup directions can use declared indexes. */
    private static Document consumerIdentity(String miningChainId, String pipelineId) {
        return new Document("miningChainId", miningChainId)
                .append("pipelineId", pipelineId);
    }

    /** One full split cursor document, used by replacement writes. */
    private static Document consumerDocument(String miningChainId, ConsumerOffset offset) {
        Document document = consumerKey(miningChainId, offset.pipelineId());
        document.putAll(consumerIdentity(miningChainId, offset.pipelineId()));
        document.putAll(consumerToDocument(offset));
        return document;
    }

    /** All split cursor documents belonging to one chain. */
    private static Document consumersOfChain(String miningChainId) {
        return new Document("miningChainId", miningChainId);
    }

    /**
     * The caller ordering error every mutator raises on a chain {@code create} has not seeded. One factory
     * because three paths raise it — the pre-read, the epoch increment and the matched count — and the text
     * is read back as the contract's own in the port suite, so a copy of it that drifted would surface in
     * another module, if anywhere.
     */
    private static IllegalStateException unseededChain(String miningChainId) {
        return new IllegalStateException("srs meta mutate on an unseeded mining chain: " + miningChainId
                + " (create must seed it first)");
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

    /**
     * Maps the legacy single-document shape used by compatibility reads and their witnesses. New roots are
     * created through this mapping with no consumers; consumer writes persist split documents instead.
     */
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
                .append(CONSUMER_WRITE_REVISION, 0L);
        if (meta.sourceRead() != null) {
            document.putAll(sourceReadFields(meta.sourceRead(), meta.sourceReadAt()));
        }
        if (meta.retention() != null) {
            document.append("retention", meta.retention());
        }
        // Zero means "no generation opened", which is also what an absent field reads back as, so a seed
        // stays a seed rather than carrying a field that says nothing.
        if (meta.epoch() != 0L) {
            document.append("epoch", meta.epoch());
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
                Map.of("id", String.valueOf(document.get("_id")), "field", field), null);
    }

    /** Reconstructs a meta record from its stored document. */
    static SrsMeta toMeta(Document document) {
        String id = document.getString("_id");
        Object consumersRaw = document.get("consumerOffsets");
        Object schemaRaw = document.get("schemaHistory");
        if (id == null || !(consumersRaw instanceof Document consumersDoc) || !(schemaRaw instanceof List<?> entries)) {
            // A stored meta missing a field this version requires is store corruption, surfaced as a
            // coded io diagnostic rather than a bare cast / unboxing crash while reconstructing.
            // Three grounds, so the name has to be chosen from all three: reporting the second one's
            // field while the first fired sends the reader to a field that is intact, and leaves the
            // one actually missing named nowhere.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id),
                            "field", id == null ? "_id"
                                    : consumersRaw instanceof Document ? "schemaHistory" : "consumerOffsets"), null);
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
                schemaHistory, document.getString("retention"), readEpoch(document, "epoch"),
                sourceReadAtFrom(document));
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
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", "perTable"), null);
        }
        List<String> selectedTables = selectedTablesFrom(document, pipelineId);
        Long selectedEpoch = selectedTablesEpochFrom(document);
        String cursorWriterToken = cursorWriterTokenFrom(document, pipelineId);
        if ((selectedTables == null) != (selectedEpoch == null)
                || (selectedTables == null) != (cursorWriterToken == null)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", "selectedTables"), null);
        }
        return new ConsumerOffset(
                pipelineId,
                perTableSeq,
                sinkAckedFrom(document),
                snapshotCompletedFrom(document),
                document.getString("cdcStartPosition"),
                readEpoch(document, "snapshotEpoch"),
                selectedTables,
                selectedEpoch,
                cursorWriterToken,
                tableAcksFrom(document, pipelineId),
                ringDoneFrom(document, pipelineId));
    }

    private static Map<String, Long> ringDoneFrom(Document document, String pipelineId) {
        Object raw = document.get(PER_TABLE_RING_DONE);
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Document perTable)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", PER_TABLE_RING_DONE), null);
        }
        Map<String, Long> seqs = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : perTable.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()
                    || !(entry.getValue() instanceof Number seq) || seq.longValue() < -1L) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", pipelineId, "field", PER_TABLE_RING_DONE + "." + entry.getKey()), null);
            }
            seqs.put(entry.getKey(), seq.longValue());
        }
        return Map.copyOf(seqs);
    }

    private static Map<String, ChainPosition> tableAcksFrom(Document document, String pipelineId) {
        Object raw = document.get(SINK_ACKED_BY_TABLE);
        if (raw == null) {
            return Map.of();
        }
        if (!(raw instanceof Document tableAcks)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", SINK_ACKED_BY_TABLE), null);
        }
        Map<String, ChainPosition> positions = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : tableAcks.entrySet()) {
            if (!(entry.getValue() instanceof Document value)
                    || !(value.get("epoch") instanceof Number epoch)
                    || !(value.get("ringSeq") instanceof Number seq)) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", pipelineId, "field", SINK_ACKED_BY_TABLE + "." + entry.getKey()), null);
            }
            positions.put(entry.getKey(), new ChainPosition(
                    new SourceOrder(epoch.longValue(), seq.longValue()), value.getString("token")));
        }
        return Map.copyOf(positions);
    }

    private static Document positionToDocument(ChainPosition position) {
        Document value = new Document("epoch", position.order().epoch())
                .append("ringSeq", position.order().seq());
        if (position.token() != null) {
            value.append("token", position.token());
        }
        return value;
    }

    /** An absent selection belongs to an older consumer and conservatively matches every table. */
    private static List<String> selectedTablesFrom(Document document, String pipelineId) {
        Object raw = document.get("selectedTables");
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof List<?> entries) || entries.stream().anyMatch(entry -> !(entry instanceof String))) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", "selectedTables"), null);
        }
        return entries.stream().map(String.class::cast).toList();
    }

    private static Long selectedTablesEpochFrom(Document document) {
        Object raw = document.get("selectedTablesEpoch");
        if (raw == null && !document.containsKey("selectedTablesEpoch")) {
            return null;
        }
        if (raw instanceof Number number && number.longValue() > 0) {
            return number.longValue();
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(document.get("_id")), "field", "selectedTablesEpoch"), null);
    }

    private static String cursorWriterTokenFrom(Document document, String pipelineId) {
        Object raw = document.get("cursorWriterToken");
        if (raw == null && !document.containsKey("cursorWriterToken")) {
            return null;
        }
        if (raw instanceof String token && !token.isBlank()) {
            return token;
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", pipelineId, "field", "cursorWriterToken"), null);
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
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", miningChainId, "field", "schemaHistory"), null);
        }
        return new SchemaVersion(version, new LinkedHashMap<>(schemaDoc), ddlSeq);
    }

    /** Reads a nested value as a document, or surfaces store corruption when it is not one. */
    private static Document asDocument(Object value, String miningChainId) {
        if (value instanceof Document document) {
            return document;
        }
        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", miningChainId, "field", "schema"), null);
    }

    /**
     * The acked position a consumer document carries, or null when it has none. A record whose token was
     * written without the order it sat at cannot be ranked against the reader's position, and reads back as
     * nothing acked: that pins a source-read advance where it stands, which only ever costs re-mining
     * changes already read - the direction that keeps them re-minable at all.
     */
    /**
     * How far the chain has read, or null when nothing has read it — the token, with the order beside it
     * when one was recorded.
     *
     * <p>A token with no order is a real state and reads back as the position it is, not as absence.
     * Two things produce it: a write-back, which names a spot in the source's log that no ring ever
     * assigned a coordinate to, and a document written before the order was recorded at all. Reading
     * either as nothing read would drop the one thing both of them do say — where to resume — and send
     * the tail to the snapshot seam instead, re-mining every change since.
     */
    private static ChainPosition sourceReadFrom(Document document) {
        String token = document.getString("sourceReadOffset");
        Object epoch = document.get("sourceReadEpoch");
        Object seq = document.get("sourceReadSeq");
        if (!(epoch instanceof Number) || !(seq instanceof Number)) {
            return token == null ? null : new ChainPosition(null, token);
        }
        return new ChainPosition(
                new SourceOrder(((Number) epoch).longValue(), ((Number) seq).longValue()), token);
    }

    /** When the read offset was last written, or null on a record whose offset predates the stamp. */
    private static Instant sourceReadAtFrom(Document document) {
        Object at = document.get("sourceReadAt");
        return at instanceof Number millis ? Instant.ofEpochMilli(millis.longValue()) : null;
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
     * consumer), the snapshot start pair and the acked position.
     */
    private static Document consumerToDocument(ConsumerOffset offset) {
        Document perTable = new Document();
        for (Map.Entry<String, Long> entry : offset.perTableSeq().entrySet()) {
            perTable.append(entry.getKey(), entry.getValue());
        }
        Document document = new Document("perTableSeq", perTable);
        if (offset.selectedTables() != null) {
            document.append("selectedTables", offset.selectedTables());
        }
        if (offset.selectedTablesEpoch() != null) {
            document.append("selectedTablesEpoch", offset.selectedTablesEpoch());
        }
        if (offset.cursorWriterToken() != null) {
            document.append("cursorWriterToken", offset.cursorWriterToken());
        }
        if (!offset.snapshotCompletedTables().isEmpty()) {
            document.append("snapshotCompletedTables", List.copyOf(offset.snapshotCompletedTables()));
        }
        if (offset.cdcStartPosition() != null) {
            document.append("cdcStartPosition", offset.cdcStartPosition());
        }
        if (offset.snapshotEpoch() != 0L) {
            document.append("snapshotEpoch", offset.snapshotEpoch());
        }
        if (offset.sinkAcked() != null) {
            document.append("sinkAckedEpoch", offset.sinkAcked().order().epoch())
                    .append("sinkAckedSeq", offset.sinkAcked().order().seq());
            if (offset.sinkAcked().token() != null) {
                document.append("sinkAckedSrcpos", offset.sinkAcked().token());
            }
        }
        if (!offset.sinkAckedByTable().isEmpty()) {
            Document tableAcks = new Document();
            offset.sinkAckedByTable().forEach((table, position) ->
                    tableAcks.append(table, positionToDocument(position)));
            document.append(SINK_ACKED_BY_TABLE, tableAcks);
        }
        if (!offset.ringDoneThrough().isEmpty()) {
            document.append(PER_TABLE_RING_DONE, new Document(offset.ringDoneThrough()));
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
