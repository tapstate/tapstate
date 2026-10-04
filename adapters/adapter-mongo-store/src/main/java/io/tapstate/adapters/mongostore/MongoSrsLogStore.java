package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsLogBatch;
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
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
 * <p>BSON compares documents field by field in declaration order, so every change key of one ring forms a
 * contiguous run of the {@code _id} index ordered by sequence. Bounded reads and trimming use that run;
 * this holds only while every key is built with {@code ring} before {@code seq}, so exactly one place
 * builds one. Independent string-keyed bounds documents in the same collection preserve the largest
 * written sequence and retired prefix even when every change is trimmed. They use the existing index
 * and are outside every change-key range.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the module
 * (rule R3). A stored document that cannot be read back into its model is coded
 * {@code io.document-unreadable}.
 *
 * <p>A record carrying a capture fence is appended inside a transaction that first writes to the matching
 * live workload claim, judged by Mongo server time. The claim write and the log write commit together, and a
 * takeover writes that same claim, so the two are ordered: replacing the owner generation makes every later
 * append from the old process fail even if that process is still alive.
 */
public final class MongoSrsLogStore implements SrsLogStore {

    private static final String RING = "ring";
    private static final String SEQ = "seq";
    private static final String LARGEST_SEQUENCE = "largestSequence";
    private static final String TRIMMED_THROUGH = "trimmedThrough";
    private static final String BOUNDS_PREFIX = "srs-log-bounds:";

    private static final TransactionOptions DURABLE_APPEND = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true))
            .build();

    /**
     * What a fenced append writes to its claim to prove it. A counter, so that the write always changes the
     * document: a write that changes nothing is not made at all, and would conflict with nothing.
     */
    private static final Document PROVE_THE_CLAIM = new Document("$inc", new Document("fencedAppends", 1L));

    private final MongoClient client;
    private final MongoCollection<Document> collection;
    private final MongoCollection<Document> workloadClaims;

    public MongoSrsLogStore(MongoCollection<Document> collection) {
        this(null, collection, null);
    }

    /** Production constructor enabling an atomic live-claim check around every fenced append. */
    public MongoSrsLogStore(
            MongoClient client,
            MongoCollection<Document> collection,
            MongoCollection<Document> workloadClaims) {
        this.client = client;
        this.collection = Objects.requireNonNull(collection, "collection")
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.workloadClaims = workloadClaims;
        if ((client == null) != (workloadClaims == null)) {
            throw new IllegalArgumentException("client and workloadClaims must be supplied together");
        }
    }

    @Override
    public void store(String ring, long seq, SrsLogRecord record) {
        storeAll(ring, seq, List.of(Objects.requireNonNull(record, "record")));
    }

    @Override
    public void storeAll(String ring, long firstSeq, List<SrsLogRecord> records) {
        Objects.requireNonNull(ring, "ring");
        Objects.requireNonNull(records, "records");
        if (records.isEmpty()) {
            return;
        }
        WorkloadClaimFence fence = records.getFirst().captureFence();
        List<WriteModel<Document>> writes = new ArrayList<>(records.size() + 1);
        long lastSeq = Math.addExact(firstSeq, records.size() - 1L);
        long seq = firstSeq;
        for (SrsLogRecord record : records) {
            Objects.requireNonNull(record, "record");
            if (!Objects.equals(fence, record.captureFence())) {
                throw new IllegalArgumentException("one SRS log batch must carry one workload claim fence");
            }
            Document key = key(ring, seq++);
            // A literal replacement pipeline preserves replacement semantics while keeping these
            // statements in the same driver UPDATE batch as the monotonic bounds update below.
            writes.add(new UpdateOneModel<>(new Document("_id", key),
                    List.of(new Document("$replaceWith", new Document("$literal", toDocument(key, record)))),
                    new UpdateOptions().upsert(true)));
        }
        // The high-water mark shares the append's existing bulk write and, for fenced capture, its
        // transaction. It survives deletion of every retained row without allocating a sequence remotely.
        writes.add(appendBounds(ring, lastSeq));
        // Ordered, so the run lands in the order the ring assigned it rather than in whatever order the
        // driver finds convenient. The run occupies consecutive sequences, and a reader that meets a gap
        // cannot tell a write still in flight from one that failed.
        if (fence == null) {
            StoreIo.run(() -> collection.bulkWrite(writes, new BulkWriteOptions().ordered(true)));
            return;
        }
        fenced(fence, session -> collection.bulkWrite(session, writes, new BulkWriteOptions().ordered(true)));
    }

    @Override
    public Optional<SrsLogRecord> load(String ring, long seq) {
        Objects.requireNonNull(ring, "ring");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", key(ring, seq))).first());
        return document == null ? Optional.empty() : Optional.of(toRecord(document));
    }

    @Override
    public long largestSequence(String ring) {
        return bounds(ring).largestSequence();
    }

    @Override
    public SrsLogBounds bounds(String ring) {
        Objects.requireNonNull(ring, "ring");
        Document bounds = StoreIo.call(() -> collection.find(new Document("_id", boundsId(ring))).first());
        if (bounds != null) {
            try {
                return new SrsLogBounds(bounds.getLong(LARGEST_SEQUENCE), bounds.getLong(TRIMMED_THROUGH));
            } catch (RuntimeException e) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", boundsId(ring)), e);
            }
        }
        // Older logs have no independent bounds record. Their surviving highest sequence is known,
        // while nothing proves a retired prefix or the original generation of those records.
        return new SrsLogBounds(largestRetainedSequence(ring), -1L);
    }

    private long largestRetainedSequence(String ring) {
        Document last = StoreIo.call(() -> collection.find(ringRange(ring, Long.MAX_VALUE))
                .sort(new Document("_id", -1))
                .limit(1)
                .first());
        if (last == null) {
            return -1L;
        }
        return sequenceOf(last);
    }

    @Override
    public SrsLogBatch readBatch(String ring, long firstSeq, int maxSize) {
        Objects.requireNonNull(ring, "ring");
        if (firstSeq < 0 || maxSize < 1) {
            throw new IllegalArgumentException("an SRS log read requires a non-negative sequence and positive size");
        }
        SrsLogBounds observed = bounds(ring);
        if (firstSeq > observed.largestSequence()) {
            return new SrsLogBatch(observed, Map.of());
        }
        long room = Long.MAX_VALUE - firstSeq;
        long lastSeq = Math.min(observed.largestSequence(), firstSeq + Math.min(room, maxSize - 1L));
        Document range = new Document("_id", new Document("$gte", key(ring, firstSeq))
                .append("$lte", key(ring, lastSeq)));
        List<Document> documents = StoreIo.call(() -> collection.find(range)
                .sort(new Document("_id", 1)).into(new ArrayList<>()));
        Map<Long, SrsLogRecord> records = new LinkedHashMap<>();
        for (Document document : documents) {
            records.put(sequenceOf(document), toRecord(document));
        }
        return new SrsLogBatch(observed, records);
    }

    @Override
    public void trim(String ring, long throughSeq) {
        Objects.requireNonNull(ring, "ring");
        SrsLogBounds observed = bounds(ring);
        long cut = Math.min(throughSeq, observed.largestSequence());
        if (cut < 0) {
            return;
        }
        // Publish the safe retired prefix before deleting it, in the same ordered bulk call. If deletion
        // fails, keeping already-confirmed rows costs space; losing the high-water mark would reuse keys.
        List<WriteModel<Document>> writes = List.of(
                new UpdateOneModel<>(new Document("_id", boundsId(ring)),
                        new Document("$max", new Document(LARGEST_SEQUENCE, observed.largestSequence())
                                .append(TRIMMED_THROUGH, cut)), new UpdateOptions().upsert(true)),
                new DeleteManyModel<>(ringRange(ring, cut)));
        StoreIo.run(() -> collection.bulkWrite(writes, new BulkWriteOptions().ordered(true)));
    }

    private static UpdateOneModel<Document> appendBounds(String ring, long lastSeq) {
        return new UpdateOneModel<>(new Document("_id", boundsId(ring)),
                new Document("$max", new Document(LARGEST_SEQUENCE, lastSeq))
                        .append("$setOnInsert", new Document(TRIMMED_THROUGH, -1L)),
                new UpdateOptions().upsert(true));
    }

    private static String boundsId(String ring) {
        return BOUNDS_PREFIX + ring;
    }

    private static long sequenceOf(Document document) {
        Object id = document.get("_id");
        if (!(id instanceof Document key) || !(key.get(SEQ) instanceof Number seq)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id)), null);
        }
        return seq.longValue();
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
        if (record.epoch() > 0) {
            document.append("epoch", record.epoch());
        }
        if (record.captureFence() != null) {
            document.append("captureFence", WorkloadClaimDocuments.stored(record.captureFence()));
        }
        // The nullable fields are written only when present, never as explicit nulls -- the same shape the
        // meta store uses, so a reader tells "no position" from "a position that is the empty string".
        if (record.srcToken() != null) {
            document.append("srcToken", record.srcToken());
        }
        if (record.before() != null) {
            document.append("before", RowImages.toDocument(record.before()));
        }
        if (record.after() != null) {
            document.append("after", RowImages.toDocument(record.after()));
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
                    document.getLong("schemaVer"),
                    readFence(document.get("captureFence", Document.class)),
                    document.containsKey("epoch") ? document.getLong("epoch") : 0L);
        } catch (RuntimeException e) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(document.get("_id"))), e);
        }
    }

    private static Map<String, Object> rowImage(Document document, String field) {
        Document image = document.get(field, Document.class);
        return image == null ? null : RowImages.toRow(image);
    }

    /**
     * Commits {@code write} only together with proof that the claim {@code fence} names is still that owner's
     * and still leased -- and the proof is a write to the claim, not a read of it.
     *
     * <p>Under snapshot isolation a read conflicts with no write. An owner that read its claim live just
     * before the lease ran out went on to commit its append after a new owner had taken the claim over: the
     * takeover wrote the claim, the append only read it, and nothing set the two against each other. Writing
     * the claim puts both on one document, so they are ordered. An append that comes after the takeover finds
     * the claim is no longer its own and is refused; one that comes first commits before the takeover can.
     *
     * <p>Run as a transaction the driver retries. The owner's own renewals write the same document, and one
     * landing inside an append makes the append's write conflict; the retry proves the claim again, so the
     * append lands if the claim is still this owner's and is refused if it is not.
     */
    private void fenced(WorkloadClaimFence fence, java.util.function.Consumer<ClientSession> write) {
        if (client == null || workloadClaims == null) {
            throw new IllegalStateException("a fenced SRS log write requires the workload-claim collection");
        }
        StoreIo.run(() -> {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    UpdateResult proved = workloadClaims.updateOne(
                            session, WorkloadClaimDocuments.live(fence), PROVE_THE_CLAIM);
                    if (proved.getMatchedCount() != 1) {
                        throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
                    }
                    write.accept(session);
                    return null;
                }, DURABLE_APPEND);
            }
        });
    }

    private static WorkloadClaimFence readFence(Document fence) {
        if (fence == null) {
            return null;
        }
        return new WorkloadClaimFence(
                new WorkloadClaimKey(
                        fence.getString("clusterId"),
                        WorkloadClaimType.valueOf(fence.getString("resourceType")),
                        fence.getString("resourceId")),
                new WorkloadOwner(fence.getString("ownerNodeId"), fence.getString("ownerBootId")),
                number(fence, "claimGeneration"),
                number(fence, "executionGeneration"),
                number(fence, "topologyRevision"));
    }

    private static long number(Document document, String field) {
        Number value = document.get(field, Number.class);
        if (value == null) {
            throw new IllegalStateException("capture fence has no " + field);
        }
        return value.longValue();
    }
}
