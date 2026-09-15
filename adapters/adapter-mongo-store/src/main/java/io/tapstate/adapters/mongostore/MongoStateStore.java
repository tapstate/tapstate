package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StateStore;
import org.bson.Document;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB pipeline-state store: one checkpoint document per pipeline, whose transitions land only
 * through an epoch-fencing compare-and-swap. The atomic conditional update on the stored epoch is the
 * fence — it is what makes two owners that both believe they hold the pipeline unable to both write.
 *
 * <p>The document is keyed by the pipeline id (as {@code _id}) and carries the state payload
 * ({@code stateJson}), the monotonic fencing epoch ({@code epoch}), and the last-write timestamp
 * ({@code touchMillis}). The epoch alone decides the swap; the state and the timestamp never do, so a
 * lagging clock or a stale state view cannot corrupt the fencing decision.
 *
 * <p>Driver IO failures during read / create / swap are translated into coded io diagnostics, so no
 * driver type escapes the module (rule R3). A create that collides with an existing checkpoint and a
 * swap on an unseeded pipeline are caller ordering errors — surfaced bare (an {@code
 * IllegalStateException}), not laundered into an io code that would hide the defect.
 */
public final class MongoStateStore implements StateStore {

    private static final FindOneAndUpdateOptions RETURN_AFTER =
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);

    private final MongoCollection<Document> collection;

    public MongoStateStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public Optional<CheckpointDoc> read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", pipelineId)).first());
        return document == null ? Optional.empty() : Optional.of(toCheckpoint(document));
    }

    @Override
    public void create(String pipelineId, String stateJson, Instant touchTime) {
        // Insert-only: insertOne fails on a duplicate _id, so a checkpoint that already exists is never
        // overwritten — overwriting would reset the fencing epoch that compareAndSwap maintains.
        Document document = toDocument(CheckpointDoc.initial(pipelineId, stateJson, touchTime));
        try {
            collection.insertOne(document);
        } catch (MongoException e) {
            throw classifyInsertFailure(e, pipelineId);
        }
    }

    /**
     * Classifies a failed seed insert: a duplicate {@code _id} is a caller ordering error (the pipeline
     * was already seeded), surfaced bare like the unseeded-swap ordering error — not laundered into an
     * io code that would hide it; any other driver failure is a coded io diagnostic.
     */
    static RuntimeException classifyInsertFailure(MongoException e, String pipelineId) {
        if (e instanceof MongoWriteException write && write.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
            return new IllegalStateException(
                    "create on an already-seeded pipeline: " + pipelineId + " (create is insert-only)", e);
        }
        return StoreIo.coded(e);
    }

    @Override
    public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(nextStateJson, "nextStateJson");
        Objects.requireNonNull(touchTime, "touchTime");
        // The atomic fence: swap the state and bump the epoch only where the stored epoch still equals
        // the writer's expectation. This is the sole legal transition write.
        Document filter = new Document("_id", pipelineId).append("epoch", expectedEpoch);
        Document update = new Document("$set",
                new Document("stateJson", nextStateJson).append("touchMillis", touchTime.toEpochMilli()))
                .append("$inc", new Document("epoch", 1L));
        Document applied = StoreIo.call(() -> collection.findOneAndUpdate(filter, update, RETURN_AFTER));
        if (applied != null) {
            return new CasOutcome.Applied(toCheckpoint(applied));
        }
        // No document matched the expected epoch. Read back to tell a fenced writer (a newer epoch
        // superseded it) from an ordering error (the pipeline was never seeded). The fence itself was
        // already decided by the atomic update above; this read only supplies the diagnostic epoch.
        Document current = StoreIo.call(() -> collection.find(new Document("_id", pipelineId)).first());
        if (current == null) {
            throw new IllegalStateException(
                    "compareAndSwap on an unseeded pipeline: " + pipelineId + " (create must seed it first)");
        }
        return new CasOutcome.Fenced(toCheckpoint(current).epoch());
    }

    @Override
    public void delete(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // Unconditional by design: the fencing epoch guards transitions of a live pipeline, and this is
        // reached only once the pipeline no longer exists, so there is no epoch left to hold. deleteOne on
        // a missing _id removes nothing and reports so without failing, which is the no-op an unseeded
        // pipeline is meant to be.
        StoreIo.run(() -> collection.deleteOne(new Document("_id", pipelineId)));
    }

    /** Maps a checkpoint to its stored document: the pipeline id as {@code _id}, the rest as fields. */
    static Document toDocument(CheckpointDoc checkpoint) {
        return new Document("_id", checkpoint.pipelineId())
                .append("stateJson", checkpoint.stateJson())
                .append("epoch", checkpoint.epoch())
                .append("touchMillis", checkpoint.touchTime().toEpochMilli());
    }

    /** Reconstructs a checkpoint from its stored document. */
    static CheckpointDoc toCheckpoint(Document document) {
        String id = document.getString("_id");
        String stateJson = document.getString("stateJson");
        Long epoch = document.getLong("epoch");
        Long touchMillis = document.getLong("touchMillis");
        if (stateJson == null || epoch == null || touchMillis == null) {
            // A stored checkpoint missing a field this version requires is store corruption, surfaced
            // as a coded io diagnostic rather than a bare unboxing crash while reconstructing.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id),
                            "field", stateJson == null ? "stateJson" : epoch == null ? "epoch" : "touchMillis"), null);
        }
        return new CheckpointDoc(id, stateJson, epoch, Instant.ofEpochMilli(touchMillis));
    }
}
