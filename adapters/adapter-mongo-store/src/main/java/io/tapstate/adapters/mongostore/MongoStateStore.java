package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
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
    private final MongoStopReservationWrites stops;

    public MongoStateStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
        this.stops = null;
    }

    /** Production binding for short authority-guarded reservation transactions. */
    public MongoStateStore(MongoClient client, MongoCollection<Document> collection,
            MongoCollection<Document> desired, MongoCollection<Document> workloadClaims) {
        this.collection = Objects.requireNonNull(collection, "collection");
        this.stops = new MongoStopReservationWrites(client, collection, desired, workloadClaims);
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
        Document filter = new Document("_id", pipelineId).append("epoch", expectedEpoch)
                .append(StopReservationDocument.FIELD, new Document("$exists", false));
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

    @Override
    public boolean supportsStopReservations() {
        return stops != null;
    }

    @Override
    public Optional<StopReservation> readStopReservation(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", pipelineId)).first());
        if (document == null || !document.containsKey(StopReservationDocument.FIELD)) {
            return Optional.empty();
        }
        if (!(document.get(StopReservationDocument.FIELD) instanceof Document marker)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", pipelineId, "field", StopReservationDocument.FIELD), null);
        }
        return Optional.of(StopReservationDocument.read(pipelineId, toCheckpoint(document).epoch(), marker));
    }

    @Override
    public Optional<StopReservation> reserveStop(
            CheckpointDoc expected, StopReservation proposal, Instant touchTime) {
        return requireStops().reserve(expected, proposal, touchTime);
    }

    @Override
    public Optional<StopReservation> rebindStop(
            StopReservation expected, StopAuthority successor, Instant touchTime) {
        return requireStops().rebind(expected, successor, touchTime);
    }

    @Override
    public Optional<StopReservation> replaceStop(
            StopReservation expected, StopReservation successor, Instant touchTime) {
        return requireStops().replace(expected, successor, touchTime);
    }

    @Override
    public Optional<CheckpointDoc> completeStop(StopReservation expected, Instant touchTime) {
        return requireStops().complete(expected, touchTime);
    }

    @Override
    public Optional<CheckpointDoc> retireStop(
            StopReservation expected, io.tapstate.core.lifecycle.DesiredState successor,
            StopAuthority authority, Instant touchTime) {
        return requireStops().retire(expected, successor, authority, touchTime);
    }

    private MongoStopReservationWrites requireStops() {
        if (stops == null) {
            throw new UnsupportedOperationException("authority-guarded stop reservations require a verified client");
        }
        return stops;
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
        Object rawId = document.get("_id");
        String id = rawId instanceof String value ? value : String.valueOf(rawId);
        if (!(rawId instanceof String) || id.isBlank()) { throw unreadable(id, "_id"); }
        if (!(document.get("stateJson") instanceof String stateJson)) { throw unreadable(id, "stateJson"); }
        long epoch = integer(document.get("epoch"), id, "epoch");
        long touchMillis = integer(document.get("touchMillis"), id, "touchMillis");
        if (epoch < 0) { throw unreadable(id, "epoch"); }
        CheckpointDoc checkpoint = new CheckpointDoc(id, stateJson, epoch, Instant.ofEpochMilli(touchMillis));
        if (document.containsKey(StopReservationDocument.FIELD)) {
            if (!(document.get(StopReservationDocument.FIELD) instanceof Document marker)) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", String.valueOf(id), "field", StopReservationDocument.FIELD), null);
            }
            StopReservationDocument.read(id, epoch, marker);
        }
        return checkpoint;
    }

    private static long integer(Object value, String id, String field) {
        if (!(value instanceof Long || value instanceof Integer)) { throw unreadable(id, field); }
        return ((Number) value).longValue();
    }

    private static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }
}
