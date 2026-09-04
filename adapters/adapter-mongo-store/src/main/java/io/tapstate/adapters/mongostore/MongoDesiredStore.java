package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB pipeline desired-state store: one desired-intent document per pipeline, keyed by the
 * pipeline id (as {@code _id}), carrying the target state and the artifact revision the intent was
 * expressed against. Desired intent is plain, not fenced: it is a straight upsert by pipeline id
 * (last write wins), not the epoch-fencing compare-and-swap the actual state store uses.
 *
 * <p>Driver IO failures are translated into coded io diagnostics, so no driver type escapes the
 * module (rule R3). A stored document missing a field this version requires, or carrying a target
 * state this version does not recognize, is store corruption — surfaced as a coded io diagnostic,
 * not a bare crash while reconstructing.
 */
public final class MongoDesiredStore implements DesiredStore {

    private final MongoCollection<Document> collection;

    public MongoDesiredStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(DesiredState desired) {
        Objects.requireNonNull(desired, "desired");
        // Upsert by the pipeline id (the document _id): a re-save of the same pipeline overwrites in
        // place (last write wins) rather than accumulating documents. Desired intent is not fenced.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", desired.pipelineId()), toDocument(desired), new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<DesiredState> read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", pipelineId)).first());
        return document == null ? Optional.empty() : Optional.of(toDesired(document));
    }

    @Override
    public List<String> pipelineIds() {
        // Only the id (the document _id) is read, never the target state or revision, so enumerating the
        // reconcile set never reconstructs — and so never fails — on a document whose content is corrupt.
        // A corrupt intent surfaces per pipeline when its intent is actually read, not for the whole batch.
        return StoreIo.call(() -> collection.find()
                .projection(Projections.include("_id"))
                .map(document -> document.getString("_id"))
                .into(new ArrayList<>()));
    }

    @Override
    public void delete(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // deleteOne on a missing _id removes nothing and reports so without failing, which is the no-op
        // an absent intent is meant to be.
        StoreIo.run(() -> collection.deleteOne(new Document("_id", pipelineId)));
    }

    /** Maps a desired state to its stored document: the pipeline id as {@code _id}, the rest as fields. */
    static Document toDocument(DesiredState desired) {
        return new Document("_id", desired.pipelineId())
                .append("targetState", desired.targetState().name())
                .append("revision", desired.revision())
                .append("purgeState", desired.purgeState())
                .append("assemblyRevision", desired.assemblyRevision())
                .append("reassemble", desired.reassemble())
                .append("rebuiltAtStateEpoch", desired.rebuiltAtStateEpoch());
    }

    /** Reconstructs a desired state from its stored document. */
    static DesiredState toDesired(Document document) {
        String id = document.getString("_id");
        String targetState = document.getString("targetState");
        String revision = document.getString("revision");
        if (targetState == null || revision == null) {
            // A stored desired doc missing a field this version requires is store corruption, surfaced
            // as a coded io diagnostic rather than a bare null-argument crash while reconstructing.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), null);
        }
        // Absent reads as false, and that is the one direction it may default in: a document written
        // before this field existed was written by a stop that could not have asked for anything to be
        // cleared, so reading it as "clear it" would act on an intent nobody expressed. Unlike the two
        // fields above it is not treated as corruption when missing -- an older document is a document
        // this version can still read, which is what makes adding a field backward compatible at all.
        // Both new fields default the same way and for the same reason as purgeState: an older
        // document was written by a version that could not have expressed either, so reading it as
        // "re-assemble" or as "the assembly is unchanged" would act on an intent nobody expressed. A
        // null assemblyRevision reads as unknown, and the only thing that turns on it is whether a
        // refusal may be skipped -- so unknown keeps the refusal.
        return new DesiredState(
                id, parseState(targetState, id), revision, document.getBoolean("purgeState", false),
                document.getString("assemblyRevision"), document.getBoolean("reassemble", false),
                document.get("rebuiltAtStateEpoch") instanceof Number epoch ? epoch.longValue() : null);
    }

    /** A stored target state this version does not recognize is corruption, not a bare enum-valueOf crash. */
    private static PipelineState parseState(String targetState, String id) {
        try {
            return PipelineState.valueOf(targetState);
        } catch (IllegalArgumentException e) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", String.valueOf(id)), e);
        }
    }
}
