package io.tapstate.adapters.mongostore;

import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ObservationStore;
import org.bson.Document;

import java.util.Optional;

/** Latest observations are written only under the original scope and actuation claim. */
final class MongoCachePipelineObservationStore implements ObservationStore {
    private final MongoCachePipelineAccess access;
    MongoCachePipelineObservationStore(MongoCachePipelineAccess access) { this.access = access; }

    @Override public Optional<Observation> read(String pipelineId) {
        access.requirePipeline(pipelineId);
        return access.observationTransaction(session -> {
            access.state(session);
            Document current = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database).find(session, new Document("_id", pipelineId)).first();
            if (current == null) return Optional.empty();
            access.requireScope(current);
            try {
                Observation decoded = MongoObservationStore.toObservation(current);
                Document canonical = MongoObservationStore.toDocument(decoded).append("scope", current.get("scope"));
                if (!canonical.equals(current)) throw access.unreadable("observation");
                return Optional.of(decoded);
            } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) {
                // Stored values are untrusted; neither numeric coercion nor raw decoder causes escape.
                throw access.unreadable("observation");
            }
        });
    }

    @Override public void save(Observation observation) {
        access.requirePipeline(observation.pipelineId());
        Document captured = MongoObservationStore.toDocument(observation);
        access.observationTransaction(session -> {
            Document state = access.state(session);
            access.proveClaim(session);
            var collection = SystemCollections.CACHE_PIPELINE_OBSERVATION.on(access.database);
            Document current = collection.find(session, new Document("_id", observation.pipelineId())).first();
            if (current != null) access.requireScope(current);
            access.updateState(session, state, new Document(), false);
            // Own a new copy on every driver attempt; no callback adopts another scope or claim.
            collection.replaceOne(session, new Document("_id", observation.pipelineId()),
                    new Document(captured).append("scope", state.get("scope")), new ReplaceOptions().upsert(true));
            return null;
        });
    }

    @Override public void delete(String pipelineId) {
        access.requirePipeline(pipelineId);
        throw MongoCachePipelineAccess.fenced();
    }
}
