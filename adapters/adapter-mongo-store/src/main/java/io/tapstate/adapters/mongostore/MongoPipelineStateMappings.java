package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.spi.store.PipelineStateMappings;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** One current mapping per Pipeline; authoring creates it in the same configuration transaction. */
public final class MongoPipelineStateMappings implements PipelineStateMappings {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoCollection<Document> mappings;
    private final MongoCollection<Document> pipelines;

    MongoPipelineStateMappings(MongoClient client, MongoDatabase database) {
        this.client = Objects.requireNonNull(client, "client");
        this.mappings = SystemCollections.OPERATOR_GENERATIONS.on(database)
                .withReadConcern(ReadConcern.MAJORITY).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.pipelines = SystemCollections.PIPELINES.on(database)
                .withReadConcern(ReadConcern.MAJORITY).withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override public Optional<PipelineStateMapping> read(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        return StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> {
                    Document pipeline = pipelines.find(session, new Document("_id", pipelineId)).first();
                    Document mapping = mappings.find(session, new Document("_id", pipelineId)).first();
                    if (pipeline == null && mapping == null) return Optional.empty();
                    PipelineStateMapping state = decode(pipelineId, mapping);
                    if ((state.phase() == PipelineStateMapping.Phase.RETIRED) != (pipeline == null)) throw unreadable(pipelineId);
                    return Optional.of(state);
                }, TRANSACTION);
            }
        });
    }

    void create(ClientSession session, String pipelineId) {
        requireNoRetainedIdentity(session, pipelineId);
        mappings.insertOne(session, new Document("_id", pipelineId).append("stateGeneration", 1L)
                .append("phase", PipelineStateMapping.Phase.ACTIVE.name()).append("operationId", null)
                .append("noteWriteFence", 0L));
    }

    void requireNoRetainedIdentity(ClientSession session, String resourceId) {
        Document retained = mappings.find(session, new Document("_id", resourceId)).first();
        if (retained != null) {
            decode(resourceId, retained);
            throw retirementRequired(resourceId);
        }
    }

    void requireActive(ClientSession session, String pipelineId) {
        PipelineStateMapping state = decode(pipelineId, mappings.find(session, new Document("_id", pipelineId)).first());
        if (state.phase() != PipelineStateMapping.Phase.ACTIVE) throw retirementRequired(pipelineId);
    }

    /** Deletion and kind changes enter the explicit retirement command, never this ordinary authoring path. */
    void refuseRemoval(ClientSession session, String pipelineId) {
        decode(pipelineId, mappings.find(session, new Document("_id", pipelineId)).first());
        throw retirementRequired(pipelineId);
    }

    static PipelineStateMapping decode(String pipelineId, Document document) {
        try {
            if (document == null || !(document.get("stateGeneration") instanceof Long generation)
                    || !(document.get("noteWriteFence") instanceof Long fence) || fence < 0 || fence == Long.MAX_VALUE) throw unreadable(pipelineId);
            return new PipelineStateMapping(pipelineId, generation,
                    PipelineStateMapping.Phase.valueOf(document.getString("phase")), document.getString("operationId"));
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) {
            throw unreadable(pipelineId);
        }
    }

    private static TapstateException unreadable(String pipelineId) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId, "field", "stateMapping"), null);
    }
    private static TapstateException retirementRequired(String pipelineId) {
        return new TapstateException(StoreError.PIPELINE_RETIREMENT_REQUIRED, Map.of("pipeline", pipelineId), null);
    }
}
