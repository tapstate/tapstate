package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/** One fixed CONFIG confirmation; no CACHE client, lease IO or initial-creation capability is used. */
final class MongoPipelineStateRebuilds implements PipelineStateRebuilds {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final Set<String> ANNEX = Set.of("formatVersion", "operationId", "originalScope", "targetScope", "preparationId",
            "previousPreparationId", "intentRevision", "originalClaim", "originalStateEpoch", "confirmationHash");
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;
    private final MongoPipelineIntentStore intents;
    private final MongoPipelineStatePreparations preparations;
    private final Runnable requireActive;

    MongoPipelineStateRebuilds(MongoClient client, MongoDatabase database, String clusterId, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId"); this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (!bindings.belongsTo(client, database.getName()) || !artifacts.belongsTo(client, database.getName()))
            throw new IllegalArgumentException("rebuild stores require one original CONFIG client and database");
        intents = new MongoPipelineIntentStore(client, database, clusterId, bindings);
        preparations = new MongoPipelineStatePreparations(client, database, clusterId, bindings, artifacts, requireActive);
    }
    @Override public PipelineStateRebuilds guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoPipelineStateRebuilds(client, database, clusterId, bindings, artifacts, () -> { requireActive.run(); additional.run(); });
    }
    @Override public PipelineRebuildPreparation begin(PipelineRebuildRequest original) {
        Objects.requireNonNull(original, "original");
        String hash = confirmationHash(original), id = UUID.randomUUID().toString();
        return transaction(session -> {
            Document mapping = requireOriginal(session, original);
            var state = MongoPipelineStateMappings.decode(original.originalScope().pipelineId(), mapping);
            if (state.phase() == PipelineStateMapping.Phase.REBUILDING) return retained(original, hash, mapping);
            if (state.phase() != PipelineStateMapping.Phase.ACTIVE || mapping.containsKey("stateRebuild")) throw fenced();
            var previous = MongoPipelinePreparationFacts.decode(state.pipelineId(), mapping, clusterId);
            if (previous.phase() != PipelinePreparation.Phase.COMPLETE || !original.originalScope().equals(previous.scope())) throw fenced();
            var pending = new Pending(this, original, id, previous.preparationId(), hash);
            long fence = mapping.getLong("noteWriteFence"); if (fence >= Long.MAX_VALUE - 1) throw fenced();
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
            if (SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(session,
                    new Document("_id", state.pipelineId()).append("stateGeneration", state.stateGeneration()).append("phase", "ACTIVE")
                            .append("noteWriteFence", fence).append("cachePreparation", mapping.get("cachePreparation")),
                    new Document("$set", new Document("phase", "REBUILDING").append("operationId", original.operationId())
                            .append("stateRebuild", encode(pending))).append("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1)
                throw new IllegalStateException("Pipeline mapping changed inside one CONFIG snapshot");
            return pending;
        });
    }
    @Override public void requirePending(PipelineRebuildPreparation original) {
        if (!(original instanceof Pending pending) || pending.owner.client != client
                || !pending.owner.database.getName().equals(database.getName()) || !pending.owner.clusterId.equals(clusterId)) throw fenced();
        pending.owner.requireActive.run();
        transaction(session -> {
            var retained = retained(pending.request, pending.hash, requireOriginal(session, pending.request));
            if (!encode(retained).equals(encode(pending))) throw fenced();
            return null;
        });
    }

    /** Later CACHE transitions call back through this original adapter, never an arbitrary port implementation. */
    static void requireOriginalPending(PipelineRebuildPreparation original) {
        if (!(original instanceof Pending pending)) throw fenced();
        pending.owner.requirePending(pending);
    }

    private Document requireOriginal(ClientSession session, PipelineRebuildRequest original) {
        var scope = original.originalScope();
        var preparation = bindings.cachePreparationForTransaction(session).orElseThrow(MongoPipelineStateRebuilds::fenced);
        var business = preparation.businessScope();
        if (!clusterId.equals(scope.clusterId()) || !business.clusterId().equals(scope.clusterId())
                || business.bindingVersion() != scope.bindingVersion() || !business.cacheIncarnation().equals(scope.cacheIncarnation())) throw fenced();
        var current = intents.loadInTransaction(session, scope.pipelineId()).orElseThrow(MongoPipelineStateRebuilds::fenced);
        if (!original.intent().equals(current) || !original.intent().desired().revision().equals(original.inputs().pipelineHash())) throw fenced();
        if (original.originalClaim().claimGeneration() >= Long.MAX_VALUE - 1
                || original.originalClaim().executionGeneration() >= Long.MAX_VALUE - 1
                || original.originalClaim().topologyRevision() >= Long.MAX_VALUE - 1) throw fenced();
        preparations.verifyInputs(session, original.targetScope(), original.inputs());
        var mapping = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", scope.pipelineId())).first();
        var state = MongoPipelineStateMappings.decode(scope.pipelineId(), mapping);
        if (state.stateGeneration() != scope.stateGeneration()) throw fenced();
        return mapping;
    }
    private Pending retained(PipelineRebuildRequest original, String hash, Document mapping) {
        var state = MongoPipelineStateMappings.decode(original.originalScope().pipelineId(), mapping);
        if (state.phase() != PipelineStateMapping.Phase.REBUILDING || !original.operationId().equals(state.operationId())) throw fenced();
        try {
            if (!(mapping.get("stateRebuild") instanceof Document annex) || !annex.keySet().equals(ANNEX)
                    || !Integer.valueOf(1).equals(annex.get("formatVersion"))
                    || !(annex.get("intentRevision") instanceof Long) || !(annex.get("originalStateEpoch") instanceof Long)) throw unreadable();
            var pending = new Pending(this, original, checkedId(annex.getString("preparationId")), checkedId(annex.getString("previousPreparationId")), hash);
            if (!encode(pending).equals(annex)) throw fenced();
            var old = MongoPipelinePreparationFacts.decode(state.pipelineId(), mapping, clusterId);
            if (old.phase() != PipelinePreparation.Phase.COMPLETE || !old.scope().equals(original.originalScope())
                    || !old.preparationId().equals(pending.previous)) throw unreadable();
            return pending;
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) { throw unreadable(); }
    }
    private static String checkedId(String id) {
        if (!UUID.fromString(id).toString().equals(id)) throw new IllegalArgumentException("invalid rebuild preparation id");
        return id;
    }
    private static Document encode(Pending value) {
        var request = value.request;
        return new Document("formatVersion", 1).append("operationId", request.operationId())
                .append("originalScope", MongoPipelinePreparationFacts.scope(request.originalScope()))
                .append("targetScope", MongoPipelinePreparationFacts.scope(request.targetScope())).append("preparationId", value.id)
                .append("previousPreparationId", value.previous).append("intentRevision", request.intent().intentRevision())
                .append("originalClaim", claim(request.originalClaim())).append("originalStateEpoch", request.originalStateEpoch())
                .append("confirmationHash", value.hash);
    }
    private static Document claim(WorkloadClaimFence claim) {
        return new Document("clusterId", claim.key().clusterId()).append("pipelineId", claim.key().resourceId())
                .append("nodeId", claim.owner().nodeId()).append("bootId", claim.owner().bootId())
                .append("claimGeneration", claim.claimGeneration()).append("executionGeneration", claim.executionGeneration())
                .append("topologyRevision", claim.topologyRevision());
    }
    private static String confirmationHash(PipelineRebuildRequest request) {
        var desired = request.intent().desired();
        return CanonicalHash.ofText(JsonWriter.write(new Document("operationId", request.operationId())
                .append("scope", MongoPipelinePreparationFacts.scope(request.originalScope())).append("intentRevision", request.intent().intentRevision())
                .append("desired", new Document("targetState", desired.targetState().name()).append("revision", desired.revision())
                        .append("purgeState", desired.purgeState()).append("assemblyRevision", desired.assemblyRevision())
                        .append("reassemble", desired.reassemble()).append("rebuiltAtStateEpoch", desired.rebuiltAtStateEpoch()))
                .append("originalClaim", claim(request.originalClaim())).append("originalStateEpoch", request.originalStateEpoch())
                .append("targetInputs", MongoPipelineSourcePreparations.encode(request.inputs()))));
    }
    private record Pending(MongoPipelineStateRebuilds owner, PipelineRebuildRequest request, String id, String previous, String hash)
            implements PipelineRebuildPreparation {
        @Override public String preparationId() { return id; }
        @Override public String previousPreparationId() { return previous; }
        @Override public String confirmationHash() { return hash; }
    }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            requireActive.run();
            try (var session = client.startSession()) {
                return session.withTransaction(() -> { requireActive.run(); T result = action.apply(session); requireActive.run(); return result; }, TRANSACTION);
            }
        });
    }
    private static TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,
            Map.of("id", "pipeline-rebuild", "field", "stateRebuild"), null); }
}
