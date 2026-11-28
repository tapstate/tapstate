package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Decodes the persisted rebuild witness used by CONFIG recovery and status reads. */
final class MongoPipelineRebuildFacts {
    private static final Set<String> ANNEX = Set.of("formatVersion", "operationId", "originalScope", "targetScope", "preparationId",
            "previousPreparationId", "intentRevision", "originalClaim", "originalStateEpoch", "confirmationHash");

    private MongoPipelineRebuildFacts() { }

    static Document rebuildingMappingFilter(String pipelineId) {
        return new Document("_id", pipelineId).append("phase", PipelineStateMapping.Phase.REBUILDING.name());
    }

    static void requireObservablePending(Document mapping, PipelineStateMapping state, PipelineIntentSnapshot intent) {
        try {
            var annex = mapping.get("stateRebuild", Document.class);
            annexVersion(annex, false);
            var scope = intent.scope();
            var target = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(),
                    scope.pipelineId(), scope.stateGeneration() + 1);
            if (!state.operationId().equals(annex.getString("operationId"))
                    || !Long.valueOf(intent.intentRevision()).equals(annex.get("intentRevision"))
                    || !scope.equals(MongoPipelinePreparationFacts.scope(annex.get("originalScope", Document.class)))
                    || !target.equals(MongoPipelinePreparationFacts.scope(annex.get("targetScope", Document.class)))
                    || !(annex.get("confirmationHash") instanceof String hash) || !hash.matches("[0-9a-f]{64}")) throw unreadable();
            var claim = pendingClaim(annex);
            if (!scope.clusterId().equals(claim.key().clusterId()) || !scope.pipelineId().equals(claim.key().resourceId())) throw unreadable();
            pendingEpoch(annex);
            checkedId(annex.getString("preparationId"));
            var preparation = MongoPipelinePreparationFacts.decode(scope.pipelineId(), mapping, scope.clusterId());
            if (preparation.phase() != PipelinePreparation.Phase.COMPLETE || !scope.equals(preparation.scope())
                    || !checkedId(annex.getString("previousPreparationId")).equals(preparation.preparationId())) throw unreadable();
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) {
            throw unreadable();
        }
    }

    static WorkloadClaimFence pendingClaim(Document annex) {
        var claim = annex.get("originalClaim", Document.class);
        if (claim == null || !claim.keySet().equals(Set.of("clusterId", "pipelineId", "nodeId", "bootId",
                "claimGeneration", "executionGeneration", "topologyRevision"))
                || !(claim.get("claimGeneration") instanceof Long generation)
                || !(claim.get("executionGeneration") instanceof Long execution)
                || !(claim.get("topologyRevision") instanceof Long topology)) throw unreadable();
        return new WorkloadClaimFence(new WorkloadClaimKey(claim.getString("clusterId"), WorkloadClaimType.PIPELINE_ACTUATION,
                claim.getString("pipelineId")), new WorkloadOwner(claim.getString("nodeId"), claim.getString("bootId")),
                generation, execution, topology);
    }

    static long pendingEpoch(Document annex) {
        if (!(annex.get("originalStateEpoch") instanceof Long epoch) || epoch < 0 || epoch >= Long.MAX_VALUE - 1) throw unreadable();
        return epoch;
    }

    static String checkedId(String id) {
        if (!UUID.fromString(id).toString().equals(id)) throw new IllegalArgumentException("invalid rebuild preparation id");
        return id;
    }
    static int annexVersion(Document annex, boolean complete) {
        if (annex == null || !(annex.get("formatVersion") instanceof Integer version) || (version != 1 && version != 2)) throw unreadable();
        var fields = new java.util.HashSet<>(ANNEX);
        if (complete) fields.add("result");
        if (version == 2) fields.add("inputClosureHash");
        if (!fields.equals(annex.keySet()) || version == 2 && (!(annex.get("inputClosureHash") instanceof String hash)
                || !hash.matches("[0-9a-f]{64}"))) throw unreadable();
        return version;
    }
    static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,
            Map.of("id", "pipeline-rebuild", "field", "stateRebuild"), null); }
}
