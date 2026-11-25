package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.util.Map;
import java.util.Set;

/** Strict preparation annex encoding; it contains no lease deadline or runtime heartbeat. */
final class MongoPipelinePreparationFacts {
    private static final Set<String> ISSUED = Set.of("phase", "scope", "preparationId", "issuerNode");
    private static final Set<String> SCOPE = Set.of("clusterId", "bindingVersion", "cacheIncarnation", "pipelineId", "stateGeneration");
    private static final Set<String> NODE = Set.of("clusterId", "nodeId", "bootId", "claimGeneration", "executionGeneration", "topologyRevision");
    private MongoPipelinePreparationFacts() {}

    static PipelinePreparation decode(String pipelineId, Document mapping, String clusterId) {
        PipelineStateMapping identity = MongoPipelineStateMappings.decode(pipelineId, mapping);
        try {
            if (!(mapping.get("cachePreparation") instanceof Document annex)) throw unreadable(pipelineId);
            PipelinePreparation.Phase phase = PipelinePreparation.Phase.valueOf(annex.getString("phase"));
            if (phase == PipelinePreparation.Phase.UNASSIGNED) {
                if (!annex.keySet().equals(Set.of("phase"))) throw unreadable(pipelineId);
                return new PipelinePreparation(pipelineId, identity.stateGeneration(), phase, null, null, null);
            }
            if (!annex.keySet().equals(ISSUED) || !(annex.get("scope") instanceof Document coordinates)
                    || !(annex.get("issuerNode") instanceof Document node)) throw unreadable(pipelineId);
            PipelineIntentScope scope = scope(coordinates);
            if (!clusterId.equals(scope.clusterId())) throw unreadable(pipelineId);
            return new PipelinePreparation(pipelineId, identity.stateGeneration(), phase, scope,
                    annex.getString("preparationId"), node(node));
        } catch (IllegalArgumentException | ClassCastException | NullPointerException invalid) {
            throw unreadable(pipelineId);
        }
    }

    static Document encode(PipelinePreparation value) {
        Document annex = new Document("phase", value.phase().name());
        if (value.phase() != PipelinePreparation.Phase.UNASSIGNED) {
            annex.append("scope", scope(value.scope())).append("preparationId", value.preparationId())
                    .append("issuerNode", node(value.issuerNode()));
        }
        return annex;
    }

    static Document scope(PipelineIntentScope scope) {
        return new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                .append("cacheIncarnation", scope.cacheIncarnation()).append("pipelineId", scope.pipelineId())
                .append("stateGeneration", scope.stateGeneration());
    }

    static PipelineIntentScope scope(Document value) {
        if (!value.keySet().equals(SCOPE) || !(value.get("bindingVersion") instanceof Long binding)
                || !(value.get("stateGeneration") instanceof Long generation)) throw new IllegalArgumentException("invalid preparation scope");
        return new PipelineIntentScope(value.getString("clusterId"), binding, value.getString("cacheIncarnation"),
                value.getString("pipelineId"), generation);
    }

    private static Document node(WorkloadClaimFence node) {
        return new Document("clusterId", node.key().clusterId()).append("nodeId", node.owner().nodeId())
                .append("bootId", node.owner().bootId()).append("claimGeneration", node.claimGeneration())
                .append("executionGeneration", node.executionGeneration()).append("topologyRevision", node.topologyRevision());
    }

    private static WorkloadClaimFence node(Document value) {
        if (!value.keySet().equals(NODE) || !(value.get("claimGeneration") instanceof Long claim)
                || !(value.get("executionGeneration") instanceof Long execution)
                || !(value.get("topologyRevision") instanceof Long topology)) throw new IllegalArgumentException("invalid preparation node");
        var owner = new WorkloadOwner(value.getString("nodeId"), value.getString("bootId"));
        return new WorkloadClaimFence(new WorkloadClaimKey(value.getString("clusterId"), WorkloadClaimType.NODE_SESSION,
                owner.nodeId()), owner, claim, execution, topology);
    }

    static TapstateException unreadable(String pipelineId) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId, "field", "cachePreparation"), null);
    }
}
