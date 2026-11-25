package io.tapstate.spi.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** An immutable observed control closure, carrying no storage creation or execution authority. */
public record SharedSrsRecoverySnapshot(CapturePreparation previous, WorkloadClaimFence captureFence,
        String controlHash, List<SrsConsumerMembership> consumers, Map<String, Pipeline> pipelines) {
    public record Pipeline(PipelineIntentScope scope, WorkloadClaimFence claim) {
        public Pipeline {
            if (scope == null || !pipelineClaim(claim, scope.clusterId(), scope.pipelineId()) || !advanceable(claim))
                throw new IllegalArgumentException("invalid shared recovery Pipeline control");
        }
    }

    public SharedSrsRecoverySnapshot {
        if (previous == null || previous.mode() != CapturePreparation.Mode.SHARED_SRS
                || captureFence == null || captureFence.key().type() != WorkloadClaimType.CAPTURE
                || !previous.scope().clusterId().equals(captureFence.key().clusterId())
                || !previous.captureId().equals(captureFence.key().resourceId()) || !advanceable(captureFence)
                || controlHash == null || !controlHash.matches("[0-9a-f]{64}")
                || consumers == null || pipelines == null)
            throw new IllegalArgumentException("invalid shared recovery control closure");
        var ordered = new TreeMap<String, Pipeline>();
        for (var entry : pipelines.entrySet()) {
            Pipeline pipeline = entry.getValue();
            if (entry.getKey() == null || pipeline == null || !entry.getKey().equals(pipeline.scope().pipelineId())
                    || !sameBusiness(previous.scope(), pipeline.scope()))
                throw new IllegalArgumentException("invalid shared recovery Pipeline scope");
            ordered.put(entry.getKey(), pipeline);
        }
        var unique = new HashSet<String>();
        var actualPipelines = new HashSet<String>();
        var sorted = new ArrayList<SrsConsumerMembership>();
        for (var consumer : consumers) {
            if (consumer == null) throw new IllegalArgumentException("missing shared recovery consumer");
            var context = consumer.context();
            String pipelineId = context.key().pipelineId();
            Pipeline pipeline = ordered.get(pipelineId);
            if (consumer.detached() || !previous.chainId().equals(context.key().miningChainId())
                    || !unique.add(context.key().consumerId()) || pipeline == null
                    || context.stateGeneration() != pipeline.scope().stateGeneration()
                    || !pipelineClaim(consumer.commandFence(), previous.scope().clusterId(), pipelineId)
                    || !finite(consumer.commandFence()))
                throw new IllegalArgumentException("incoherent shared recovery consumer closure");
            actualPipelines.add(pipelineId);
            sorted.add(consumer);
        }
        if (!actualPipelines.equals(ordered.keySet()))
            throw new IllegalArgumentException("incomplete shared recovery consumer closure");
        sorted.sort(Comparator.comparing(consumer -> consumer.context().key().consumerId()));
        consumers = List.copyOf(sorted);
        pipelines = Collections.unmodifiableMap(ordered);
    }

    private static boolean sameBusiness(CacheBusinessScope business, PipelineIntentScope scope) {
        return business.clusterId().equals(scope.clusterId()) && business.bindingVersion() == scope.bindingVersion()
                && business.cacheIncarnation().equals(scope.cacheIncarnation());
    }
    private static boolean pipelineClaim(WorkloadClaimFence claim, String cluster, String pipeline) {
        return claim != null && claim.key().type() == WorkloadClaimType.PIPELINE_ACTUATION
                && cluster.equals(claim.key().clusterId()) && pipeline.equals(claim.key().resourceId());
    }
    private static boolean finite(WorkloadClaimFence claim) {
        return claim.claimGeneration() < Long.MAX_VALUE && claim.executionGeneration() < Long.MAX_VALUE
                && claim.topologyRevision() < Long.MAX_VALUE;
    }
    private static boolean advanceable(WorkloadClaimFence claim) {
        return finite(claim) && claim.claimGeneration() < Long.MAX_VALUE - 1
                && claim.executionGeneration() < Long.MAX_VALUE - 1;
    }
}
