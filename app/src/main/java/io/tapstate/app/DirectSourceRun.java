package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.*;
import java.io.Serializable;
import java.util.*;

/** Only immutable original coordinates travel to private-channel readers and writers. */
record DirectSourceRun(ExecutionFence execution,DirectChannelContext context,long epoch,long snapshotEpoch,Set<String> tables,
        String captureNode,String captureBoot,long captureGeneration,long captureExecution,long captureTopology)
        implements Serializable {
    DirectSourceRun {
        Objects.requireNonNull(execution,"execution"); Objects.requireNonNull(context,"context");
        tables = Set.copyOf(tables); new WorkloadOwner(captureNode,captureBoot);
        if (snapshotEpoch<0 || snapshotEpoch>epoch || !context.scope().equals(execution.scope()) || epoch < 1 || epoch == Long.MAX_VALUE || tables.isEmpty()
                || tables.stream().anyMatch(String::isBlank) || captureGeneration < 1 || captureExecution < 0 || captureTopology < 0)
            throw new IllegalArgumentException("a private source requires its original execution, epoch and selection");
    }
    static DirectSourceRun of(ExecutionFence original,DirectChannelProgress opened) {
        var window = Objects.requireNonNull(opened.current(),"opened capture"); var capture = window.captureFence();
        var run = new DirectSourceRun(original,opened.context(),opened.epoch(),opened.cursor().snapshotEpoch(),opened.tables(),capture.owner().nodeId(),
                capture.owner().bootId(),capture.claimGeneration(),capture.executionGeneration(),capture.topologyRevision());
        run.requireOriginal(opened); return run;
    }
    long loadEpoch(){return snapshotEpoch==0?epoch:snapshotEpoch;}
    WorkloadClaimFence pipelineClaim() {
        return new WorkloadClaimFence(new WorkloadClaimKey(context.scope().clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,execution.pipelineId()),
                new WorkloadOwner(execution.ownerNodeId(),execution.ownerBootId()),execution.claimGeneration(),execution.executionGeneration(),execution.topologyRevision());
    }
    WorkloadClaimFence captureClaim() {
        return new WorkloadClaimFence(new WorkloadClaimKey(context.scope().clusterId(),WorkloadClaimType.CAPTURE,context.captureId()),
                new WorkloadOwner(captureNode,captureBoot),captureGeneration,captureExecution,captureTopology);
    }
    CacheBusinessScope cacheScope() {
        var scope = context.scope(); return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation());
    }
    void requireOriginal(DirectChannelProgress progress) {
        var current = progress.current();
        if (!context.equals(progress.context()) || !tables.equals(progress.tables()) || snapshotEpoch!=progress.cursor().snapshotEpoch() || current == null || current.epoch() != epoch
                || !captureClaim().equals(current.captureFence()) || !pipelineClaim().equals(current.pipelineFence())) throw refused();
    }
    TapstateException refused() { return new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline",execution.pipelineId()),null); }
}
