package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.function.Supplier;

/** Completes only the original deletion; no current same-name definition or execution is adopted on retry. */
final class PipelineDeletionCoordinator {
    /** Durable retirement checks stay outside stores; their callback guard performs only local checks. */
    record Isolated(Optional<PipelineRetirementRequest.CacheOriginal> cache,Runnable requireRetired,Runnable requireLocal) {
        Isolated {cache=Objects.requireNonNull(cache);Objects.requireNonNull(requireRetired);Objects.requireNonNull(requireLocal);}
    }
    @FunctionalInterface interface Isolation {
        Isolated isolate(PipelineRetirementRequest original,Optional<OperatorRunManifest> captured);
    }
    private final PipelineRetirements configuration;private final PipelineCacheRetirements cache;
    private final SrsConsumerStore consumers;private final PipelinePrivateNoteRetirement notes;
    private final Supplier<WorkloadClaimFence> node;private final Runnable activeNode;private final Isolation isolation;
    PipelineDeletionCoordinator(PipelineRetirements configuration,PipelineCacheRetirements cache,SrsConsumerStore consumers,
            PipelinePrivateNoteRetirement notes,Supplier<WorkloadClaimFence> node,Runnable activeNode,Isolation isolation) {
        this.configuration=Objects.requireNonNull(configuration);this.cache=Objects.requireNonNull(cache);this.consumers=Objects.requireNonNull(consumers);
        this.notes=Objects.requireNonNull(notes);this.node=Objects.requireNonNull(node);this.activeNode=Objects.requireNonNull(activeNode);this.isolation=Objects.requireNonNull(isolation);
    }
    PipelineRetirementResult delete(PipelineRetirementRequest original,Optional<OperatorRunManifest> manifest) {
        activeNode.run();var completed=configuration.completed(original.scope().pipelineId(),original.operationId());
        if(completed.isPresent())return same(original,completed.orElseThrow());
        var retired=isolation.isolate(original,manifest);retired.requireRetired().run();activeNode.run();
        return execute(configuration.begin(original),retired);
    }
    Optional<PipelineRetirementResult> resume(PipelineIntentScope scope,String operationId,Optional<OperatorRunManifest> manifest) {
        activeNode.run();var completed=configuration.completed(scope.pipelineId(),operationId);
        if(completed.isPresent()) {
            if(!completed.orElseThrow().scope().equals(scope))throw fenced();return completed;
        }
        var pending=configuration.pending(scope,operationId);if(pending.isEmpty())return Optional.empty();
        var original=pending.orElseThrow();var retired=isolation.isolate(original.request(),manifest);
        return Optional.of(execute(original,retired));
    }
    private PipelineRetirementResult execute(PipelineRetirementPreparation original,Isolated isolated) {
        Runnable local=() -> {activeNode.run();isolated.requireLocal().run();activeNode.run();};
        Runnable guard=() -> {activeNode.run();isolated.requireRetired().run();activeNode.run();};
        guard.run();configuration.requirePending(original);var currentNode=Objects.requireNonNull(node.get());
        var originalConsumers=consumers.guardedBy(local);
        var members=originalConsumers.forPipeline(original.request().scope());guard.run();
        for(var member:members) {
            guard.run();if(member.detached())continue;
            originalConsumers.detachForRetirement(original,member,currentNode,isolated.cache()).orElseThrow(PipelineDeletionCoordinator::fenced);
            guard.run();
        }
        guard.run();var receipt=cache.retire(original,currentNode,isolated.cache());guard.run();
        notes.delete(original,configuration,guard,local);guard.run();return configuration.complete(receipt);
    }
    private PipelineRetirementResult same(PipelineRetirementRequest original,PipelineRetirementResult result) {
        if(!original.scope().equals(result.scope()) || !original.definitionHash().equals(result.definitionHash()))throw fenced();return result;
    }
    private static TapstateException fenced(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
}
