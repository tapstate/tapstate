package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ProcessorRuntimeContext;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * One pipeline's current run, as the engine reports it.
 *
 * <p>The engine reports a run's processors from its own periodic collection, so a run is known to exist
 * before its processors are, and for the first moments of one the readings arrive from some members and
 * not yet others. That window is short and it is real -- measured at about five seconds on a two-member
 * cluster -- and during it a reading that named only the processors it had would be wrong rather than
 * stale. Which members it was assembled from therefore travels with it.
 *
 * @param pipelineId   the pipeline this run carries
 * @param executionId  the engine's identity for this execution of it -- it changes when the run is
 *                     re-planned, which is how a reader tells a restarted run from the one it replaced
 * @param measuredAt   when the newest reading behind {@code vertices} was taken, or null when none have
 *                     been taken
 * @param measuredFrom the engine identities of the members whose readings this is assembled from; empty
 *                     when none have reported
 * @param vertices     the vertices measured at that moment, in the order the engine named them
 */
public record LivePipelineRun(
        String pipelineId,
        String executionId,
        Instant measuredAt,
        Set<String> measuredFrom,
        List<LivePipelineVertex> vertices,
        List<ProcessorRuntimeContext> processorContexts,
        NativeExecution nativeExecution) {

    public LivePipelineRun {
        pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
        measuredFrom = Set.copyOf(Objects.requireNonNull(measuredFrom, "measuredFrom"));
        vertices = List.copyOf(Objects.requireNonNull(vertices, "vertices"));
        processorContexts = List.copyOf(Objects.requireNonNull(processorContexts, "processorContexts"));
    }

    public LivePipelineRun(String pipelineId, String executionId, Instant measuredAt, Set<String> measuredFrom,
            List<LivePipelineVertex> vertices, List<ProcessorRuntimeContext> processorContexts) {
        this(pipelineId, executionId, measuredAt, measuredFrom, vertices, processorContexts, null);
    }

    public LivePipelineRun(String pipelineId, String executionId, Instant measuredAt, Set<String> measuredFrom,
            List<LivePipelineVertex> vertices) {
        this(pipelineId, executionId, measuredAt, measuredFrom, vertices, List.of());
    }

    public LivePipelineRun withProcessorContexts(List<ProcessorRuntimeContext> contexts) {
        return new LivePipelineRun(pipelineId, executionId, measuredAt, measuredFrom, vertices, contexts, nativeExecution);
    }

    /** Native facts qualified against the actual job, configuration, and measured execution. */
    public LivePipelineRun withNativeExecution(NativeExecution scope, List<ProcessorRuntimeContext> contexts) {
        return new LivePipelineRun(pipelineId, executionId, measuredAt, measuredFrom, vertices, contexts, scope);
    }

    /** Internal read-port scope; raw metric identities remain separate from initialized native identities. */
    public record NativeExecution(String jobId, String runtimeExecutionId, String measuredExecutionId,
            long claimGeneration, long executionGeneration, long profileGeneration) {
        public NativeExecution {
            Objects.requireNonNull(jobId, "jobId");
            Objects.requireNonNull(runtimeExecutionId, "runtimeExecutionId");
            Objects.requireNonNull(measuredExecutionId, "measuredExecutionId");
            if (jobId.isBlank() || runtimeExecutionId.isBlank() || measuredExecutionId.isBlank()
                    || claimGeneration < 1 || executionGeneration < 1 || profileGeneration < 1) {
                throw new IllegalArgumentException("native execution scope is incomplete");
            }
        }

        public boolean matches(ProcessorRuntimeContext context) {
            return jobId.equals(context.jobId()) && runtimeExecutionId.equals(context.runtimeExecutionId())
                    && claimGeneration == context.claimGeneration()
                    && executionGeneration == context.executionGeneration()
                    && profileGeneration == context.profileGeneration();
        }
    }
}
