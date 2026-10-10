package io.tapstate.core.lifecycle;

import java.util.Collection;
import java.util.Map;

/**
 * Where the plan of each pipeline's current execution is read from: the one record every read face answers "how
 * wide does this run, and why" from, so that no two of them work it out again and disagree.
 */
public interface ExecutionPlans {

    /** Reads nothing: a deployment that records no plans answers every pipeline with none. */
    ExecutionPlans NONE = pipelineIds -> Map.of();

    /**
     * The plan of each of {@code pipelineIds}' current execution, for those that have one: read in one go, since
     * a reader asking about every pipeline must not ask the cluster once per pipeline.
     */
    Map<String, ExecutionPlan> current(Collection<String> pipelineIds);
}
