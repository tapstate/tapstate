package io.tapstate.app;

import io.tapstate.core.lifecycle.ExecutionPlan;

/**
 * Where a run's plan is written down as the run is submitted, and let go of once the pipeline stops: the record
 * every read face answers "how wide does this run, and why" from.
 */
interface ExecutionPlanRecorder {

    /** Writes nothing down, for the drives that report no plan. */
    ExecutionPlanRecorder NONE = new ExecutionPlanRecorder() {
        @Override
        public void record(ExecutionPlan plan) {
        }

        @Override
        public void forget(String pipelineId) {
        }
    };

    /** Records {@code plan} as its pipeline's current one, in place of whatever run came before it. */
    void record(ExecutionPlan plan);

    /**
     * Lets go of {@code pipelineId}'s plan as its current one: nothing of it runs any more. It stays the one
     * {@link #last} answers with, so a run started later can say how it differs from it.
     */
    void forget(String pipelineId);

    /**
     * The plan {@code pipelineId}'s latest run was submitted on, whether or not that run is still going, or null
     * where none was recorded - what the next run's plan is compared against. A recorder that keeps nothing
     * answers null.
     */
    default ExecutionPlan last(String pipelineId) {
        return null;
    }
}
