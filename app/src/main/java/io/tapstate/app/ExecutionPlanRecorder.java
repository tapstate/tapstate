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

    /** Lets go of {@code pipelineId}'s plan: nothing of it runs any more. */
    void forget(String pipelineId);
}
