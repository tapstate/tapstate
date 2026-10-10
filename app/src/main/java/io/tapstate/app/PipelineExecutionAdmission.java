package io.tapstate.app;

import com.hazelcast.jet.core.DAG;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.WorkloadClaim;

/** Admission and durable receipts around the existing pipeline actuator, before physical side effects. */
interface PipelineExecutionAdmission {
    PipelineExecutionAdmission NONE = new PipelineExecutionAdmission() { };

    default boolean prepare(String pipelineId, DagSource.PlannedStart planned,
            PipelineActuationOwnership ownership) {
        return ownership.mayStart(pipelineId);
    }

    default PipelineActuationOwnership.Execution begin(String pipelineId, DagSource.PlannedStart planned,
            PipelineActuationOwnership ownership) {
        return ownership.beginExecution(pipelineId);
    }

    default void guard(String pipelineId, PipelineActuationOwnership.Execution execution, DAG dag) { }

    default void submitted(String pipelineId, PipelineActuationOwnership.Execution execution, String nativeJobId) { }

    default void refused(String pipelineId, TapstateException failure) { }

    default void failedAfterAllocation(String pipelineId, PipelineActuationOwnership.Execution execution,
            TapstateException failure) { }

    default void stopped(String pipelineId, WorkloadClaim stoppedClaim, boolean jobOver) { }

    default boolean mayComplete(String pipelineId) { return true; }
}
