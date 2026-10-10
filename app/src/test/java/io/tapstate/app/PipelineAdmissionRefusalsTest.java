package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PipelineAdmissionRefusalsTest {
    @Test void aCodedPhysicalStartFailureIsReportedForItsAllocatedExecution() {
        Engine engine = mock(Engine.class);
        DagSource dags = mock(DagSource.class);
        PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
        PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        PipelineExecutionAdmission admission = mock(PipelineExecutionAdmission.class);
        NestStateTeardown teardown = mock(NestStateTeardown.class);
        when(teardown.defaultDatabase()).thenReturn("default");
        var failure = new TapstateException(io.tapstate.runtime.engine.EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        var prepared = new DagSource.StartPreparation(DagSource.NestCapacity.none(), Set.of(), Optional.empty(),
                () -> fence -> { throw failure; }, Map.of());
        when(dags.prepareStart("p", "default")).thenReturn(prepared);
        when(admission.prepare(eq("p"), any(), eq(ownership))).thenReturn(true);
        when(ownership.mayStart("p")).thenReturn(true);
        var execution = new PipelineActuationOwnership.Execution(true, new ExecutionFence("p", 1, 9, 2), 7L);
        when(admission.begin(eq("p"), any(), eq(ownership))).thenReturn(execution);
        var actuator = new EngineLifecycleActuator(engine, dags, captures, teardown, ownership,
                ExecutionPlanRecorder.NONE, Clock.systemUTC(), (pipeline, connectors) -> Set.of(), admission);

        assertThatThrownBy(() -> actuator.start("p")).isSameAs(failure);

        verify(admission).failedAfterAllocation("p", execution, failure);
        verify(ownership, never()).startRefusedBeforeItsRun(anyString());
        verify(engine, never()).submitFenced(anyString(), any(), anyMap(), any(), anyLong(), anyLong(), anyLong());
    }

    @Test void aCapacityRefusalIsRecordedBeforeAnyPhysicalStartOrExecutionAllocation() {
        Engine engine = mock(Engine.class);
        DagSource dags = mock(DagSource.class);
        PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
        PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        PipelineExecutionAdmission admission = mock(PipelineExecutionAdmission.class);
        NestStateTeardown teardown = mock(NestStateTeardown.class);
        when(teardown.defaultDatabase()).thenReturn("default");
        var prepared = new DagSource.StartPreparation(DagSource.NestCapacity.none(), Set.of(), Optional.empty(),
                () -> fence -> { throw new AssertionError("a refused plan must never build a physical DAG"); }, Map.of());
        when(dags.prepareStart("p", "default")).thenReturn(prepared);
        var refusal = new TapstateException(LifecycleError.CLUSTER_CAPACITY_UNPROVEN,
                Map.of("pipeline", "p", "reason", "missing geometry"), null);
        when(admission.prepare(eq("p"), any(), eq(ownership))).thenThrow(refusal);
        var actuator = new EngineLifecycleActuator(engine, dags, captures, teardown, ownership,
                ExecutionPlanRecorder.NONE, Clock.systemUTC(), (pipeline, connectors) -> Set.of(), admission);

        assertThatThrownBy(() -> actuator.start("p")).isSameAs(refusal);

        verify(admission).refused("p", refusal);
        verify(ownership).startRefusedBeforeItsRun("p");
        verify(admission, never()).begin(anyString(), any(), any());
        verifyNoInteractions(captures);
        verify(teardown, never()).finishPending(anyString());
    }

    @Test void aControllerWithoutALocalCaptureFrameReadsTheExactExecutionFailure() {
        Engine engine = mock(Engine.class);
        PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
        PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        WorkloadClaim claim = currentClaim();
        when(ownership.currentClaim("p")).thenReturn(Optional.of(claim));
        var original = new TapstateException(io.tapstate.runtime.srs.CaptureError.START_FROM_OUTSIDE_WINDOW,
                Map.of("requested", "old", "earliest", "head", "retention", "2h"), null);
        when(captures.captureFailure("p", WorkloadClaimFence.from(claim))).thenReturn(Optional.of(original));
        var actuator = new EngineLifecycleActuator(engine, mock(DagSource.class), captures, mock(NestStateTeardown.class),
                ownership, ExecutionPlanRecorder.NONE, Clock.systemUTC(), ConnectorReadiness.NONE);

        assertThat(actuator.failure("p")).contains(original);

        verify(captures).captureFailure("p", WorkloadClaimFence.from(claim));
        verify(captures, never()).captureFailure("p");
    }

    @Test void aPreviousNativeJobsFailureCannotKillANewExecutionWaitingForItsCapture() {
        Engine engine = mock(Engine.class);
        PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
        PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(currentClaim()));
        when(engine.nativeRun("p")).thenReturn(Optional.of(new Engine.NativeRun("old-job", 1, 7, 2,
                com.hazelcast.jet.core.JobStatus.FAILED, Optional.empty())));
        when(engine.failureOf("p")).thenReturn(Optional.of(new IllegalStateException("previous run failed")));
        var actuator = new EngineLifecycleActuator(engine, mock(DagSource.class), captures, mock(NestStateTeardown.class),
                ownership, ExecutionPlanRecorder.NONE, Clock.systemUTC(), ConnectorReadiness.NONE);

        assertThat(actuator.failure("p")).isEmpty();
    }

    @Test void anEngineLostOnThisMemberStillReportsItsCodedFailureForTheCurrentRun() {
        Engine engine = mock(Engine.class);
        PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(currentClaim()));
        var lost = new TapstateException(io.tapstate.runtime.engine.EngineError.OUT_OF_MEMORY, Map.of("pipeline", "p"), null);
        when(engine.lost("p")).thenReturn(Optional.of(lost));
        var actuator = new EngineLifecycleActuator(engine, mock(DagSource.class), mock(PipelineCaptureCoordinator.class),
                mock(NestStateTeardown.class), ownership, ExecutionPlanRecorder.NONE, Clock.systemUTC(), ConnectorReadiness.NONE);

        assertThat(actuator.failure("p")).contains(lost);
    }

    private static WorkloadClaim currentClaim() {
        return new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "p"),
                new WorkloadOwner("b", "boot-b"), 1, 8, 7, Instant.parse("2026-10-10T06:01:00Z"),
                8, 1, Set.of("a", "b", "c"), 0, false, 2);
    }
}
