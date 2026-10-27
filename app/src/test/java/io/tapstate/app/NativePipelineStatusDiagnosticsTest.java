package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NativePipelineStatusDiagnosticsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("node", 1, "cache", "orders", 1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimFence CLAIM = new WorkloadClaimFence(
            new WorkloadClaimKey("node", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), OWNER, 1, 1, 1);
    private static final ExecutionFence EXECUTION = new ExecutionFence(SCOPE, CLAIM);
    private final PipelineIntentStore intents = mock(PipelineIntentStore.class);
    private final PipelineStatePreparations preparations = mock(PipelineStatePreparations.class);
    private final StateStore states = mock(StateStore.class);
    private final Engine engine = mock(Engine.class);
    private final PipelineActuationOwnership owner = mock(PipelineActuationOwnership.class);
    private final PipelineActuationOwnership.ReportingExecution proof = mock(PipelineActuationOwnership.ReportingExecution.class);
    private final AtomicBoolean active = new AtomicBoolean(true);
    private NativePipelineStatusDiagnostics diagnostics;

    @BeforeEach void setup() {
        var snapshot = new PipelineIntentSnapshot(SCOPE, 1, new DesiredState("orders", PipelineState.RUNNING, "revision", false));
        var node = new WorkloadClaimFence(new WorkloadClaimKey("node", WorkloadClaimType.NODE_SESSION, "node"), OWNER, 1, 0, 0);
        var preparation = new PipelinePreparation("orders", 1, PipelinePreparation.Phase.COMPLETE, SCOPE,
                "c35d6c46-a935-49e4-a765-922d97352db5", node);
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(snapshot));
        when(preparations.read("orders")).thenReturn(Optional.of(preparation));
        when(states.read("orders")).thenReturn(Optional.of(checkpoint(PipelineState.RUNNING)));
        when(engine.isLost()).thenReturn(true);
        when(engine.failureOfLostRun("orders", EXECUTION)).thenReturn(Optional.of(
                new TapstateException(EngineError.OUT_OF_MEMORY, Map.of("pipeline", "orders"), new OutOfMemoryError())));
        when(owner.reportingExecution(SCOPE)).thenReturn(Optional.of(proof));
        when(proof.execution()).thenReturn(EXECUTION); when(proof.current()).thenReturn(true);
        diagnostics = new NativePipelineStatusDiagnostics(new CacheBusinessScope("node", 1, "cache"), intents,
                preparations, engine, owner, active::get, scope -> { assertThat(scope).isEqualTo(SCOPE); return states; });
    }
    private static CheckpointDoc checkpoint(PipelineState state) {
        return new CheckpointDoc("orders", StateJson.of(state), state == PipelineState.RUNNING ? 1 : 2,
                Instant.parse("2026-10-23T00:00:00Z"));
    }
    @Test void anExactCurrentRunCanReportItsLossWithoutWritingAnything() {
        assertThat(diagnostics.failure("orders", PipelineState.RUNNING)).hasValueSatisfying(
                failure -> assertThat(failure.code()).isEqualTo(EngineError.OUT_OF_MEMORY.code()));
        verify(owner).reportingExecution(SCOPE); verifyNoMoreInteractions(owner);
        verify(states, never()).create(anyString(), anyString(), any());
        verify(states, never()).compareAndSwap(anyString(), anyLong(), anyString(), any());
        verify(states, never()).delete(anyString());
    }
    @Test void aStaleRunningObservationCannotFailAnAlreadyCompletedCheckpoint() {
        when(states.read("orders")).thenReturn(Optional.of(checkpoint(PipelineState.COMPLETED)));
        assertThat(diagnostics.failure("orders", PipelineState.RUNNING)).isEmpty();
    }
    @Test void completionDuringTheDiagnosticReadInvalidatesTheFailure() {
        when(states.read("orders")).thenReturn(Optional.of(checkpoint(PipelineState.RUNNING)),
                Optional.of(checkpoint(PipelineState.COMPLETED)));
        assertThat(diagnostics.failure("orders", PipelineState.RUNNING)).isEmpty();
    }
    @Test void unreadableCheckpointProvesNoFailure() {
        when(states.read("orders")).thenThrow(new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "orders", "field", "state"), null));
        assertThat(diagnostics.failure("orders", PipelineState.RUNNING)).isEmpty();
    }
    @Test void noSubmissionReceiptOrExpiredReportingProofProducesNoDiagnosis() {
        when(engine.failureOfLostRun("orders", EXECUTION)).thenReturn(Optional.empty());
        assertThat(diagnostics.failure("orders", PipelineState.RUNNING)).isEmpty();
        when(engine.failureOfLostRun("orders", EXECUTION)).thenReturn(Optional.of(
                new TapstateException(EngineError.OUT_OF_MEMORY, Map.of("pipeline", "orders"), null)));
        when(proof.current()).thenReturn(false);
        assertThat(diagnostics.failure("orders", PipelineState.RUNNING)).isEmpty();
    }
    @Test void aRetiredOwnerAndNonActiveObservedStatesCannotDiagnoseAnotherRun() {
        active.set(false); assertThat(diagnostics.failure("orders", PipelineState.RUNNING)).isEmpty();
        active.set(true);
        for (var state : java.util.List.of(PipelineState.STOPPED, PipelineState.COMPLETED, PipelineState.FAILED))
            assertThat(diagnostics.failure("orders", state)).isEmpty();
    }
}
