package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RunBoundCaptureReadingsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "flow", 7);
    private static final WorkloadClaimFence CLAIM = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
            new WorkloadOwner("node", "boot"), 2, 3, 1);
    private static final ExecutionFence ORIGINAL = new ExecutionFence(SCOPE, CLAIM);
    private static final Instant BEGAN = Instant.parse("2026-01-01T00:00:00Z");

    @Test void noCurrentEngineRunReportsAbsenceWithoutReadingAnOldCapture() {
        var captures = mock(PipelineCaptureCoordinator.class);
        var readings = new RunBoundCaptureReadings(captures, ignored -> Optional.empty(), ignored -> { throw new AssertionError("no current run"); });
        assertThat(readings.snapshotProgress("flow")).isEqualTo(SnapshotReading.NONE);
        assertThat(readings.runSnapshotProgress("flow")).isEqualTo(SnapshotReading.NONE);
        assertThat(readings.capturedRows("flow")).isEqualTo(CaptureReading.NONE);
        verifyNoInteractions(captures);
    }

    @Test void allThreeReadingsUseTheExactOriginalRunAndKeepTheirDifferentMeanings() {
        var captures = mock(PipelineCaptureCoordinator.class);
        var run = mock(PipelineCaptureCoordinator.class);
        when(captures.forRun(SCOPE, CLAIM, ORIGINAL)).thenReturn(run);
        var completed = new SnapshotReading(Map.of("orders", new TableSnapshot(20, null, null)), BEGAN);
        var current = new SnapshotReading(Map.of("orders", new TableSnapshot(1, null, null)), BEGAN);
        var received = new CaptureReading(Map.of("orders", Map.of("r", 1L, "u", 3L)), Map.of("orders", 40L), BEGAN);
        when(run.snapshotProgress("flow")).thenReturn(completed);
        when(run.runSnapshotProgress("flow")).thenReturn(current);
        var readings = new RunBoundCaptureReadings(captures, ignored -> Optional.of(ORIGINAL), ignored -> received);
        assertThat(readings.snapshotProgress("flow")).isEqualTo(completed);
        assertThat(readings.runSnapshotProgress("flow")).isEqualTo(current);
        assertThat(readings.capturedRows("flow")).isEqualTo(received);
        verify(captures, times(2)).forRun(SCOPE, CLAIM, ORIGINAL);
        verify(captures, never()).snapshotProgress(anyString());
        verify(captures, never()).runSnapshotProgress(anyString());
        verify(captures, never()).capturedRows(anyString());
        verify(run, never()).capturedRows(anyString());
    }

    @Test void jobReplacementDuringArrivalCollectionRefusesItsCounters() {
        var captures = mock(PipelineCaptureCoordinator.class);
        var selected = new AtomicReference<>(Optional.of(ORIGINAL));
        var readings = new RunBoundCaptureReadings(captures, ignored -> selected.get(), ignored -> {
            selected.set(Optional.empty());
            return new CaptureReading(Map.of("orders", Map.of("i", 10L)), Map.of(), BEGAN);
        });
        assertThatThrownBy(() -> readings.capturedRows("flow")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
        verifyNoInteractions(captures);
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void replacementOrRemovalDuringAReadRefusesTheOldResult(boolean replaced) {
        var captures = mock(PipelineCaptureCoordinator.class);
        var run = mock(PipelineCaptureCoordinator.class);
        when(captures.forRun(SCOPE, CLAIM, ORIGINAL)).thenReturn(run);
        var selected = new AtomicReference<>(Optional.of(ORIGINAL));
        when(run.snapshotProgress("flow")).thenAnswer(call -> {
            selected.set(replaced ? Optional.of(new ExecutionFence(new PipelineIntentScope("cluster", 1, "cache", "flow", 8),
                    new WorkloadClaimFence(CLAIM.key(), new WorkloadOwner("node", "replacement"), 3, 4, 2))) : Optional.empty());
            return new SnapshotReading(Map.of("orders", new TableSnapshot(20, null, null)), BEGAN);
        });
        var readings = new RunBoundCaptureReadings(captures, ignored -> selected.get(), ignored -> CaptureReading.NONE);
        assertThatThrownBy(() -> readings.snapshotProgress("flow")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
    }
}
