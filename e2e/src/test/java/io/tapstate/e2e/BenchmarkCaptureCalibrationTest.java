package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Disabled passive accounting stays unavailable after the real dispatcher's separate shutdown drain. */
class BenchmarkCaptureCalibrationTest {
    private static final String SHA = BenchmarkJdiCostObserver.Arm.OBSERVABILITY.sha256;

    @Test
    void passiveAccountingCanBeUnavailableAfterTheDispatcherHasDrained() {
        var passive = capture(BenchmarkJdiTelemetrySession.Mode.PASSIVE_JDWP, false);
        assertThatCode(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.PASSIVE_JDWP, SHA, Optional.of(passive)))
                .doesNotThrowAnyException();
        var projected = PipelineBenchmarkLiveRunIT.telemetryEvidence(Optional.of(passive));
        assertThat(projected).containsEntry("scopedAccountingDrained", false);
        assertThat(projected.get("scopedCosts"))
                .isEqualTo(Map.of("state", "UNAVAILABLE", "reason", "PASSIVE_JDWP"));
    }

    @Test
    void anExplicitCalibrationCannotSilentlySkipAMissingConnectorDirectory() {
        String property = "tapstate.e2e.connectors-dir";
        String previous = System.getProperty(property);
        try {
            System.clearProperty(property);
            assertThatThrownBy(BenchmarkCaptureCalibrationLiveRunIT::requireConnectors)
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(property);
        } finally {
            if (previous == null) { System.clearProperty(property); } else { System.setProperty(property, previous); }
        }
    }

    @Test
    void activeAccountingMustActuallyDrain() {
        assertThatThrownBy(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.ACTIVE_CAPTURE, SHA,
                Optional.of(capture(BenchmarkJdiTelemetrySession.Mode.ACTIVE_CAPTURE, false))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("drain is invalid");
        assertThatCode(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.ACTIVE_CAPTURE, SHA,
                Optional.of(capture(BenchmarkJdiTelemetrySession.Mode.ACTIVE_CAPTURE, true))))
                .doesNotThrowAnyException();
    }

    @Test
    void aMissingCaptureOrAPlainModeWithAnObserverIsRejected() {
        assertThatCode(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN, SHA, Optional.empty())).doesNotThrowAnyException();
        assertThatThrownBy(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.PASSIVE_JDWP, SHA, Optional.empty()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("availability disagrees");
        assertThatThrownBy(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.PLAIN, SHA,
                Optional.of(capture(BenchmarkJdiTelemetrySession.Mode.PASSIVE_JDWP, false))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("availability disagrees");
    }

    @Test
    void aCaptureFromAnotherArtifactOrModeCannotJoinTheBatch() {
        var active = Optional.of(capture(BenchmarkJdiTelemetrySession.Mode.ACTIVE_CAPTURE, true));
        assertThatThrownBy(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.ACTIVE_CAPTURE, "other-artifact", active))
                .isInstanceOf(AssertionError.class).hasMessageContaining("artifact or drain is invalid");
        assertThatThrownBy(() -> BenchmarkCaptureCalibrationLiveRunIT.verifyCapture(
                BenchmarkCaptureCalibrationLiveRunIT.Mode.PASSIVE_JDWP, SHA, active))
                .isInstanceOf(AssertionError.class).hasMessageContaining("mode, artifact");
    }

    private static BenchmarkJdiTelemetrySession.Evidence capture(BenchmarkJdiTelemetrySession.Mode mode,
            boolean accountingDrained) {
        var health = new BenchmarkJdiTelemetrySession.Health(1, 2, true, false, 0, 0, Map.of());
        var pending = new BenchmarkJdiTelemetrySession.Pending(Map.of(), Map.of(), Map.of());
        var boundary = new BenchmarkJdiTelemetrySession.Boundary(100, 0, 0, health, pending);
        return new BenchmarkJdiTelemetrySession.Evidence(BenchmarkJdiCostObserver.Arm.OBSERVABILITY, SHA,
                mode, Set.of(BenchmarkJdiTelemetrySession.Feature.DISPATCHER), Map.of(), Map.of(), Map.of(),
                boundary, boundary, boundary, Map.of(), 1, 0, 0, 1, 0, 0, accountingDrained,
                mode == BenchmarkJdiTelemetrySession.Mode.PASSIVE_JDWP
                        ? Set.of(BenchmarkJdiCostObserver.Unavailable.SCOPED_COST_CAPTURE) : Set.of());
    }
}
