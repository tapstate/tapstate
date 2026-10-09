package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AFilteredChangeStillAdvancesItsTableTest {

    @Test
    void acceptsTheFirstCurrentObservationAfterTypedNoObservation() {
        AtomicInteger laterReads = new AtomicInteger();
        var first = ControlPlane.interpretState(404, coded(MonitorError.NO_OBSERVATION.code()), "filtered_pipeline");

        PipelineState state = AFilteredChangeStillAdvancesItsTableIT.requireCurrentRunning(first, () -> {
            laterReads.incrementAndGet();
            return Optional.of(PipelineState.RUNNING);
        });

        assertThat(state).isEqualTo(PipelineState.RUNNING);
        assertThat(laterReads.get()).isEqualTo(1);
    }

    @Test
    void aPresentFailedStateIsRejectedWithoutReadingARecovery() {
        AtomicInteger laterReads = new AtomicInteger();
        assertThatThrownBy(() -> AFilteredChangeStillAdvancesItsTableIT.requireCurrentRunning(
                Optional.of(PipelineState.FAILED), () -> {
                    laterReads.incrementAndGet();
                    return Optional.of(PipelineState.RUNNING);
                })).isInstanceOf(AssertionError.class);
        assertThat(laterReads.get()).isZero();
    }

    @Test
    void theFirstPresentFailedStateIsNotWaitedOut() {
        AtomicInteger laterReads = new AtomicInteger();
        assertThatThrownBy(() -> AFilteredChangeStillAdvancesItsTableIT.requireCurrentRunning(Optional.empty(),
                () -> Optional.of(laterReads.getAndIncrement() == 0 ? PipelineState.FAILED : PipelineState.RUNNING)))
                .isInstanceOf(AssertionError.class);
        assertThat(laterReads.get()).isEqualTo(1);
    }

    @Test
    void otherHttpRefusalsRemainLoudDuringTheWait() {
        for (int status : new int[] {401, 403, 404, 500}) {
            AtomicInteger laterReads = new AtomicInteger();
            assertThatThrownBy(() -> AFilteredChangeStillAdvancesItsTableIT.requireCurrentRunning(Optional.empty(),
                    () -> {
                        laterReads.incrementAndGet();
                        return ControlPlane.interpretState(status, coded(LifecycleError.UNKNOWN_PIPELINE.code()),
                                "filtered_pipeline");
                    })).isInstanceOf(AssertionError.class).hasMessageContaining("got " + status);
            assertThat(laterReads.get()).isEqualTo(1);
        }
    }

    private static String coded(String code) {
        return JsonWriter.write(Map.of("code", code, "params", Map.of("pipeline", "filtered_pipeline"),
                "message", "rendered"));
    }
}
