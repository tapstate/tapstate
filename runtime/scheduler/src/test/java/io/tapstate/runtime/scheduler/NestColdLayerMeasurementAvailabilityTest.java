package io.tapstate.runtime.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.NestStateWindow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Cumulative readings stay scoped across missing publications and counting-lifetime resets. */
class NestColdLayerMeasurementAvailabilityTest {
    @Test
    void theFirstCompleteReadingUsesCumulativeCountersAndThenRecoveryUsesTheNextWindow() {
        RecordingAlert alert = new RecordingAlert();
        NestColdLayerWatch watch = new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, alert);
        watch.saw("p", Map.of("ns", legacy(200, 200)));
        assertThat(alert.crossed).isEmpty();

        watch.saw("p", Map.of("ns", legacy(300, 240).withKeyCounts(900, 600)));
        NestStateWindow first = alert.crossed.getFirst();
        assertThat(first.accesses()).isEqualTo(300);
        assertThat(first.keyAccesses()).hasValue(900);
        assertThat(first.coldKeys()).hasValue(600);
        assertThat(first.servedFromCold()).hasValue(2.0 / 3.0);
        report("first-complete", first);

        watch.saw("p", Map.of("ns", legacy(400, 240).withKeyCounts(1_200, 600)));
        assertThat(alert.crossed).hasSize(1);
        NestStateWindow recovered = alert.cleared.getFirst();
        assertThat(recovered.accesses()).isEqualTo(100);
        assertThat(recovered.keyAccesses()).hasValue(300);
        assertThat(recovered.coldKeys()).hasValue(0);
        assertThat(recovered.servedFromCold()).hasValue(0.0);
        report("next-measured-window", recovered);
    }

    @Test
    void aMissingPublicationKeepsTheLastJudgedMeasuredBoundary() {
        RecordingAlert alert = new RecordingAlert();
        NestColdLayerWatch watch = new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, alert);
        watch.saw("p", Map.of("ns", legacy(100, 100).withKeyCounts(400, 400)));
        watch.saw("p", Map.of("ns", legacy(150, 120)));
        assertThat(alert.cleared).isEmpty();

        watch.saw("p", Map.of("ns", legacy(200, 140).withKeyCounts(800, 480)));
        assertThat(alert.crossed).hasSize(1);
        NestStateWindow recovered = alert.cleared.getFirst();
        assertThat(recovered.accesses()).isEqualTo(100);
        assertThat(recovered.keyAccesses()).hasValue(400);
        assertThat(recovered.coldKeys()).hasValue(80);
        assertThat(recovered.servedFromCold()).hasValue(0.2);
        report("publication-resumed", recovered);
    }

    @Test
    void resetReadingsDoNotCarryCountersFromThePreviousRunIntoTheNewRun() {
        RecordingAlert alert = new RecordingAlert();
        NestColdLayerWatch watch = new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, alert);
        watch.saw("p", Map.of("ns", legacy(1_000, 900).withKeyCounts(4_000, 3_600)));
        watch.saw("p", Map.of("ns", legacy(30, 10)));
        assertThat(alert.cleared).isEmpty();

        watch.saw("p", Map.of("ns", legacy(100, 20).withKeyCounts(400, 40)));
        NestStateWindow recovered = alert.cleared.getFirst();
        assertThat(recovered.accesses()).isEqualTo(100);
        assertThat(recovered.keyAccesses()).hasValue(400);
        assertThat(recovered.coldKeys()).hasValue(40);
        assertThat(recovered.servedFromCold()).hasValue(0.1);
        report("counter-reset", recovered);
    }

    private static NestStateReading legacy(long accesses, long backfills) {
        return new NestStateReading(8, accesses, backfills, backfills);
    }

    private static void report(String phase, NestStateWindow window) {
        System.out.printf("NEST_MEASUREMENT %s accesses=%d keyAccesses=%d coldKeys=%d share=%.6f%n",
                phase, window.accesses(), window.keyAccesses().orElseThrow(),
                window.coldKeys().orElseThrow(), window.servedFromCold().orElseThrow());
    }

    private static final class RecordingAlert implements NestColdLayerAlert {
        private final List<NestStateWindow> crossed = new ArrayList<>();
        private final List<NestStateWindow> cleared = new ArrayList<>();

        @Override public void crossed(String pipeline, String namespace, NestStateWindow window) {
            crossed.add(window);
        }
        @Override public void cleared(String pipeline, String namespace, NestStateWindow window) {
            cleared.add(window);
        }
    }
}
