package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.NestStateWindow;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class NestColdLayerWatchTest {

    private static final NestColdLayerPressure PRESSURE = new NestColdLayerPressure(0.5, 100);

    private final RecordingAlert alert = new RecordingAlert();

    @Test
    void aNamespaceUnderPressureOnTheFirstReadingIsReportedWithoutWaitingForASecond() {
        // The counts already start at the beginning of the run, so there is nothing to difference against.
        // Holding the first reading back would blind the interval a badly sized namespace announces itself
        // in soonest -- the initial load, where the working set is at its largest.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 900)));

        assertThat(alert.crossed).containsExactly("p1/ns");
    }

    @Test
    void aNamespaceThatGoesOnBeingUnderPressureIsNotReportedAgainOnEveryPass() {
        // This is a level, not an event: it either is or is not the case right now. Saying so once per
        // convergence pass for as long as it holds would bury the log it is written to, and every line
        // after the first carries nothing the first did not.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 900)));
        watch.saw("p1", Map.of("ns", reading(2_000, 1_800)));
        watch.saw("p1", Map.of("ns", reading(3_000, 2_700)));

        assertThat(alert.crossed).containsExactly("p1/ns");
    }

    @Test
    void aNamespaceThatStartsBeingServedFromMemoryAgainIsReportedAsRecovered() {
        // The counterpart of only saying it once: whoever read the first line has no way to learn it stopped
        // being true, and would go on believing a namespace that recovered hours ago is still on disk.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 900)));
        watch.saw("p1", Map.of("ns", reading(2_000, 900)));

        assertThat(alert.crossed).containsExactly("p1/ns");
        assertThat(alert.cleared).containsExactly("p1/ns");
    }

    @Test
    void aNamespaceThatRecoveredAndFellBackIsReportedEachTimeItFalls() {
        // Edge-triggered has to mean both edges, or the second fall is the one nobody hears about.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 900)));
        watch.saw("p1", Map.of("ns", reading(2_000, 900)));
        watch.saw("p1", Map.of("ns", reading(3_000, 1_900)));

        assertThat(alert.crossed).containsExactly("p1/ns", "p1/ns");
        assertThat(alert.cleared).containsExactly("p1/ns");
    }

    @Test
    void aNamespaceServedFromMemoryThroughoutIsNeverReported() {
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 10)));
        watch.saw("p1", Map.of("ns", reading(2_000, 20)));

        assertThat(alert.crossed).isEmpty();
        assertThat(alert.cleared).isEmpty();
    }

    @Test
    void smallSuccessiveWindowsAccumulateBeforeAThresholdDecision() {
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        for (int accesses = 10; accesses < 100; accesses += 10) {
            Map<String, NestStateReading> sample = Map.of("ns", reading(accesses, accesses));
            assertThat(watch.assessment("p1", sample)).isEmpty();
            watch.saw("p1", sample);
        }
        Map<String, NestStateReading> enough = Map.of("ns", reading(100, 100));
        assertThat(watch.assessment("p1", enough)).containsEntry("ns", 1L);
        watch.saw("p1", enough);

        assertThat(alert.crossed).containsExactly("p1/ns");
        assertThat(alert.windows).singleElement().satisfies(window -> {
            assertThat(window.accesses()).isEqualTo(100);
            assertThat(window.backfills()).isEqualTo(100);
        });
    }

    @Test
    void aQuietIntervalDoesNotPretendTheColdLayerRecovered() {
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);
        watch.saw("p1", Map.of("ns", reading(100, 100)));

        Map<String, NestStateReading> quiet = Map.of("ns", reading(110, 100));
        assertThat(watch.assessment("p1", quiet)).isEmpty();
        watch.saw("p1", quiet);
        assertThat(alert.cleared).isEmpty();

        Map<String, NestStateReading> recovered = Map.of("ns", reading(200, 100));
        assertThat(watch.assessment("p1", recovered)).containsEntry("ns", 0L);
        watch.saw("p1", recovered);
        assertThat(alert.cleared).containsExactly("p1/ns");
    }

    @Test
    void aCounterRestartDoesNotCarryAnOldThresholdDecision() {
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);
        watch.saw("p1", Map.of("ns", reading(1_000, 900)));

        Map<String, NestStateReading> restarted = Map.of("ns", reading(10, 10));
        assertThat(watch.assessment("p1", restarted)).isEmpty();
        watch.saw("p1", restarted);
        assertThat(alert.cleared).isEmpty();

        Map<String, NestStateReading> enough = Map.of("ns", reading(100, 100));
        assertThat(watch.assessment("p1", enough)).containsEntry("ns", 1L);
        watch.saw("p1", enough);
        assertThat(alert.crossed).containsExactly("p1/ns", "p1/ns");
    }

    @Test
    void anExecutionChangeStartsANewWindowEvenWhenCountersHaveNotFallen() {
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);
        var oldScope = new ObservationStore.Scope("inc-a", 1);
        var newScope = new ObservationStore.Scope("inc-a", 2);
        watch.saw("p1", oldScope, Map.of("ns", reading(100, 100)));

        Map<String, NestStateReading> fresh = Map.of("ns", reading(100, 0));
        assertThat(watch.assessment("p1", newScope, fresh)).containsEntry("ns", 0L);
        watch.saw("p1", newScope, fresh);
        watch.saw("p1", oldScope, Map.of("ns", reading(200, 200)));

        assertThat(watch.assessment("p1", newScope, Map.of("ns", reading(200, 100))))
                .containsEntry("ns", 1L);
        assertThat(alert.crossed).containsExactly("p1/ns");
        assertThat(alert.cleared).isEmpty();
    }

    @Test
    void aNamespaceThatStopsBeingReportedIsForgottenRatherThanCalledRecovered() {
        // A pipeline that stopped is not a namespace that started being served from memory again, and
        // saying so would be an all-clear nobody earned. Dropping what was held for it also means the next
        // run of the same namespace is judged from its own counts rather than against a dead run's.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 900)));
        watch.saw("p1", Map.of());

        assertThat(alert.cleared).isEmpty();
        assertThat(watch.watching()).isEmpty();
    }

    @Test
    void aNamespaceThatCameBackAfterBeingForgottenIsReportedAgainRatherThanTakenAsStillKnownBad() {
        // The proof that forgetting drops the alerting state too and not only the reading: were the state
        // kept, this run -- a fresh one, under pressure from its first reading -- would be silently taken
        // for the previous run still being over and nothing would be said at all.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 900)));
        watch.saw("p1", Map.of());
        watch.saw("p1", Map.of("ns", reading(500, 450)));

        assertThat(alert.crossed).containsExactly("p1/ns", "p1/ns");
    }

    @Test
    void deletedPipelinesReleaseTheirWindowWithoutReportingRecovery() {
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);
        watch.saw("p1", Map.of("ns", reading(100, 100)));
        watch.saw("p2", Map.of("ns", reading(100, 100)));

        watch.forgetPipelinesOutside(Set.of("p2"));

        assertThat(watch.watching()).containsExactly("p2/ns");
        assertThat(alert.cleared).isEmpty();
    }

    @Test
    void eachPipelineAndNamespaceIsJudgedOnItsOwnCounts() {
        // One namespace under pressure must not carry another over the line with it, and the reading held
        // for one must not be differenced against another's -- which is what keying on the namespace alone
        // would do the moment two pipelines ran nests of the same shape.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 900), "other", reading(1_000, 10)));
        watch.saw("p2", Map.of("ns", reading(1_000, 10)));

        assertThat(alert.crossed).containsExactly("p1/ns");
        // Nothing is withdrawn either. Held under the namespace alone, the second pipeline's healthy reading
        // would be differenced against the first's and read as that namespace having just recovered -- an
        // all-clear for a namespace still on disk, which is worse than never having raised it.
        assertThat(alert.cleared).isEmpty();
    }

    @Test
    void whatIsReportedCarriesTheWindowItWasJudgedOnAndNotTheRunningTotals() {
        // Whoever reads this needs the numbers behind it, and they have to be the window's: the totals
        // would describe a run that may have been healthy for most of its life.
        NestColdLayerWatch watch = new NestColdLayerWatch(PRESSURE, alert);

        watch.saw("p1", Map.of("ns", reading(1_000, 10)));
        watch.saw("p1", Map.of("ns", reading(2_000, 960)));

        assertThat(alert.windows).singleElement().satisfies(window -> {
            assertThat(window.accesses()).isEqualTo(1_000);
            assertThat(window.backfills()).isEqualTo(950);
            assertThat(window.servedFromCold()).hasValue(0.95);
        });
    }

    /** A reading of a namespace holding 100 of its 10000 keys in memory, having served what is passed. */
    private static NestStateReading reading(long accesses, long backfills) {
        return new NestStateReading(100, accesses, backfills, backfills, OptionalLong.of(10_000));
    }

    /** Collects what was reported, named so a test asserts on the pipeline and namespace together. */
    private static final class RecordingAlert implements NestColdLayerAlert {

        private final List<String> crossed = new ArrayList<>();
        private final List<String> cleared = new ArrayList<>();
        private final List<NestStateWindow> windows = new ArrayList<>();

        @Override
        public void crossed(String pipelineId, String namespace, NestStateWindow window) {
            crossed.add(pipelineId + "/" + namespace);
            windows.add(window);
        }

        @Override
        public void cleared(String pipelineId, String namespace, NestStateWindow window) {
            cleared.add(pipelineId + "/" + namespace);
        }
    }
}
