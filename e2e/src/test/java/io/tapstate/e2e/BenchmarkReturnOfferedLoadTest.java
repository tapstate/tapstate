package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnOfferedLoadTest {
    private static final BenchmarkWorkloadDefinitions.Workload COPY = BenchmarkWorkloadDefinitions.steadyPilot("copy");
    private static final BenchmarkWorkloadDefinitions.Phase CDC = COPY.phase("cdc-update");
    private static final long MS = 1_000_000L;
    // The fixed cohort is ids 24001..72000, so source batches 240..719 at the declared offer.
    private static final long OFFER = CDC.batchInterval().toNanos();
    private static final long NOMINAL_SPAN = 479 * OFFER;

    @Test void a_held_offer_with_a_matching_return_span_passes() {
        var load = evaluate(OFFER, NOMINAL_SPAN - NOMINAL_SPAN / 200, NOMINAL_SPAN + NOMINAL_SPAN / 300);
        assertThat(load).containsEntry("state", "PASS").containsEntry("reason", "FIXED_OFFER_SUSTAINED")
                .containsEntry("firstCohortBatch", 240).containsEntry("lastCohortBatch", 719)
                .containsEntry("nominalIssueSpanNanos", NOMINAL_SPAN).containsEntry("actualIssueSpanNanos", NOMINAL_SPAN);
    }

    @Test void a_source_slower_than_its_declared_offer_is_not_a_fixed_load() {
        var load = evaluate(OFFER * 9 / 8, 479 * OFFER * 9 / 8, 479 * OFFER * 9 / 8);
        assertThat(load).containsEntry("state", "FAIL").containsEntry("reason", "SOURCE_DID_NOT_HOLD_THE_FIXED_OFFER")
                .containsEntry("sourcePaceHeld", false).containsEntry("completionsTrackedIssuance", true);
    }

    @Test void a_return_span_longer_than_issuance_means_a_growing_backlog() {
        var load = evaluate(OFFER, NOMINAL_SPAN * 106 / 100, NOMINAL_SPAN * 107 / 100);
        assertThat(load).containsEntry("state", "FAIL").containsEntry("reason", "COMPLETIONS_DID_NOT_TRACK_THE_FIXED_OFFER");
    }

    @Test void a_return_span_shorter_than_issuance_means_a_draining_backlog() {
        var load = evaluate(OFFER, NOMINAL_SPAN * 93 / 100, NOMINAL_SPAN * 94 / 100);
        assertThat(load).containsEntry("state", "FAIL").containsEntry("reason", "COMPLETIONS_DID_NOT_TRACK_THE_FIXED_OFFER");
    }

    @Test void an_enclosure_reaching_outside_the_band_is_not_tracked() {
        var load = evaluate(OFFER, NOMINAL_SPAN, NOMINAL_SPAN * 106 / 100);
        assertThat(load).containsEntry("completionsTrackedIssuance", false);
    }

    @Test void the_unpaced_correctness_profile_and_an_incomplete_ledger_are_refused() {
        var unpaced = BenchmarkWorkloadDefinitions.byId("copy");
        var phase = unpaced.phase("cdc-update");
        var ledger = ledger(phase.batches().size(), MS);
        assertThat(BenchmarkReturnOfferedLoad.evidence(unpaced, phase, ledger, 10 * MS, 10 * MS)).containsKey("state");
        assertThatThrownBy(() -> BenchmarkReturnOfferedLoad.evidence(COPY, CDC, ledger(959, OFFER), NOMINAL_SPAN, NOMINAL_SPAN))
                .hasMessageContaining("batch rosters differ");
        assertThatThrownBy(() -> BenchmarkReturnOfferedLoad.evidence(COPY, COPY.phase("warm-up"), ledger(1, MS), 1, 1))
                .hasMessageContaining("measured phase input");
    }

    private static java.util.Map<String, Object> evaluate(long step, long durationLower, long durationUpper) {
        return BenchmarkReturnOfferedLoad.evidence(COPY, CDC, ledger(CDC.batches().size(), step), durationLower, durationUpper);
    }
    private static List<BenchmarkForkEnvironment.BatchResult> ledger(int batches, long step) {
        var rows = new ArrayList<BenchmarkForkEnvironment.BatchResult>(batches);
        long origin = 937_000_000_000_000L;
        for (int index = 0; index < batches; index++) {
            rows.add(new BenchmarkForkEnvironment.BatchResult(index, origin + index * step, origin + index * step + 5 * MS));
        }
        return rows;
    }
}
