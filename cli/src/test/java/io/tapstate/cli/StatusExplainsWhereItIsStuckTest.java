package io.tapstate.cli;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The five situations the status checklist can tell apart, one case each, plus the rule that decides what
 * happens when more than one of them is true at once.
 *
 * <p>Five separate cases rather than one parameterised sweep, because what is being pinned is that they are
 * <em>distinguishable</em>: each situation has to produce an answer the other four do not, or the checklist
 * has added a paragraph to the output without adding an answer to the question. Each case therefore asserts
 * on the sentence that belongs to its own rule and nothing more general.
 *
 * <p>The sixth case is the one that stops the list from quietly becoming a report. Every rule feeds on
 * readings that are frequently true together -- a run whose publisher died also has an old record count, no
 * table loading, and a chain that has not advanced -- so without an order, five rules would print five
 * paragraphs and leave the reader to pick. First match wins, and the case that pins it makes all five true
 * at once.
 */
@DisplayName("the status checklist tells five situations apart")
class StatusExplainsWhereItIsStuckTest {

    private static final String ID = "orders_sync";

    /** A reading taken a moment ago: old enough to be real, far short of the silence that means trouble. */
    private static final long FRESH = 2_000;

    /** Four minutes and twelve seconds, well past the silence a live publisher can produce. */
    private static final long LONG_SILENCE = 252_000;

    /** A run that is driving records, erroring at nothing, and holding no chain back. */
    private static MetricsFacts moving() {
        return new MetricsFacts(0L, 128L, Map.of());
    }

    @Test
    void anOldReadingSaysThePublisherMayHaveStopped() {
        StatusDiagnosis.Answer answer =
                StatusDiagnosis.of(ID, "RUNNING", null, null, LONG_SILENCE, moving(), 0L);

        assertThat(answer.conclusion()).contains("4m12s").contains("publisher may have stopped");
        assertThat(answer.readings()).anyMatch(reading -> reading.startsWith("status.observedAt"));
        assertThat(answer.next()).contains("converging");
    }

    @Test
    void aCodedFailureIsReportedAsTheReasonTheRunDied() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(
                ID, "FAILED", "engine.job-failed", "the job stopped", FRESH, moving(), 0L);

        assertThat(answer.conclusion()).contains("engine.job-failed");
        assertThat(answer.readings()).contains("status.failure = engine.job-failed");
        assertThat(answer.next()).isEqualTo("tapstate logs " + ID);
    }

    @Test
    void errorsCountedAgainstARunningStateSayTheServerKeepsFailingToBringItUp() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(
                ID, "RUNNING", null, null, FRESH, new MetricsFacts(3L, 128L, Map.of()), 0L);

        // The count rises on any throw from a convergence pass -- a plan that cannot be built throws the
        // same way an unreachable store does -- so the sentence names the symptom and sends the reader to
        // the one place the cause is actually written down.
        assertThat(answer.conclusion()).contains("3 passes in a row have thrown");
        assertThat(answer.next()).contains("server's own log");
        assertThat(answer.readings())
                .contains("metrics.reconcileFailuresInARow = 3", "status.state = running");
        // And it says what it is not claiming: the state is left alone on purpose, because nothing here has
        // seen the job die and reporting it as failed would be this product guessing.
        assertThat(answer.cannotSay()).anyMatch(line -> line.contains("still alive"));
    }

    @Test
    void noRecordsAndNoRowsLoadedSayNothingHasMoved() {
        StatusDiagnosis.Answer answer =
                StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, new MetricsFacts(0L, 0L, Map.of()), 0L);

        assertThat(answer.conclusion()).contains("nothing has moved");
        assertThat(answer.readings()).contains("metrics.recordCount = 0", "snapshot = no rows loaded");
        // The honest half: this reading cannot separate a stuck read from a source with nothing new, and
        // says so rather than letting the reader assume it did.
        assertThat(answer.cannotSay()).anyMatch(line -> line.contains("how far the source could be read to"));
    }

    @Test
    void aRunWithNoLiveJobIsNotToldThatNothingHasMoved() {
        // Both readings this rule matches on are also what a pipeline with no live job looks like: nothing
        // asks a job that is not there for a record count, and the snapshot face is dropped when a run
        // stops. So the rule is gated on the state, exactly as the error-count rule above it is -- without
        // that, the two commonest healthy shapes there are, a run somebody stopped on purpose and a
        // bounded run that finished after moving every row, are both told nothing has moved and sent to
        // read the logs.
        for (String atRest : List.of("STOPPED", "COMPLETED", "PAUSED")) {
            StatusDiagnosis.Answer answer =
                    StatusDiagnosis.of(ID, atRest, null, null, FRESH, new MetricsFacts(0L, null, Map.of()), 0L);

            assertThat(answer.conclusion())
                    .as("a pipeline in %s has no live job to have moved anything", atRest)
                    .doesNotContain("nothing has moved");
            assertThat(answer.conclusion()).contains("nothing on this checklist matched");
            assertThat(answer.next())
                    .as("and it is not sent to the logs to look for a failure nothing has reported")
                    .isNull();
        }
    }

    @Test
    void aRunningSnapshotThatHasLoadedNothingStillMatches() {
        // The case the rule exists for, and the one a count of selected tables could never reach: the
        // snapshot face holds an entry for every selected table from the moment the run starts and keeps
        // it for the life of the run, so its size is never zero on a snapshot pipeline. What is zero when
        // nothing has moved is how much it has loaded.
        StatusDiagnosis.Answer answer =
                StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, new MetricsFacts(0L, null, Map.of()), 0L);

        assertThat(answer.conclusion()).contains("nothing has moved");
        assertThat(answer.readings()).contains("metrics.recordCount = not published", "snapshot = no rows loaded");
    }

    @Test
    void aSnapshotThatHasLoadedRowsIsNotToldNothingHasMoved() {
        StatusDiagnosis.Answer answer =
                StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, new MetricsFacts(0L, 0L, Map.of()), 4_096L);

        assertThat(answer.conclusion()).doesNotContain("nothing has moved");
        assertThat(answer.readings()).contains("snapshot = 4096 row(s) loaded");
    }

    @Test
    void aChainThatHasNotAdvancedIsNamedWithHowLongItHasStood() {
        MetricsFacts metrics = MetricsFacts.of(Map.of(
                "recordCount", 128L,
                "frontierStalledMillis.orders", 96_000L));

        StatusDiagnosis.Answer answer = StatusDiagnosis.of(
                ID, "RUNNING", null, null, FRESH, metrics, 0L);

        assertThat(answer.conclusion()).contains("stopped advancing").contains("orders");
        assertThat(answer.readings()).contains("metrics.frontierStalledMillis.orders = 1m36s");
    }

    @Test
    void millisecondScalePausesAreNotCalledStoppedChains() {
        MetricsFacts metrics = MetricsFacts.of(Map.of(
                "reconcileFailuresInARow", 0L,
                "recordCount", 11L,
                "frontierStalledMillis.shipments", 9L,
                "frontierStalledMillis.orders", 196L));

        StatusDiagnosis.Answer answer =
                StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, metrics, 11L);

        assertThat(answer.conclusion()).contains("nothing on this checklist matched");
    }

    @Test
    void whenEveryRuleIsTrueAtOnceTheFirstOneAnswers() {
        // All five readings at once, which is the ordinary shape of a run whose publisher died: the record
        // count stopped, no table is loading, a chain is standing still, errors were counted, and the last
        // thing published was a failure. Without an order this prints five paragraphs and answers nothing.
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", "engine.job-failed",
                "the job stopped", LONG_SILENCE, new MetricsFacts(7L, 0L, Map.of("orders", 96_000L)), 0L);

        assertThat(answer.conclusion()).contains("publisher may have stopped");
        assertThat(answer.conclusion()).doesNotContain("engine.job-failed");
        assertThat(answer.readings()).hasSize(1);
    }
}
