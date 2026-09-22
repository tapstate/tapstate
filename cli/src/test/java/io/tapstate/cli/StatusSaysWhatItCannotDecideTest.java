package io.tapstate.cli;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the checklist says when none of its five rules matched, which is the case it would be easiest to get
 * wrong and the one it exists for.
 *
 * <p>The wrong answer here is "healthy". Five rules coming up empty is a statement about five rules, not
 * about the pipeline: the product does not collect how far the source could be read to, cannot see a job
 * that died while the run was meant to be paused, and may have been handed an observation with no time on
 * it at all. Reporting that as well-being is the exact failure this whole line was scoped against -- a
 * product that defaults to healthy when its signals are insufficient.
 *
 * <p>So the no-match answer hands back every reading it went through and names, one by one, the questions
 * these faces cannot answer and what is missing to answer them. A reader can then go and look at the one
 * thing the product genuinely cannot see, instead of trusting a silence.
 *
 * <p>The face that could not be read at all is kept apart from the face with nothing in it, everywhere. A
 * metrics call that failed and a metrics answer with no errors in it are one word apart on screen and a
 * different situation apart in the world, and collapsing them is how a checklist comes to report "nothing
 * wrong" about something it never saw.
 */
@DisplayName("a checklist that matched nothing says what it could not decide")
class StatusSaysWhatItCannotDecideTest {

    private static final String ID = "orders_sync";
    private static final long FRESH = 2_000;

    private static MetricsFacts moving() {
        // No streak rather than a streak of nought: the cell is published only while passes are throwing,
        // so a healthy pipeline has no such key and this reads as not published.
        return new MetricsFacts(null, 128L, Map.of());
    }

    @Test
    void nothingMatchedIsNotReportedAsHealthy() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, moving(), 1L);

        assertThat(answer.conclusion()).contains("nothing on this checklist matched");
        assertThat(answer.conclusion()).doesNotContainIgnoringCase("healthy");
        assertThat(answer.conclusion()).doesNotContainIgnoringCase("all is well");
    }

    @Test
    void itHandsBackEveryReadingItWentThrough() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, moving(), 1L);

        // Each of the four faces the checklist consults appears, so a conclusion of "nothing matched" can be
        // checked against what was actually read rather than taken on trust.
        assertThat(answer.readings()).contains(
                "status.observedAt = 2s ago",
                "status.failure = none",
                "metrics.reconcileFailuresInARow = not published",
                "metrics.recordCount = 128",
                "metrics.frontierStalledMillis = none at or above 1m0s",
                "snapshot = 1 row(s) loaded");
    }

    @Test
    void itAlwaysNamesTheSourcePositionItDoesNotCollect() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, moving(), 1L);

        assertThat(answer.cannotSay())
                .anyMatch(line -> line.contains("whether the source has changes waiting")
                        && line.contains("how far the source could be read to is not collected"));
    }

    @Test
    void anObservationWithNoTimeOnItIsNamedRatherThanDatedFromHere() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, null, moving(), 1L);

        assertThat(answer.readings()).contains("status.observedAt = not known");
        assertThat(answer.cannotSay()).anyMatch(line -> line.contains("how old any of this is"));
    }

    @Test
    void aPausedRunIsToldThatADeadJobWouldNotShowHere() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "PAUSED", null, null, FRESH, moving(), 0L);

        assertThat(answer.cannotSay()).anyMatch(line -> line.contains("paused run's job is still alive"));
    }

    @Test
    void aRunningRunIsNotToldAboutThePausedGap() {
        // The mirror of the case above. Without it, a version that printed every limitation on every status
        // would pass that one, and the list would stop being about this pipeline.
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, moving(), 0L);

        assertThat(answer.cannotSay()).noneMatch(line -> line.contains("paused run's job is still alive"));
    }

    @Test
    void aFaceThatCouldNotBeReadIsSaidToBeUnreadNotEmpty() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, null, null);

        assertThat(answer.readings()).contains("metrics = could not be read", "snapshot = could not be read");
        assertThat(answer.cannotSay())
                .anyMatch(line -> line.contains("anything the metrics face answers"))
                .anyMatch(line -> line.contains("how much of the initial load is done"));
        // And it must not have concluded anything about movement from a face it never read: "no records
        // driven" over an unanswered metrics call is a finding invented out of a failed request.
        assertThat(answer.conclusion()).doesNotContain("nothing has moved");
    }

    @Test
    void aSnapshotFaceThatAnsweredStillSaysWhatItCannotMeasure() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, moving(), 1L);

        // It reports rows loaded and no total to measure them against, so a load still running and one
        // that finished hours ago read the same here. Left unsaid, a number beside the word snapshot is
        // read as progress in flight, which is the reading this whole answer exists to stop.
        assertThat(answer.cannotSay())
                .anyMatch(line -> line.contains("whether an initial load is still running")
                        && line.contains("no total to measure them against"));
    }

    @Test
    void aNoMatchAnswerNeverComesBackWithAnEmptyCannotSayList() {
        StatusDiagnosis.Answer answer = StatusDiagnosis.of(ID, "RUNNING", null, null, FRESH, moving(), 1L);

        assertThat(answer.cannotSay()).isNotEmpty();
    }
}
