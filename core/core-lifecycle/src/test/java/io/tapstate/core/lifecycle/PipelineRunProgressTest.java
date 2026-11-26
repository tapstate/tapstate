package io.tapstate.core.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PipelineRunProgressTest {
    private static final Instant START = Instant.parse("2026-11-26T06:00:00Z");

    @Test
    void sourceReadAndCdcNeverStandInForTheLastTargetConfirmation() {
        var ready = PipelineRunProgress.empty("run")
                .reach(PipelineRunStage.CDC_READY, START);
        var landing = load(START.plusSeconds(2), InitialLoad.Stage.LANDING);
        var inProgress = ready.observeInitialLoad(landing);
        assertThat(inProgress.initialLoad()).isEqualTo(new PipelineRunProgress.LoadProgress(1, 2));
        assertThat(inProgress.milestones()).extracting(PipelineRunProgress.Milestone::stage)
                .containsExactly(PipelineRunStage.CDC_READY);

        var landed = inProgress.observeInitialLoad(load(START.plusSeconds(3), InitialLoad.Stage.LANDED));
        assertThat(landed.milestones()).containsExactly(
                new PipelineRunProgress.Milestone(PipelineRunStage.CDC_READY, START),
                new PipelineRunProgress.Milestone(PipelineRunStage.INITIAL_LOAD_COMPLETED, START.plusSeconds(3)));
        assertThat(landed.observeInitialLoad(landing)).isSameAs(landed);
        assertThat(landed.observeInitialLoad(load(START.plusSeconds(4), InitialLoad.Stage.LANDED))).isSameAs(landed);
    }

    @Test
    void lateExitCannotClearTheNextOccurrenceOfAStartupActivity() {
        var stage = PipelineRunStage.WAITING_FOR_CAPTURE;
        var retry = PipelineRunProgress.empty("run").enter(stage, START).enter(stage, START.plusSeconds(1));
        assertThat(retry.leave(stage, START, true)).isSameAs(retry);

        var failure = retry.leave(stage, START.plusSeconds(1), true);
        assertThat(failure.active()).isEmpty();
        assertThat(failure.failedStage()).isEqualTo(stage);
    }

    private static InitialLoad load(Instant at, InitialLoad.Stage items) {
        return new InitialLoad("execution", at, "snapshot_and_cdc", Map.of(
                "orders", new InitialLoad.Table(InitialLoad.Stage.LANDED, 2L, 2L),
                "items", new InitialLoad.Table(items, 1L, 1L)));
    }
}
