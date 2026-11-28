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

    @Test
    void fullLoadAndCdcOnlyRunsAwaitOnlyTheirOwnMilestones() {
        var snapshot = PipelineRunProgress.empty("snapshot").beginExecution(PipelineRunProgress.Mode.SNAPSHOT_ONLY)
                .reach(PipelineRunStage.ENGINE_RUNNING, START);
        assertThat(snapshot.awaitingMilestones()).isTrue();
        var landed = snapshot.observeInitialLoad(new PipelineRunProgress.LoadProgress(2, 2), START.plusSeconds(1));
        assertThat(landed.awaitingMilestones()).isFalse();
        assertThat(landed.milestones()).extracting(PipelineRunProgress.Milestone::stage)
                .containsExactly(PipelineRunStage.ENGINE_RUNNING, PipelineRunStage.INITIAL_LOAD_COMPLETED);

        var cdc = PipelineRunProgress.empty("cdc").beginExecution(PipelineRunProgress.Mode.CDC_ONLY)
                .reach(PipelineRunStage.ENGINE_RUNNING, START);
        assertThat(cdc.awaitingMilestones()).isTrue();
        var ready = cdc.reach(PipelineRunStage.CDC_READY, START.plusSeconds(1));
        assertThat(ready.awaitingMilestones()).isFalse();
        assertThat(ready.milestones()).extracting(PipelineRunProgress.Milestone::stage)
                .containsExactly(PipelineRunStage.ENGINE_RUNNING, PipelineRunStage.CDC_READY);
    }

    @Test
    void aConfirmedFailureStopsWaitingAndARecoveredExecutionCanResumeTheSameRun() {
        var active = PipelineRunProgress.empty("run").enter(PipelineRunStage.EXECUTION_PLAN_PREPARING, START);
        var failed = active.leave(PipelineRunStage.EXECUTION_PLAN_PREPARING, START, true).markRunFailed();
        assertThat(failed.awaitingMilestones()).isFalse();
        assertThat(failed.failedStage()).isEqualTo(PipelineRunStage.EXECUTION_PLAN_PREPARING);
        assertThat(failed.milestones()).isEmpty();
        assertThat(failed.enter(PipelineRunStage.WAITING_FOR_CAPTURE, START.plusSeconds(1))).isSameAs(failed);
        assertThat(failed.markRunFailed()).isSameAs(failed);

        var recovered = failed.beginExecution(PipelineRunProgress.Mode.SNAPSHOT_AND_CDC);
        assertThat(recovered.runId()).isEqualTo(failed.runId());
        assertThat(recovered.failedStage()).isNull();
        assertThat(recovered.runFailed()).isFalse();
        assertThat(recovered.awaitingMilestones()).isTrue();
    }

    private static InitialLoad load(Instant at, InitialLoad.Stage items) {
        return new InitialLoad("execution", at, "snapshot_and_cdc", Map.of(
                "orders", new InitialLoad.Table(InitialLoad.Stage.LANDED, 2L, 2L),
                "items", new InitialLoad.Table(items, 1L, 1L)));
    }
}
