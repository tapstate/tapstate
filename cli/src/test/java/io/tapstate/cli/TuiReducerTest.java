package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiReducerTest {

    @Test
    void clampsPaletteSelectionAndKeepsTheTransitionPure() {
        TuiAppState initial = TuiAppState.initial("ready");
        TuiAppState opened = TuiReducer.reduce(initial,
                new TuiAction.OpenPalette(List.of("ls", "pwd", "help"), "commands"));

        TuiAppState moved = TuiReducer.reduce(opened, new TuiAction.MovePalette(99));

        assertThat(initial.paletteOpen()).isFalse();
        assertThat(opened.paletteIndex()).isZero();
        assertThat(moved.paletteIndex()).isEqualTo(2);
        assertThat(moved.palette()).containsExactly("ls", "pwd", "help");
    }

    @Test
    void selectingACommandClosesThePaletteAndUpdatesTheCommandBar() {
        TuiAppState opened = TuiReducer.reduce(TuiAppState.initial("ready"),
                new TuiAction.OpenPalette(List.of("ls", "pwd"), "commands"));

        TuiAppState selected = TuiReducer.reduce(opened,
                new TuiAction.SelectPaletteCommand("pwd", "selected: pwd · Enter run"));

        assertThat(selected.paletteOpen()).isFalse();
        assertThat(selected.palette()).isEmpty();
        assertThat(selected.command()).isEqualTo("pwd");
        assertThat(selected.notice()).isEqualTo("selected: pwd · Enter run");
    }

    @Test
    void promptAndCommandUpdatesDoNotEraseOtherPresentationState() {
        TuiAppState opened = TuiReducer.reduce(TuiAppState.initial("ready"),
                new TuiAction.OpenPalette(List.of("ls"), "commands"));
        TuiAppState withCommand = TuiReducer.reduce(opened, new TuiAction.SetCommand("ls"));
        TuiDashboard.Prompt prompt = TuiDashboard.Prompt.text("Password", "", "Enter", true);
        TuiAppState withPrompt = TuiReducer.reduce(withCommand, new TuiAction.SetPrompt(prompt));

        assertThat(withPrompt.command()).isEqualTo("ls");
        assertThat(withPrompt.paletteOpen()).isTrue();
        assertThat(withPrompt.prompt()).isSameAs(prompt);
    }

    @Test
    void appendsRecentActivityAndKeepsOnlyTheNewestEntries() {
        TuiAppState state = TuiAppState.initial("ready");
        for (int index = 1; index <= TuiAppState.MAX_ACTIVITY + 2; index++) {
            state = TuiReducer.reduce(state, new TuiAction.AppendActivity("entry-" + index));
        }

        assertThat(state.activity()).hasSize(TuiAppState.MAX_ACTIVITY);
        assertThat(state.activity().getFirst()).isEqualTo("entry-" + 3);
        assertThat(state.activity().getLast()).isEqualTo("entry-" + (TuiAppState.MAX_ACTIVITY + 2));
    }

    @Test
    void activityRedactsCredentialShapedValues() {
        TuiAppState state = TuiReducer.reduce(TuiAppState.initial("ready"),
                new TuiAction.AppendActivity("token: tok_live password=hunter2"));

        assertThat(state.activity()).containsExactly("token: [redacted] password=[redacted]");
    }

    @Test
    void acceptsOnlyTheCurrentResourceRefreshCompletion() {
        TuiAppState state = TuiAppState.initial("ready");
        state = TuiReducer.reduce(state, new TuiAction.RefreshStarted(7L, 0L));

        TuiResourceRefreshResult stale = new TuiResourceRefreshResult(6L, 0L,
                List.of(new TuiDashboard.ResourceSummary("pipeline", "old", "RUNNING", null, true, false)),
                List.of(new TuiDashboard.PipelineSummary("old", "RUNNING", "old", null)),
                "2026-09-01T00:00:00Z", "stale");
        TuiAppState afterStale = TuiReducer.reduce(state, new TuiAction.RefreshCompleted(stale));

        assertThat(afterStale.resources()).isEmpty();
        assertThat(afterStale.refreshInFlight()).isTrue();

        TuiResourceRefreshResult current = new TuiResourceRefreshResult(7L, 0L,
                List.of(new TuiDashboard.ResourceSummary("pipeline", "new", "PAUSED", null, true, false)),
                List.of(new TuiDashboard.PipelineSummary("new", "PAUSED", "new", null)),
                "2026-09-01T00:00:01Z", "refreshed");
        TuiAppState afterCurrent = TuiReducer.reduce(afterStale, new TuiAction.RefreshCompleted(current));

        assertThat(afterCurrent.resources()).extracting(TuiDashboard.ResourceSummary::id)
                .containsExactly("new");
        assertThat(afterCurrent.pipelines()).extracting(TuiDashboard.PipelineSummary::state)
                .containsExactly("PAUSED");
        assertThat(afterCurrent.refreshInFlight()).isFalse();
        assertThat(afterCurrent.lastRefreshAt()).isEqualTo("2026-09-01T00:00:01Z");
    }

    @Test
    void dropsRefreshResultsFromAnOlderContext() {
        TuiAppState state = TuiAppState.initial("ready");
        state = TuiReducer.reduce(state, new TuiAction.ContextSession(
                new TuiContextSessionAction.Initialize(null)));
        state = TuiReducer.reduce(state, new TuiAction.RefreshStarted(1L,
                state.contextSession().generation()));

        TuiResourceRefreshResult result = new TuiResourceRefreshResult(1L,
                state.contextSession().generation() - 1L, List.of(), List.of(), null, "old context");

        assertThat(TuiReducer.reduce(state, new TuiAction.RefreshCompleted(result)))
                .isSameAs(state);
    }
}
