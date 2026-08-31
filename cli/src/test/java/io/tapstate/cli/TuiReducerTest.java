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
}
