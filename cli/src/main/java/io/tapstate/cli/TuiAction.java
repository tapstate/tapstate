package io.tapstate.cli;

/** Pure state transitions accepted by the TUI reducer. */
sealed interface TuiAction
        permits TuiAction.SetCommand, TuiAction.ClearCommand, TuiAction.SetNotice,
        TuiAction.OpenPalette, TuiAction.ClosePalette, TuiAction.MovePalette,
        TuiAction.SelectPaletteCommand, TuiAction.SetPrompt, TuiAction.ClearPrompt {

    record SetCommand(String value) implements TuiAction {
    }

    record ClearCommand() implements TuiAction {
    }

    record SetNotice(String value) implements TuiAction {
    }

    record OpenPalette(java.util.List<String> commands, String notice) implements TuiAction {
    }

    record ClosePalette(String notice) implements TuiAction {
    }

    record MovePalette(int delta) implements TuiAction {
    }

    record SelectPaletteCommand(String command, String notice) implements TuiAction {
    }

    record SetPrompt(TuiDashboard.Prompt prompt) implements TuiAction {
    }

    record ClearPrompt() implements TuiAction {
    }
}
