package io.tapstate.cli;

/** Pure state transitions accepted by the TUI reducer. */
sealed interface TuiAction
        permits TuiAction.SetCommand, TuiAction.ClearCommand, TuiAction.SetNotice,
        TuiAction.OpenPalette, TuiAction.ClosePalette, TuiAction.MovePalette,
        TuiAction.SelectPaletteCommand, TuiAction.SetPrompt, TuiAction.ClearPrompt,
        TuiAction.AppendActivity, TuiAction.Tick, TuiAction.ContextSession,
        TuiAction.RefreshStarted, TuiAction.RefreshCompleted {

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

    record AppendActivity(String value) implements TuiAction {
    }

    record Tick() implements TuiAction {
    }

    record ContextSession(TuiContextSessionAction action) implements TuiAction {
        public ContextSession {
            if (action == null) {
                throw new IllegalArgumentException("context session action is required");
            }
        }
    }

    record RefreshStarted(long requestId, long contextGeneration) implements TuiAction {
        public RefreshStarted {
            if (requestId <= 0 || contextGeneration < 0) {
                throw new IllegalArgumentException("refresh request id and context generation are required");
            }
        }
    }

    record RefreshCompleted(TuiResourceRefreshResult result) implements TuiAction {
        public RefreshCompleted {
            if (result == null) {
                throw new IllegalArgumentException("refresh result is required");
            }
        }
    }
}
