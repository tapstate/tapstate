package io.tapstate.cli;

/**
 * Immutable input delivered to the TUI kernel. Background work may post events, but only the UI
 * thread maps and reduces them into presentation state.
 */
sealed interface TuiEvent permits TuiEvent.Key, TuiEvent.Resize, TuiEvent.Tick,
        TuiEvent.InputClosed, TuiEvent.ActionPosted {

    record Key(int code) implements TuiEvent {
    }

    record Resize(int width, int height) implements TuiEvent {
    }

    record Tick() implements TuiEvent {
    }

    record InputClosed() implements TuiEvent {
    }

    record ActionPosted(TuiAction action) implements TuiEvent {
        public ActionPosted {
            if (action == null) {
                throw new IllegalArgumentException("action is required");
            }
        }
    }
}
