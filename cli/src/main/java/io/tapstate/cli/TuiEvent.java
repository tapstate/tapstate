package io.tapstate.cli;

/**
 * Immutable input delivered to the TUI kernel. Background work may post events, but only the UI
 * thread maps and reduces them into presentation state.
 */
sealed interface TuiEvent permits TuiEvent.Key, TuiEvent.Resize, TuiEvent.Tick,
        TuiEvent.InputClosed, TuiEvent.ActionPosted, TuiEvent.ContextSessionPosted,
        TuiEvent.ResourceRefreshCompleted {

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

    record ContextSessionPosted(TuiContextSessionAction action) implements TuiEvent {
        public ContextSessionPosted {
            if (action == null) {
                throw new IllegalArgumentException("context session action is required");
            }
        }
    }

    record ResourceRefreshCompleted(TuiResourceRefreshResult result) implements TuiEvent {
        public ResourceRefreshCompleted {
            if (result == null) {
                throw new IllegalArgumentException("refresh result is required");
            }
        }
    }
}
