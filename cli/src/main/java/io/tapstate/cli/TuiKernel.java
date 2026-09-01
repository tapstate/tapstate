package io.tapstate.cli;

import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Owns the TUI state transition boundary. The terminal loop binds this kernel once; workers can
 * only post immutable events and cannot obtain a terminal writer.
 */
final class TuiKernel {

    private final Queue<TuiEvent> mailbox = new ConcurrentLinkedQueue<>();
    private TuiAppState state;
    private TuiViewport viewport = TuiViewport.DEFAULT;
    private boolean exitRequested;
    private Thread uiThread;

    TuiKernel(TuiAppState initialState) {
        this.state = Objects.requireNonNull(initialState, "initialState");
    }

    TuiAppState state() {
        return state;
    }

    TuiViewport viewport() {
        return viewport;
    }

    boolean exitRequested() {
        return exitRequested;
    }

    void post(TuiEvent event) {
        mailbox.add(Objects.requireNonNull(event, "event"));
    }

    boolean drain() {
        assertUiThread();
        boolean changed = false;
        TuiEvent event;
        while ((event = mailbox.poll()) != null) {
            dispatchBound(event);
            changed = true;
        }
        return changed;
    }

    void dispatch(TuiEvent event) {
        assertUiThread();
        dispatchBound(Objects.requireNonNull(event, "event"));
    }

    private void dispatchBound(TuiEvent event) {
        switch (event) {
            case TuiEvent.Resize resize -> viewport = new TuiViewport(resize.width(), resize.height());
            case TuiEvent.InputClosed ignored -> exitRequested = true;
            default -> {
                for (TuiAction action : TuiEventMapper.actions(state, event)) {
                    state = TuiReducer.reduce(state, action);
                }
            }
        }
    }

    private void assertUiThread() {
        Thread current = Thread.currentThread();
        if (uiThread == null) {
            uiThread = current;
            return;
        }
        if (uiThread != current) {
            throw new IllegalStateException("TUI state may only be reduced on the UI thread");
        }
    }

    private static final class TuiEventMapper {
        private TuiEventMapper() {
        }

        static List<TuiAction> actions(TuiAppState state, TuiEvent event) {
            return switch (event) {
                case TuiEvent.Key key -> List.of(new TuiAction.SetCommand(
                        TuiCommandBar.accept(state.command(), key.code()).value()));
                case TuiEvent.Tick ignored -> List.of(new TuiAction.Tick());
                case TuiEvent.ActionPosted posted -> List.of(posted.action());
                case TuiEvent.ContextSessionPosted posted -> List.of(new TuiAction.ContextSession(posted.action()));
                case TuiEvent.ResourceRefreshCompleted completed -> List.of(
                        new TuiAction.RefreshCompleted(completed.result()));
                case TuiEvent.Resize ignored -> List.of();
                case TuiEvent.InputClosed ignored -> List.of();
            };
        }
    }
}
