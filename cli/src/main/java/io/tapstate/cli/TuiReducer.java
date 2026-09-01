package io.tapstate.cli;

import java.util.List;

/**
 * Pure reducer for presentation state. It has no terminal, transport, or command parsing dependency,
 * making the event-loop seam cheap to exercise in ordinary unit tests.
 */
final class TuiReducer {

    private TuiReducer() {
    }

    static TuiAppState reduce(TuiAppState state, TuiAction action) {
        if (state == null || action == null) {
            throw new IllegalArgumentException("state and action are required");
        }
        return switch (action) {
            case TuiAction.SetCommand set -> copy(state, set.value(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks());
            case TuiAction.ClearCommand ignored -> copy(state, "", state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks());
            case TuiAction.SetNotice set -> copy(state, state.command(), set.value(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks());
            case TuiAction.OpenPalette open -> {
                List<String> commands = open.commands() == null ? List.of() : List.copyOf(open.commands());
                yield copy(state, state.command(), open.notice(), !commands.isEmpty(), 0, commands, state.prompt(),
                        state.activity(), state.ticks());
            }
            case TuiAction.ClosePalette close -> copy(state, state.command(), close.notice(), false, 0, List.of(),
                    state.prompt(), state.activity(), state.ticks());
            case TuiAction.MovePalette move -> {
                if (!state.paletteOpen() || state.palette().isEmpty()) {
                    yield state;
                }
                int index = Math.max(0, Math.min(state.palette().size() - 1,
                        state.paletteIndex() + move.delta()));
                yield copy(state, state.command(), state.notice(), true, index, state.palette(), state.prompt(),
                        state.activity(), state.ticks());
            }
            case TuiAction.SelectPaletteCommand select -> copy(state, select.command(), select.notice(), false, 0,
                    List.of(), state.prompt(), state.activity(), state.ticks());
            case TuiAction.SetPrompt set -> copy(state, state.command(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), set.prompt(), state.activity(), state.ticks());
            case TuiAction.ClearPrompt ignored -> copy(state, state.command(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), null, state.activity(), state.ticks());
            case TuiAction.AppendActivity append -> {
                String value = TuiActivity.redact(append.value());
                if (value.isBlank()) {
                    yield state;
                }
                List<String> activity = new java.util.ArrayList<>(state.activity());
                activity.add(value);
                yield copy(state, state.command(), state.notice(), state.paletteOpen(), state.paletteIndex(),
                        state.palette(), state.prompt(), activity, state.ticks());
            }
            case TuiAction.SetResultPane set -> copy(state, state.command(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks(),
                    state.contextSession(), set.pane(), state.operation());
            case TuiAction.SetOperation set -> copy(state, state.command(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks(),
                    state.contextSession(), state.resultPane(), set.operation());
            case TuiAction.SetNavigation set -> copy(state, state.command(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks(),
                    state.contextSession(), state.resultPane(), state.operation(), set.navigation());
            case TuiAction.Tick ignored -> copy(state, state.command(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks() + 1);
            case TuiAction.ContextSession contextSession -> contextSession(state, contextSession.action());
            case TuiAction.RefreshStarted refresh -> startRefresh(state, refresh);
            case TuiAction.RefreshCancelled refresh -> cancelRefresh(state, refresh.requestId());
            case TuiAction.RefreshCompleted refresh -> completeRefresh(state, refresh.result());
        };
    }

    private static TuiAppState copy(TuiAppState state, String command, String notice, boolean paletteOpen,
                                    int paletteIndex, List<String> palette, TuiDashboard.Prompt prompt,
                                    List<String> activity, long ticks) {
        return copy(state, command, notice, paletteOpen, paletteIndex, palette, prompt, activity, ticks,
                state.contextSession(), state.resultPane(), state.operation());
    }

    private static TuiAppState copy(TuiAppState state, String command, String notice, boolean paletteOpen,
                                    int paletteIndex, List<String> palette, TuiDashboard.Prompt prompt,
                                    List<String> activity, long ticks, TuiContextSessionState contextSession) {
        return copy(state, command, notice, paletteOpen, paletteIndex, palette, prompt, activity, ticks,
                contextSession, state.resultPane(), state.operation());
    }

    private static TuiAppState copy(TuiAppState state, String command, String notice, boolean paletteOpen,
                                    int paletteIndex, List<String> palette, TuiDashboard.Prompt prompt,
                                    List<String> activity, long ticks, TuiContextSessionState contextSession,
                                    TuiCommandBar.ResultPane resultPane, TuiOperation operation) {
        return copy(state, command, notice, paletteOpen, paletteIndex, palette, prompt, activity, ticks,
                contextSession, resultPane, operation, state.navigation());
    }

    private static TuiAppState copy(TuiAppState state, String command, String notice, boolean paletteOpen,
                                    int paletteIndex, List<String> palette, TuiDashboard.Prompt prompt,
                                    List<String> activity, long ticks, TuiContextSessionState contextSession,
                                    TuiCommandBar.ResultPane resultPane, TuiOperation operation,
                                    TuiNavigation navigation) {
        return new TuiAppState(command, notice, paletteOpen, paletteIndex, palette, prompt, activity, ticks,
                contextSession, state.resources(), state.pipelines(), state.refreshInFlight(),
                state.refreshRequestId(), state.refreshContextGeneration(), state.lastRefreshAt(), resultPane, operation,
                navigation);
    }

    private static TuiAppState contextSession(TuiAppState state, TuiContextSessionAction action) {
        TuiContextSessionState next = TuiContextSessionReducer.reduce(state.contextSession(), action);
        if (next.generation() == state.contextSession().generation()) {
            return copy(state, state.command(), state.notice(), state.paletteOpen(), state.paletteIndex(),
                    state.palette(), state.prompt(), state.activity(), state.ticks(), next);
        }
        return new TuiAppState(state.command(), state.notice(), state.paletteOpen(), state.paletteIndex(),
                state.palette(), state.prompt(), state.activity(), state.ticks(), next, List.of(), List.of(),
                false, state.refreshRequestId(), next.generation(), null, state.resultPane(), state.operation(),
                TuiNavigation.initial(List.of()));
    }

    private static TuiAppState startRefresh(TuiAppState state, TuiAction.RefreshStarted refresh) {
        if (state.contextSession().generation() != refresh.contextGeneration()
                || refresh.requestId() <= state.refreshRequestId()) {
            return state;
        }
        return refreshed(state, state.resources(), state.pipelines(), true, refresh.requestId(),
                refresh.contextGeneration(), state.lastRefreshAt(), state.notice());
    }

    private static TuiAppState completeRefresh(TuiAppState state, TuiResourceRefreshResult result) {
        if (!state.refreshInFlight() || state.refreshRequestId() != result.requestId()
                || state.refreshContextGeneration() != result.contextGeneration()
                || state.contextSession().generation() != result.contextGeneration()) {
            return state;
        }
        return refreshed(state, result.resources(), result.pipelines(), false, result.requestId(),
                result.contextGeneration(), result.refreshedAt(), result.notice());
    }

    private static TuiAppState cancelRefresh(TuiAppState state, long requestId) {
        if (!state.refreshInFlight() || state.refreshRequestId() != requestId) {
            return state;
        }
        return refreshed(state, state.resources(), state.pipelines(), false, requestId,
                state.refreshContextGeneration(), state.lastRefreshAt(), state.notice());
    }

    private static TuiAppState refreshed(TuiAppState state, List<TuiDashboard.ResourceSummary> resources,
                                         List<TuiDashboard.PipelineSummary> pipelines, boolean inFlight,
                                         long requestId, long contextGeneration, String refreshedAt, String notice) {
        return new TuiAppState(state.command(), notice, state.paletteOpen(), state.paletteIndex(), state.palette(),
                state.prompt(), state.activity(), state.ticks(), state.contextSession(), resources, pipelines,
                inFlight, requestId, contextGeneration, refreshedAt, state.resultPane(), state.operation(),
                TuiNavigation.initial(resources.stream().map(TuiDashboard.ResourceSummary::id).toList()));
    }
}
