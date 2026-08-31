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
            case TuiAction.SetCommand set -> new TuiAppState(
                    set.value(), state.notice(), state.paletteOpen(), state.paletteIndex(), state.palette(),
                    state.prompt(), state.activity());
            case TuiAction.ClearCommand ignored -> new TuiAppState(
                    "", state.notice(), state.paletteOpen(), state.paletteIndex(), state.palette(), state.prompt(),
                    state.activity());
            case TuiAction.SetNotice set -> new TuiAppState(
                    state.command(), set.value(), state.paletteOpen(), state.paletteIndex(), state.palette(),
                    state.prompt(), state.activity());
            case TuiAction.OpenPalette open -> {
                List<String> commands = open.commands() == null ? List.of() : List.copyOf(open.commands());
                yield new TuiAppState(state.command(), open.notice(), !commands.isEmpty(), 0, commands, state.prompt(),
                        state.activity());
            }
            case TuiAction.ClosePalette close -> new TuiAppState(
                    state.command(), close.notice(), false, 0, List.of(), state.prompt(), state.activity());
            case TuiAction.MovePalette move -> {
                if (!state.paletteOpen() || state.palette().isEmpty()) {
                    yield state;
                }
                int index = Math.max(0, Math.min(state.palette().size() - 1,
                        state.paletteIndex() + move.delta()));
                yield new TuiAppState(state.command(), state.notice(), true, index, state.palette(), state.prompt(),
                        state.activity());
            }
            case TuiAction.SelectPaletteCommand select -> new TuiAppState(
                    select.command(), select.notice(), false, 0, List.of(), state.prompt(), state.activity());
            case TuiAction.SetPrompt set -> new TuiAppState(
                    state.command(), state.notice(), state.paletteOpen(), state.paletteIndex(), state.palette(),
                    set.prompt(), state.activity());
            case TuiAction.ClearPrompt ignored -> new TuiAppState(
                    state.command(), state.notice(), state.paletteOpen(), state.paletteIndex(), state.palette(), null,
                    state.activity());
            case TuiAction.AppendActivity append -> {
                String value = TuiActivity.redact(append.value());
                if (value.isBlank()) {
                    yield state;
                }
                List<String> activity = new java.util.ArrayList<>(state.activity());
                activity.add(value);
                yield new TuiAppState(state.command(), state.notice(), state.paletteOpen(), state.paletteIndex(),
                        state.palette(), state.prompt(), activity);
            }
        };
    }
}
