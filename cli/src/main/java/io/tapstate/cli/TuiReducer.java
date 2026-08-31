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
            case TuiAction.Tick ignored -> copy(state, state.command(), state.notice(), state.paletteOpen(),
                    state.paletteIndex(), state.palette(), state.prompt(), state.activity(), state.ticks() + 1);
        };
    }

    private static TuiAppState copy(TuiAppState state, String command, String notice, boolean paletteOpen,
                                    int paletteIndex, List<String> palette, TuiDashboard.Prompt prompt,
                                    List<String> activity, long ticks) {
        return new TuiAppState(command, notice, paletteOpen, paletteIndex, palette, prompt, activity, ticks);
    }
}
