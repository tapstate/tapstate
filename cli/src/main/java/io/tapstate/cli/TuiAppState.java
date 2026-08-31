package io.tapstate.cli;

import java.util.List;

/**
 * Presentation state for the full-screen CLI. Connection and workspace facts stay owned by the
 * Repl; this record contains only state that can be reduced without I/O.
 */
record TuiAppState(String command, String notice, boolean paletteOpen, int paletteIndex,
                   List<String> palette, TuiDashboard.Prompt prompt, List<String> activity, long ticks) {

    static final int MAX_ACTIVITY = 8;

    TuiAppState(String command, String notice, boolean paletteOpen, int paletteIndex,
                List<String> palette, TuiDashboard.Prompt prompt) {
        this(command, notice, paletteOpen, paletteIndex, palette, prompt, List.of(), 0L);
    }

    TuiAppState(String command, String notice, boolean paletteOpen, int paletteIndex,
                List<String> palette, TuiDashboard.Prompt prompt, List<String> activity) {
        this(command, notice, paletteOpen, paletteIndex, palette, prompt, activity, 0L);
    }

    TuiAppState {
        command = command == null ? "" : command;
        notice = notice == null ? "" : notice;
        palette = palette == null ? List.of() : List.copyOf(palette);
        activity = activity == null ? List.of() : List.copyOf(activity);
        if (activity.size() > MAX_ACTIVITY) {
            activity = activity.subList(activity.size() - MAX_ACTIVITY, activity.size());
        }
        paletteIndex = palette.isEmpty() ? 0 : Math.max(0, Math.min(paletteIndex, palette.size() - 1));
        if (!paletteOpen) {
            palette = List.of();
            paletteIndex = 0;
        }
    }

    static TuiAppState initial(String notice) {
        return new TuiAppState("", notice, false, 0, List.of(), null, List.of(), 0L);
    }
}
