package io.tapstate.cli;

import java.util.List;

/**
 * Presentation state for the full-screen CLI. Connection and workspace facts stay owned by the
 * Repl; this record contains only state that can be reduced without I/O.
 */
record TuiAppState(String command, String notice, boolean paletteOpen, int paletteIndex,
                   List<String> palette, TuiDashboard.Prompt prompt, List<String> activity, long ticks,
                   TuiContextSessionState contextSession, List<TuiDashboard.ResourceSummary> resources,
                   List<TuiDashboard.PipelineSummary> pipelines, boolean refreshInFlight, long refreshRequestId,
                   long refreshContextGeneration, String lastRefreshAt,
                   TuiCommandBar.ResultPane resultPane, TuiOperation operation, TuiNavigation navigation) {

    static final int MAX_ACTIVITY = 200;

    TuiAppState(String command, String notice, boolean paletteOpen, int paletteIndex,
                List<String> palette, TuiDashboard.Prompt prompt) {
        this(command, notice, paletteOpen, paletteIndex, palette, prompt, List.of(), 0L,
                TuiContextSessionState.initial(), List.of(), List.of(), false, 0L, 0L, null, null, null,
                TuiNavigation.initial(List.of()));
    }

    TuiAppState(String command, String notice, boolean paletteOpen, int paletteIndex,
                List<String> palette, TuiDashboard.Prompt prompt, List<String> activity) {
        this(command, notice, paletteOpen, paletteIndex, palette, prompt, activity, 0L,
                TuiContextSessionState.initial(), List.of(), List.of(), false, 0L, 0L, null, null, null,
                TuiNavigation.initial(List.of()));
    }

    TuiAppState {
        command = TuiActivity.command(command);
        notice = TuiActivity.result(notice);
        palette = palette == null ? List.of() : List.copyOf(palette);
        activity = activity == null ? List.of() : activity.stream().map(TuiActivity::result).toList();
        contextSession = contextSession == null ? TuiContextSessionState.initial() : contextSession;
        resources = resources == null ? List.of() : List.copyOf(resources);
        pipelines = pipelines == null ? List.of() : List.copyOf(pipelines);
        lastRefreshAt = lastRefreshAt == null || lastRefreshAt.isBlank() ? null : lastRefreshAt;
        resultPane = resultPane == null ? null : TuiCommandBar.project(
                new CommandResult(resultPane.keepRunning(), resultPane.exitCode()),
                String.join("\n", resultPane.lines()));
        navigation = navigation == null ? TuiNavigation.initial(List.of()) : navigation;
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
        return new TuiAppState("", notice, false, 0, List.of(), null, List.of(), 0L,
                TuiContextSessionState.initial(), List.of(), List.of(), false, 0L, 0L, null, null, null,
                TuiNavigation.initial(List.of()));
    }
}
