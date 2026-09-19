package io.tapstate.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Render-owned state for the selected pipeline's bounded live log view. */
record WorkbenchLogsState(String pipelineId, List<RemoteLogLine> lines, Optional<RemoteLogCursor> cursor,
        boolean loading, boolean following, boolean wrapped, boolean truncated, boolean newLines, int scrollOffset,
        Optional<String> error) {
    static final int MAX_LINES = 3_000;

    WorkbenchLogsState {
        Objects.requireNonNull(pipelineId, "pipelineId");
        lines = List.copyOf(lines);
        Objects.requireNonNull(cursor, "cursor");
        Objects.requireNonNull(error, "error");
    }
    static WorkbenchLogsState loading(String id) {
        return new WorkbenchLogsState(id, List.of(), Optional.empty(), true, true, true, false, false, 0, Optional.empty());
    }
    WorkbenchLogsState append(LogsOutcome.Found page) {
        ArrayList<RemoteLogLine> next = new ArrayList<>(lines);
        next.addAll(page.lines());
        if (next.size() > MAX_LINES) next.subList(0, next.size() - MAX_LINES).clear();
        boolean hasNew = !page.lines().isEmpty() && scrollOffset > 0;
        return new WorkbenchLogsState(pipelineId, next, Optional.ofNullable(page.nextCursor()), false, following,
                wrapped, truncated || page.truncated(), hasNew, scrollOffset, Optional.empty());
    }
    WorkbenchLogsState fail(String message) {
        return new WorkbenchLogsState(pipelineId, lines, cursor, false, false, wrapped, truncated, newLines, scrollOffset,
                Optional.of(message));
    }
    WorkbenchLogsState toggleFollow() { return new WorkbenchLogsState(pipelineId, lines, cursor, loading, !following, wrapped, truncated, newLines, scrollOffset, error); }
    WorkbenchLogsState toggleWrap() { return new WorkbenchLogsState(pipelineId, lines, cursor, loading, following, !wrapped, truncated, newLines, scrollOffset, error); }
    WorkbenchLogsState scroll(int delta) { return new WorkbenchLogsState(pipelineId, lines, cursor, loading, following, wrapped, truncated, false, Math.max(0, scrollOffset + delta), error); }
    WorkbenchLogsState toOldest() { return new WorkbenchLogsState(pipelineId, lines, cursor, loading, false, wrapped, truncated, false, lines.size(), error); }
    WorkbenchLogsState toNewest() { return new WorkbenchLogsState(pipelineId, lines, cursor, loading, true, wrapped, truncated, false, 0, error); }
}
