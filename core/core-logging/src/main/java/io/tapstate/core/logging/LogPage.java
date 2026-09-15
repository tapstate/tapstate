package io.tapstate.core.logging;

import java.util.List;

/**
 * One page from a node-local pipeline log ring. Lines are oldest to newest and {@code nextCursor}
 * is the last line accepted by the page. A caller resumes by presenting that cursor as {@code after}.
 *
 * @param lines      the retained lines in this page
 * @param nextCursor the position after the newest returned line, or {@code null} when no position exists
 * @param truncated  whether the requested cursor preceded the oldest still-retained line
 */
public record LogPage(List<LogLine> lines, LogCursor nextCursor, boolean truncated) {

    public LogPage {
        lines = lines == null ? List.of() : List.copyOf(lines);
    }
}
