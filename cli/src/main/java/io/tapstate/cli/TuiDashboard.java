package io.tapstate.cli;

import org.jline.utils.AttributedString;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The first TUI screen: a deliberately small, pure dashboard renderer.
 *
 * <p>It knows nothing about terminals, input, or HTTP.  The runtime owns those concerns and hands the
 * produced rows to JLine {@code Display}; keeping this layer pure makes resize and fallback behaviour
 * inexpensive to test and prevents a screen widget from reaching around the control-client boundary.
 */
final class TuiDashboard {

    static final int MIN_WIDTH = 44;
    static final int MIN_HEIGHT = 8;

    enum Connection {
        ONLINE("online"),
        CONNECTING("connecting"),
        OFFLINE("offline");

        private final String label;

        Connection(String label) {
            this.label = label;
        }
    }

    record State(Path workspace, String context, String principal, Connection connection, String notice) {
        State {
            if (workspace == null || connection == null) {
                throw new IllegalArgumentException("workspace and connection are required");
            }
        }

        static State offline(Path workspace, String context) {
            return new State(workspace, context, null, Connection.OFFLINE, null);
        }
    }

    List<AttributedString> render(State state, int width, int height) {
        if (width < MIN_WIDTH || height < MIN_HEIGHT) {
            return compact(state, width);
        }
        List<AttributedString> rows = new ArrayList<>();
        String identity = state.context() == null ? "no environment" : state.context();
        if (state.principal() != null && !state.principal().isBlank()) {
            identity += " / " + state.principal();
        }
        rows.add(row("tapstate  " + identity + "  " + marker(state.connection()) + " "
                + state.connection().label, width));
        rows.add(row("workspace: " + state.workspace().toAbsolutePath().normalize(), width));
        rows.add(row("", width));
        rows.add(row("  Pipelines     use start, stop, status, logs", width));
        rows.add(row("  Sources       use ls and discover-schema", width));
        rows.add(row("  Connectors    use connectors and test", width));
        while (rows.size() < height - 2) {
            rows.add(row("", width));
        }
        String notice = state.notice() == null || state.notice().isBlank()
                ? "Tab complete  ·  Enter run  ·  Ctrl-P commands  ·  q quit"
                : state.notice();
        rows.add(row("[COMMAND] >", width));
        rows.add(row(notice, width));
        return List.copyOf(rows);
    }

    private static List<AttributedString> compact(State state, int width) {
        int safeWidth = Math.max(width, 1);
        return List.of(
                row("tapstate " + state.connection().label, safeWidth),
                row("workspace: " + state.workspace().toAbsolutePath().normalize(), safeWidth),
                row("terminal is too narrow for the dashboard", safeWidth));
    }

    private static String marker(Connection connection) {
        return connection == Connection.ONLINE ? "●" : connection == Connection.CONNECTING ? "○" : "·";
    }

    private static AttributedString row(String text, int width) {
        String clipped = text.length() <= width ? text : text.substring(0, Math.max(0, width));
        return new AttributedString(clipped + " ".repeat(Math.max(0, width - clipped.length())));
    }
}
