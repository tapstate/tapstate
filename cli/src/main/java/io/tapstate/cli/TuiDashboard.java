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

    record Prompt(String question, String input, String hint, boolean secret, List<String> options,
                  int selectedIndex, List<String> lines) {
        Prompt {
            question = question == null ? "" : question;
            input = input == null ? "" : input;
            hint = hint == null ? "" : hint;
            options = options == null ? List.of() : List.copyOf(options);
            selectedIndex = options.isEmpty() ? 0 : Math.max(0, Math.min(selectedIndex, options.size() - 1));
            lines = lines == null ? List.of() : List.copyOf(lines);
        }

        static Prompt text(String question, String input, String hint, boolean secret) {
            return new Prompt(question, input, hint, secret, List.of(), 0, List.of());
        }

        static Prompt choice(String question, List<String> options, int selectedIndex) {
            return new Prompt(question, "", "↑/↓ choose · Enter select · Esc cancel", false,
                    options, selectedIndex, List.of());
        }

        static Prompt lines(String question, List<String> lines, String input) {
            return new Prompt(question, input, "end with a single '.'", false, List.of(), 0, lines);
        }
    }

    record State(Path workspace, String context, String principal, Connection connection, String notice,
                 String command, List<String> palette, int paletteIndex, Prompt prompt,
                 String endpoint, String clusterName, String authStatus, List<String> activity) {
        State {
            if (workspace == null || connection == null) {
                throw new IllegalArgumentException("workspace and connection are required");
            }
            command = command == null ? "" : command;
            palette = palette == null ? List.of() : List.copyOf(palette);
            activity = activity == null ? List.of() : List.copyOf(activity);
            paletteIndex = palette.isEmpty() ? 0 : Math.max(0, Math.min(paletteIndex, palette.size() - 1));
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command, List<String> palette, int paletteIndex, Prompt prompt) {
            this(workspace, context, principal, connection, notice, command, palette, paletteIndex, prompt,
                    null, null, null, List.of());
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command, List<String> palette, int paletteIndex, Prompt prompt,
              String endpoint, String clusterName, String authStatus) {
            this(workspace, context, principal, connection, notice, command, palette, paletteIndex, prompt,
                    endpoint, clusterName, authStatus, List.of());
        }

        State(Path workspace, String context, String principal, Connection connection, String notice) {
            this(workspace, context, principal, connection, notice, "", List.of(), 0, null);
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command) {
            this(workspace, context, principal, connection, notice, command, List.of(), 0, null);
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command, List<String> palette, int paletteIndex) {
            this(workspace, context, principal, connection, notice, command, palette, paletteIndex, null);
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
        rows.add(row("  Server       " + (state.endpoint() == null ? "not connected" : state.endpoint()), width));
        if (state.clusterName() != null && !state.clusterName().isBlank()) {
            rows.add(row("  Cluster      " + state.clusterName(), width));
        }
        rows.add(row("  Auth         " + authLabel(state), width));
        rows.add(row("  Pipelines     use start, stop, status, logs", width));
        rows.add(row("  Sources       use ls and discover-schema", width));
        rows.add(row("  Connectors    use connectors and test", width));
        int bodyRows = Math.max(0, height - 2 - rows.size());
        if (!state.palette().isEmpty() && bodyRows > 0) {
            int visible = Math.min(bodyRows, state.palette().size());
            int start = Math.min(Math.max(0, state.paletteIndex() - visible + 1),
                    state.palette().size() - visible);
            for (int index = start; index < start + visible; index++) {
                String marker = index == state.paletteIndex() ? "› " : "  ";
                rows.add(row("  " + marker + state.palette().get(index), width));
            }
        }
        if (state.prompt() != null && bodyRows > 0) {
            Prompt prompt = state.prompt();
            int remaining = Math.max(0, height - 2 - rows.size());
            if (!prompt.options().isEmpty()) {
                int visible = Math.min(remaining, prompt.options().size());
                int start = Math.min(Math.max(0, prompt.selectedIndex() - visible + 1),
                        prompt.options().size() - visible);
                for (int index = start; index < start + visible; index++) {
                    String marker = index == prompt.selectedIndex() ? "› " : "  ";
                    rows.add(row("  " + marker + prompt.options().get(index), width));
                }
            } else if (!prompt.lines().isEmpty()) {
                int visible = Math.min(remaining, prompt.lines().size());
                int start = prompt.lines().size() - visible;
                for (int index = start; index < prompt.lines().size(); index++) {
                    rows.add(row("  | " + prompt.lines().get(index), width));
                }
            }
        } else if (state.palette().isEmpty()) {
            rows.add(row("  Activity", width));
            if (state.activity().isEmpty()) {
                rows.add(row("  none yet", width));
            } else {
                int remaining = Math.max(0, height - 2 - rows.size());
                int visible = Math.min(remaining, state.activity().size());
                int start = state.activity().size() - visible;
                for (int index = start; index < state.activity().size(); index++) {
                    rows.add(row("  " + state.activity().get(index), width));
                }
            }
        }
        while (rows.size() < height - 2) {
            rows.add(row("", width));
        }
        String notice = state.notice() == null || state.notice().isBlank()
                ? "Tab complete  ·  Enter run  ·  Ctrl-P commands  ·  q quit"
                : state.notice();
        if (state.prompt() == null) {
            rows.add(row("[COMMAND] > " + state.command(), width));
            rows.add(row(notice, width));
        } else {
            Prompt prompt = state.prompt();
            String promptInput = prompt.secret() ? "•".repeat(prompt.input().codePointCount(0, prompt.input().length()))
                    : prompt.input();
            rows.add(row("[PROMPT] " + prompt.question(), width));
            rows.add(row("> " + promptInput + (prompt.hint().isBlank() ? "" : "  " + prompt.hint()), width));
        }
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

    private static String authLabel(State state) {
        if (state.authStatus() != null && !state.authStatus().isBlank()) {
            return state.authStatus();
        }
        if (state.connection() == Connection.CONNECTING) {
            return "resolving context";
        }
        if (state.principal() != null && !state.principal().isBlank()) {
            return "authenticated";
        }
        return state.connection() == Connection.OFFLINE ? "not connected" : "not authenticated";
    }

    private static AttributedString row(String text, int width) {
        String clipped = text.length() <= width ? text : text.substring(0, Math.max(0, width));
        return new AttributedString(clipped + " ".repeat(Math.max(0, width - clipped.length())));
    }
}
