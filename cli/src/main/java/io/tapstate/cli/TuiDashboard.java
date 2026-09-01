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

    static final int COMPACT_WIDTH = 60;
    static final int SPLIT_WIDTH = 100;
    static final int MIN_HEIGHT = 8;

    enum Connection {
        ONBOARDING("no context"),
        SIGNED_OUT("signed out"),
        SESSION_EXPIRED("session expired"),
        ISSUER_MISMATCH("issuer mismatch"),
        ONLINE("online"),
        CONNECTING("connecting"),
        OFFLINE("offline");

        private final String label;

        Connection(String label) {
            this.label = label;
        }

        String label() {
            return label;
        }
    }

    /** A redacted, render-ready summary of one local workspace artifact. */
    record ResourceSummary(String kind, String id, String detail, String connector,
                           boolean readable, boolean misplaced) {
        ResourceSummary {
            kind = kind == null || kind.isBlank() ? "other" : kind;
            id = id == null || id.isBlank() ? "?" : id;
            detail = detail == null ? "" : detail;
            connector = connector == null || connector.isBlank() ? null : connector;
        }
    }

    /** A redacted, render-ready summary of one remote pipeline. */
    record PipelineSummary(String id, String state, String detail, String revision) {
        PipelineSummary {
            id = id == null || id.isBlank() ? "?" : id;
            state = state == null || state.isBlank() ? "UNKNOWN" : state;
            detail = detail == null ? "" : detail;
            revision = revision == null || revision.isBlank() ? null : revision;
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

        static Prompt confirmWrite(String command, String target, String issuer) {
            String safeCommand = command == null || command.isBlank() ? "write command" : command;
            String safeTarget = target == null || target.isBlank() ? "selected target" : target;
            String safeIssuer = issuer == null || issuer.isBlank() ? "unknown issuer" : issuer;
            return choice("Run " + safeCommand + " on " + safeTarget + " (issuer " + safeIssuer + ")?",
                    List.of("Cancel", "Run"), 0);
        }
    }

    record State(Path workspace, String context, String principal, Connection connection, String notice,
                 String command, List<String> palette, int paletteIndex, Prompt prompt,
                 String endpoint, String clusterName, String authStatus, List<String> activity,
                 List<ResourceSummary> resources, List<PipelineSummary> pipelines, String refreshedAt) {
        State {
            if (workspace == null || connection == null) {
                throw new IllegalArgumentException("workspace and connection are required");
            }
            command = command == null ? "" : command;
            palette = palette == null ? List.of() : List.copyOf(palette);
            activity = activity == null ? List.of() : List.copyOf(activity);
            resources = resources == null ? List.of() : List.copyOf(resources);
            pipelines = pipelines == null ? List.of() : List.copyOf(pipelines);
            refreshedAt = refreshedAt == null || refreshedAt.isBlank() ? null : refreshedAt;
            paletteIndex = palette.isEmpty() ? 0 : Math.max(0, Math.min(paletteIndex, palette.size() - 1));
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command, List<String> palette, int paletteIndex, Prompt prompt) {
            this(workspace, context, principal, connection, notice, command, palette, paletteIndex, prompt,
                    null, null, null, List.of(), List.of(), List.of(), null);
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command, List<String> palette, int paletteIndex, Prompt prompt,
              String endpoint, String clusterName, String authStatus) {
            this(workspace, context, principal, connection, notice, command, palette, paletteIndex, prompt,
                    endpoint, clusterName, authStatus, List.of(), List.of(), List.of(), null);
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command, List<String> palette, int paletteIndex, Prompt prompt,
              String endpoint, String clusterName, String authStatus, List<String> activity) {
            this(workspace, context, principal, connection, notice, command, palette, paletteIndex, prompt,
                    endpoint, clusterName, authStatus, activity, List.of(), List.of(), null);
        }

        State(Path workspace, String context, String principal, Connection connection, String notice,
              String command, List<String> palette, int paletteIndex, Prompt prompt,
              String endpoint, String clusterName, String authStatus, List<String> activity,
              List<ResourceSummary> resources) {
            this(workspace, context, principal, connection, notice, command, palette, paletteIndex, prompt,
                    endpoint, clusterName, authStatus, activity, resources, List.of(), null);
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

        static State onboarding(Path workspace) {
            return new State(workspace, null, null, Connection.ONBOARDING, null);
        }
    }

    List<AttributedString> render(State state, int width, int height) {
        if (width < COMPACT_WIDTH || height < MIN_HEIGHT) {
            return compact(state, width, height);
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
        int bodyRows = Math.max(0, height - 2 - rows.size());
        List<String> body = new ArrayList<>();
        if (!state.palette().isEmpty() && bodyRows > 0) {
            int visible = Math.min(bodyRows, state.palette().size());
            int start = Math.min(Math.max(0, state.paletteIndex() - visible + 1),
                    state.palette().size() - visible);
            for (int index = start; index < start + visible; index++) {
                String marker = index == state.paletteIndex() ? "› " : "  ";
                body.add("  " + marker + state.palette().get(index));
            }
        }
        if (state.prompt() != null && bodyRows > 0) {
            Prompt prompt = state.prompt();
            if (!prompt.options().isEmpty()) {
                int visible = Math.min(bodyRows, prompt.options().size());
                int start = Math.min(Math.max(0, prompt.selectedIndex() - visible + 1),
                        prompt.options().size() - visible);
                for (int index = start; index < start + visible; index++) {
                    String marker = index == prompt.selectedIndex() ? "› " : "  ";
                    body.add("  " + marker + prompt.options().get(index));
                }
            } else if (!prompt.lines().isEmpty()) {
                int visible = Math.min(bodyRows, prompt.lines().size());
                int start = prompt.lines().size() - visible;
                for (int index = start; index < prompt.lines().size(); index++) {
                    body.add("  | " + prompt.lines().get(index));
                }
            }
        } else if (state.connection() == Connection.ONBOARDING) {
            body.add("  No context is bound to this workspace.");
            body.add("  [Enter] Create or choose context");
            body.add("  [o] Stay offline");
        } else if (width >= SPLIT_WIDTH) {
            appendSplitBody(rows, state.resources(), state.pipelines(), state.refreshedAt(),
                    state.activity(), bodyRows, width);
            body = null;
        } else {
            body.addAll(resourceRows(state.resources()));
            if (!state.pipelines().isEmpty() || state.refreshedAt() != null) {
                body.addAll(pipelineRows(state.pipelines(), state.refreshedAt()));
            }
            body.addAll(activityRows(state.activity()));
        }
        if (body != null) {
            appendBody(rows, body, bodyRows, width);
        }
        while (rows.size() < height - 2) {
            rows.add(row("", width));
        }
        String notice = state.notice() == null || state.notice().isBlank()
                ? defaultNotice(state)
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

    private static List<String> resourceRows(List<ResourceSummary> resources) {
        if (resources.isEmpty()) {
            return List.of("  Resources", "    no local resources");
        }
        List<ResourceSummary> pipelines = resources.stream().filter(r -> r.kind().equals("pipeline")).toList();
        List<ResourceSummary> sources = resources.stream().filter(r -> r.kind().equals("source")).toList();
        List<ResourceSummary> other = resources.stream()
                .filter(r -> !r.kind().equals("pipeline") && !r.kind().equals("source"))
                .toList();
        List<String> rows = new ArrayList<>();
        rows.add("  Resources");
        addResourceGroup(rows, "Pipelines", pipelines);
        addResourceGroup(rows, "Sources", sources);
        java.util.Map<String, Integer> connectorCounts = new java.util.LinkedHashMap<>();
        for (ResourceSummary source : sources) {
            if (source.readable() && !source.misplaced() && source.connector() != null) {
                connectorCounts.merge(source.connector(), 1, Integer::sum);
            }
        }
        rows.add("    Connectors (" + connectorCounts.size() + ")");
        if (connectorCounts.isEmpty()) {
            rows.add("      none");
        } else {
            connectorCounts.forEach((connector, count) ->
                    rows.add("      " + connector + " · " + count + (count == 1 ? " source" : " sources")));
        }
        if (!other.isEmpty()) {
            addResourceGroup(rows, "Other resources", other);
        }
        return rows;
    }

    private static List<String> activityRows(List<String> activity) {
        List<String> rows = new ArrayList<>();
        rows.add("  Activity");
        if (activity == null || activity.isEmpty()) {
            rows.add("  none yet");
        } else {
            activity.stream().map(line -> "  " + line).forEach(rows::add);
        }
        return rows;
    }

    private static List<String> pipelineRows(List<PipelineSummary> pipelines, String refreshedAt) {
        List<String> rows = new ArrayList<>();
        String suffix = refreshedAt == null ? "" : " · refreshed " + refreshedAt;
        rows.add("  Pipelines (" + pipelines.size() + ")" + suffix);
        if (pipelines.isEmpty()) {
            rows.add(refreshedAt == null ? "    awaiting remote refresh" : "    no remote pipelines");
            return rows;
        }
        for (PipelineSummary pipeline : pipelines) {
            String revision = pipeline.revision() == null ? "" : "  rev " + pipeline.revision();
            String detail = pipeline.detail().isBlank() ? "" : "  " + pipeline.detail();
            rows.add("    " + pipeline.id() + "  " + pipeline.state() + revision + detail);
        }
        return rows;
    }

    private static void appendSplitBody(List<AttributedString> rows, List<ResourceSummary> resources,
                                        List<PipelineSummary> pipelines, String refreshedAt, List<String> activity,
                                        int capacity, int width) {
        if (capacity <= 0) {
            return;
        }
        List<String> left = resourceRows(resources);
        boolean hasRemoteSnapshot = !pipelines.isEmpty() || refreshedAt != null;
        List<String> right = hasRemoteSnapshot
                ? new ArrayList<>(pipelineRows(pipelines, refreshedAt))
                : new ArrayList<>();
        List<String> activityPane = activityRows(activity);
        if (hasRemoteSnapshot) {
            right.set(0, right.getFirst() + "  ·  Activity");
            if (activityPane.size() > 1) {
                right.addAll(activityPane.subList(1, activityPane.size()));
            }
        } else {
            right.addAll(activityPane);
        }
        int gap = 3;
        // Keep enough room for resource details (especially misplaced-artifact hints) while
        // leaving a useful activity pane on the right.
        int leftWidth = Math.max(32, Math.min(48, (width - gap) / 2));
        int rightWidth = Math.max(1, width - leftWidth - gap);
        int total = Math.max(left.size(), right.size());
        int visible = Math.min(total, capacity);
        boolean truncated = total > capacity;
        if (truncated) {
            visible = Math.max(1, capacity - 1);
        }
        for (int index = 0; index < visible; index++) {
            String leftValue = index < left.size() ? left.get(index) : "";
            String rightValue = index < right.size() ? right.get(index) : "";
            rows.add(row(fit(leftValue, leftWidth) + " ".repeat(gap) + fit(rightValue, rightWidth), width));
        }
        if (truncated) {
            rows.add(row("  +" + (total - visible) + " more", width));
        }
    }

    private static void addResourceGroup(List<String> rows, String label, List<ResourceSummary> resources) {
        rows.add("    " + label + " (" + resources.size() + ")");
        for (ResourceSummary resource : resources) {
            String detail = resource.detail();
            rows.add("      " + resource.id() + (detail.isBlank() ? "" : "  " + detail));
        }
    }

    private static void appendBody(List<AttributedString> rows, List<String> body, int capacity, int width) {
        if (capacity <= 0 || body.isEmpty()) {
            return;
        }
        if (body.size() <= capacity) {
            body.forEach(line -> rows.add(row(line, width)));
            return;
        }
        if (capacity == 1) {
            rows.add(row("  +" + body.size() + " more", width));
            return;
        }
        int shown = capacity - 1;
        for (int index = 0; index < shown; index++) {
            rows.add(row(body.get(index), width));
        }
        rows.add(row("  +" + (body.size() - shown) + " more", width));
    }

    private static List<AttributedString> compact(State state, int width, int height) {
        int safeWidth = Math.max(width, 1);
        // Put the connection state first on narrow terminals so it remains visible even when
        // the workspace path or principal has to be clipped.
        String identity = state.connection().label + " · " + identity(state);
        String notice = state.notice() == null || state.notice().isBlank()
                ? compactNotice(state)
                : state.notice();
        List<String> lines = List.of(
                "tapstate " + middleClip(identity, Math.max(1, safeWidth - "tapstate ".length())),
                "workspace: " + state.workspace().toAbsolutePath().normalize(),
                "status: " + authLabel(state),
                "[COMMAND] > " + state.command(),
                notice);
        int visible = Math.max(1, Math.min(height, lines.size()));
        return lines.subList(0, visible).stream()
                .map(line -> row(line, safeWidth))
                .toList();
    }

    private static String identity(State state) {
        String identity = state.context() == null ? "no environment" : state.context();
        if (state.principal() != null && !state.principal().isBlank()) {
            identity += " / " + state.principal();
        }
        return identity;
    }

    private static String defaultNotice(State state) {
        if (state.connection() == Connection.ONBOARDING) {
            return "Enter create or choose context · o stay offline";
        }
        return "Tab complete  ·  Enter run  ·  Ctrl-P commands  ·  Ctrl-D quit";
    }

    private static String compactNotice(State state) {
        if (state.connection() == Connection.ONBOARDING) {
            return "Enter create or choose context · o stay offline";
        }
        return "compact terminal · Ctrl-P commands · Ctrl-D quit";
    }

    private static String middleClip(String value, int width) {
        if (value == null || value.length() <= width) {
            return value == null ? "" : value;
        }
        if (width <= 1) {
            return value.substring(0, width);
        }
        int leading = (width - 1) / 2;
        int trailing = width - 1 - leading;
        return value.substring(0, leading) + "…" + value.substring(value.length() - trailing);
    }

    private static String fit(String value, int width) {
        if (value == null || value.isEmpty()) {
            return " ".repeat(Math.max(0, width));
        }
        String clipped = value.length() <= width ? value : value.substring(0, Math.max(0, width));
        return clipped + " ".repeat(Math.max(0, width - clipped.length()));
    }

    private static String marker(Connection connection) {
        return connection == Connection.ONLINE ? "●" : connection == Connection.CONNECTING ? "○" : "·";
    }

    private static String authLabel(State state) {
        if (state.authStatus() != null && !state.authStatus().isBlank()) {
            return state.authStatus();
        }
        return switch (state.connection()) {
            case ONBOARDING -> "no context bound";
            case SIGNED_OUT -> "sign in to run online commands";
            case SESSION_EXPIRED -> "sign in again to renew the session";
            case ISSUER_MISMATCH -> "select a matching context or server";
            case CONNECTING -> "resolving context";
            case OFFLINE -> "not connected";
            case ONLINE -> state.principal() != null && !state.principal().isBlank()
                    ? "authenticated" : "not authenticated";
        };
    }

    private static AttributedString row(String text, int width) {
        String clipped = text.length() <= width ? text : text.substring(0, Math.max(0, width));
        return new AttributedString(clipped + " ".repeat(Math.max(0, width - clipped.length())));
    }
}
