package io.tapstate.cli;

import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Margin;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.style.Overflow;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.Text;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

import java.util.ArrayList;
import java.util.List;

/** TamboUI renderer with a quiet work area and one focused command composer. */
final class TamboDashboard {

    void render(Frame frame, TuiDashboard.State state) {
        render(frame, state, 0);
    }

    void render(Frame frame, TuiDashboard.State state, int workspaceScroll) {
        Rect screen = frame.area().inner(Margin.uniform(1));
        List<Rect> vertical = Layout.vertical().constraints(
                Constraint.length(2), Constraint.fill(), Constraint.length(4)).split(screen);
        renderStatus(frame, vertical.get(0), state);
        renderWorkspace(frame, vertical.get(1), state, workspaceScroll);
        renderCommandBar(frame, vertical.get(2), state);
    }

    private void renderStatus(Frame frame, Rect area, TuiDashboard.State state) {
        String context = state.context() == null ? "no context" : state.context();
        String principal = state.principal() == null || state.principal().isBlank() ? "" : " · " + state.principal();
        String path = state.workspace().getFileName() == null ? state.workspace().toString() : state.workspace().getFileName().toString();
        frame.renderWidget(Paragraph.builder()
                .text(Text.from("TAPSTATE\n" + context + principal + "  ·  " + state.connection().label() + "  ·  " + path))
                .foreground(connectionColor(state.connection())).build(), area);
    }

    private void renderWorkspace(Frame frame, Rect area, TuiDashboard.State state, int workspaceScroll) {
        List<String> lines = state.prompt() != null ? promptLines(state.prompt())
                : !state.palette().isEmpty() ? paletteLines(state, area.height()) : contentLines(state);
        if (state.prompt() == null && state.palette().isEmpty() && state.resultPane() != null
                && workspaceScroll > 0 && !lines.isEmpty()) {
            int start = Math.min(workspaceScroll, lines.size() - 1);
            lines = lines.subList(start, lines.size());
        }
        Paragraph.Builder paragraph = Paragraph.builder()
                .text(Text.from(String.join("\n", lines)))
                .overflow(Overflow.WRAP_WORD);
        frame.renderWidget(paragraph.build(), area);
    }

    private void renderCommandBar(Frame frame, Rect area, TuiDashboard.State state) {
        String line = state.prompt() == null ? state.command() + "▌"
                : state.prompt().question() + "  " + state.prompt().input();
        Block block = Block.builder().borders(Borders.ALL).borderColor(Color.DARK_GRAY).build();
        frame.renderWidget(block, area);
        frame.renderWidget(Paragraph.builder().text(Text.from(line + "\n" + commandHint(state)))
                .overflow(Overflow.WRAP_WORD)
                .foreground(Color.BRIGHT_WHITE).build(), block.inner(area));
    }

    private static List<String> contentLines(TuiDashboard.State state) {
        if (state.resultPane() != null && !state.resultPane().lines().isEmpty()) {
            List<String> lines = new ArrayList<>(List.of("Last command", ""));
            lines.addAll(state.resultPane().lines());
            return lines;
        }
        if (state.connection() == TuiDashboard.Connection.ONBOARDING || state.connection() == TuiDashboard.Connection.OFFLINE) {
            return List.of("Ready to work locally.", "", "Try: validate ./work",
                    "     new --kind source --connector mysql --dry-run", "     explain source", "",
                    "Connect to a server: connect host:port", "Type to see suggestions · Ctrl-P all commands.");
        }
        List<String> lines = new ArrayList<>();
        lines.add(state.resources().isEmpty() ? "No resources yet." : "Resources");
        for (TuiDashboard.ResourceSummary resource : state.resources()) {
            lines.add(resource.kind() + "  " + resource.id());
        }
        if (!state.pipelines().isEmpty()) {
            lines.add(""); lines.add("Pipelines");
            for (TuiDashboard.PipelineSummary pipeline : state.pipelines()) lines.add(pipeline.state() + "  " + pipeline.id());
        }
        return lines;
    }

    private static List<String> paletteLines(TuiDashboard.State state, int height) {
        boolean suggestions = state.notice() != null && state.notice().startsWith("suggestions");
        List<String> lines = new ArrayList<>(List.of(suggestions ? "Suggestions" : "Commands", ""));
        int visible = Math.max(1, height - 2);
        int start = Math.min(Math.max(0, state.paletteIndex() - visible + 1),
                Math.max(0, state.palette().size() - visible));
        int end = Math.min(state.palette().size(), start + visible);
        for (int index = start; index < end; index++) {
            String command = state.palette().get(index);
            String summary = commandSummary(command);
            String suffix = summary.isBlank() ? "" : "  " + summary;
            lines.add((index == state.paletteIndex() ? "› " : "  ") + command + suffix);
        }
        return lines;
    }

    private static String commandSummary(String command) {
        String verb = command == null ? "" : command.split("\\s", 2)[0];
        if (verb.equals("ls")) {
            return "List workspace resources";
        }
        Cli.VerbHelp help = Cli.VERB_HELP.get(verb);
        if (help == null) {
            help = Cli.BUILTIN_HELP.get(verb);
        }
        return help == null ? "" : help.summary();
    }

    private static List<String> promptLines(TuiDashboard.Prompt prompt) {
        List<String> lines = new ArrayList<>(List.of(prompt.question(), ""));
        if (!prompt.options().isEmpty()) for (int index = 0; index < prompt.options().size(); index++) lines.add((index == prompt.selectedIndex() ? "› " : "  ") + prompt.options().get(index));
        else lines.addAll(prompt.lines());
        return lines;
    }

    private static String commandHint(TuiDashboard.State state) {
        if (state.prompt() != null) return state.prompt().hint();
        if (state.notice() != null && !state.notice().isBlank() && !state.notice().startsWith("ready")) return state.notice();
        return state.connection() == TuiDashboard.Connection.ONBOARDING || state.connection() == TuiDashboard.Connection.OFFLINE
                ? "Local: validate, new, explain, demo · Server: connect host:port · Type for suggestions · Ctrl-P commands"
                : "Enter run · Type for suggestions · Ctrl-P commands · Esc clear · Ctrl-C cancel";
    }

    private static Color connectionColor(TuiDashboard.Connection connection) {
        return switch (connection) {
            case ONLINE -> Color.GREEN;
            case CONNECTING -> Color.YELLOW;
            case OFFLINE, SIGNED_OUT, SESSION_EXPIRED, ISSUER_MISMATCH -> Color.BRIGHT_WHITE;
            case ONBOARDING -> Color.CYAN;
        };
    }
}
