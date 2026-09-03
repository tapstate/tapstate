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
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.scrollbar.Scrollbar;
import dev.tamboui.widgets.scrollbar.ScrollbarOrientation;
import dev.tamboui.widgets.scrollbar.ScrollbarState;

import java.util.ArrayList;
import java.util.List;

/** TamboUI renderer with a quiet work area and one focused command composer. */
final class TamboDashboard {

    void render(Frame frame, TuiDashboard.State state) {
        render(frame, state, 0);
    }

    void render(Frame frame, TuiDashboard.State state, int workspaceScroll) {
        Rect screen = frame.area().inner(Margin.uniform(1));
        int suggestionRows = inlineSuggestions(state) ? inlineSuggestionRows(state, screen.height()) : 0;
        List<Rect> vertical = Layout.vertical().constraints(
                Constraint.length(2), Constraint.fill(), Constraint.length(5 + suggestionRows)).split(screen);
        renderStatus(frame, vertical.get(0), state);
        renderWorkspace(frame, vertical.get(1), state, workspaceScroll, suggestionRows > 0);
        renderCommandBar(frame, vertical.get(2), state, suggestionRows);
    }

    private void renderStatus(Frame frame, Rect area, TuiDashboard.State state) {
        String context = state.context() == null ? "no context" : state.context();
        String principal = state.principal() == null || state.principal().isBlank() ? "" : " · " + state.principal();
        String path = state.workspace().getFileName() == null ? state.workspace().toString() : state.workspace().getFileName().toString();
        frame.renderWidget(Paragraph.builder()
                .text(Text.from("TAPSTATE\n" + context + principal + "  ·  " + state.connection().label() + "  ·  " + path))
                .foreground(connectionColor(state.connection())).build(), area);
    }

    private void renderWorkspace(Frame frame, Rect area, TuiDashboard.State state, int workspaceScroll,
                                 boolean inlineSuggestions) {
        List<String> lines = state.prompt() != null ? promptLines(state.prompt())
                : !state.palette().isEmpty() && !inlineSuggestions ? paletteLines(state, area.height()) : contentLines(state);
        boolean scrollable = state.prompt() == null && state.palette().isEmpty()
                && state.resultPane() != null && lines.size() > area.height();
        Rect textArea = area;
        Rect scrollbarArea = Rect.ZERO;
        if (scrollable) {
            List<Rect> horizontal = Layout.horizontal().constraints(
                    Constraint.fill(), Constraint.length(1)).split(area);
            textArea = horizontal.get(0);
            scrollbarArea = horizontal.get(1);
        }
        int maxScroll = Math.max(0, lines.size() - textArea.height());
        int scroll = Math.min(Math.max(0, workspaceScroll), maxScroll);
        if (scroll > 0 && !lines.isEmpty()) {
            lines = lines.subList(Math.min(scroll, lines.size() - 1), lines.size());
        }
        Paragraph.Builder paragraph = Paragraph.builder()
                .text(Text.from(String.join("\n", lines)))
                .overflow(Overflow.WRAP_WORD);
        frame.renderWidget(paragraph.build(), textArea);
        if (scrollable) {
            ScrollbarState scrollbarState = new ScrollbarState()
                    .contentLength(lines.size() + scroll)
                    .viewportContentLength(textArea.height())
                    .position(scroll);
            frame.renderStatefulWidget(Scrollbar.builder()
                    .orientation(ScrollbarOrientation.VERTICAL_RIGHT)
                    .thumbSymbol("┃")
                    .trackSymbol("┊")
                    .beginSymbol("")
                    .endSymbol("")
                    .thumbColor(Color.LIGHT_CYAN)
                    .trackColor(Color.DARK_GRAY)
                    .build(), scrollbarArea, scrollbarState);
        }
    }

    private void renderCommandBar(Frame frame, Rect area, TuiDashboard.State state, int suggestionRows) {
        List<Rect> rows = suggestionRows > 0
                ? Layout.vertical().constraints(Constraint.length(suggestionRows), Constraint.length(3), Constraint.fill()).split(area)
                : Layout.vertical().constraints(Constraint.length(3), Constraint.fill()).split(area);
        int inputRow = suggestionRows > 0 ? 1 : 0;
        int hintRow = suggestionRows > 0 ? 2 : 1;
        if (suggestionRows > 0) {
            renderSuggestions(frame, rows.get(0), state);
        }
        renderComposer(frame, rows.get(inputRow), state);
        frame.renderWidget(Paragraph.builder().text(Text.from(commandHint(state, rows.get(hintRow).width())))
                .overflow(Overflow.WRAP_WORD)
                .foreground(Color.GRAY).build(), rows.get(hintRow));
    }

    private void renderComposer(Frame frame, Rect area, TuiDashboard.State state) {
        String line = state.prompt() == null ? state.command() + "▌" : state.prompt().input() + "▌";
        Color composerBackground = Color.rgb(38, 40, 43);
        Block block = Block.builder().borders(Borders.ALL).borderType(BorderType.ROUNDED)
                .borderColor(Color.GRAY).background(composerBackground).build();
        frame.renderWidget(block, area);
        frame.renderWidget(Paragraph.builder().text(Text.from(line))
                .overflow(Overflow.WRAP_WORD)
                .foreground(Color.LIGHT_CYAN).background(composerBackground).build(), block.inner(area));
    }

    private void renderSuggestions(Frame frame, Rect area, TuiDashboard.State state) {
        int visible = Math.min(area.height(), state.palette().size());
        int start = Math.min(Math.max(0, state.paletteIndex() - visible + 1),
                Math.max(0, state.palette().size() - visible));
        for (int offset = 0; offset < visible; offset++) {
            int index = start + offset;
            String command = state.palette().get(index);
            String summary = commandSummary(command);
            String text = (index == state.paletteIndex() ? "› " : "  ") + command
                    + (summary.isBlank() ? "" : "  " + summary);
            Paragraph.Builder row = Paragraph.builder().text(Text.from(text)).overflow(Overflow.CLIP);
            if (index == state.paletteIndex()) {
                row.foreground(Color.BRIGHT_WHITE).background(Color.rgb(57, 57, 57));
            } else {
                row.foreground(Color.GRAY);
            }
            frame.renderWidget(row.build(), new Rect(area.x(), area.y() + offset, area.width(), 1));
        }
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

    private static String commandHint(TuiDashboard.State state, int width) {
        if (state.prompt() != null) return state.prompt().hint();
        if (inlineSuggestions(state)) return "↑/↓ choose · Enter select · Esc clear";
        String compact = "Enter run · Type for suggestions · Ctrl-P commands";
        String full = compact + " · Esc clear · Ctrl-C cancel";
        return state.connection() == TuiDashboard.Connection.ONBOARDING || state.connection() == TuiDashboard.Connection.OFFLINE
                ? (width >= 84 ? "Local: validate, new, explain, demo · Server: connect host:port · " + compact : compact)
                : (width >= 84 ? full : compact);
    }

    private static boolean inlineSuggestions(TuiDashboard.State state) {
        return state.prompt() == null && !state.palette().isEmpty()
                && state.notice() != null && state.notice().startsWith("suggestions");
    }

    private static int inlineSuggestionRows(TuiDashboard.State state, int screenHeight) {
        return Math.min(state.palette().size(), Math.min(6, Math.max(0, screenHeight - 8)));
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
