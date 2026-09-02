package io.tapstate.cli;

import dev.tamboui.terminal.Frame;
import dev.tamboui.text.Text;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

import java.util.ArrayList;
import java.util.List;

/**
 * TamboUI rendering adapter for the workbench state.
 *
 * <p>The reducer continues to own all screen state. This adapter deliberately has no terminal or
 * command concerns: TamboUI owns layout, buffering, and frame diffing.
 */
final class TamboDashboard {

    void render(Frame frame, TuiDashboard.State state) {
        List<Rect> vertical = Layout.vertical().constraints(
                Constraint.length(3), Constraint.fill(), Constraint.length(3)).split(frame.area());
        renderHeader(frame, vertical.get(0), state);
        renderBody(frame, vertical.get(1), state);
        renderCommandBar(frame, vertical.get(2), state);
    }

    private void renderHeader(Frame frame, Rect area, TuiDashboard.State state) {
        String context = state.context() == null ? "no context" : state.context();
        String principal = state.principal() == null || state.principal().isBlank() ? "" : " · " + state.principal();
        Block block = Block.builder().title(" TAPSTATE ").borders(Borders.ALL).borderColor(Color.CYAN).build();
        frame.renderWidget(block, area);
        frame.renderWidget(Paragraph.builder()
                .text(Text.from(context + principal + "  ·  " + state.connection().label()))
                .foreground(connectionColor(state.connection()))
                .build(), block.inner(area));
    }

    private void renderBody(Frame frame, Rect area, TuiDashboard.State state) {
        if (state.connection() == TuiDashboard.Connection.ONBOARDING) {
            renderPanel(frame, area, "WELCOME", List.of(
                    "No context is bound to this workspace.",
                    "",
                    "[Enter] Create or choose context",
                    "[o] Stay offline"), Color.CYAN);
            return;
        }
        List<Rect> columns = Layout.horizontal().constraints(
                Constraint.percentage(42), Constraint.percentage(58)).spacing(1).split(area);
        renderPanel(frame, columns.get(0), "WORKSPACE", workspaceLines(state), Color.CYAN);
        renderPanel(frame, columns.get(1), "ACTIVITY", activityLines(state), Color.MAGENTA);
        if (!state.palette().isEmpty()) {
            renderPanel(frame, columns.get(1), "COMMAND PALETTE", paletteLines(state), Color.YELLOW);
        }
        if (state.prompt() != null) {
            renderPanel(frame, columns.get(1), "PROMPT", promptLines(state.prompt()), Color.YELLOW);
        }
    }

    private void renderCommandBar(Frame frame, Rect area, TuiDashboard.State state) {
        String line = state.prompt() == null
                ? "[COMMAND]  > " + state.command()
                : "[PROMPT] " + state.prompt().question() + "  > " + state.prompt().input();
        String notice = state.notice() == null || state.notice().isBlank()
                ? "Enter run · Tab complete · Esc clear · Ctrl+C cancel"
                : state.notice();
        Block block = Block.builder().borders(Borders.ALL).borderColor(Color.DARK_GRAY).build();
        frame.renderWidget(block, area);
        frame.renderWidget(Paragraph.builder().text(Text.from(line + "\n" + notice)).foreground(Color.BRIGHT_WHITE).build(),
                block.inner(area));
    }

    private static void renderPanel(Frame frame, Rect area, String title, List<String> lines, Color color) {
        Block block = Block.builder().title(" " + title + " ").borders(Borders.ALL).borderColor(color).build();
        frame.renderWidget(block, area);
        frame.renderWidget(Paragraph.builder().text(Text.from(String.join("\n", lines))).build(), block.inner(area));
    }

    private static List<String> workspaceLines(TuiDashboard.State state) {
        List<String> lines = new ArrayList<>();
        lines.add("Path: " + state.workspace().toAbsolutePath().normalize());
        lines.add("Server: " + valueOr(state.endpoint(), "not connected"));
        lines.add("Auth: " + valueOr(state.authStatus(), state.connection().label()));
        if (state.clusterName() != null && !state.clusterName().isBlank()) {
            lines.add("Cluster: " + state.clusterName());
        }
        lines.add("");
        lines.add("Resources: " + state.resources().size());
        for (TuiDashboard.ResourceSummary resource : state.resources().stream().limit(5).toList()) {
            lines.add("  " + resource.kind() + "  " + resource.id());
        }
        return lines;
    }

    private static List<String> activityLines(TuiDashboard.State state) {
        List<String> lines = new ArrayList<>();
        if (state.resultPane() != null) {
            lines.addAll(state.resultPane().lines());
        }
        lines.addAll(state.activity());
        if (lines.isEmpty()) {
            lines.add("No recent activity.");
        }
        return lines;
    }

    private static List<String> paletteLines(TuiDashboard.State state) {
        List<String> lines = new ArrayList<>();
        for (int index = 0; index < state.palette().size(); index++) {
            lines.add((index == state.paletteIndex() ? "› " : "  ") + state.palette().get(index));
        }
        return lines;
    }

    private static List<String> promptLines(TuiDashboard.Prompt prompt) {
        List<String> lines = new ArrayList<>();
        lines.add(prompt.question());
        lines.add("");
        if (!prompt.options().isEmpty()) {
            for (int index = 0; index < prompt.options().size(); index++) {
                lines.add((index == prompt.selectedIndex() ? "› " : "  ") + prompt.options().get(index));
            }
        } else {
            lines.addAll(prompt.lines());
            lines.add("> " + prompt.input());
        }
        lines.add("");
        lines.add(prompt.hint());
        return lines;
    }

    private static String valueOr(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Color connectionColor(TuiDashboard.Connection connection) {
        return switch (connection) {
            case ONLINE -> Color.GREEN;
            case CONNECTING -> Color.YELLOW;
            case OFFLINE, SIGNED_OUT, SESSION_EXPIRED, ISSUER_MISMATCH -> Color.RED;
            case ONBOARDING -> Color.CYAN;
        };
    }
}
