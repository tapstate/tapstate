package io.tapstate.cli;

import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiDashboardTest {

    private final TuiDashboard dashboard = new TuiDashboard();

    @Test
    void rendersTheOrientationAndCommandSurfacesAtAStableHeight() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", "alice@example.com",
                        TuiDashboard.Connection.ONLINE, null), 72, 14);

        assertThat(lines).hasSize(14);
        assertThat(lines.getFirst().toString()).contains("tapstate  dev / alice@example.com  ● online");
        assertThat(lines.get(1).toString()).contains("workspace:");
        assertThat(lines.get(12).toString()).startsWith("[COMMAND] >");
        assertThat(lines.getLast().toString()).contains("Ctrl-P commands");
        assertThat(lines).allSatisfy(line -> assertThat(line.toString()).hasSize(72));
    }

    @Test
    void fallsBackToAPlainCompactSurfaceWhenTheTerminalIsTooNarrow() {
        List<AttributedString> lines = dashboard.render(
                TuiDashboard.State.offline(Path.of("orders"), null), 30, 8);

        assertThat(lines).hasSize(3);
        assertThat(lines.getFirst().toString()).startsWith("tapstate offline");
        assertThat(lines.get(1).toString()).startsWith("workspace:");
        assertThat(lines.getLast().toString()).startsWith("terminal is too narrow");
        assertThat(lines).allSatisfy(line -> assertThat(line.toString()).hasSize(30));
    }

    @Test
    void rendersTheCurrentCommandBufferInTheCommandBar() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", null,
                        TuiDashboard.Connection.OFFLINE, "ready", "connect http://127.0.0.1:8081"), 72, 14);

        assertThat(lines.get(12).toString()).startsWith("[COMMAND] > connect http://127.0.0.1:8081");
    }

    @Test
    void rendersTheSelectedCommandPaletteInTheBody() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", null,
                        TuiDashboard.Connection.OFFLINE, "commands: ↑/↓ choose", "",
                        List.of("ls", "pwd", "context"), 1), 72, 14);

        assertThat(lines.stream().map(AttributedString::toString).toList())
                .anyMatch(line -> line.contains("› pwd"));
        assertThat(lines.get(12).toString()).startsWith("[COMMAND] >");
    }

    @Test
    void rendersASecretPromptWithoutEchoingItsValue() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", "alice",
                        TuiDashboard.Connection.ONLINE, "ready", "", List.of(), 0,
                        TuiDashboard.Prompt.text("Password", "hunter2", "Enter submit", true)), 72, 14);

        assertThat(lines.stream().map(AttributedString::toString).toList())
                .anyMatch(line -> line.contains("[PROMPT] Password"))
                .noneMatch(line -> line.contains("hunter2"))
                .anyMatch(line -> line.contains("•••••••"));
    }

    @Test
    void rendersPromptChoicesAndKeepsTheSelectedOptionVisible() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", null,
                        TuiDashboard.Connection.OFFLINE, "ready", "", List.of(), 0,
                        TuiDashboard.Prompt.choice("Context action", List.of("Create", "Quit"), 1)), 72, 14);

        assertThat(lines.stream().map(AttributedString::toString).toList())
                .anyMatch(line -> line.contains("› Quit"))
                .anyMatch(line -> line.contains("[PROMPT] Context action"));
    }
}
