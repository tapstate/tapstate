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

        assertThat(lines).hasSize(5);
        assertThat(lines.getFirst().toString()).startsWith("tapstate offline");
        assertThat(lines.get(1).toString()).startsWith("workspace:");
        assertThat(lines).anyMatch(line -> line.toString().startsWith("status:"));
        assertThat(lines).anyMatch(line -> line.toString().startsWith("[COMMAND] >"));
        assertThat(lines.getLast().toString()).contains("compact terminal");
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

    @Test
    void rendersConnectionDetailsWithoutExposingCredentials() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", "alice@example.com",
                        TuiDashboard.Connection.ONLINE, null, "", List.of(), 0, null,
                        "http://127.0.0.1:8081", "tapstate-prod",
                        "persistent session · refresh on demand · expires 1h 20m"), 96, 16);

        assertThat(lines.stream().map(AttributedString::toString).toList())
                .anyMatch(line -> line.contains("Server       http://127.0.0.1:8081"))
                .anyMatch(line -> line.contains("Cluster      tapstate-prod"))
                .anyMatch(line -> line.contains("Auth         persistent session · refresh on demand · expires 1h 20m"))
                .noneMatch(line -> line.contains("token") || line.contains("secret"));
    }

    @Test
    void rendersMachineAndUnauthenticatedStatesExplicitly() {
        List<String> machine = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", "machine",
                        TuiDashboard.Connection.ONLINE, null, "", List.of(), 0, null,
                        "http://127.0.0.1:8081", null, "machine token"), 72, 14)
                .stream().map(AttributedString::toString).toList();
        List<String> unauthenticated = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", null,
                        TuiDashboard.Connection.ONLINE, null, "", List.of(), 0, null,
                        "http://127.0.0.1:8081", null, "not authenticated"), 72, 14)
                .stream().map(AttributedString::toString).toList();

        assertThat(machine).anyMatch(line -> line.contains("Auth         machine token"));
        assertThat(unauthenticated).anyMatch(line -> line.contains("Auth         not authenticated"));
    }

    @Test
    void rendersConnectingAsASeparateTransitionState() {
        List<String> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", null,
                        TuiDashboard.Connection.CONNECTING, "running: ls pipeline", "", List.of(), 0, null,
                        null, null, null), 72, 14)
                .stream().map(AttributedString::toString).toList();

        assertThat(lines.getFirst()).contains("○ connecting");
        assertThat(lines).anyMatch(line -> line.contains("Auth         resolving context"));
    }

    @Test
    void rendersRecentActivityInTheNormalBodyWithoutEchoingSecrets() {
        List<String> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", "alice@example.com",
                        TuiDashboard.Connection.ONLINE, "ready", "", List.of(), 0, null,
                        "http://127.0.0.1:8081", "tapstate-prod", "authenticated",
                        List.of("> ls", "✓ 2 collections", "✕ token: [redacted]")), 96, 16)
                .stream().map(AttributedString::toString).toList();

        assertThat(lines).anyMatch(line -> line.contains("Activity"));
        assertThat(lines).anyMatch(line -> line.contains("> ls"));
        assertThat(lines).anyMatch(line -> line.contains("✓ 2 collections"));
        assertThat(lines).noneMatch(line -> line.contains("tok_live") || line.contains("hunter2"));
    }

    @Test
    void rendersLocalResourceSummariesAndConnectorCounts() {
        List<TuiDashboard.ResourceSummary> resources = List.of(
                new TuiDashboard.ResourceSummary("source", "orders", "mysql · cdc", "mysql", true, false),
                new TuiDashboard.ResourceSummary("source", "audit", "unreadable", null, false, false),
                new TuiDashboard.ResourceSummary("pipeline", "sync", "2 sources · view · serve", null, true, false),
                new TuiDashboard.ResourceSummary("view", "orders_view", "", null, true, false),
                new TuiDashboard.ResourceSummary("pipeline", "stray", "misplaced: declares 'source'", null, true, true));

        List<String> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", "alice@example.com",
                        TuiDashboard.Connection.ONLINE, null, "", List.of(), 0, null,
                        "http://127.0.0.1:8081", "tapstate-prod", "authenticated", List.of(), resources), 120, 24)
                .stream().map(AttributedString::toString).toList();

        assertThat(lines).anyMatch(line -> line.contains("Pipelines (2)"))
                .anyMatch(line -> line.contains("sync  2 sources · view · serve"))
                .anyMatch(line -> line.contains("Sources (2)"))
                .anyMatch(line -> line.contains("audit  unreadable"))
                .anyMatch(line -> line.contains("Connectors (1)"))
                .anyMatch(line -> line.contains("mysql · 1 source"))
                .anyMatch(line -> line.contains("Other resources (1)"))
                .anyMatch(line -> line.contains("stray  misplaced: declares 'source'"));
    }

    @Test
    void rendersAnExplicitEmptyWorkspaceState() {
        List<String> lines = dashboard.render(
                TuiDashboard.State.offline(Path.of("empty"), null), 80, 14)
                .stream().map(AttributedString::toString).toList();

        assertThat(lines).anyMatch(line -> line.contains("Resources"))
                .anyMatch(line -> line.contains("no local resources"))
                .noneMatch(line -> line.contains("use start, stop"));
    }

    @Test
    void truncatesLongResourceRowsWithoutOverflowingTheTerminal() {
        List<TuiDashboard.ResourceSummary> resources = List.of(
                new TuiDashboard.ResourceSummary("source", "a-very-long-source-name-that-does-not-fit",
                        "mongodb · snapshot", "mongodb", true, false),
                new TuiDashboard.ResourceSummary("pipeline", "a-very-long-pipeline-name-that-does-not-fit",
                        "1 source", null, true, false));

        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", null,
                        TuiDashboard.Connection.OFFLINE, null, "", List.of(), 0, null,
                        null, null, null, List.of(), resources), 64, 10);

        assertThat(lines).hasSize(10).allSatisfy(line -> assertThat(line.toString()).hasSize(64));
        assertThat(lines).anyMatch(line -> line.toString().contains("+"));
    }

    @Test
    void rendersWideWorkspacesAsResourceAndActivityPanes() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", "alice@example.com",
                        TuiDashboard.Connection.ONLINE, null, "", List.of(), 0, null,
                        "http://127.0.0.1:8081", "tapstate-prod", "authenticated",
                        List.of("> ls pipeline", "✓ 2 pipelines"), List.of(
                                new TuiDashboard.ResourceSummary("pipeline", "orders", "1 source", null, true, false)),
                        List.of(new TuiDashboard.PipelineSummary("orders", "RUNNING", "status refreshed", "17")),
                        "2026-09-01T00:00:02Z"),
                120, 24);

        assertThat(lines).anyMatch(line -> line.toString().contains("Resources")
                && line.toString().contains("Activity"));
        assertThat(lines).anyMatch(line -> line.toString().contains("orders  1 source"));
        assertThat(lines).anyMatch(line -> line.toString().contains("orders  RUNNING  rev 17"));
        assertThat(lines).anyMatch(line -> line.toString().contains("refreshed 2026-09-01T00:00:02Z"));
        assertThat(lines).anyMatch(line -> line.toString().contains("> ls pipeline"));
        assertThat(lines).allSatisfy(line -> assertThat(line.toString()).hasSize(120));
    }

    @Test
    void keepsRemotePipelinePaneOutOfCompactFrames() {
        List<AttributedString> lines = dashboard.render(
                new TuiDashboard.State(Path.of("orders"), "dev", null,
                        TuiDashboard.Connection.ONLINE, null, "", List.of(), 0, null,
                        null, null, "authenticated", List.of(), List.of(),
                        List.of(new TuiDashboard.PipelineSummary("orders", "RUNNING", "safe", null)),
                        "2026-09-01T00:00:02Z"),
                50, 12);

        assertThat(lines).noneMatch(line -> line.toString().contains("Pipelines")
                || line.toString().contains("safe"));
    }
}
