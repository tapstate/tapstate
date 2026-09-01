package io.tapstate.cli;

import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiSecretProjectionSafetyTest {

    private static final String PASSWORD = "pw-frame-secret";
    private static final String TOKEN = "tok-frame-secret";
    private static final String CSI = "\u001b[2J";
    private static final String OSC_BEL = "\u001b]8;;https://example.invalid\u0007linked\u001b]8;;\u0007";
    private static final String OSC_ST = "\u001b]0;window-title\u001b\\";

    @Test
    void historyReturnsOnlySafeDisplayTextForCredentialBearingCommands() {
        TuiCommandHistory history = new TuiCommandHistory();
        history.record("auth login alice " + PASSWORD + CSI + OSC_BEL);
        history.record("connect --token " + TOKEN + OSC_ST);
        history.record("request Authorization: Bearer " + TOKEN + CSI);

        assertThat(history.entries()).allSatisfy(this::assertSafe);
        assertThat(history.previous("draft")).doesNotContain(PASSWORD, TOKEN).doesNotContain("\u001b");
        assertThat(history.matches("request")).allSatisfy(this::assertSafe);
        assertThat(history.matches("connect")).allSatisfy(this::assertSafe);
    }

    @Test
    void reducerKeepsAllPresentationSinksSafeAcrossSecretBearingActions() {
        TuiAppState state = TuiAppState.initial("ready");
        TuiCommandBar.ResultPane result = TuiCommandBar.project(
                new CommandResult(false, Cli.EXIT_DIAGNOSTIC),
                "Authorization: Bearer " + TOKEN + CSI + "\npassword=" + PASSWORD + OSC_BEL);

        state = TuiReducer.reduce(state, new TuiAction.SetCommand(
                "connect --token " + TOKEN + OSC_ST));
        state = TuiReducer.reduce(state, new TuiAction.SetNotice(
                "password=" + PASSWORD + " Authorization: Bearer " + TOKEN + CSI));
        state = TuiReducer.reduce(state, new TuiAction.SetPrompt(
                TuiDashboard.Prompt.text("Password " + PASSWORD + CSI, PASSWORD, "Enter" + OSC_BEL, true)));
        state = TuiReducer.reduce(state, new TuiAction.AppendActivity(
                "Authorization: Bearer " + TOKEN + OSC_ST + " password=" + PASSWORD));
        state = TuiReducer.reduce(state, new TuiAction.SetResultPane(result));

        assertSafe(state.command());
        assertSafe(state.notice());
        assertSafe(state.prompt().question());
        assertSafe(state.prompt().input());
        assertSafe(state.prompt().hint());
        state.activity().forEach(this::assertSafe);
        state.resultPane().lines().forEach(this::assertSafe);
        assertSafe(state.resultPane().notice());
        assertSafe(state.toString());
    }

    @Test
    void dashboardFramesDoNotEchoSecretsFromPromptActivityResultOrResourceRows() {
        TuiDashboard.Prompt prompt = new TuiDashboard.Prompt(
                "Password " + PASSWORD + CSI, PASSWORD,
                "Enter submit " + OSC_ST, true,
                List.of("Cancel", "Authorization: Bearer " + TOKEN), 1,
                List.of("password=" + PASSWORD + CSI));
        TuiCommandBar.ResultPane result = TuiCommandBar.project(
                new CommandResult(false, Cli.EXIT_DIAGNOSTIC),
                "token=" + TOKEN + OSC_ST + "\nAuthorization: Bearer " + TOKEN);
        TuiDashboard.State state = new TuiDashboard.State(
                Path.of("orders"), "dev", "alice@example.com", TuiDashboard.Connection.ONLINE,
                "password=" + PASSWORD + CSI, "connect --token " + TOKEN + OSC_BEL,
                List.of(), 0, prompt, "https://127.0.0.1:8081", "tapstate-prod",
                "authenticated",
                List.of("Authorization: Bearer " + TOKEN + OSC_ST, "password=" + PASSWORD),
                List.of(new TuiDashboard.ResourceSummary("source", "orders",
                        "Authorization: Bearer " + TOKEN + CSI, "mysql-password=" + PASSWORD, true, false)),
                List.of(new TuiDashboard.PipelineSummary("orders", "RUNNING",
                        "token=" + TOKEN + OSC_BEL, "17")),
                "2026-09-01T00:00:02Z", result);

        List<String> promptFrame = new TuiDashboard().render(state, 120, 24).stream()
                .map(AttributedString::toString).toList();
        assertThat(promptFrame).anyMatch(line -> line.contains("[PROMPT]"));
        promptFrame.forEach(this::assertSafe);

        List<String> normalFrame = new TuiDashboard().render(
                new TuiDashboard.State(Path.of("orders"), "dev", "alice@example.com",
                        TuiDashboard.Connection.ONLINE, "ready", "ls", List.of(), 0, null,
                        "https://127.0.0.1:8081", "tapstate-prod", "authenticated",
                        List.of("Authorization: Bearer " + TOKEN + CSI),
                        List.of(new TuiDashboard.ResourceSummary("source", "orders",
                                "password=" + PASSWORD, "mysql", true, false)),
                        List.of(new TuiDashboard.PipelineSummary("orders", "RUNNING",
                                "token=" + TOKEN, "17")),
                        "2026-09-01T00:00:02Z", result), 120, 24).stream()
                .map(AttributedString::toString).toList();
        assertThat(normalFrame).anyMatch(line -> line.contains("Activity"));
        normalFrame.forEach(this::assertSafe);
    }

    private void assertSafe(String value) {
        assertThat(value).doesNotContain(PASSWORD, TOKEN, "\u001b");
    }
}
