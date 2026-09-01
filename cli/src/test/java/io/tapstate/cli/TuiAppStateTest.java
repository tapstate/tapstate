package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TuiAppStateTest {

    @Test
    void reducerKeepsTheProjectedResultAndOperationInPresentationState() {
        TuiAppState state = TuiAppState.initial("ready");
        TuiCommandBar.ResultPane pane = new TuiCommandBar.ResultPane(
                true, Cli.EXIT_OK, true, java.util.List.of("done"), "done");
        TuiOperation operation = TuiOperation.command("op-1", "help");

        state = TuiReducer.reduce(state, new TuiAction.SetResultPane(pane));
        state = TuiReducer.reduce(state, new TuiAction.SetOperation(operation));

        assertThat(state.resultPane()).isEqualTo(pane);
        assertThat(state.operation()).isEqualTo(operation);
    }

    @Test
    void presentationSnapshotDoesNotRetainSecretsOrTerminalControls() {
        String password = "pw-state-secret";
        String token = "tok-state-secret";
        String ansi = "\u001b[2J";
        TuiAppState state = TuiAppState.initial("ready");

        state = TuiReducer.reduce(state, new TuiAction.SetCommand(
                "auth login alice " + password + " " + ansi));
        state = TuiReducer.reduce(state, new TuiAction.SetNotice(
                "Authorization: Bearer " + token + ansi));
        state = TuiReducer.reduce(state, new TuiAction.SetPrompt(
                TuiDashboard.Prompt.text("Password " + password, password, "Enter", true)));
        state = TuiReducer.reduce(state, new TuiAction.AppendActivity(
                "Authorization: Bearer " + token + ansi));
        state = TuiReducer.reduce(state, new TuiAction.SetResultPane(new TuiCommandBar.ResultPane(
                false, Cli.EXIT_DIAGNOSTIC, false,
                java.util.List.of("password=" + password + ansi, "token=" + token),
                "token=" + token)));

        assertThat(state.command()).doesNotContain(password, ansi);
        assertThat(state.notice()).doesNotContain(token, ansi);
        assertThat(state.prompt().question()).doesNotContain(password, ansi);
        assertThat(state.prompt().input()).doesNotContain(password, ansi);
        assertThat(state.activity().toString()).doesNotContain(token, ansi);
        assertThat(state.resultPane().toString()).doesNotContain(password, token, ansi);
        assertThat(state.toString()).doesNotContain(password, token, ansi);
    }
}
