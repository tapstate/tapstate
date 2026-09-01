package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TuiWriteConfirmationTest {

    @Test
    void confirmationKeepsCommandTargetAndIssuerTogether() {
        TuiWriteConfirmation confirmation = TuiWriteConfirmation.open(
                "pipeline.start", "production", "01J5ABCD");

        assertThat(confirmation.command()).isEqualTo("pipeline.start");
        assertThat(confirmation.target()).isEqualTo("production");
        assertThat(confirmation.issuer()).isEqualTo("01J5ABCD");
        assertThat(confirmation.question())
                .isEqualTo("Run pipeline.start on production (issuer 01J5ABCD)?");
        assertThat(confirmation.options()).containsExactly("Cancel", "Run");
        assertThat(confirmation.decision()).isEqualTo(TuiWriteConfirmation.Decision.PENDING);
    }

    @Test
    void confirmationMustBeExplicitBeforeExecution() {
        TuiWriteConfirmation pending = TuiWriteConfirmation.open(
                "pipeline.start", "production", "01J5ABCD");

        assertThat(pending.allowsExecution()).isFalse();
        assertThat(pending.confirm().allowsExecution()).isTrue();
        assertThat(pending.cancel().allowsExecution()).isFalse();
    }

    @Test
    void commandTargetAndIssuerCannotBeMissing() {
        assertThatThrownBy(() -> TuiWriteConfirmation.open("pipeline.start", "", "01J5ABCD"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TuiWriteConfirmation.open("pipeline.start", "production", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TuiWriteConfirmation.open(" ", "production", "01J5ABCD"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void confirmationTextDoesNotExposeSecretsOrTerminalControls() {
        TuiWriteConfirmation confirmation = TuiWriteConfirmation.open(
                "token revoke tok_live", "prod\u001b[2J", "issuer-token=secret");

        assertThat(confirmation.question()).doesNotContain("tok_live", "secret", "\u001b");
    }

    @Test
    void firstWriteAfterContextSwitchUsesTheNewTargetAndResolvedIssuer() {
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context("dev")));
        state = TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.RecoveryCompleted(state.generation(),
                        new TuiContextSessionAction.Recovery.Online("admin", "issuer-dev")));
        state = TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.SwitchContext(context("prod")));
        state = TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.RecoveryCompleted(state.generation(),
                        new TuiContextSessionAction.Recovery.Online("operator", "issuer-prod")));

        TuiWriteConfirmation confirmation = TuiWriteConfirmation.open(
                "pipeline.start", state.context().name(), state.issuer());

        assertThat(state.firstWriteConfirmationRequired()).isTrue();
        assertThat(confirmation.question())
                .isEqualTo("Run pipeline.start on prod (issuer issuer-prod)?");
        assertThat(confirmation.target()).isEqualTo("prod");
        assertThat(confirmation.issuer()).isEqualTo("issuer-prod");
    }

    private static ResolvedContext.Named context(String name) {
        return new ResolvedContext.Named(name, new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);
    }
}
