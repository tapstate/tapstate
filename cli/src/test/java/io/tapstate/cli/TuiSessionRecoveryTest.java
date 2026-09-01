package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TuiSessionRecoveryTest {

    @Test
    void defersNetworkRecoveryUntilAfterTheCallerCanDrawTheFirstFrame() {
        List<Runnable> submitted = new ArrayList<>();
        List<TuiEvent> events = new ArrayList<>();
        TuiSessionRecovery recovery = new TuiSessionRecovery(
                ignored -> new TuiSessionRecovery.Attempt("issuer", null), submitted::add);
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context()));

        recovery.start(state, events::add);

        assertThat(submitted).hasSize(1);
        assertThat(events).isEmpty();
        submitted.getFirst().run();
        assertThat(events).singleElement().isInstanceOf(TuiEvent.ContextSessionPosted.class);
    }

    @Test
    void keepsTheRecoveredAccessGrantOutOfTheEventUntilTheCurrentGenerationIsAccepted() {
        List<Runnable> submitted = new ArrayList<>();
        List<TuiEvent> events = new ArrayList<>();
        ResolvedContext.Named context = context();
        AuthService.ActiveSession active = active(context);
        TuiSessionRecovery recovery = new TuiSessionRecovery(
                ignored -> new TuiSessionRecovery.Attempt("issuer", active), submitted::add);
        TuiContextSessionState connecting = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context));

        recovery.start(connecting, events::add);
        submitted.getFirst().run();
        TuiEvent.ContextSessionPosted posted = (TuiEvent.ContextSessionPosted) events.getFirst();
        TuiContextSessionAction.RecoveryCompleted completion =
                (TuiContextSessionAction.RecoveryCompleted) posted.action();
        TuiContextSessionState online = TuiContextSessionReducer.reduce(connecting, completion);

        assertThat(posted.action().toString()).doesNotContain("jwt-recovered");
        assertThat(recovery.take(online)).containsSame(active);
        assertThat(recovery.take(online)).isEmpty();
    }

    @Test
    void mapsAuthServiceFailuresToCredentialFreeContextStates() {
        assertThat(completionFor(CliError.AUTH_ISSUER_MISMATCH)).isInstanceOf(
                TuiContextSessionAction.Recovery.IssuerMismatch.class);
        assertThat(completionFor(CliError.AUTH_SESSION_REJECTED)).isInstanceOf(
                TuiContextSessionAction.Recovery.SessionExpired.class);
        assertThat(completionFor(CliError.AUTH_SESSION_UNREACHABLE)).isInstanceOf(
                TuiContextSessionAction.Recovery.Offline.class);
    }

    @Test
    void mapsUnexpectedRecoveryRuntimeFailuresToOffline() {
        List<Runnable> submitted = new ArrayList<>();
        List<TuiEvent> events = new ArrayList<>();
        TuiSessionRecovery recovery = new TuiSessionRecovery(ignored -> {
            throw new IllegalStateException("transport failed");
        }, submitted::add);
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context()));

        recovery.start(state, events::add);
        submitted.getFirst().run();

        TuiEvent.ContextSessionPosted posted = (TuiEvent.ContextSessionPosted) events.getFirst();
        assertThat(((TuiContextSessionAction.RecoveryCompleted) posted.action()).recovery())
                .isInstanceOf(TuiContextSessionAction.Recovery.Offline.class);
    }

    @Test
    void dropsACompletedOldGenerationBeforeItsCredentialHandoffCanBeInstalled() {
        List<Runnable> submitted = new ArrayList<>();
        List<TuiEvent> events = new ArrayList<>();
        ResolvedContext.Named dev = context("dev");
        ResolvedContext.Named prod = context("prod");
        AuthService.ActiveSession active = active(dev);
        TuiSessionRecovery recovery = new TuiSessionRecovery(
                candidate -> new TuiSessionRecovery.Attempt(
                        candidate.name().equals("dev") ? "dev-issuer" : "prod-issuer",
                        candidate.name().equals("dev") ? active : null),
                submitted::add);
        TuiContextSessionState devState = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(dev));
        recovery.start(devState, events::add);
        Runnable oldGeneration = submitted.removeFirst();

        TuiContextSessionState prodState = TuiContextSessionReducer.reduce(devState,
                new TuiContextSessionAction.SwitchContext(prod));
        recovery.start(prodState, events::add);
        oldGeneration.run();

        assertThat(recovery.take(prodState)).isEmpty();
        assertThat(events).singleElement().isInstanceOf(TuiEvent.ContextSessionPosted.class);
    }

    private static TuiContextSessionAction.Recovery completionFor(CliError error) {
        List<Runnable> submitted = new ArrayList<>();
        List<TuiEvent> events = new ArrayList<>();
        TuiSessionRecovery recovery = new TuiSessionRecovery(ignored -> {
            throw new TapstateException(error, Map.of(), null);
        }, submitted::add);
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context()));

        recovery.start(state, events::add);
        submitted.getFirst().run();

        TuiEvent.ContextSessionPosted posted = (TuiEvent.ContextSessionPosted) events.getFirst();
        return ((TuiContextSessionAction.RecoveryCompleted) posted.action()).recovery();
    }

    private static ResolvedContext.Named context() {
        return context("dev");
    }

    private static ResolvedContext.Named context(String name) {
        return new ResolvedContext.Named(name, new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);
    }

    private static AuthService.ActiveSession active(ResolvedContext.Named context) {
        Instant now = Instant.parse("2026-09-01T00:00:00Z");
        AuthSessionRecord record = new AuthSessionRecord(AuthSessionRecord.CURRENT_VERSION,
                context.definition().authRef(), context.definition().id(), "issuer", "alice", List.of("read"),
                "tss_s01.recovered-session", now, now.plusSeconds(3600), now.plusSeconds(7200));
        return new AuthService.ActiveSession(context.definition().seeds().getFirst(), "jwt-recovered", record);
    }
}
