package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TuiContextSessionReducerTest {

    @Test
    void startsUnboundInOnboardingWithoutRequestingNetworkRecovery() {
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(null));

        assertThat(state.connection()).isEqualTo(TuiDashboard.Connection.ONBOARDING);
        assertThat(state.recoveryRequested()).isFalse();
        assertThat(state.context()).isNull();
    }

    @Test
    void preservesResolverFailureNoticeWhenInitialContextIsUnbound() {
        TuiAppState initial = TuiAppState.initial("");

        TuiAppState resolved = TuiApp.initializeContextSessionState(initial, null, "cli.context.invalid");

        assertThat(resolved.contextSession().connection()).isEqualTo(TuiDashboard.Connection.ONBOARDING);
        assertThat(resolved.notice()).isEqualTo("cli.context.invalid");
    }

    @Test
    void acceptsOnlyRecoveryEventsForTheCurrentContextGeneration() {
        ResolvedContext.Named dev = context("dev");
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(dev));
        long generation = state.generation();

        TuiContextSessionState online = TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.RecoveryCompleted(generation,
                        new TuiContextSessionAction.Recovery.Online("admin", "urn:tapstate:cluster:dev")));
        TuiContextSessionState late = TuiContextSessionReducer.reduce(online,
                new TuiContextSessionAction.RecoveryCompleted(generation - 1,
                        new TuiContextSessionAction.Recovery.Offline("old server")));

        assertThat(online.connection()).isEqualTo(TuiDashboard.Connection.ONLINE);
        assertThat(late).isSameAs(online);
    }

    @Test
    void contextSwitchClearsPresentationScopeAndRequiresTheFirstWriteConfirmation() {
        TuiContextSessionState connected = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context("dev")));
        connected = TuiContextSessionReducer.reduce(connected,
                new TuiContextSessionAction.RecoveryCompleted(connected.generation(),
                        new TuiContextSessionAction.Recovery.Online("admin", "urn:tapstate:cluster:dev")));

        TuiContextSessionState switched = TuiContextSessionReducer.reduce(connected,
                new TuiContextSessionAction.SwitchContext(context("prod")));

        assertThat(switched.context().name()).isEqualTo("prod");
        assertThat(switched.connection()).isEqualTo(TuiDashboard.Connection.CONNECTING);
        assertThat(switched.principal()).isNull();
        assertThat(switched.issuer()).isNull();
        assertThat(switched.generation()).isGreaterThan(connected.generation());
        assertThat(switched.recoveryRequested()).isTrue();
        assertThat(switched.firstWriteConfirmationRequired()).isTrue();
    }

    @Test
    void refusesContextSwitchWhileAWriteIsInFlight() {
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context("dev")));
        state = TuiContextSessionReducer.reduce(state, new TuiContextSessionAction.SetWriteInFlight("op-42"));

        TuiContextSessionState blocked = TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.SwitchContext(context("prod")));

        assertThat(blocked.context().name()).isEqualTo("dev");
        assertThat(blocked.writeOperationId()).isEqualTo("op-42");
        assertThat(blocked.notice()).contains("op-42");
    }

    @Test
    void mapsRecoverableAuthenticationOutcomesToVisibleConnectionStates() {
        TuiContextSessionState initial = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context("dev")));

        assertThat(complete(initial, new TuiContextSessionAction.Recovery.SignedOut("issuer")).connection())
                .isEqualTo(TuiDashboard.Connection.SIGNED_OUT);
        assertThat(complete(initial, new TuiContextSessionAction.Recovery.SessionExpired("admin", "issuer"))
                .connection()).isEqualTo(TuiDashboard.Connection.SESSION_EXPIRED);
        assertThat(complete(initial, new TuiContextSessionAction.Recovery.IssuerMismatch("mismatch")).connection())
                .isEqualTo(TuiDashboard.Connection.ISSUER_MISMATCH);
        assertThat(complete(initial, new TuiContextSessionAction.Recovery.Offline("unreachable")).connection())
                .isEqualTo(TuiDashboard.Connection.OFFLINE);
    }

    private static TuiContextSessionState complete(TuiContextSessionState state,
                                                   TuiContextSessionAction.Recovery recovery) {
        return TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.RecoveryCompleted(state.generation(), recovery));
    }

    private static ResolvedContext.Named context(String name) {
        return new ResolvedContext.Named(name, new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);
    }
}
