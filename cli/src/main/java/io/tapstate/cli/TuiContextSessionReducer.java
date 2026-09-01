package io.tapstate.cli;

/** Pure context/session state transitions applied only by the TUI thread. */
final class TuiContextSessionReducer {

    private TuiContextSessionReducer() {
    }

    static TuiContextSessionState reduce(TuiContextSessionState state, TuiContextSessionAction action) {
        if (state == null || action == null) {
            throw new IllegalArgumentException("state and action are required");
        }
        return switch (action) {
            case TuiContextSessionAction.Initialize initialize -> initialize(state, initialize.context());
            case TuiContextSessionAction.SwitchContext switchContext -> switchContext(state, switchContext.context());
            case TuiContextSessionAction.RecoveryCompleted completed -> complete(state, completed);
            case TuiContextSessionAction.SetWriteInFlight write -> copy(state, state.connection(), state.principal(),
                    state.issuer(), state.notice(), state.recoveryRequested(),
                    state.firstWriteConfirmationRequired(), write.operationId());
            case TuiContextSessionAction.ClearWriteInFlight ignored -> copy(state, state.connection(), state.principal(),
                    state.issuer(), state.notice(), state.recoveryRequested(),
                    state.firstWriteConfirmationRequired(), null);
            case TuiContextSessionAction.ConsumeFirstWriteConfirmation ignored -> copy(state, state.connection(),
                    state.principal(), state.issuer(), state.notice(), state.recoveryRequested(), false,
                    state.writeOperationId());
        };
    }

    private static TuiContextSessionState initialize(TuiContextSessionState state, ResolvedContext.Named context) {
        if (context == null) {
            return new TuiContextSessionState(state.generation() + 1, null, TuiDashboard.Connection.ONBOARDING,
                    null, null, "no context bound", false, false, null);
        }
        return new TuiContextSessionState(state.generation() + 1, context, TuiDashboard.Connection.CONNECTING,
                null, null, "resolving context", true, false, null);
    }

    private static TuiContextSessionState switchContext(TuiContextSessionState state, ResolvedContext.Named context) {
        if (state.writeOperationId() != null) {
            return copy(state, state.connection(), state.principal(), state.issuer(),
                    "cannot switch context while write operation " + state.writeOperationId() + " is in flight",
                    state.recoveryRequested(), state.firstWriteConfirmationRequired(), state.writeOperationId());
        }
        return new TuiContextSessionState(state.generation() + 1, context, TuiDashboard.Connection.CONNECTING,
                null, null, "resolving context", true, true, null);
    }

    private static TuiContextSessionState complete(TuiContextSessionState state,
                                                   TuiContextSessionAction.RecoveryCompleted completed) {
        if (state.generation() != completed.generation() || !state.recoveryRequested()) {
            return state;
        }
        return switch (completed.recovery()) {
            case TuiContextSessionAction.Recovery.Online online -> copy(state, TuiDashboard.Connection.ONLINE,
                    online.principal(), online.issuer(), "resumed " + safePrincipal(online.principal()), false,
                    state.firstWriteConfirmationRequired(), state.writeOperationId());
            case TuiContextSessionAction.Recovery.SignedOut signedOut -> copy(state,
                    TuiDashboard.Connection.SIGNED_OUT, null, signedOut.issuer(), "sign in to continue", false,
                    state.firstWriteConfirmationRequired(), state.writeOperationId());
            case TuiContextSessionAction.Recovery.Offline offline -> copy(state, TuiDashboard.Connection.OFFLINE,
                    null, null, offline.reason(), false, state.firstWriteConfirmationRequired(),
                    state.writeOperationId());
            case TuiContextSessionAction.Recovery.SessionExpired expired -> copy(state,
                    TuiDashboard.Connection.SESSION_EXPIRED, expired.principal(), expired.issuer(),
                    "session expired", false, state.firstWriteConfirmationRequired(), state.writeOperationId());
            case TuiContextSessionAction.Recovery.IssuerMismatch mismatch -> copy(state,
                    TuiDashboard.Connection.ISSUER_MISMATCH, null, null, mismatch.reason(), false,
                    state.firstWriteConfirmationRequired(), state.writeOperationId());
        };
    }

    private static String safePrincipal(String principal) {
        return principal == null || principal.isBlank() ? "session" : principal;
    }

    private static TuiContextSessionState copy(TuiContextSessionState state, TuiDashboard.Connection connection,
                                               String principal, String issuer, String notice, boolean recoveryRequested,
                                               boolean firstWriteConfirmationRequired, String writeOperationId) {
        return new TuiContextSessionState(state.generation(), state.context(), connection, principal, issuer, notice,
                recoveryRequested, firstWriteConfirmationRequired, writeOperationId);
    }
}
