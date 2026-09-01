package io.tapstate.cli;

/**
 * Reducible context and session facts for the terminal workbench. Credentials and resource data do
 * not belong here: this state is safe to render, inspect, and replace when the selected context changes.
 */
record TuiContextSessionState(long generation, ResolvedContext.Named context,
                              TuiDashboard.Connection connection, String principal, String issuer,
                              String notice, boolean recoveryRequested, boolean firstWriteConfirmationRequired,
                              String writeOperationId) {

    TuiContextSessionState {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must not be negative");
        }
        connection = connection == null ? TuiDashboard.Connection.ONBOARDING : connection;
        principal = blankToNull(principal);
        issuer = blankToNull(issuer);
        notice = notice == null ? "" : notice;
        writeOperationId = blankToNull(writeOperationId);
    }

    static TuiContextSessionState initial() {
        return new TuiContextSessionState(0L, null, TuiDashboard.Connection.ONBOARDING,
                null, null, "", false, false, null);
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
