package io.tapstate.cli;

/** Actions accepted by the context/session reducer; recovery results carry no credential material. */
sealed interface TuiContextSessionAction permits TuiContextSessionAction.Initialize,
        TuiContextSessionAction.SwitchContext, TuiContextSessionAction.RecoveryCompleted,
        TuiContextSessionAction.SetWriteInFlight, TuiContextSessionAction.ClearWriteInFlight,
        TuiContextSessionAction.ConsumeFirstWriteConfirmation {

    record Initialize(ResolvedContext.Named context) implements TuiContextSessionAction {
    }

    record SwitchContext(ResolvedContext.Named context) implements TuiContextSessionAction {
        public SwitchContext {
            if (context == null) {
                throw new IllegalArgumentException("context is required");
            }
        }
    }

    record RecoveryCompleted(long generation, Recovery recovery) implements TuiContextSessionAction {
        public RecoveryCompleted {
            if (generation < 0 || recovery == null) {
                throw new IllegalArgumentException("generation and recovery are required");
            }
        }
    }

    record SetWriteInFlight(String operationId) implements TuiContextSessionAction {
        public SetWriteInFlight {
            if (operationId == null || operationId.isBlank()) {
                throw new IllegalArgumentException("operation id is required");
            }
        }
    }

    record ClearWriteInFlight() implements TuiContextSessionAction {
    }

    record ConsumeFirstWriteConfirmation() implements TuiContextSessionAction {
    }

    sealed interface Recovery permits Recovery.Online, Recovery.SignedOut, Recovery.Offline,
            Recovery.SessionExpired, Recovery.IssuerMismatch {
        record Online(String principal, String issuer) implements Recovery {
        }

        record SignedOut(String issuer) implements Recovery {
        }

        record Offline(String reason) implements Recovery {
        }

        record SessionExpired(String principal, String issuer) implements Recovery {
        }

        record IssuerMismatch(String reason) implements Recovery {
        }
    }
}
