package io.tapstate.cli;

import java.util.List;

/** Pure confirmation state that binds a write command to its target and issuer. */
record TuiWriteConfirmation(String command, String target, String issuer, Decision decision) {

    enum Decision {
        PENDING,
        CONFIRMED,
        CANCELLED
    }

    private static final List<String> OPTIONS = List.of("Cancel", "Run");

    TuiWriteConfirmation {
        command = required(TuiCommandBar.safeDisplayText(command), "write command");
        target = required(TuiCommandBar.safeDisplayText(target), "write target");
        issuer = required(TuiCommandBar.safeDisplayText(issuer), "write issuer");
        decision = decision == null ? Decision.PENDING : decision;
    }

    static TuiWriteConfirmation open(String command, String target, String issuer) {
        return new TuiWriteConfirmation(command, target, issuer, Decision.PENDING);
    }

    String question() {
        return "Run " + command + " on " + target + " (issuer " + issuer + ")?";
    }

    List<String> options() {
        return OPTIONS;
    }

    boolean allowsExecution() {
        return decision == Decision.CONFIRMED;
    }

    TuiWriteConfirmation confirm() {
        return decision == Decision.PENDING ? withDecision(Decision.CONFIRMED) : this;
    }

    TuiWriteConfirmation cancel() {
        return decision == Decision.PENDING ? withDecision(Decision.CANCELLED) : this;
    }

    private TuiWriteConfirmation withDecision(Decision nextDecision) {
        return new TuiWriteConfirmation(command, target, issuer, nextDecision);
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}
