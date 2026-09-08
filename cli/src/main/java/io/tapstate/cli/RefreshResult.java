package io.tapstate.cli;

import io.tapstate.core.common.TapstateErrorCode;

import java.util.Map;
import java.util.Objects;

/** Immutable refresh completion tagged so its consumer can reject stale work deterministically. */
record RefreshResult(long contextGeneration, long requestSequence, Outcome outcome) {

    RefreshResult {
        if (contextGeneration < 0) {
            throw new IllegalArgumentException("Context generation must not be negative");
        }
        if (requestSequence <= 0) {
            throw new IllegalArgumentException("Request sequence must be positive");
        }
        Objects.requireNonNull(outcome, "outcome");
        if (outcome instanceof Success success
                && (success.snapshot().contextGeneration() != contextGeneration
                || success.snapshot().requestSequence() != requestSequence)) {
            throw new IllegalArgumentException("Success snapshot identity must match its refresh result");
        }
    }

    boolean isCurrent(long currentContextGeneration, long latestRequestSequence) {
        return contextGeneration == currentContextGeneration && requestSequence == latestRequestSequence;
    }

    static Outcome success(WorkbenchSnapshot snapshot) {
        return new Success(snapshot);
    }

    static Outcome empty() {
        return new Empty();
    }

    static Outcome offline() {
        return new Offline();
    }

    static Outcome diagnostic(TapstateErrorCode code, Map<String, String> arguments) {
        return new Diagnostic(code, arguments);
    }

    /** The complete set of states a refresh may publish. */
    sealed interface Outcome permits Success, Empty, Offline, Diagnostic {
    }

    /** A refresh produced a non-empty workbench snapshot. */
    record Success(WorkbenchSnapshot snapshot) implements Outcome {
        public Success {
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }

    /** A refresh completed normally and found no data. */
    record Empty() implements Outcome {
    }

    /** A refresh could not reach its remote source. */
    record Offline() implements Outcome {
    }

    /** A refresh completed with a first-party coded diagnostic and its named arguments. */
    record Diagnostic(TapstateErrorCode code, Map<String, String> arguments) implements Outcome {
        public Diagnostic {
            Objects.requireNonNull(code, "code");
            arguments = Map.copyOf(arguments);
        }
    }
}
