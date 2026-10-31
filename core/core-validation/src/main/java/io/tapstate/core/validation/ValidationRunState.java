package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Objects;

/** Legal state edges only; a store must separately enforce authority and completion evidence. */
public enum ValidationRunState {
    STARTING, RUNNING, STOPPING, COMPLETED, STOPPED, FAILED;

    public boolean terminal() {
        return this == COMPLETED || this == STOPPED || this == FAILED;
    }

    public boolean canTransitionTo(ValidationRunState next, ValidationMode mode) {
        Objects.requireNonNull(next, "next");
        Objects.requireNonNull(mode, "mode");
        return switch (this) {
            case STARTING -> next == RUNNING || next == STOPPING || next == FAILED;
            case RUNNING -> next == STOPPING || next == FAILED
                    || next == COMPLETED && mode == ValidationMode.FULL;
            case STOPPING -> next == STOPPED || next == FAILED;
            case COMPLETED, STOPPED, FAILED -> false;
        };
    }

    public void requireTransitionTo(ValidationRunState next, ValidationMode mode) {
        if (!canTransitionTo(next, mode)) {
            throw new TapstateException(ValidationError.ILLEGAL_TRANSITION,
                    Map.of("from", name(), "to", next.name(), "mode", mode.name()), null);
        }
    }
}
