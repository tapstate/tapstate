package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationTableFailure;
import java.util.Objects;

/** Internal carrier for a diagnosed table read failure. Publication and authority errors pass through. */
public final class ValidationPairException extends RuntimeException {
    private final ValidationTableFailure.Side side;

    public ValidationPairException(ValidationTableFailure.Side side, TapstateException cause) {
        super(Objects.requireNonNull(cause));
        this.side = Objects.requireNonNull(side);
    }

    public ValidationTableFailure.Side side() { return side; }
    public TapstateException diagnosis() { return (TapstateException) getCause(); }
}
