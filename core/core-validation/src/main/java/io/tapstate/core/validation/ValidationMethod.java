package io.tapstate.core.validation;

/** The strength of an independent check, separate from its execution mode. */
public enum ValidationMethod {
    COUNT,
    UNIQUE_KEY,
    FULL_COLUMN;

    public boolean supports(ValidationMode mode) {
        return mode == ValidationMode.FULL
                || mode == ValidationMode.INCREMENTAL && this == FULL_COLUMN;
    }
}
