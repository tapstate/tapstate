package io.tapstate.core.validation;

/** Full methods share one slot; incremental work owns a separate slot. */
public enum ValidationMode {
    FULL,
    INCREMENTAL
}
