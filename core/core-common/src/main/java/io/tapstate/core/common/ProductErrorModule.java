package io.tapstate.core.common;

/** Explicit product module allocations; module numbers are never inferred from upstream codes. */
public enum ProductErrorModule {
    DSL("001"),
    CONNECTOR("006"),
    CONTROL("021");

    private final String number;

    ProductErrorModule(String number) { this.number = number; }

    public String number() { return number; }
}
