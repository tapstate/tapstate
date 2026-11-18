package io.tapstate.core.common;

/** Explicit product module allocations; module numbers are never inferred from upstream codes. */
public enum ProductErrorModule {
    DSL("001"),
    CONNECTOR("006"),
    CONTROL("021"),
    SOURCE("022"),
    PIPELINE("023"),
    ARTIFACT("024"),
    PIPELINE_DRAFT("025"),
    MCP("026"),
    SCHEMA("027"),
    LIFECYCLE("028"),
    START_CHECK("029"),
    VALIDATION("030"),
    BOOT("031"),
    ROLE("032"),
    CLI("033");

    private final String number;

    ProductErrorModule(String number) { this.number = number; }

    public String number() { return number; }
}
