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
    CLI("033"),
    ALERT("034"),
    CAPTURE("035"),
    ACTUATION("036"),
    CLUSTER("037"),
    CONNECTOR_CATALOG("038"),
    DATA_BROWSER("039"),
    MONITOR("040"),
    POSITION("041"),
    MIGRATION("042"),
    ROW_VALUE("043"),
    ENGINE("044"),
    TRANSFORM("045"),
    NEST_SIZING("046"),
    STORE("047");

    private final String number;

    ProductErrorModule(String number) { this.number = number; }

    public String number() { return number; }
}
