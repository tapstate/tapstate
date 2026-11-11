package io.tapstate.core.model;

/** The result type a rollup field asks its value to be converted to, exactly or not at all. */
@Doc("The type a rollup field's result is converted to. The conversion must be exact; a value it would round, "
        + "truncate or overflow is refused with a code.")
public enum RollupResultType {
    @Doc("A 64-bit integer. A decimal converts only when it is integral and fits.")
    INT64("int64"),
    @Doc("An exact decimal. An integer always converts.")
    DECIMAL("decimal"),
    @Doc("A binary floating-point number, rounded as binary floating point rounds. Only on request.")
    DOUBLE("double");

    private final String yaml;

    RollupResultType(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
