package io.tapstate.core.model;

/** Whether a target receives the fields the product itself adds to a row. */
@Doc("Whether the target receives the product-added row field that marks, for example, a stale aggregate row.")
public enum InternalFields {
    @Doc("Write the product-added markers meant for consumers; internal bookkeeping is never written.")
    DELIVER("deliver"),
    @Doc("Write no product-added field at all, for a target that accepts only the fields it declared.")
    STRIP("strip");

    private final String yaml;

    InternalFields(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
