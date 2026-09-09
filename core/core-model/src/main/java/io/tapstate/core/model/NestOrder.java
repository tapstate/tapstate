package io.tapstate.core.model;

/** Nest event-processing order (§5.1). */
@Doc("Order in which a nest transform processes main and sub-table change events.")
public enum NestOrder {
    @Doc("Process the main-table event before its related sub-table events.")
    MAIN_FIRST("main_first"),
    @Doc("Process the sub-table events before the related main-table event.")
    SUB_FIRST("sub_first");

    private final String yaml;

    NestOrder(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
