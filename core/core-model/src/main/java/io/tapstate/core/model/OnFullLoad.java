package io.tapstate.core.model;

/** The requested treatment of target rows before a new full load. */
@Doc("Treatment of existing target rows before a new full load; recovery and CDC-only runs never clear rows.")
public enum OnFullLoad {
    @Doc("Clear existing target rows before a new full load.")
    CLEAR("clear"),
    @Doc("Keep existing target rows and write the full load using the configured write mode.")
    APPEND("append"),
    @Doc("Refuse a new full load when the target table is not empty.")
    FAIL("fail");

    private final String yaml;

    OnFullLoad(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
