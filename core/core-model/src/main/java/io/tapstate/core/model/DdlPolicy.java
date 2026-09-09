package io.tapstate.core.model;

/** Sink-side DDL event policy (§8, X5). Default is {@code fail} — stop rather than drift. */
@Doc("How the sink reacts to incoming schema-change (DDL) events; defaults to fail to avoid silent drift.")
public enum DdlPolicy {
    @Doc("Apply the schema change to the target so its structure tracks the source.")
    APPLY("apply"),
    @Doc("Skip the schema change and keep replicating data against the existing target structure.")
    IGNORE("ignore"),
    @Doc("Stop the pipeline on a schema change rather than risk silent drift.")
    FAIL("fail");

    private final String yaml;

    DdlPolicy(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
