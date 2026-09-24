package io.tapstate.core.model;

/** {@code srs.schema_evolution} values (§4). */
@Doc("How the source reacts to schema changes detected at the origin during streaming.")
public enum SrsSchemaEvolution {
    @Doc("Track schema changes and propagate them downstream as they occur.")
    TRACK("track"),
    @Doc("Ignore schema changes and keep reading against the originally discovered schema.")
    IGNORE("ignore");

    private final String yaml;

    SrsSchemaEvolution(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
