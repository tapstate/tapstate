package io.tapstate.core.model;

/**
 * Source read mode (§4): two axes — boundedness × change-awareness. Legality of
 * a connector × mode pair is ruled by the capability matrix at validate time, not here.
 */
@Doc("Source read mode: how the connector reads, along boundedness × change-awareness.")
public enum SourceMode {
    @Doc("Change data capture — an unbounded stream of inserts, updates and deletes.")
    CDC("cdc"),
    @Doc("One-shot bounded read of current rows; no change capture.")
    SNAPSHOT("snapshot"),
    @Doc("Unbounded push stream from a message system.")
    STREAM("stream"),
    @Doc("Bounded read from files.")
    FILE("file"),
    @Doc("Pull from an API or SaaS endpoint.")
    API("api");

    private final String yaml;

    SourceMode(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
