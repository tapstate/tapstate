package io.tapstate.core.model;

/** Embed mode of a nest child (§5.1): 1:N array vs 1:1 object. */
@Doc("How a nested child is embedded into its parent: as a one-to-many array or a one-to-one object.")
public enum EmbedAs {
    @Doc("One-to-many relationship: matching child rows are embedded as an array.")
    ARRAY("array"),
    @Doc("One-to-one relationship: a single matching child row is embedded as an object.")
    OBJECT("object");

    private final String yaml;

    EmbedAs(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
