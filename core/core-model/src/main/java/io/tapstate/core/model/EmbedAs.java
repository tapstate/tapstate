package io.tapstate.core.model;

/** Shape a nest child contributes to its parent document. */
@Doc("How a nested child contributes to its parent: as an array, an object, or fields merged in place.")
public enum EmbedAs {
    @Doc("One-to-many relationship: matching child rows are embedded as an array.")
    ARRAY("array"),
    @Doc("One-to-one relationship: a single matching child row is embedded as an object.")
    OBJECT("object"),
    @Doc("One-to-one or many-to-one relationship: the matched row's fields are merged into the parent.")
    FLAT("flat");

    private final String yaml;

    EmbedAs(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
