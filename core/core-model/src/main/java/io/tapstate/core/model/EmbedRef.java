package io.tapstate.core.model;

/**
 * Which side of an embed's join carries the other's identity. The two directions are written
 * identically - {@code on:} is a field pair either way - so the author says which one it is rather
 * than the join being read back from table metadata, which answers nothing at all for a table that
 * declares no key or was never discovered.
 */
@Doc("Which side of the join carries the other's identity: the embedded rows, or the row they hang under.")
public enum EmbedRef {
    @Doc("The embedded rows carry the identity of the row they hang under.")
    CHILD("child"),
    @Doc("The row they hang under carries the identity of the embedded row it points at.")
    PARENT("parent");

    private final String yaml;

    EmbedRef(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
