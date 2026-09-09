package io.tapstate.core.model;

/** Bulk rename case rule (§8, X4); applied to the source name before prefix/suffix. */
@Doc("Case conversion applied to the source name before any prefix or suffix is added.")
public enum RenameCase {
    @Doc("Convert the name to all uppercase letters.")
    UPPER("upper"),
    @Doc("Convert the name to all lowercase letters.")
    LOWER("lower"),
    @Doc("Convert the name to camelCase, lowercasing the first word.")
    CAMEL("camel"),
    @Doc("Convert the name to PascalCase, capitalizing the first letter of each word.")
    PASCAL("pascal");

    private final String yaml;

    RenameCase(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
