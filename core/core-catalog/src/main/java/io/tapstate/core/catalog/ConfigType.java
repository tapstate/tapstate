package io.tapstate.core.catalog;

/**
 * The normalized type of a connection config field, mapped from the connector's Formily schema and
 * input component. Layout/container types (void, object) are not fields and never reach here.
 */
public enum ConfigType {
    STRING("string"),
    NUMBER("number"),
    BOOLEAN("boolean"),
    ARRAY("array");

    private final String yaml;

    ConfigType(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
