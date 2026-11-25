package io.tapstate.spi.store;

import java.util.Objects;

/** Database names captured from one trusted storage binding, never inferred from a requested placement. */
public record OperatorDatabaseBinding(CacheBusinessScope scope, String cacheDatabase,
                                      String configurationDatabase, String masterDatabase) {
    public OperatorDatabaseBinding {
        Objects.requireNonNull(scope, "scope");
        for (String name : new String[]{cacheDatabase, configurationDatabase, masterDatabase}) {
            if (name == null || name.isBlank()) throw new IllegalArgumentException("storage binding requires database names");
        }
        if (cacheDatabase.equalsIgnoreCase(configurationDatabase) || cacheDatabase.equalsIgnoreCase(masterDatabase)
                || configurationDatabase.equalsIgnoreCase(masterDatabase)) throw new IllegalArgumentException("storage databases must be distinct");
    }
}
