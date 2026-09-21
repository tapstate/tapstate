package io.tapstate.app;

import java.util.Objects;

/** One operator-state namespace in the database that owns it. */
record OperatorStateLocation(String database, String namespace) {

    OperatorStateLocation {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(namespace, "namespace");
    }
}
