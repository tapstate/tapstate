package io.tapstate.core.model;

import java.util.Objects;

/** Where one nest keeps its durable operator state. */
@Doc("Durable operator-state placement for one nest.")
public record NestStateStorage(
        @Doc(value = "MongoDB database holding this nest's state, shape record, and dead letters.",
                required = true)
        String database) {

    public NestStateStorage {
        Objects.requireNonNull(database, "database");
    }
}
