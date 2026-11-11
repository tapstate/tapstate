package io.tapstate.core.model;

import java.util.List;
import java.util.Objects;

/**
 * The argument of {@code first} and {@code last}: which element wins is decided by {@code by}, compared left
 * to right, and the field is then that element's {@code value}.
 */
@Doc("Picks one element by an ordering and reads a value from it.")
public record RollupPick(
        @Doc(value = "The value read from the chosen element: a $column or an =expression.", required = true)
        String value,
        @Doc(value = "The ordering: one $column or =expression, or a list compared left to right where a later "
                + "entry only breaks ties of the earlier ones. An element whose ordering has a null is not a "
                + "candidate.", required = true)
        @YamlScalarOrList
        List<String> by) {

    public RollupPick {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(by, "by");
        if (by.isEmpty()) {
            throw new IllegalArgumentException("a pick needs at least one ordering expression");
        }
        by = List.copyOf(by);
    }
}
