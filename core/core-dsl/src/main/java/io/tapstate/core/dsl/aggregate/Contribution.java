package io.tapstate.core.dsl.aggregate;

import java.util.List;

/**
 * One element's input to a function: the argument's value, for first and last the ordering values, and the
 * element's key, which breaks ties and fixes the order a double sum is folded in.
 */
public record Contribution(Object value, List<Object> by, List<Object> key) {

    public Contribution {
        by = by == null ? List.of() : java.util.Collections.unmodifiableList(new java.util.ArrayList<>(by));
        key = key == null ? List.of() : java.util.Collections.unmodifiableList(new java.util.ArrayList<>(key));
    }

    public static Contribution of(Object value, List<Object> key) {
        return new Contribution(value, List.of(), key);
    }
}
