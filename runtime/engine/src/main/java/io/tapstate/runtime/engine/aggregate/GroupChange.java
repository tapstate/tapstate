package io.tapstate.runtime.engine.aggregate;

import io.tapstate.core.event.SourceOrder;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * What the membership stage tells the group stage about one input row: that the row is now a member of
 * {@code group} with these inputs to the fields, or, with no inputs, that it is no longer one.
 *
 * @param group     the values of the group's key, in the order of the output key
 * @param inputKey  the input row's identity, as bytes that sort the way the row's key does
 * @param order     the order of the event that caused this
 * @param inputs    one entry per field in declared order, or null when the row leaves the group
 */
public record GroupChange(List<Object> group, byte[] inputKey, SourceOrder order, List<FieldInput> inputs) {

    public GroupChange {
        group = Collections.unmodifiableList(new ArrayList<>(group));
        Objects.requireNonNull(inputKey, "inputKey");
        Objects.requireNonNull(order, "order");
        inputs = inputs == null ? null : List.copyOf(inputs);
    }

    public static GroupChange set(List<Object> group, byte[] inputKey, SourceOrder order, List<FieldInput> inputs) {
        return new GroupChange(group, inputKey, order, Objects.requireNonNull(inputs, "inputs"));
    }

    public static GroupChange retract(List<Object> group, byte[] inputKey, SourceOrder order) {
        return new GroupChange(group, inputKey, order, null);
    }

    public boolean retracts() {
        return inputs == null;
    }

    /**
     * What one input row gives one field.
     *
     * @param in    whether the row contributes at all: its filter held and, for a function of a value, the
     *              value is not null; for first and last, no ordering value is null
     * @param value the argument's value; null for a count of rows, and allowed to be null for first and last
     * @param by    first and last only: the ordering values
     */
    public record FieldInput(boolean in, Object value, List<Object> by) {

        public static final FieldInput OUT = new FieldInput(false, null, List.of());

        public FieldInput {
            by = by == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(by));
        }

        public static FieldInput of(Object value) {
            return new FieldInput(true, value, List.of());
        }

        public static FieldInput of(Object value, List<Object> by) {
            return new FieldInput(true, value, by);
        }
    }
}
