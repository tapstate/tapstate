package io.tapstate.core.dsl.aggregate;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One aggregate step, checked against its input and ready to run: how an input row is identified, which
 * rows are members, what the group of a row is, and the fields with their types fixed. It travels with the
 * job, so it holds text and values, never a compiled program.
 *
 * @param step     the step's id
 * @param key      the input columns that identify one input row, in order
 * @param where    the membership predicate as a row expression, or null when every row is a member
 * @param groupBy  output key name to the row expression giving its value, in the order of the output key
 * @param groupTypes the type each group expression was found to have
 * @param fields   the output fields, in declared order
 */
public record AggregateStepSpec(
        String step,
        List<String> key,
        String where,
        Map<String, String> groupBy,
        Map<String, ValueType> groupTypes,
        List<RollupSpec> fields) implements Serializable {
    private static final long serialVersionUID = 1L;

    public AggregateStepSpec {
        Objects.requireNonNull(step, "step");
        key = List.copyOf(key);
        groupBy = Collections.unmodifiableMap(new LinkedHashMap<>(groupBy));
        groupTypes = Collections.unmodifiableMap(new LinkedHashMap<>(groupTypes));
        fields = List.copyOf(fields);
    }

    /**
     * The column that keys the one row of a step with no group: it holds the step's id. A row has to be
     * found again to be replaced, and a row with no key column is written as a new row every time.
     */
    public static final String GLOBAL_KEY = "aggregate";

    /** Whether the step has no group names, so that every member row is in its one group. */
    public boolean global() {
        return groupBy.isEmpty();
    }

    /** The names a group is told apart by, in declared order. Empty for the one global group. */
    public List<String> groupNames() {
        return List.copyOf(groupBy.keySet());
    }

    /** The output key: the group names in declared order, or {@link #GLOBAL_KEY} alone when there are none. */
    public List<String> outputKey() {
        return global() ? List.of(GLOBAL_KEY) : groupNames();
    }

    /** The output row's columns with their types: the key columns, then the fields. */
    public Map<String, ValueType> outputColumns() {
        Map<String, ValueType> columns = new LinkedHashMap<>();
        if (global()) {
            columns.put(GLOBAL_KEY, ValueType.of(io.tapstate.core.common.TapstateType.STRING));
        }
        columns.putAll(groupTypes);
        for (RollupSpec field : fields) {
            columns.put(field.field(), field.resultType());
        }
        return Collections.unmodifiableMap(columns);
    }
}
