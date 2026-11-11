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

    /** The output key: the group names, in declared order. Empty for the one global group. */
    public List<String> outputKey() {
        return List.copyOf(groupBy.keySet());
    }

    /** The output row's columns with their types: the group names, then the fields. */
    public Map<String, ValueType> outputColumns() {
        Map<String, ValueType> columns = new LinkedHashMap<>(groupTypes);
        for (RollupSpec field : fields) {
            columns.put(field.field(), field.resultType());
        }
        return Collections.unmodifiableMap(columns);
    }
}
