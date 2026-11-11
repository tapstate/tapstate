package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.AggregateStepRules;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.common.NumericType;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** What an aggregate step is to the assembly: how it is compiled for a run and what it publishes. */
final class AggregateSteps {

    private AggregateSteps() {
    }

    /** The aggregate a step declares, or null where the step is anything else. */
    static TransformBody.Aggregate of(Step step) {
        return step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Aggregate aggregate
                ? aggregate : null;
    }

    /**
     * The step as a run executes it: every expression checked against the columns that reach it, and its
     * input rows told apart by the key the step names or, where it names none, by the key of the one
     * table its rows come from.
     *
     * <p>The table's key is taken only while every column of it still reaches the step under its own
     * name. A step in between that drops or renames one leaves rows that can no longer be told apart by
     * it, and the step is refused until it says which columns do.
     *
     * @param input    the columns reaching the step, which have to be known
     * @param tableKey the key of the table the rows come from, empty where it has none
     * @param from     how the input is named in a refusal
     */
    static AggregateStepSpec compile(Step step, TransformBody.Aggregate aggregate, NodeColumns input,
            List<String> tableKey, String from) {
        List<String> inherited = tableKey != null && !tableKey.isEmpty()
                && input.columns().keySet().containsAll(tableKey) ? tableKey : List.of();
        if (aggregate.key() == null && inherited.isEmpty()) {
            throw new TapstateException(AggregateError.KEY_REQUIRED, Map.of("step", step.id(), "from", from), null);
        }
        return AggregateStepRules.compile(step.id(), aggregate, NodeColumns.valueTypes(input), true, inherited, from);
    }

    /**
     * The table a step's rows land in: named by the step, one column per group name and per field, and
     * keyed by the group names, which is what tells its rows apart.
     */
    static TargetTable target(AggregateStepSpec spec) {
        List<String> key = spec.outputKey();
        List<TargetField> fields = new ArrayList<>();
        spec.outputColumns().forEach((name, type) -> fields.add(new TargetField(name, null, key.contains(name),
                type.type(), numeric(type))));
        return new TargetTable(spec.step(), fields, key.isEmpty() ? List.of() : List.of(new TargetIndex(key, true)));
    }

    private static NumericType numeric(ValueType type) {
        return type.type() == TapstateType.DECIMAL && type.scale() != null
                ? new NumericType(null, null, null, null, null, null, 38, type.scale()) : null;
    }
}
