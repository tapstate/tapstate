package io.tapstate.runtime.engine;

import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.model.Step;
import io.tapstate.runtime.engine.aggregate.AggregateStores;
import java.util.Objects;
import java.util.function.Function;

/**
 * What the aggregate steps of a pipeline run on.
 *
 * @param specs  each aggregate step as it was checked against the columns that reach it, with the key
 *               its input rows are told apart by
 * @param stores where a vertex opens the two stores of its step
 */
public record AggregateBinding(Function<Step, AggregateStepSpec> specs, AggregateStores stores) {

    public AggregateBinding {
        Objects.requireNonNull(specs, "specs");
        Objects.requireNonNull(stores, "stores");
    }
}
