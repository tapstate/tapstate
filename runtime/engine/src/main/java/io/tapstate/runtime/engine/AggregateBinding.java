package io.tapstate.runtime.engine;

import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.model.Step;
import io.tapstate.runtime.engine.aggregate.AggregateProcessor;
import io.tapstate.runtime.engine.aggregate.AggregateStores;
import java.util.Objects;
import java.util.function.Function;

/**
 * What the aggregate steps of a pipeline run on.
 *
 * @param specs  each aggregate step as it was checked against the columns that reach it, with the key
 *               its input rows are told apart by
 * @param stores where a vertex opens the two stores of its step
 * @param replayFloor where a restart would resume each chain, read by a step to forget what it keeps only
 *               to answer a second delivery; with none, a step keeps all of it
 * @param sweep  how often a step looks for what it can forget
 */
public record AggregateBinding(Function<Step, AggregateStepSpec> specs, AggregateStores stores,
        ReplayFloorFactory replayFloor, AggregateProcessor.Sweep sweep) {

    public AggregateBinding {
        Objects.requireNonNull(specs, "specs");
        Objects.requireNonNull(stores, "stores");
        Objects.requireNonNull(replayFloor, "replayFloor");
        Objects.requireNonNull(sweep, "sweep");
    }

    /** A binding whose steps forget nothing. */
    public AggregateBinding(Function<Step, AggregateStepSpec> specs, AggregateStores stores) {
        this(specs, stores, ReplayFloorFactory.NONE, AggregateProcessor.Sweep.DEFAULT);
    }
}
