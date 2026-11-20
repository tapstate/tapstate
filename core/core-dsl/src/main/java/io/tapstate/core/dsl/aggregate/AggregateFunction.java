package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.model.RollupFunction;

/**
 * One function of the library: how it starts, how its result type follows from its argument's, and what it
 * would keep to be maintained incrementally. The result type is fixed when the pipeline is applied; nothing
 * at render chooses it.
 */
public interface AggregateFunction {

    RollupFunction function();

    /** What the function keeps when maintained rather than recomputed. */
    StateKind stateKind();

    /** Whether an input can be taken back out of the running value. */
    boolean retractable();

    /**
     * The result type for an argument of type {@code argument}, or an {@link IllegalArgumentException}
     * whose message says why that argument cannot be applied. An unknown argument type of a sum or average
     * is refused separately, by the caller, since it has a code of its own.
     */
    ValueType resultType(ValueType argument);

    /** A fresh application, folding arguments of the declared type {@code argument}. */
    Accumulator init(ValueType argument);
}
