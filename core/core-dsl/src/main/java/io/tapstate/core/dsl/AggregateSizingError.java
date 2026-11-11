package io.tapstate.core.dsl;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import java.util.Set;

/**
 * The {@code aggregate} domain's authoring-time statements of what a step will keep in state. They are
 * warnings in the sense that a batch carrying one is applied and the pipeline runs; they say what the
 * definition costs, not that anything is wrong with it.
 *
 * <p>They are stated for every aggregate step, unlike a nest's sizing, which speaks only past a line.
 * A nest's state is a cache in front of storage, so being large is the design. An aggregate step's state
 * is the only copy of what its totals are computed from, it holds the input's values, and its size is a
 * fixed multiple of the input the author can read off the definition: the number is the thing to know
 * before deploying, whatever it is.
 *
 * <p>{@code placeholders()} is the named-argument contract: every raise site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum AggregateSizingError implements TapstateErrorCode {

    /**
     * What the step keeps, sized on the rows its inputs were discovered to hold. {@code entriesPerRow}
     * is two and one more for each field that keeps its inputs; {@code estimatedEntries} is that times
     * {@code inputRows}, an upper estimate since a row failing the step's condition keeps one entry.
     */
    STATE_ESTIMATE("aggregate.state-estimate",
            Set.of("pipeline", "step", "entriesPerRow", "inputRows", "estimatedEntries")),

    /** What the step keeps per input row, where an input reports no row count to size the whole on. */
    STATE_ESTIMATE_UNSIZED("aggregate.state-estimate-unsized", Set.of("pipeline", "step", "entriesPerRow"));

    private final String code;
    private final Set<String> placeholders;

    AggregateSizingError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    /** A cost is told, never refused: the step is correct at any size. */
    @Override
    public Severity severity() {
        return Severity.WARNING;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
