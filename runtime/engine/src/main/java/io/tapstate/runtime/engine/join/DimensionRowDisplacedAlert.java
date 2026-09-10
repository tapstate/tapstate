package io.tapstate.runtime.engine.join;

import java.io.Serializable;

/**
 * Where it is said that a dimension row was displaced by a second row filed under the same join key.
 *
 * <p>The mirror a join looks dimension rows up in holds one row per key, so a second row arriving under
 * an occupied key replaces the first and every fact row under that key joins to whichever arrived last.
 * The target then holds fewer rows than the query describes. This does not repair that - publishing both
 * rows moves dimension state, output cardinality and row identity together, and none of that happens
 * here. What it removes is the silence: a target quietly short of rows is indistinguishable from a
 * correct one and every row it does hold looks entirely ordinary, so the moment of replacement is the
 * only chance anything has to report it.
 *
 * <p>A port rather than a logger, for the same reason the gauge beside it is one: this ring knows when
 * the thing happened and nothing about how it should be worded. What reaches an operator is coded and
 * phrased at the assembly layer, which is where such text belongs.
 *
 * <p>Carried to the member that runs the vertex, which is why it is serializable.
 */
@FunctionalInterface
public interface DimensionRowDisplacedAlert extends Serializable {

    /** Says nothing at all, for a caller that has nowhere to report to. */
    DimensionRowDisplacedAlert NONE = (source, dimensionKey) -> {
    };

    /**
     * A row of {@code source} that was filed under {@code dimensionKey} has been replaced by a different
     * row arriving under the same key, and is now unreachable.
     */
    void displaced(String source, String dimensionKey);
}
