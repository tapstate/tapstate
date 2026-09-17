package io.tapstate.core.lifecycle;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Where in a pipeline's graph a duration was spent: the value set of the {@code stage} attribute.
 *
 * <p><strong>One value per family of processor the engine draws a vertex with, and nothing else.</strong>
 * The set is read off the graph rather than chosen for it: the builder wires source vertices to a chain of
 * transform vertices to sink vertices, and a nest or a join draws a sub-graph of its own whose vertices all
 * run that family's processors. Each of those families is a stage. A vertex that runs no processing of its
 * own — the passthrough a union or a merge is made of, which exists so that ordinals downstream stay
 * unique — has no stage, because no time is spent in it that a reader would want to see on its own.
 *
 * <p>The names are the engine's own words for those families, which are also the words a pipeline's
 * author already uses. A step's id is not a stage: ids are named by the author per pipeline, so a set
 * keyed on them would be as wide as the data made it.
 *
 * <p>This is not the load phase. Whether a row arrived while the source was still loading or after it had
 * caught up is a separate question about the <em>row</em>, and it is not carried by this attribute or by
 * any other yet; this one answers where in the graph a <em>duration</em> was measured.
 */
public enum Stage {
    /** Reading rows off the source: the vertex with no inbound edge. */
    SOURCE,
    /** The stateless linear family — a filter, a projection, an unwind, a scripted row transform. */
    TRANSFORM,
    /** Assembling documents from several streams: every vertex a nest step draws. */
    NEST,
    /** Widening driving rows against mirrored dimensions: every vertex a join step draws. */
    JOIN,
    /** Writing rows out, to a target or into the state store as a view. */
    SINK;

    /** The spelling this stage has as an attribute value: the lower-case word. */
    public String attributeValue() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Every value the {@code stage} attribute may carry. */
    public static Set<String> attributeValues() {
        return Arrays.stream(values()).map(Stage::attributeValue).collect(Collectors.toUnmodifiableSet());
    }
}
