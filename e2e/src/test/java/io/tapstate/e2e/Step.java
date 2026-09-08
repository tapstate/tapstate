package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;

import java.util.List;
import java.util.Map;

/**
 * One stage of a specification. Steps run in declaration order; the order is the scenario.
 *
 * <p>Lifecycle steps are spelled exactly as the product spells them - {@code start}, {@code pause},
 * {@code resume}, {@code stop} - and carry the product's own verb enum, so neither the word nor the
 * value can drift from what the product accepts. {@code run} is deliberately not a step: the
 * product already reserves it for a different meaning, apply-then-start.
 *
 * <p>There is no rewind step: re-snapshotting is the explicit {@code stop} then {@code start} pair,
 * which is exactly what the product's verb set offers.
 */
public sealed interface Step {

    /** Drives one lifecycle verb and returns once the intent is recorded, not once it converges. */
    record Lifecycle(LifecycleVerb verb) implements Step {}

    /**
     * Holds or releases one source stream, leaving every other stream of the same pipeline running.
     *
     * <p>The same two words the whole-pipeline form uses, carrying the source they apply to - so a
     * reader learns the scope from whether a source is named, not from a second pair of verbs. Held
     * means the source's bytes stop reaching the product while the source itself keeps accepting
     * writes; releasing lets what was written meanwhile through, in the order the source recorded it.
     * That ordering is the whole reason the hold is worth having: it is how a specification produces
     * an arrival order that disagrees with the source order, which no amount of waiting can arrange.
     *
     * <p>Holding is a state and not a count, so holding a held stream is nothing rather than an error,
     * and a stream still held when the steps run out is released by the run that held it - the gate
     * belongs to the harness and dies with it either way.
     */
    record StreamLifecycle(StreamVerb verb, String sourceId) implements Step {}

    /**
     * Drives one word that expands into several of the product's verbs. The expansion is the
     * binding's, not this record's: what a specification says is the word, and a harness that wrote
     * the pair out instead would go on passing if the product stopped expanding it that way.
     */
    record Composed(ComposedVerb verb) implements Step {}

    /** Produces changes against a seeded table while the pipeline is running. */
    record Cdc(TableAlias table, Change change) implements Step {}

    /**
     * What one cdc step does to the table it names. Two shapes, and the difference is whether the
     * specification decides which rows move.
     *
     * <p>{@link Generated} asks for a number of changes and leaves the rest to the driver, which is
     * enough whenever the case is about a count arriving. {@link Update} and {@link Delete} name the
     * row and, for an update, the value - which is what a case about an assembled document needs,
     * because the thing it has to read back is a field, not a total. A count is satisfied by changing
     * any row; only a named row and a named value can hold an implementation to changing the right one.
     *
     * <p>{@link Insert} arrived later than the other two, when a witness needed it: an assembly has to be
     * seeded by value, because a child row without its join key belongs to no parent, and a table seeded
     * that way refuses a generated insert - that form writes the {@code (id, seq)} shape. Adding a row
     * upstream of an assembly was therefore not expressible at all until this existed.
     */
    sealed interface Change {

        /** A number of changes of one kind, with the driver choosing which rows move. */
        record Generated(CdcOp op, long rows) implements Change {}

        /** Sets columns on the one row the settings locate. */
        record Update(Map<String, Object> where, Map<String, Object> set) implements Change {

            public Update {
                where = Map.copyOf(where);
                set = Map.copyOf(set);
            }
        }

        /** Removes the one row the settings locate. */
        record Delete(Map<String, Object> where) implements Change {

            public Delete {
                where = Map.copyOf(where);
            }
        }

        /** Adds the given rows, spelled the way a seed spells them: columns and values, nothing derived. */
        record Insert(List<Map<String, Object>> values) implements Change {

            public Insert {
                values = List.copyOf(values);
            }
        }
    }

    /** Polls a matcher until it holds or the bound expires. */
    record Await(Matcher matcher) implements Step {}

    /** Checks a matcher once, now. */
    record Assertion(Matcher matcher) implements Step {}
}
