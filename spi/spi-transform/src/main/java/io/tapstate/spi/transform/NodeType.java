package io.tapstate.spi.transform;

/**
 * The kinds of transform node a pipeline can carry. A closed set of seven.
 *
 * <p>Four are stateless and row-level — they implement the {@link TransformPort} seam directly:
 * {@link #FILTER}, {@link #MAP}, {@link #JS} and {@link #UNWIND}. Three are not: {@link #UNION}
 * merges many input streams into one, and {@link #NEST} / {@link #JOIN} accumulate state across
 * events. The three non-row-level kinds reserve their names here; their execution contracts land
 * with the execution engine.
 *
 * <p><b>Stateless does not mean one row in, one row out.</b> {@link #UNWIND} answers one event with
 * as many as the array it expands has elements, and it is still stateless: each event is answered
 * from itself alone, nothing is carried between them. The seam has allowed that since it was
 * written, which is why a fan-out needs no kind of its own beyond this name.
 */
public enum NodeType {

    /** Drops events that fail a predicate; a stateless row-level transform. */
    FILTER,

    /** Reshapes each event's fields declaratively; a stateless row-level transform. */
    MAP,

    /** Reshapes each event with a script; a stateless row-level transform. */
    JS,

    /** Expands one event into one per element of an array field; a stateless row-level transform. */
    UNWIND,

    /** Merges many input streams into one; a fan-in node, not a row-level transform. */
    UNION,

    /** Embeds related rows as a nested field, keeping state across events; a stateful node. */
    NEST,

    /** Joins two streams on a key, keeping state across events; a stateful node. */
    JOIN
}
