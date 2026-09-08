package io.tapstate.runtime.engine.join;

import io.tapstate.core.event.Envelope;

import java.util.Objects;

/**
 * One change, and which of the join's sources it is a change to.
 *
 * <p>The source is carried beside the event rather than read out of it. What a plan calls a source is
 * the name the SQL uses - an alias where the SQL wrote one - and what an event calls its stream is the
 * name the pipeline reads under; the same table can appear under both spellings and, self-joined,
 * under two aliases at once. Whoever wired the edge knows which is which; the event does not.
 *
 * @param source the name the plan calls the source by
 * @param event  the change itself
 */
public record SourceChange(String source, Envelope event) {

    public SourceChange {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(event, "event");
    }
}
