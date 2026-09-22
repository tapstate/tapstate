package io.tapstate.core.lifecycle;

/**
 * A processor that says which {@link Stage} of the graph it runs. Every processor family the engine wires
 * a vertex with declares its stage this way, so that where a duration was spent is answered by the
 * processor that spent it and never guessed from a vertex's name. The one vertex that declares none is
 * the passthrough, because nothing is processed in it.
 */
public interface Staged {

    /** The stage this processor's time is measured under. */
    Stage stage();
}
