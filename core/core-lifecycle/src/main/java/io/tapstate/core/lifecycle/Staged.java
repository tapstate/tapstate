package io.tapstate.core.lifecycle;

/**
 * A processor that says which {@link Stage} of the graph it runs. Every processor family the engine wires
 * a vertex with declares its stage this way, so that where a duration was spent is answered by the
 * processor that spent it and never guessed from a vertex's name. Topology and transparent authority
 * envelopes leave the processing family and its measurement with the actual business processor.
 */
public interface Staged {

    /** The stage this processor's time is measured under. */
    Stage stage();
}
