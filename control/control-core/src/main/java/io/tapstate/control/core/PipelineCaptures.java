package io.tapstate.control.core;

import java.util.List;

/**
 * The captures a pipeline reads through, by the identity their claims are filed under.
 *
 * <p>Derived, not live: a capture's identity is a function of the stored source contract, so every member
 * computes the same ids from the same artifacts without asking anybody. It is a port only because that
 * derivation lives above this ring, and a node that asked the member currently running the capture would
 * get an answer only that member could give -- which is exactly the property the read face must not have.
 */
@FunctionalInterface
public interface PipelineCaptures {

    /** The capture ids this pipeline reads through, or empty when it reads through none. */
    List<String> captureIds(String pipelineId);

    /** A build that resolves no captures, which says so rather than reporting a pipeline with none. */
    static PipelineCaptures none() {
        return pipelineId -> List.of();
    }
}
