package io.tapstate.control.core;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * The capture ids of each of these pipelines, every one answered as {@link #captureIds} answers it
     * alone. Every pipeline asked about is in the answer.
     *
     * <p>For a caller answering for many pipelines at once. Pipelines share their sources, so working their
     * captures out one pipeline at a time reads a shared source again for every pipeline naming it; an
     * implementation that can read what they share once overrides this. The default asks
     * {@link #captureIds} for each in turn, which answers the same at that cost.
     */
    default Map<String, List<String>> captureIdsByPipeline(Collection<String> pipelineIds) {
        Map<String, List<String>> byPipeline = new LinkedHashMap<>();
        for (String pipelineId : pipelineIds) {
            byPipeline.put(pipelineId, captureIds(pipelineId));
        }
        return byPipeline;
    }

    /** A build that resolves no captures, which says so rather than reporting a pipeline with none. */
    static PipelineCaptures none() {
        return pipelineId -> List.of();
    }
}
