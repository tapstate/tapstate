package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/**
 * Which mining chains a pipeline reads.
 *
 * <p>A port, because this is the one part of the answer the control layer cannot work out for itself: a
 * chain's identity comes from the connector config, the selected tables and the srs key, and the side
 * that fills a chain and the side that reads it each derive it independently from those. A third
 * derivation here would be the first one to drift, and it would drift the way that hurts — reporting a
 * position from a chain no run is on, and writing one back to it.
 */
public interface PipelineChains {

    /** The chains {@code pipelineId} reads, in the order its sources are declared. */
    List<Chain> of(String pipelineId);

    /**
     * One chain a pipeline reads.
     *
     * @param chainId  the mining chain the ring and the durable record are both keyed by
     * @param sourceId the source resource this pipeline reads it through
     * @param tables   the tables selected on it, in the resolved order
     */
    record Chain(String chainId, String sourceId, List<String> tables) {

        public Chain {
            Objects.requireNonNull(chainId, "chainId");
            Objects.requireNonNull(sourceId, "sourceId");
            tables = tables == null ? List.of() : List.copyOf(tables);
        }
    }
}
