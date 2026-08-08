package io.tapstate.runtime.engine;

import java.util.Map;
import java.util.Objects;

/**
 * What propagating a frontier through a job needs that the engine will not decide: which chain each of
 * the pipeline's sources reads. The chain is the stream name the source stamps on every event it emits,
 * and resolving a source id to it means resolving the source's connector and config - which stays with
 * whoever wired the pipeline, so the engine still never sees the table universe.
 *
 * <p>Numbering the chains onto the axes bounds travel on follows from this and nothing else, so there is
 * one answer to what a chain is called rather than a name here and a numbering somewhere else that could
 * disagree. Both the assembler stamping at its sources and the builder compiling the levels ask this one
 * object, and the numbering is derived from the names in a fixed order, so asking twice cannot answer
 * differently.
 *
 * <p>A job built without one propagates no bounds at all: every level keeps what it has promised to
 * itself, exactly as it did before there was anywhere to send it. That is a job whose frontier does not
 * advance, never one that advances too far.
 */
public record FrontierBinding(Map<String, String> chainBySource) {

    public FrontierBinding {
        chainBySource = Map.copyOf(Objects.requireNonNull(chainBySource, "chainBySource"));
    }

    /**
     * The chain the source keyed {@code sourceId} reads. A source with none bound is a graph being built
     * from one binding and walked from another, so it fails rather than leaving that source's changes
     * outside every promise the job makes.
     */
    public String chainOf(String sourceId) {
        String chain = chainBySource.get(sourceId);
        if (chain == null) {
            throw new IllegalStateException("no chain is bound for source '" + sourceId
                    + "', so nothing can be said about how far what it reads has travelled");
        }
        return chain;
    }

    /** Which axis each chain this job draws from puts its bounds on. */
    public ChainAxes axes() {
        return ChainAxes.assign(chainBySource.values());
    }
}
