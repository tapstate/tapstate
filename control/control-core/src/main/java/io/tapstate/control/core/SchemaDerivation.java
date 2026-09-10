package io.tapstate.control.core;

/**
 * Works a pipeline's own model out again from the world it reads, and records it.
 *
 * <p><b>Why apply needs this at all.</b> A pipeline holds its own copy of what its sources were
 * discovered to be, rather than reading the discovery live - which is what stops the shape of a run's
 * input moving under a run already using it. The cost of a copy is that it has to be refreshed by
 * something, and the case that most needs refreshing is the one where the pipeline document itself did
 * not change: a source moved, the author re-applied, and the artifact's content hash is byte-identical.
 * Apply skips the write for that batch, so anything hung on "did we write" would fail to fire in
 * exactly the case it exists for. This runs whether or not anything was written.
 *
 * <p>The drift gate belongs to start, not apply. A refresh may still fail with a coded diagnostic,
 * including a live pipeline whose models cannot be refreshed yet. Apply has already committed its
 * artifacts when it calls this port: it reports such failures as per-pipeline warnings and attempts
 * the remaining pipelines. A failure may leave some models refreshed and others unchanged; the
 * comparison face exposes that drift, and a subsequent apply retries even when the artifact is unchanged.
 * Programmer errors remain uncaught.
 *
 * <p>Implemented above the layer that can assemble a pipeline, which is why apply reaches it through an
 * interface rather than calling it. An assembly that has no derivation to offer says so by naming
 * {@link #none()} - a service that silently derived nothing would be indistinguishable from one whose
 * every pipeline was already up to date.
 */
public interface SchemaDerivation {

    /**
     * Re-derives and records the model of one pipeline. A pipeline whose sources have never been
     * discovered records nothing and is not an error: authoring against an undiscovered source is
     * allowed, and the start that needs the model refuses on its own.
     */
    void derive(String pipelineId);

    /** Derives nothing, for an assembly that runs no derivation. Named, never defaulted. */
    static SchemaDerivation none() {
        return pipelineId -> {
        };
    }
}
