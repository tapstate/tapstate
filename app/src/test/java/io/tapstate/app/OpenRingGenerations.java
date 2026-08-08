package io.tapstate.app;

import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StorePort;

/**
 * Opens a ring generation on the chains a set of sources read, standing in for the capture run that opens
 * them in production.
 *
 * <p>Assembling a job reads the generation its sources are reading under, and a live pipeline always has
 * one: starting a pipeline runs its capture before it builds the job, so the chain is open by the time the
 * source vertex is built. A test that builds a job on its own skips that step, so it seeds the same
 * durable state here rather than being handed a generation that no ring was ever opened under.
 */
final class OpenRingGenerations {

    private OpenRingGenerations() {
    }

    /** Seeds each source's chain if it has no record yet and opens a generation on it. */
    static void forSources(StorePort store, String... sourceIds) {
        SrsMetaStore meta = store.meta();
        for (String sourceId : sourceIds) {
            String chainId = SourceCaptureResolution
                    .of(StoredArtifacts.requireSource(store.artifacts(), sourceId))
                    .chainId()
                    .value();
            if (meta.read(chainId).isEmpty()) {
                meta.create(chainId, null);
            }
            meta.openEpoch(chainId);
        }
    }
}
