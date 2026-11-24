package io.tapstate.spi.store;

import java.util.Set;

/** CONFIG authorization for newly discovered SRS tables under unchanged declarations. */
public interface PipelineTableExpansion {
    PipelineIntentScope scope();
    PipelinePreparationInputs inputs();
    Set<String> dynamicSources();
    void requireCurrent();
    /** Requests one retaining reassembly only if the original intent and declarations are still current. */
    boolean requestRestart(PipelineIntentSnapshot original, long checkpointEpoch);
}
