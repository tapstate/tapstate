package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.DesiredState;
import java.util.List;
import java.util.Optional;

/** Configuration-owned latest intents; cache epochs are meaningful only within the captured scope. */
public interface PipelineIntentStore {
    /** Every transaction callback attempt retains and rechecks this original authorization. */
    PipelineIntentStore guardedBy(Runnable requireActive);

    /** Includes a deletion marker so an earlier writer cannot reuse revision zero after removal. */
    Optional<PipelineIntentSnapshot> load(String pipelineId);

    /**
     * The runtime-only read also requires this scope's completed CONFIG preparation. Unsupported
     * implementations refuse rather than using the ordinary authoring read as authorization.
     */
    default Optional<PipelineIntentSnapshot> loadForActuation(String pipelineId) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED, java.util.Map.of(), null);
    }


    /** Expected zero means never written; a refusal leaves the latest value and all versions unchanged. */
    boolean compareAndSet(PipelineIntentScope scope, long expectedIntentRevision, DesiredState desired);

    /** Only the current native full-recovery permission may publish its first RUNNING intent. */
    default boolean startRecovered(CacheRecoveryStore.FullPreparation confirmation,long expectedIntentRevision,String pipelineHash) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,java.util.Map.of(),null);
    }

    /** Retains only the next revision and coordinates, and removes the id from the reconcile set. */
    boolean remove(PipelineIntentScope scope, long expectedIntentRevision);

    /** Projects live ids only, without decoding intent bodies. */
    List<String> pipelineIds();
}
