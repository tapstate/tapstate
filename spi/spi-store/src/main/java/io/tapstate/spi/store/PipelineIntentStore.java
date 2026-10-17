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

    /** Expected zero means never written; a refusal leaves the latest value and all versions unchanged. */
    boolean compareAndSet(PipelineIntentScope scope, long expectedIntentRevision, DesiredState desired);

    /** Retains only the next revision and coordinates, and removes the id from the reconcile set. */
    boolean remove(PipelineIntentScope scope, long expectedIntentRevision);

    /** Projects live ids only, without decoding intent bodies. */
    List<String> pipelineIds();
}
