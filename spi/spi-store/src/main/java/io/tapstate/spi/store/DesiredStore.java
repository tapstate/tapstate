package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.DesiredState;
import java.util.List;
import java.util.Optional;

/**
 * The pipeline desired-state store: persists one desired-intent doc per pipeline — the target state a
 * user asked the pipeline to reach, at the artifact revision that intent was expressed against. A pure
 * interface over the desired-intent model in the core ring (rule R2); it exposes the persistence
 * surface only and does not decide convergence.
 *
 * <p>Desired intent is the split-off counterpart to the epoch-fencing pipeline state store: it is
 * plain intent, not a fenced transition, so it is a plain upsert by pipeline id rather than a
 * compare-and-swap. {@link #save} upserts the desired doc for its pipeline (last write wins);
 * {@link #read} returns the current desired doc for a pipeline, or empty when none is set;
 * {@link #pipelineIds} returns the id of every pipeline with a stored intent, the set the converge
 * side reconciles.
 */
public interface DesiredStore {

    /** Upserts the desired intent for its pipeline id (last write wins). */
    void save(DesiredState desired);

    /** Returns the current desired intent for a pipeline, or empty if none is set. */
    Optional<DesiredState> read(String pipelineId);

    record StartIntent(Optional<DesiredState> desired, Optional<PipelineIntentScope> scope, long revision) {}
    record StartPreparation(PipelineIntentScope scope, PipelinePreparationInputs inputs) {}

    default StartIntent readForStart(String pipelineId) {
        return new StartIntent(read(pipelineId), Optional.empty(), 0);
    }

    default void saveStart(DesiredState desired, StartIntent original) {
        save(desired);
    }

    default void saveStart(DesiredState desired, StartIntent original, Optional<StartPreparation> preparation) {
        saveStart(desired, original);
    }

    /**
     * Lists the id of every pipeline that has a stored desired intent — the set the converge side
     * reconciles. It returns ids only, not reconstructed intents, so enumerating the set never fails on
     * a single corrupt document; a corrupt intent surfaces per pipeline when its {@link #read} is taken.
     */
    List<String> pipelineIds();

    /**
     * Removes a pipeline's desired intent, so {@link #pipelineIds} no longer offers it to the converge
     * side. This is what keeps a removed pipeline from being reconciled forever against an artifact that
     * no longer exists.
     *
     * <p>Removing an intent that is not there is a no-op, not an error: a pipeline that was never given
     * a target state has none to remove, and the caller reclaiming after a removal should not have to
     * ask first — nor should a second attempt behave differently from the first.
     */
    void delete(String pipelineId);
}
