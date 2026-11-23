package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.DesiredState;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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


    /** The configuration revision belongs to the same snapshot as this runtime intent, when supported. */
    record ActuationReading(PipelineIntentSnapshot intent, java.util.OptionalLong configurationRevision) {}

    default Optional<ActuationReading> readForActuation(String pipelineId) {
        return loadForActuation(pipelineId).map(intent -> new ActuationReading(intent, java.util.OptionalLong.empty()));
    }

    /** Expected zero means never written; a refusal leaves the latest value and all versions unchanged. */
    boolean compareAndSet(PipelineIntentScope scope, long expectedIntentRevision, DesiredState desired);

    /** A durable request to prepare exactly the definition and intent that HTTP accepted. */
    record PendingStart(PipelineIntentSnapshot intent, PipelinePreparationInputs inputs) {}

    default boolean compareAndSetStart(PipelineIntentScope scope, long expected, DesiredState desired,
            PipelinePreparationInputs inputs) {
        throw new UnsupportedOperationException("deferred first start is not supported");
    }

    default Optional<PendingStart> pendingStart(String pipelineId) { return Optional.empty(); }

    default void failStart(PendingStart pending, IoError error) {
        throw new UnsupportedOperationException("deferred first start is not supported");
    }

    /** Records an optional public diagnostic for the same pending start and under the same fence. */
    default void failStart(PendingStart pending, IoError error, Map<String, Object> params) {
        failStart(pending, error);
    }

    /** Only the current native full-recovery permission may publish its first RUNNING intent. */
    default boolean startRecovered(CacheRecoveryStore.FullPreparation confirmation,long expectedIntentRevision,String pipelineHash) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,java.util.Map.of(),null);
    }

    /** Retains only the next revision and coordinates, and removes the id from the reconcile set. */
    boolean remove(PipelineIntentScope scope, long expectedIntentRevision);

    /** Scheduling hints only; the admitted pass still reads its original intent and proves ownership. */
    record DispatchSnapshot(List<String> pipelineIds, Set<String> stopping) {
        public DispatchSnapshot {
            pipelineIds = List.copyOf(pipelineIds);
            stopping = Set.copyOf(stopping);
        }
    }

    default Optional<DispatchSnapshot> dispatchSnapshot() { return Optional.empty(); }

    /** Projects live ids only, without decoding intent bodies. */
    List<String> pipelineIds();

    /** Scheduling hint only. Zero means no new RUNNING intent to prioritize. */
    record ScanCandidate(String pipelineId, long startRevision, PipelineIntentScope scope,
            long intentRevision, io.tapstate.core.lifecycle.PipelineState targetState) {
        public ScanCandidate(String pipelineId, long startRevision) {
            this(pipelineId, startRevision, null, 0, null);
        }
    }

    /** The scheduler must still load and prove the original intent before acting. */
    default List<ScanCandidate> scanCandidates() {
        return pipelineIds().stream().map(id -> new ScanCandidate(id, 0)).toList();
    }
}
