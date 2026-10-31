package io.tapstate.spi.store;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineNode;
import java.util.*;

/** Original target preparation receipts, separate from operator state and necessary connector notes. */
public interface SinkPreparationStore {
    long MAX_MODEL_ROW_BYTES=1048576;
    long MAX_SCOPE_MODEL_BYTES=8388608;
    default boolean hasPreparedModels(){return false;}
    /** Original v2 body, before any assessment of a new observation; changed authoring cannot use this shortcut. */
    default Optional<Prepared> retainedModel(Key key,OnFullLoad policy,boolean fullLoad,PipelinePreparationInputs authoring){return Optional.empty();}
    default Map<Key,Prepared> retainedModels(Collection<Key> keys,OnFullLoad policy,boolean fullLoad,PipelinePreparationInputs authoring) {
        var result=new LinkedHashMap<Key,Prepared>();for(var key:keys)retainedModel(key,policy,fullLoad,authoring).ifPresent(model -> result.put(key,model));return Map.copyOf(result);
    }

    /** Read-only original model resolution; a legacy candidate must exactly match its recorded hash. */
    default Prepared resolveModel(Prepared candidate,PipelinePreparationInputs authoring) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",candidate.input().key().scope().pipelineId()),null);
    }
    default List<Prepared> resolveModels(List<Prepared> candidates,PipelinePreparationInputs authoring) {
        return candidates.stream().map(candidate -> resolveModel(candidate,authoring)).toList();
    }
    /** Freezes the complete roster and bodies, or atomically backfills exact-hash legacy candidates. */
    default void prepareModels(List<Prepared> originalTargets) {
        if(originalTargets.isEmpty()){prepareTargets(List.of());return;}
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",originalTargets.getFirst().input().key().scope().pipelineId()),null);
    }
    default void prepareTargets(List<Input> originalTargets,List<Prepared> models) {
        if(models.isEmpty())prepareTargets(originalTargets);
        else if(originalTargets.size()==models.size())prepareModels(models);
        else throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",originalTargets.getFirst().key().scope().pipelineId()),null);
    }
    record Prepared(Input input,byte[] canonical) {
        public Prepared { Objects.requireNonNull(input,"input");canonical=Objects.requireNonNull(canonical,"canonical").clone(); }
        @Override public byte[] canonical(){return canonical.clone();}
        @Override public boolean equals(Object other){return other instanceof Prepared p && input.equals(p.input) && Arrays.equals(canonical,p.canonical);}
        @Override public int hashCode(){return 31*input.hashCode()+Arrays.hashCode(canonical);}
    }
    /** Controlled planning freezes one complete original target roster before a target may be opened. */
    void prepareTargets(List<Input> originalTargets);
    Snapshot load(Input originalTarget);
    /** Issues the target call once; an already issued unknown attempt is never another permission. */
    Attempt begin(Input originalTarget);
    Snapshot complete(Attempt originalAttempt,Result originalResult);

    record Key(PipelineIntentScope scope,PipelineNode.Purpose purpose,String nodeId,String target) {
        public Key {
            Objects.requireNonNull(scope,"scope"); Objects.requireNonNull(purpose,"purpose");
            if (purpose == PipelineNode.Purpose.CAPTURE || nodeId == null || nodeId.isBlank() || target == null || target.isBlank())
                throw new IllegalArgumentException("target preparation requires one original view or sync node and table");
        }
    }
    record Input(Key key,String modelHash,OnFullLoad policy,boolean fullLoad) {
        public Input {
            Objects.requireNonNull(key,"key"); Objects.requireNonNull(policy,"policy");
            if (modelHash == null || !modelHash.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("target preparation requires a complete model hash");
        }
    }
    enum Phase { PREPARING, READY }
    record Result(boolean created) {}
    record Snapshot(Input input,String operationId,long revision,Phase phase,WorkloadClaimFence originalClaim,Result result) {
        public Snapshot {
            Objects.requireNonNull(input,"input"); Objects.requireNonNull(operationId,"operationId"); Objects.requireNonNull(phase,"phase");
            if (!UUID.fromString(operationId).toString().equals(operationId)
                    || phase == Phase.PREPARING && (revision < 0 || revision > 1 || result != null || (revision == 0) != (originalClaim == null))
                    || phase == Phase.READY && (revision != 2 || originalClaim == null || result == null))
                throw new IllegalArgumentException("invalid original target preparation phase");
            if (originalClaim != null && (originalClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                    || originalClaim.executionGeneration() < 1 || !originalClaim.key().clusterId().equals(input.key().scope().clusterId())
                    || !originalClaim.key().resourceId().equals(input.key().scope().pipelineId())))
                throw new IllegalArgumentException("target preparation belongs to a different Pipeline claim");
        }
    }
    interface Attempt { Snapshot original(); }
}
