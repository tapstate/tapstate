package io.tapstate.spi.store;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineNode;
import java.util.*;

/** Original target preparation receipts, separate from operator state and necessary connector notes. */
public interface SinkPreparationStore {
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
