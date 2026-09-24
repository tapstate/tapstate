package io.tapstate.spi.store;

import java.util.Objects;

/** The result of one atomic artifact batch write. */
public record ArtifactBatchWrite(String refusedId, ArtifactMutation refusal) {

    public ArtifactBatchWrite {
        if (refusedId == null) {
            if (refusal != null) {
                throw new IllegalArgumentException("an applied batch cannot carry a refusal");
            }
        } else {
            Objects.requireNonNull(refusal, "refusal");
        }
    }

    /** A batch whose every requested write took effect. */
    public static ArtifactBatchWrite applied() {
        return new ArtifactBatchWrite(null, null);
    }

    /** A batch that wrote nothing because one named condition did not hold. */
    public static ArtifactBatchWrite refused(String id, ArtifactMutation outcome) {
        return new ArtifactBatchWrite(Objects.requireNonNull(id, "id"), Objects.requireNonNull(outcome, "outcome"));
    }

    /** Whether every write in the batch took effect. */
    public boolean appliedSuccessfully() {
        return refusedId == null;
    }
}
