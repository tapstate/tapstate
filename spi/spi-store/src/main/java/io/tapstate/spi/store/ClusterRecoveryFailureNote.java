package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** The first qualified failure for one successor, retained before its cached authority expires. */
public record ClusterRecoveryFailureNote(WorkloadClaimFence pipelineClaim,
        ClusterRecoveryStore.FailureStage stage, ClusterRecoveryDiagnostic diagnostic, Instant recordedAt) {
    public ClusterRecoveryFailureNote {
        pipelineClaim = Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        stage = Objects.requireNonNull(stage, "stage");
        diagnostic = Objects.requireNonNull(diagnostic, "diagnostic");
        recordedAt = Objects.requireNonNull(recordedAt, "recordedAt");
        if (pipelineClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION || pipelineClaim.executionGeneration() < 1
                || pipelineClaim.profileGeneration() < 1
                || (stage != ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION
                        && stage != ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION)
                || ((stage == ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION)
                        != (diagnostic.reason() == ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED))) {
            throw new IllegalArgumentException("failure note must describe one allocated successor and its actual failure stage");
        }
    }
}
