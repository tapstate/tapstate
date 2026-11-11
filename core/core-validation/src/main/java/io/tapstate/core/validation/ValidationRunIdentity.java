package io.tapstate.core.validation;

import java.util.Objects;

/** Fixed execution identity; no field is resolved through the task's latest run. */
public record ValidationRunIdentity(ValidationOwner owner, String taskId, String runId,
                                    ValidationMode mode, String specHash,
                                    long keyMappingRevision, String comparisonRulesVersion) {
    public ValidationRunIdentity {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(mode, "mode");
        taskId = ValidationError.identifier(taskId, "taskId");
        runId = ValidationError.identifier(runId, "runId");
        specHash = ValidationError.identifier(specHash, "specHash");
        comparisonRulesVersion = ValidationError.identifier(comparisonRulesVersion, "comparisonRulesVersion");
        ValidationError.positive(keyMappingRevision, "keyMappingRevision");
    }

    /** Method, spec revision, request origin and owner never create another mode slot. */
    public ModeSlot slot() {
        return new ModeSlot(owner.clusterId(), owner.pipelineId(), taskId, mode);
    }

    public record ModeSlot(String clusterId, String pipelineId, String taskId, ValidationMode mode) {
        public ModeSlot {
            clusterId = ValidationError.identifier(clusterId, "clusterId");
            pipelineId = ValidationError.identifier(pipelineId, "pipelineId");
            taskId = ValidationError.identifier(taskId, "taskId");
            Objects.requireNonNull(mode, "mode");
        }
    }
}
