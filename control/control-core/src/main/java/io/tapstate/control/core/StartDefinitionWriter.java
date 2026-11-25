package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;

import java.util.List;

/**
 * Writes the definition a start's answers changed, held to the definition the answers were given against,
 * through the validation and the audit an edit of the pipeline goes through.
 */
@FunctionalInterface
public interface StartDefinitionWriter {

    /**
     * Writes {@code changed} in place of the definition stored under {@code expectedContentHash}, answering
     * with the content hash written; refused with {@code pipeline.version-conflict} when the stored definition
     * has moved.
     *
     * @param changes the fields changed, for a writer that carries the same change somewhere else as well
     */
    String write(String principal, PipelineResource changed, String expectedContentHash,
            List<StartAction.Change> changes);

    /** A writer with nowhere else to carry a change: the edit path a {@code PUT} of the pipeline takes. */
    static StartDefinitionWriter through(ApplyService apply) {
        return (principal, changed, expectedContentHash, changes) -> {
            ArtifactWriteResult written = apply.replace(
                    principal, changed, expectedContentHash, ControlOperations.PIPELINE_UPDATE);
            PipelineProjectionService.throwForWriteRefusal(changed.id(), written.write());
            return written.artifact().contentHash();
        };
    }
}
