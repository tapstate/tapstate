package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes a definition a start's answers changed, and carries the same change into the pipeline's draft in
 * the same transaction.
 *
 * <p>The draft is the editor's copy of the pipeline, and publishing it replaces the definition with what
 * the draft says. A draft left as it was would therefore undo the answer the next time it is published --
 * silently, if it is first moved onto the new definition without its content changing -- or refuse to
 * publish at all, because it is based on a definition that is no longer stored. Changing both together, or
 * neither, is what leaves the next publish neither refused nor undoing anything.
 *
 * <p>The definition is validated, planned and audited exactly as an edit of the pipeline is; only the store
 * write differs, because only the draft store can write the two in one transaction.
 */
public final class DraftSyncingDefinitionWriter implements StartDefinitionWriter {

    private final ApplyService apply;
    private final PipelineDraftStore drafts;
    private final AuditGate auditGate;
    private final Clock clock;

    public DraftSyncingDefinitionWriter(ApplyService apply, PipelineDraftStore drafts, AuditGate auditGate, Clock clock) {
        this.apply = Objects.requireNonNull(apply, "apply");
        this.drafts = Objects.requireNonNull(drafts, "drafts");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public String write(String principal, PipelineResource changed, String expectedContentHash,
            List<StartAction.Change> changes) {
        ApplyPlan plan = apply.planDraftPublication(changed);
        PreparedArtifact prepared = plan.artifacts().getFirst();
        PipelineDraft.DefinitionChange change = new PipelineDraft.DefinitionChange(changed.id(), expectedContentHash,
                prepared.resource(), prepared.contentHash(), plan.workspacePreconditions(),
                draft -> PipelineDraftEdits.carry(draft, changes, expectedContentHash, prepared.contentHash(),
                        clock.instant(), principal));
        PipelineDraftMutation outcome = auditGate.dispatch(ControlOperations.PIPELINE_UPDATE,
                new AuditContext(principal, changed.id(), expectedContentHash), () -> drafts.changeDefinition(change));
        if (outcome != PipelineDraftMutation.REPLACED) {
            throw new TapstateException(PipelineError.VERSION_CONFLICT, Map.of("id", changed.id()), null);
        }
        return prepared.contentHash();
    }
}
