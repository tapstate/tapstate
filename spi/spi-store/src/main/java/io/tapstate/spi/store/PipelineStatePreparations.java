package io.tapstate.spi.store;

import java.util.Optional;

/** Configuration-owned, one-time first-checkpoint preparation; no CACHE or lease IO is performed here. */
public interface PipelineStatePreparations {
    PipelineStatePreparations guardedBy(Runnable requireActive);

    /** Load-only, including UNASSIGNED; missing known facts are never initialized by a query. */
    Optional<PipelinePreparation> read(String pipelineId);

    /** Only this call's winning CAS may return a first-creation invocation, after commit is confirmed. */
    FirstPipelineCreationResult issueFirstCreate(PipelineIntentScope scope, WorkloadClaimFence originalNode);

    /** A source snapshot and resolved selection are mandatory when first preparing a private direct tail. */
    FirstPipelineCreationResult issueFirstCreate(PipelineIntentScope scope, WorkloadClaimFence originalNode, PipelinePreparationInputs inputs);

    /** Read-only same-domain validation of captured Pipeline and Source definitions. */
    void validateSources(PipelineIntentScope scope, PipelinePreparationInputs inputs);

    /** Performed after obtaining an existing CACHE reservation and before seeding with its original fence. */
    void requireIssued(PipelineIntentScope scope, String preparationId);

    /** Publishes the matching verified initial receipt; repeated completion returns existing facts. */
    PipelinePreparation complete(PipelinePreparationReceipt receipt);
}
