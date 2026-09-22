package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Application service for durable draft CAS, preview compilation, and atomic publication. */
public final class PipelineDraftService {

    private final PipelineDraftStore store;
    private final PipelineDraftCompiler compiler;
    private final CanonicalWriter writer;
    private final Clock clock;
    private final AuditGate auditGate;

    public PipelineDraftService(PipelineDraftStore store) {
        this(store, new PipelineDraftCompiler(), new CanonicalWriter(), Clock.systemUTC(), null);
    }

    PipelineDraftService(PipelineDraftStore store, PipelineDraftCompiler compiler,
            CanonicalWriter writer, Clock clock) {
        this(store, compiler, writer, clock, null);
    }

    public PipelineDraftService(PipelineDraftStore store, AuditGate auditGate) {
        this(store, new PipelineDraftCompiler(), new CanonicalWriter(), Clock.systemUTC(), auditGate);
    }

    PipelineDraftService(PipelineDraftStore store, PipelineDraftCompiler compiler,
            CanonicalWriter writer, Clock clock, AuditGate auditGate) {
        this.store = Objects.requireNonNull(store, "store");
        this.compiler = Objects.requireNonNull(compiler, "compiler");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.auditGate = auditGate;
    }

    public Optional<PipelineDraft> find(String pipelineId) {
        return store.get(Objects.requireNonNull(pipelineId, "pipelineId"));
    }

    public List<PipelineDraft> list() {
        return store.list();
    }

    public PipelineDraftMutation create(PipelineDraft draft) {
        return store.create(Objects.requireNonNull(draft, "draft"));
    }

    public PipelineDraftMutation create(String principal, PipelineDraft draft) {
        return audited(ControlOperations.PIPELINE_DRAFT_CREATE, principal, draft.pipelineId(),
                () -> create(draft));
    }

    public PipelineDraftMutation save(String pipelineId, long expectedRevision, PipelineDraft replacement) {
        return store.replace(Objects.requireNonNull(pipelineId, "pipelineId"), expectedRevision,
                Objects.requireNonNull(replacement, "replacement"));
    }

    public PipelineDraftMutation save(String principal, String pipelineId, long expectedRevision,
            PipelineDraft replacement) {
        return audited(ControlOperations.PIPELINE_DRAFT_REPLACE, principal, pipelineId,
                () -> save(pipelineId, expectedRevision, replacement));
    }

    public PipelineDraftMutation discard(String principal, String pipelineId, long expectedRevision) {
        return audited(ControlOperations.PIPELINE_DRAFT_DELETE, principal, pipelineId,
                () -> store.delete(Objects.requireNonNull(pipelineId, "pipelineId"), expectedRevision));
    }

    /** Compiles the requested revision without writing an Artifact or publication marker. */
    public PipelineResource preview(String pipelineId, long expectedRevision) {
        PipelineDraft draft = store.get(Objects.requireNonNull(pipelineId, "pipelineId"))
                .orElseThrow(() -> new TapstateException(
                        PipelineDraftError.NOT_FOUND, java.util.Map.of("id", pipelineId), null));
        if (draft.revision() != expectedRevision) {
            throw new TapstateException(
                    PipelineDraftError.REVISION_CONFLICT, java.util.Map.of("id", pipelineId), null);
        }
        return compile(pipelineId, draft);
    }

    /**
     * Compiles and hands one conditional publication to the store. The store owns the transaction that
     * advances the Artifact and the draft markers; this service never performs a split write.
     */
    public PublishResult publish(String pipelineId, long expectedDraftRevision,
            String expectedArtifactHash, String updatedBy) {
        return audited(ControlOperations.PIPELINE_DRAFT_PUBLISH, updatedBy, pipelineId,
                () -> publishUnchecked(pipelineId, expectedDraftRevision, expectedArtifactHash, updatedBy));
    }

    private PublishResult publishUnchecked(String pipelineId, long expectedDraftRevision,
            String expectedArtifactHash, String updatedBy) {
        PipelineDraft draft = store.get(Objects.requireNonNull(pipelineId, "pipelineId"))
                .orElse(null);
        if (draft == null) {
            return new PublishResult(PipelineDraftMutation.NOT_FOUND, null, null);
        }
        if (draft.revision() != expectedDraftRevision) {
            return new PublishResult(PipelineDraftMutation.REVISION_CONFLICT, null, null);
        }
        PipelineResource artifact = compile(pipelineId, draft);
        String artifactHash = CanonicalHash.of(artifact);
        String baseHash = expectedArtifactHash == null ? draft.baseArtifactHash() : expectedArtifactHash;
        PipelineDraft.Publication publication = new PipelineDraft.Publication(
                pipelineId, expectedDraftRevision, baseHash, artifact, artifactHash,
                Instant.now(clock), updatedBy);
        PipelineDraftMutation outcome = store.publish(publication);
        return new PublishResult(outcome, outcome == PipelineDraftMutation.PUBLISHED ? artifact : null,
                outcome == PipelineDraftMutation.PUBLISHED ? artifactHash : null);
    }

    private PipelineResource compile(String pipelineId, PipelineDraft draft) {
        try {
            return compiler.compile(draft);
        } catch (IllegalArgumentException error) {
            throw new TapstateException(PipelineDraftError.INVALID,
                    java.util.Map.of("id", pipelineId,
                            "reason", error.getMessage() == null ? "draft cannot be compiled" : error.getMessage()),
                    error);
        }
    }

    private <T> T audited(Operation operation, String principal, String pipelineId,
            java.util.function.Supplier<T> action) {
        if (auditGate == null) {
            return action.get();
        }
        return auditGate.dispatch(operation, new AuditContext(principal, pipelineId), action);
    }

    public record PublishResult(PipelineDraftMutation mutation, PipelineResource artifact, String artifactHash) {
        public boolean published() {
            return mutation == PipelineDraftMutation.PUBLISHED;
        }
    }
}
