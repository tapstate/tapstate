package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import io.tapstate.spi.store.PipelineDraftSummary;

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
    private final ApplyService validation;
    private final ArtifactQueryService artifacts;

    public PipelineDraftService(PipelineDraftStore store) {
        this(store, new PipelineDraftCompiler(), new CanonicalWriter(), Clock.systemUTC(), null, null, null);
    }

    PipelineDraftService(PipelineDraftStore store, PipelineDraftCompiler compiler,
            CanonicalWriter writer, Clock clock) {
        this(store, compiler, writer, clock, null, null, null);
    }

    public PipelineDraftService(PipelineDraftStore store, AuditGate auditGate) {
        this(store, new PipelineDraftCompiler(), new CanonicalWriter(), Clock.systemUTC(), auditGate, null, null);
    }

    public PipelineDraftService(PipelineDraftStore store, AuditGate auditGate, ApplyService validation) {
        this(store, new PipelineDraftCompiler(), new CanonicalWriter(), Clock.systemUTC(), auditGate, validation, null);
    }

    public PipelineDraftService(PipelineDraftStore store, ArtifactQueryService artifacts,
            AuditGate auditGate, ApplyService validation) {
        this(store, new PipelineDraftCompiler(), new CanonicalWriter(), Clock.systemUTC(), auditGate,
                validation, artifacts);
    }

    PipelineDraftService(PipelineDraftStore store, PipelineDraftCompiler compiler,
            CanonicalWriter writer, Clock clock, AuditGate auditGate) {
        this(store, compiler, writer, clock, auditGate, null, null);
    }

    PipelineDraftService(PipelineDraftStore store, PipelineDraftCompiler compiler,
            CanonicalWriter writer, Clock clock, AuditGate auditGate, ApplyService validation) {
        this(store, compiler, writer, clock, auditGate, validation, null);
    }

    private PipelineDraftService(PipelineDraftStore store, PipelineDraftCompiler compiler,
            CanonicalWriter writer, Clock clock, AuditGate auditGate, ApplyService validation,
            ArtifactQueryService artifacts) {
        this.store = Objects.requireNonNull(store, "store");
        this.compiler = Objects.requireNonNull(compiler, "compiler");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.auditGate = auditGate;
        this.validation = validation;
        this.artifacts = artifacts;
    }

    public Optional<PipelineDraft> find(String pipelineId) {
        return store.get(Objects.requireNonNull(pipelineId, "pipelineId"));
    }

    public List<PipelineDraft> list() {
        return store.list();
    }

    public List<PipelineDraft> list(int offset, int limit) {
        return store.list(offset, limit);
    }

    public List<PipelineDraftSummary> listSummaries() {
        return store.listSummaries();
    }

    public List<PipelineDraftSummary> listSummaries(int offset, int limit) {
        return store.listSummaries(offset, limit);
    }

    public PipelineDraftMutation create(PipelineDraft draft) {
        Objects.requireNonNull(draft, "draft");
        Instant now = Instant.now(clock);
        StoredResource existing = artifacts == null ? null
                : artifacts.getResource(draft.pipelineId()).orElse(null);
        if (existing != null && !(existing.resource() instanceof PipelineResource)) {
            return PipelineDraftMutation.ARTIFACT_CONFLICT;
        }
        String baseArtifactHash = existing == null ? null : existing.contentHash();
        if (!Objects.equals(draft.baseArtifactHash(), baseArtifactHash)) {
            return PipelineDraftMutation.ARTIFACT_CONFLICT;
        }
        return store.create(new PipelineDraft(draft.pipelineId(), draft.schemaVersion(), 1, draft.mode(),
                draft.name(), draft.description(), draft.graph(), draft.wizard(), baseArtifactHash, null, null,
                now, now, draft.updatedBy()));
    }

    public PipelineDraftMutation create(String principal, PipelineDraft draft) {
        return audited(ControlOperations.PIPELINE_DRAFT_CREATE, principal, draft.pipelineId(),
                () -> create(withActor(draft, principal)));
    }

    public PipelineDraftMutation save(String pipelineId, long expectedRevision, PipelineDraft replacement) {
        return saveOwned(pipelineId, expectedRevision, replacement, replacement.updatedBy());
    }

    public PipelineDraftMutation save(String principal, String pipelineId, long expectedRevision,
            PipelineDraft replacement) {
        return audited(ControlOperations.PIPELINE_DRAFT_REPLACE, principal, pipelineId,
                () -> saveOwned(pipelineId, expectedRevision, replacement, principal));
    }

    private PipelineDraftMutation saveOwned(String pipelineId, long expectedRevision,
            PipelineDraft replacement, String principal) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(replacement, "replacement");
        PipelineDraft current = store.get(pipelineId).orElse(null);
        if (current == null) {
            return PipelineDraftMutation.NOT_FOUND;
        }
        if (current.mode() != replacement.mode()) {
            return PipelineDraftMutation.MODE_CONFLICT;
        }
        PipelineDraft serverOwned = new PipelineDraft(pipelineId, replacement.schemaVersion(),
                expectedRevision + 1, current.mode(), replacement.name(), replacement.description(),
                replacement.graph(), replacement.wizard(), current.baseArtifactHash(),
                current.publishedDraftRevision(), current.publishedArtifactHash(), current.createdAt(),
                Instant.now(clock), requirePrincipal(principal));
        return store.replace(pipelineId, expectedRevision, serverOwned);
    }

    public PipelineDraftMutation discard(String principal, String pipelineId, long expectedRevision) {
        return audited(ControlOperations.PIPELINE_DRAFT_DELETE, principal, pipelineId,
                () -> store.delete(Objects.requireNonNull(pipelineId, "pipelineId"), expectedRevision));
    }

    /** Explicitly adopts the artifact version the caller has just read after a publish conflict. */
    public PipelineDraftMutation rebase(String principal, String pipelineId, long expectedRevision,
            String expectedArtifactHash) {
        return audited(ControlOperations.PIPELINE_DRAFT_REBASE, principal, pipelineId,
                () -> rebaseOwned(pipelineId, expectedRevision, expectedArtifactHash, principal));
    }

    private PipelineDraftMutation rebaseOwned(String pipelineId, long expectedRevision,
            String expectedArtifactHash, String principal) {
        PipelineDraft current = store.get(Objects.requireNonNull(pipelineId, "pipelineId")).orElse(null);
        if (current == null) {
            return PipelineDraftMutation.NOT_FOUND;
        }
        if (current.revision() != expectedRevision) {
            return PipelineDraftMutation.REVISION_CONFLICT;
        }
        StoredResource artifact = artifacts == null ? null : artifacts.getResource(pipelineId).orElse(null);
        if (artifact != null && !(artifact.resource() instanceof PipelineResource)) {
            return PipelineDraftMutation.ARTIFACT_CONFLICT;
        }
        String actualHash = artifact == null ? null : artifact.contentHash();
        if (!Objects.equals(expectedArtifactHash, actualHash)) {
            return PipelineDraftMutation.ARTIFACT_CONFLICT;
        }
        PipelineDraft rebased = new PipelineDraft(current.pipelineId(), current.schemaVersion(),
                expectedRevision + 1, current.mode(), current.name(), current.description(), current.graph(),
                current.wizard(), actualHash, current.publishedDraftRevision(), current.publishedArtifactHash(),
                current.createdAt(), Instant.now(clock), requirePrincipal(principal));
        return store.rebase(pipelineId, expectedRevision, current.baseArtifactHash(), rebased);
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
        PipelineResource candidate = compile(pipelineId, draft);
        return validateAndPrepare(pipelineId, candidate);
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
        if (expectedArtifactHash != null && !Objects.equals(expectedArtifactHash, draft.baseArtifactHash())) {
            return new PublishResult(PipelineDraftMutation.ARTIFACT_CONFLICT, null, null);
        }
        PipelineResource candidate = compile(pipelineId, draft);
        ApplyPlan plan = validation == null ? null : validation.planDraftPublication(updatedBy, candidate);
        PipelineResource artifact = plan == null ? candidate
                : (PipelineResource) plan.artifacts().getFirst().resource();
        String artifactHash = CanonicalHash.of(artifact);
        PipelineDraft.Publication publication = new PipelineDraft.Publication(
                pipelineId, expectedDraftRevision, draft.baseArtifactHash(), artifact, artifactHash,
                Instant.now(clock), updatedBy, plan == null ? java.util.Map.of() : plan.workspacePreconditions());
        PipelineDraftMutation outcome = store.publish(publication);
        List<ValidationDiagnostic> warnings = outcome == PipelineDraftMutation.PUBLISHED && validation != null
                ? validation.refreshPublishedPipeline(pipelineId) : List.of();
        return new PublishResult(outcome, outcome == PipelineDraftMutation.PUBLISHED ? artifact : null,
                outcome == PipelineDraftMutation.PUBLISHED ? artifactHash : null, warnings);
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

    private PipelineResource validateAndPrepare(String pipelineId, PipelineResource candidate) {
        if (validation == null) {
            return candidate;
        }
        try {
            return (PipelineResource) validation.prepareTyped(candidate);
        } catch (TapstateException error) {
            String reason = validationReason(new ValidationDiagnostic(error.code().code(), error.args()));
            throw new TapstateException(PipelineDraftError.INVALID,
                    java.util.Map.of("id", pipelineId, "reason", reason), error);
        }
    }

    static String validationReason(ValidationDiagnostic diagnostic) {
        Object detail = diagnostic.params().get("detail");
        if (detail instanceof String message && !message.isBlank()) {
            return diagnostic.code() + ": " + message;
        }
        StringBuilder reason = new StringBuilder(diagnostic.code());
        Object path = diagnostic.params().get("path");
        if (path instanceof String value && !value.isBlank()) {
            reason.append(" at ").append(value);
        }
        if ("dsl.upsert-needs-key".equals(diagnostic.code())) {
            Object source = diagnostic.params().get("source");
            Object table = diagnostic.params().get("table");
            if (source instanceof String sourceId && table instanceof String tableName) {
                reason.append(" (source: ").append(sourceId).append(", table: ").append(tableName).append(')');
            }
        }
        Object ref = diagnostic.params().get("ref");
        if (ref instanceof String value && !value.isBlank()) {
            reason.append(" (ref: ").append(value).append(')');
        }
        return reason.toString();
    }

    private static PipelineDraft withActor(PipelineDraft draft, String principal) {
        return new PipelineDraft(draft.pipelineId(), draft.schemaVersion(), draft.revision(), draft.mode(),
                draft.name(), draft.description(), draft.graph(), draft.wizard(), draft.baseArtifactHash(),
                draft.publishedDraftRevision(), draft.publishedArtifactHash(), draft.createdAt(),
                draft.updatedAt(), requirePrincipal(principal));
    }

    private static String requirePrincipal(String principal) {
        if (principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("authenticated principal must not be blank");
        }
        return principal;
    }

    private <T> T audited(Operation operation, String principal, String pipelineId,
            java.util.function.Supplier<T> action) {
        if (auditGate == null) {
            return action.get();
        }
        return auditGate.dispatch(operation, new AuditContext(principal, pipelineId), action);
    }

    public record PublishResult(PipelineDraftMutation mutation, PipelineResource artifact, String artifactHash,
            List<ValidationDiagnostic> warnings) {
        public PublishResult {
            warnings = warnings == null ? List.of() : List.copyOf(warnings);
        }

        public PublishResult(PipelineDraftMutation mutation, PipelineResource artifact, String artifactHash) {
            this(mutation, artifact, artifactHash, List.of());
        }

        public boolean published() {
            return mutation == PipelineDraftMutation.PUBLISHED;
        }
    }
}
