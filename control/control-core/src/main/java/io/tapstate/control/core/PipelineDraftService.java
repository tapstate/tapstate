package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
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

    public PipelineDraftService(PipelineDraftStore store) {
        this(store, new PipelineDraftCompiler(), new CanonicalWriter(), Clock.systemUTC());
    }

    PipelineDraftService(PipelineDraftStore store, PipelineDraftCompiler compiler,
            CanonicalWriter writer, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.compiler = Objects.requireNonNull(compiler, "compiler");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.clock = Objects.requireNonNull(clock, "clock");
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

    public PipelineDraftMutation save(String pipelineId, long expectedRevision, PipelineDraft replacement) {
        return store.replace(Objects.requireNonNull(pipelineId, "pipelineId"), expectedRevision,
                Objects.requireNonNull(replacement, "replacement"));
    }

    /** Compiles the requested revision without writing an Artifact or publication marker. */
    public PipelineResource preview(String pipelineId, long expectedRevision) {
        PipelineDraft draft = store.get(Objects.requireNonNull(pipelineId, "pipelineId"))
                .orElseThrow(() -> new IllegalArgumentException("pipeline draft not found: " + pipelineId));
        if (draft.revision() != expectedRevision) {
            throw new IllegalStateException("pipeline draft revision conflict: " + pipelineId);
        }
        return compiler.compile(draft);
    }

    /**
     * Compiles and hands one conditional publication to the store. The store owns the transaction that
     * advances the Artifact and the draft markers; this service never performs a split write.
     */
    public PublishResult publish(String pipelineId, long expectedDraftRevision,
            String expectedArtifactHash, String updatedBy) {
        PipelineDraft draft = store.get(Objects.requireNonNull(pipelineId, "pipelineId"))
                .orElse(null);
        if (draft == null) {
            return new PublishResult(PipelineDraftMutation.NOT_FOUND, null, null);
        }
        if (draft.revision() != expectedDraftRevision) {
            return new PublishResult(PipelineDraftMutation.REVISION_CONFLICT, null, null);
        }
        PipelineResource artifact = compiler.compile(draft);
        String canonical = writer.write(artifact);
        String artifactHash = CanonicalHash.of(canonical);
        String baseHash = expectedArtifactHash == null ? draft.baseArtifactHash() : expectedArtifactHash;
        PipelineDraft.Publication publication = new PipelineDraft.Publication(
                pipelineId, expectedDraftRevision, baseHash, artifact, artifactHash,
                Instant.now(clock), updatedBy);
        PipelineDraftMutation outcome = store.publish(publication);
        return new PublishResult(outcome, outcome == PipelineDraftMutation.PUBLISHED ? artifact : null,
                outcome == PipelineDraftMutation.PUBLISHED ? artifactHash : null);
    }

    public record PublishResult(PipelineDraftMutation mutation, PipelineResource artifact, String artifactHash) {
        public boolean published() {
            return mutation == PipelineDraftMutation.PUBLISHED;
        }
    }
}
