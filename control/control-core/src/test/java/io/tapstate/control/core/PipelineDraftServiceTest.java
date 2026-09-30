package io.tapstate.control.core;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineDraftServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-24T00:00:00Z");
    private final MemoryDraftStore store = new MemoryDraftStore();
    private final PipelineDraftService service = new PipelineDraftService(store,
            new PipelineDraftCompiler(), new CanonicalWriter(), Clock.fixed(NOW, ZoneOffset.UTC), null, null);

    @Test
    void createAssignsInitialRevisionAndServerOwnedMetadata() {
        PipelineDraft submitted = draft(9, "client", null, 8L, "forged-published",
                Instant.EPOCH, Instant.EPOCH);

        assertThat(service.create("alice", submitted)).isEqualTo(PipelineDraftMutation.CREATED);

        PipelineDraft saved = store.get("orders").orElseThrow();
        assertThat(saved.revision()).isEqualTo(1);
        assertThat(saved.baseArtifactHash()).isNull();
        assertThat(saved.publishedDraftRevision()).isNull();
        assertThat(saved.publishedArtifactHash()).isNull();
        assertThat(saved.createdAt()).isEqualTo(NOW);
        assertThat(saved.updatedAt()).isEqualTo(NOW);
        assertThat(saved.updatedBy()).isEqualTo("alice");
    }

    @Test
    void rebaseRequiresTheCurrentArtifactHashAndAdvancesTheDraftRevision() {
        store.seed(draft(5, "alice", "old-hash", 4L, "published-hash", NOW, NOW));

        assertThat(service.rebase("bob", "orders", 5, "new-hash"))
                .isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
        assertThat(service.rebase("bob", "orders", 5, null)).isEqualTo(PipelineDraftMutation.REPLACED);

        PipelineDraft rebased = store.get("orders").orElseThrow();
        assertThat(rebased.revision()).isEqualTo(6);
        assertThat(rebased.baseArtifactHash()).isNull();
        assertThat(rebased.publishedDraftRevision()).isEqualTo(4L);
        assertThat(rebased.publishedArtifactHash()).isEqualTo("published-hash");
        assertThat(rebased.updatedBy()).isEqualTo("bob");
    }

    @Test
    void createRejectsAnUnverifiedClientArtifactHash() {
        assertThat(service.create(draft(1, "alice", "unverified", null, null, NOW, NOW)))
                .isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
        assertThat(store.get("orders")).isEmpty();
    }

    @Test
    void savePreservesPublicationBaseAndCreationMetadata() {
        store.seed(draft(5, "original", "server-base", 4L, "server-published",
                Instant.parse("2026-09-20T00:00:00Z"), Instant.parse("2026-09-23T00:00:00Z")));
        PipelineDraft replacement = draft(88, "client", "forged-base", 77L, "forged-published",
                Instant.EPOCH, Instant.EPOCH);

        assertThat(service.save("bob", "orders", 5, replacement)).isEqualTo(PipelineDraftMutation.REPLACED);

        PipelineDraft saved = store.get("orders").orElseThrow();
        assertThat(saved.revision()).isEqualTo(6);
        assertThat(saved.baseArtifactHash()).isEqualTo("server-base");
        assertThat(saved.publishedDraftRevision()).isEqualTo(4L);
        assertThat(saved.publishedArtifactHash()).isEqualTo("server-published");
        assertThat(saved.createdAt()).isEqualTo(Instant.parse("2026-09-20T00:00:00Z"));
        assertThat(saved.updatedAt()).isEqualTo(NOW);
        assertThat(saved.updatedBy()).isEqualTo("bob");
    }

    @Test
    void findAndListExposeTheDraftStoreReadModel() {
        PipelineDraft saved = draft(2, "alice", null, null, null, NOW, NOW);
        store.seed(saved);

        assertThat(service.find("orders")).contains(saved);
        assertThat(service.find("missing")).isEmpty();
        assertThat(service.list()).containsExactly(saved);
    }

    @Test
    void saveReturnsNotFoundAndModeConflictWithoutReplacingTheDraft() {
        PipelineDraft current = draft(1, "alice", null, null, null, NOW, NOW);
        store.seed(current);
        PipelineDraft dag = new PipelineDraft("orders", 1, 1, PipelineDraft.Mode.DAG,
                "Orders", "", new PipelineDraft.Graph(List.of(), List.of(),
                        new PipelineDraft.Viewport(0, 0, 1)), null,
                null, null, null, NOW, NOW, "bob");

        assertThat(service.save("orders", 1, current)).isEqualTo(PipelineDraftMutation.REPLACED);
        assertThat(service.save("missing", 1, current)).isEqualTo(PipelineDraftMutation.NOT_FOUND);
        store.seed(current);
        assertThat(service.save("orders", 1, dag)).isEqualTo(PipelineDraftMutation.MODE_CONFLICT);
        assertThat(store.get("orders")).contains(current);
    }

    @Test
    void discardUsesExpectedRevisionAndRemovesOnlyTheMatchingDraft() {
        store.seed(draft(3, "alice", null, null, null, NOW, NOW));

        assertThat(service.discard("alice", "orders", 2)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
        assertThat(service.find("orders")).isPresent();
        assertThat(service.discard("alice", "orders", 3)).isEqualTo(PipelineDraftMutation.DELETED);
        assertThat(service.find("orders")).isEmpty();
        assertThat(service.discard("alice", "orders", 3)).isEqualTo(PipelineDraftMutation.NOT_FOUND);
    }

    @Test
    void recreatedDraftDoesNotReuseRevisionAndRejectsRequestsFromItsPreviousIncarnation() {
        PipelineDraft initial = draft(1, "alice", null, null, null, NOW, NOW);
        assertThat(service.create(initial)).isEqualTo(PipelineDraftMutation.CREATED);
        assertThat(service.discard("alice", "orders", 1)).isEqualTo(PipelineDraftMutation.DELETED);

        assertThat(service.create(initial)).isEqualTo(PipelineDraftMutation.CREATED);
        PipelineDraft recreated = service.find("orders").orElseThrow();
        assertThat(recreated.revision()).isEqualTo(2);
        assertThat(service.save("alice", "orders", 1, initial)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
        assertThat(service.publish("orders", 1, null, "alice").mutation())
                .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
        assertThat(service.find("orders")).contains(recreated);
    }

    @Test
    void previewCompilesOnlyTheRequestedRevision() {
        PipelineDraft complete = draft(4, "alice", null, null, null, NOW, NOW);
        store.seed(complete);

        PipelineResource preview = service.preview("orders", 4);

        assertThat(preview.id()).isEqualTo("orders");
        assertThat(((io.tapstate.core.model.ServeBlock.Inline) preview.serve()).sync().getFirst().id())
                .isEqualTo("atlas_orders");
        assertThatThrownBy(() -> service.preview("missing", 1))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> service.preview("orders", 3))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }

    @Test
    void previewWrapsCompilerFailuresAsInvalidDrafts() {
        PipelineDraft incomplete = new PipelineDraft("orders", 1, 2, PipelineDraft.Mode.WIZARD,
                "Orders", "", null,
                new PipelineDraft.Wizard(new PipelineDraft.Root("root", "crm", "orders", List.of(), List.of()),
                        List.of(), List.of(), null),
                null, null, null, NOW, NOW, "alice");
        store.seed(incomplete);

        assertThatThrownBy(() -> service.preview("orders", 2))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("wizard output is required for publication");
    }

    @Test
    void publishReturnsTheArtifactOnlyAfterTheStorePublishesIt() {
        PipelineDraft complete = draft(6, "alice", null, null, null, NOW, NOW);
        store.seed(complete);

        PipelineDraftService.PublishResult published = service.publish("orders", 6, null, "alice");

        assertThat(published.mutation()).isEqualTo(PipelineDraftMutation.PUBLISHED);
        assertThat(published.published()).isTrue();
        assertThat(published.artifact()).isNotNull();
        assertThat(published.artifactHash()).hasSize(64);
        assertThat(store.lastPublication.pipelineId()).isEqualTo("orders");
        assertThat(service.publish("missing", 1, null, "alice").mutation()).isEqualTo(PipelineDraftMutation.NOT_FOUND);
        assertThat(service.publish("orders", 5, null, "alice").mutation())
                .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
        assertThat(service.publish("orders", 6, "stale-hash", "alice").mutation())
                .isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
    }

    @Test
    void validationReasonKeepsReferenceLocationAndTarget() {
        assertThat(PipelineDraftService.validationReason(new ValidationDiagnostic(
                "dsl.missing-reference", Map.of("path", "transforms[0].from", "ref", "missing-table"))))
                .isEqualTo("dsl.missing-reference at transforms[0].from (ref: missing-table)");
    }

    @Test
    void validationReasonNamesTheKeylessUpsertInput() {
        assertThat(PipelineDraftService.validationReason(new ValidationDiagnostic(
                "dsl.upsert-needs-key", Map.of("path", "serve.sync", "source", "mysql", "table", "BB_0727"))))
                .isEqualTo("dsl.upsert-needs-key at serve.sync (source: mysql, table: BB_0727)");
    }

    @Test
    void validationReasonPrefersDetailsAndOmitsIncompleteLocationHints() {
        assertThat(PipelineDraftService.validationReason(new ValidationDiagnostic(
                "dsl.invalid", Map.of("detail", "unsupported field", "path", "ignored"))))
                .isEqualTo("dsl.invalid: unsupported field");
        assertThat(PipelineDraftService.validationReason(new ValidationDiagnostic("dsl.invalid", Map.of())))
                .isEqualTo("dsl.invalid");
        assertThat(PipelineDraftService.validationReason(new ValidationDiagnostic(
                "dsl.upsert-needs-key", Map.of("path", "serve.sync", "source", "mysql"))))
                .isEqualTo("dsl.upsert-needs-key at serve.sync");
    }

    private static PipelineDraft draft(long revision, String updatedBy, String baseHash,
            Long publishedRevision, String publishedHash, Instant createdAt, Instant updatedAt) {
        return new PipelineDraft("orders", 1, revision, PipelineDraft.Mode.WIZARD,
                "Orders", "", null,
                new PipelineDraft.Wizard(new PipelineDraft.Root("root", "crm", "orders", List.of(), List.of()),
                        List.of(), List.of(), new PipelineDraft.Output("atlas",
                                Map.of("sourceId", "atlas", "table", "orders"))),
                baseHash, publishedRevision, publishedHash, createdAt, updatedAt, updatedBy);
    }

    private static final class MemoryDraftStore implements PipelineDraftStore {
        private final Map<String, PipelineDraft> drafts = new LinkedHashMap<>();
        private final Map<String, Long> lastRevision = new LinkedHashMap<>();
        private PipelineDraft.Publication lastPublication;

        void seed(PipelineDraft draft) {
            drafts.put(draft.pipelineId(), draft);
            lastRevision.merge(draft.pipelineId(), draft.revision(), Math::max);
        }

        @Override
        public Optional<PipelineDraft> get(String pipelineId) {
            return Optional.ofNullable(drafts.get(pipelineId));
        }

        @Override
        public List<PipelineDraft> list() {
            return new ArrayList<>(drafts.values());
        }

        @Override
        public PipelineDraftMutation create(PipelineDraft draft) {
            if (drafts.containsKey(draft.pipelineId())) return PipelineDraftMutation.ALREADY_EXISTS;
            long revision = Math.max(draft.revision(), lastRevision.getOrDefault(draft.pipelineId(), 0L) + 1);
            PipelineDraft created = withRevision(draft, revision);
            drafts.put(draft.pipelineId(), created);
            lastRevision.put(draft.pipelineId(), revision);
            return PipelineDraftMutation.CREATED;
        }

        @Override
        public PipelineDraftMutation replace(String pipelineId, long expectedRevision,
                PipelineDraft replacement) {
            return replaceGuarded(pipelineId, expectedRevision, replacement.baseArtifactHash(), replacement);
        }

        @Override
        public PipelineDraftMutation rebase(String pipelineId, long expectedRevision,
                String expectedBaseArtifactHash, PipelineDraft replacement) {
            return replaceGuarded(pipelineId, expectedRevision, expectedBaseArtifactHash, replacement);
        }

        private PipelineDraftMutation replaceGuarded(String pipelineId, long expectedRevision,
                String expectedBaseArtifactHash, PipelineDraft replacement) {
            PipelineDraft current = drafts.get(pipelineId);
            if (current == null) return PipelineDraftMutation.NOT_FOUND;
            if (current.revision() != expectedRevision) return PipelineDraftMutation.REVISION_CONFLICT;
            if (current.mode() != replacement.mode()) return PipelineDraftMutation.MODE_CONFLICT;
            if (!Objects.equals(current.baseArtifactHash(), expectedBaseArtifactHash)
                    || !Objects.equals(current.publishedDraftRevision(), replacement.publishedDraftRevision())
                    || !Objects.equals(current.publishedArtifactHash(), replacement.publishedArtifactHash())) {
                return PipelineDraftMutation.REVISION_CONFLICT;
            }
            drafts.put(pipelineId, replacement);
            lastRevision.put(pipelineId, replacement.revision());
            return PipelineDraftMutation.REPLACED;
        }

        @Override
        public PipelineDraftMutation delete(String pipelineId, long expectedRevision) {
            PipelineDraft current = drafts.get(pipelineId);
            if (current == null) return PipelineDraftMutation.NOT_FOUND;
            if (current.revision() != expectedRevision) return PipelineDraftMutation.REVISION_CONFLICT;
            drafts.remove(pipelineId);
            return PipelineDraftMutation.DELETED;
        }

        @Override
        public PipelineDraftMutation publish(PipelineDraft.Publication publication) {
            PipelineDraft current = drafts.get(publication.pipelineId());
            if (current == null) return PipelineDraftMutation.NOT_FOUND;
            if (current.revision() != publication.expectedDraftRevision()) return PipelineDraftMutation.REVISION_CONFLICT;
            if (!Objects.equals(current.baseArtifactHash(), publication.expectedArtifactHash())) {
                return PipelineDraftMutation.ARTIFACT_CONFLICT;
            }
            lastPublication = publication;
            return PipelineDraftMutation.PUBLISHED;
        }

        private static PipelineDraft withRevision(PipelineDraft draft, long revision) {
            return new PipelineDraft(draft.pipelineId(), draft.schemaVersion(), revision, draft.mode(), draft.name(),
                    draft.description(), draft.graph(), draft.wizard(), draft.baseArtifactHash(),
                    draft.publishedDraftRevision(), draft.publishedArtifactHash(), draft.createdAt(),
                    draft.updatedAt(), draft.updatedBy());
        }
    }
}
