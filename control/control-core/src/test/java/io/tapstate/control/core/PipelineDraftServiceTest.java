package io.tapstate.control.core;

import io.tapstate.core.model.Resource;
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

import static org.assertj.core.api.Assertions.assertThat;

class PipelineDraftServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-24T00:00:00Z");
    private final MemoryDraftStore store = new MemoryDraftStore();
    private final PipelineDraftService service = new PipelineDraftService(store,
            new PipelineDraftCompiler(), new CanonicalWriter(), Clock.fixed(NOW, ZoneOffset.UTC), null, null);

    @Test
    void createAssignsInitialRevisionAndServerOwnedMetadata() {
        PipelineDraft submitted = draft(9, "client", "forged-base", 8L, "forged-published",
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
    void validationReasonKeepsReferenceLocationAndTarget() {
        assertThat(PipelineDraftService.validationReason(new ValidationDiagnostic(
                "dsl.missing-reference", Map.of("path", "transforms[0].from", "ref", "missing-table"))))
                .isEqualTo("dsl.missing-reference at transforms[0].from (ref: missing-table)");
    }

    private static PipelineDraft draft(long revision, String updatedBy, String baseHash,
            Long publishedRevision, String publishedHash, Instant createdAt, Instant updatedAt) {
        return new PipelineDraft("orders", 1, revision, PipelineDraft.Mode.WIZARD,
                "Orders", "", null,
                new PipelineDraft.Wizard(new PipelineDraft.Root("root", "crm", "orders", List.of(), List.of()),
                        List.of(), List.of(), null),
                baseHash, publishedRevision, publishedHash, createdAt, updatedAt, updatedBy);
    }

    private static final class MemoryDraftStore implements PipelineDraftStore {
        private final Map<String, PipelineDraft> drafts = new LinkedHashMap<>();

        void seed(PipelineDraft draft) {
            drafts.put(draft.pipelineId(), draft);
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
            return drafts.putIfAbsent(draft.pipelineId(), draft) == null
                    ? PipelineDraftMutation.CREATED : PipelineDraftMutation.ALREADY_EXISTS;
        }

        @Override
        public PipelineDraftMutation replace(String pipelineId, long expectedRevision,
                PipelineDraft replacement) {
            PipelineDraft current = drafts.get(pipelineId);
            if (current == null) return PipelineDraftMutation.NOT_FOUND;
            if (current.revision() != expectedRevision) return PipelineDraftMutation.REVISION_CONFLICT;
            if (current.mode() != replacement.mode()) return PipelineDraftMutation.MODE_CONFLICT;
            drafts.put(pipelineId, replacement);
            return PipelineDraftMutation.REPLACED;
        }

        @Override
        public PipelineDraftMutation delete(String pipelineId, long expectedRevision) {
            return PipelineDraftMutation.NOT_FOUND;
        }

        @Override
        public PipelineDraftMutation publish(PipelineDraft.Publication publication) {
            return PipelineDraftMutation.NOT_FOUND;
        }
    }
}
