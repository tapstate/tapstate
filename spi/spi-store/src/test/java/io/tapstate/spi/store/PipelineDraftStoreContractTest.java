package io.tapstate.spi.store;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineDraftStoreContractTest {

    @Test
    void onlyOneWriterCanAdvanceAnAuthoringRevision() {
        InMemoryDraftStore store = new InMemoryDraftStore();
        PipelineDraft initial = draft(PipelineDraft.Mode.DAG, 1, graph(), null);

        assertThat(store.create(initial)).isEqualTo(PipelineDraftMutation.CREATED);
        PipelineDraft writerA = draft(PipelineDraft.Mode.DAG, 2, graphWithNode("a"), null);
        PipelineDraft writerB = draft(PipelineDraft.Mode.DAG, 2, graphWithNode("b"), null);

        assertThat(store.replace("orders", 1, writerA)).isEqualTo(PipelineDraftMutation.REPLACED);
        assertThat(store.replace("orders", 1, writerB)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
        assertThat(store.get("orders").orElseThrow().graph().nodes()).extracting(PipelineDraft.Node::id)
                .containsExactly("a");
    }

    @Test
    void modeCannotChangeAndPublishChecksBothConditionsBeforeEitherMutation() {
        InMemoryDraftStore store = new InMemoryDraftStore();
        PipelineDraft initial = draft(PipelineDraft.Mode.DAG, 1, graph(), null);
        store.create(initial);

        PipelineDraft wizard = draft(PipelineDraft.Mode.WIZARD, 2, null, emptyWizard());
        assertThat(store.replace("orders", 1, wizard)).isEqualTo(PipelineDraftMutation.MODE_CONFLICT);

        Resource artifact = artifact("orders");
        PipelineDraft.Publication staleArtifact = new PipelineDraft.Publication(
                "orders", 1, "old-hash", artifact, "new-hash", Instant.parse("2026-09-21T00:00:00Z"), "test");
        assertThat(store.publish(staleArtifact)).isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
        assertThat(store.get("orders").orElseThrow().publishedDraftRevision()).isNull();
        assertThat(store.artifacts).isEmpty();
    }

    @Test
    void successfulPublishAdvancesArtifactAndMarkersTogether() {
        InMemoryDraftStore store = new InMemoryDraftStore();
        store.create(draft(PipelineDraft.Mode.DAG, 1, graph(), null));
        Resource artifact = artifact("orders");

        assertThat(store.publish(new PipelineDraft.Publication("orders", 1, null, artifact, "hash-2",
                Instant.parse("2026-09-21T00:00:00Z"), "test"))).isEqualTo(PipelineDraftMutation.PUBLISHED);
        PipelineDraft saved = store.get("orders").orElseThrow();
        assertThat(saved.baseArtifactHash()).isEqualTo("hash-2");
        assertThat(saved.publishedDraftRevision()).isEqualTo(1L);
        assertThat(store.artifacts).containsEntry("orders", artifact);
    }

    private static PipelineDraft draft(PipelineDraft.Mode mode, long revision, PipelineDraft.Graph graph,
            PipelineDraft.Wizard wizard) {
        Instant now = Instant.parse("2026-09-21T00:00:00Z");
        return new PipelineDraft("orders", 1, revision, mode, "Orders", "", graph, wizard, null, null, null,
                now, now, "test");
    }

    private static PipelineDraft.Graph graph() {
        return new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1));
    }

    private static PipelineDraft.Graph graphWithNode(String id) {
        return new PipelineDraft.Graph(List.of(new PipelineDraft.Node(id, "source", "crm", "orders", Map.of(), Map.of())),
                List.of(), new PipelineDraft.Viewport(0, 0, 1));
    }

    private static PipelineDraft.Wizard emptyWizard() {
        return new PipelineDraft.Wizard(new PipelineDraft.Root("orders", "crm", "orders", List.of(), List.of()),
                List.of(), List.of(), null);
    }

    private static Resource artifact(String id) {
        return new PipelineResource(id, null, List.of(SourceRef.bare("crm")), null, null, null, null, Map.of());
    }

    private static final class InMemoryDraftStore implements PipelineDraftStore {
        private final Map<String, PipelineDraft> drafts = new LinkedHashMap<>();
        private final Map<String, Resource> artifacts = new LinkedHashMap<>();

        @Override
        public synchronized Optional<PipelineDraft> get(String pipelineId) {
            return Optional.ofNullable(drafts.get(pipelineId));
        }

        @Override
        public synchronized List<PipelineDraft> list() {
            return new ArrayList<>(drafts.values());
        }

        @Override
        public synchronized PipelineDraftMutation create(PipelineDraft draft) {
            return drafts.putIfAbsent(draft.pipelineId(), draft) == null
                    ? PipelineDraftMutation.CREATED : PipelineDraftMutation.ALREADY_EXISTS;
        }

        @Override
        public synchronized PipelineDraftMutation replace(String pipelineId, long expectedRevision,
                PipelineDraft replacement) {
            PipelineDraft current = drafts.get(pipelineId);
            if (current == null) {
                return PipelineDraftMutation.NOT_FOUND;
            }
            if (current.mode() != replacement.mode()) {
                return PipelineDraftMutation.MODE_CONFLICT;
            }
            if (current.revision() != expectedRevision) {
                return PipelineDraftMutation.REVISION_CONFLICT;
            }
            drafts.put(pipelineId, replacement);
            return PipelineDraftMutation.REPLACED;
        }

        @Override
        public synchronized PipelineDraftMutation publish(PipelineDraft.Publication publication) {
            PipelineDraft current = drafts.get(publication.pipelineId());
            if (current == null) {
                return PipelineDraftMutation.NOT_FOUND;
            }
            if (current.revision() != publication.expectedDraftRevision()) {
                return PipelineDraftMutation.REVISION_CONFLICT;
            }
            if (!java.util.Objects.equals(current.baseArtifactHash(), publication.expectedArtifactHash())) {
                return PipelineDraftMutation.ARTIFACT_CONFLICT;
            }
            artifacts.put(publication.pipelineId(), publication.artifact());
            drafts.put(publication.pipelineId(), new PipelineDraft(current.pipelineId(), current.schemaVersion(),
                    current.revision(), current.mode(), current.name(), current.description(), current.graph(), current.wizard(),
                    publication.publishedArtifactHash(), publication.expectedDraftRevision(),
                    publication.publishedArtifactHash(), current.createdAt(), publication.publishedAt(), publication.updatedBy()));
            return PipelineDraftMutation.PUBLISHED;
        }
    }
}
