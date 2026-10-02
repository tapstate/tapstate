package io.tapstate.control.core;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An answer that rewrites the definition reaches the pipeline's draft too, so the editor's next publish
 * neither undoes it nor is refused: a draft based on the definition being replaced gets the same change and
 * moves to the new one, and a draft that was already behind is left for its author.
 */
class StartDraftSyncTest {

    private static final String KEY = "target-not-empty/warehouse/orders";
    private static final Instant T0 = Instant.parse("2026-10-01T00:00:00Z");

    private final StartChecksBench bench = new StartChecksBench();
    private final Drafts drafts = new Drafts(bench.artifacts);
    private final PipelineDraftCompiler compiler = new PipelineDraftCompiler();

    @AfterEach
    void close() {
        bench.executor.close();
    }

    @Test
    void aDraftBasedOnTheReplacedDefinitionGetsTheSameChangeAndTheNextPublishKeepsIt() {
        String published = publish(draft(1));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));

        service().start("alice", "pl", List.of(new StartDecision(KEY, "clear")), published);

        String changed = bench.hash("pl");
        PipelineDraft synced = drafts.get("pl").orElseThrow();
        assertThat(synced.revision()).isEqualTo(2);
        assertThat(synced.baseArtifactHash()).isEqualTo(changed);
        assertThat(synced.publishedDraftRevision()).isEqualTo(2L);
        assertThat(synced.publishedArtifactHash()).isEqualTo(changed);
        assertThat(target(synced).config()).containsEntry("onFullLoad", "clear");
        assertThat(bench.apply.planDraftPublication(compiler.compile(synced)).artifacts().getFirst().contentHash())
                .as("the draft now publishes exactly the definition stored").isEqualTo(changed);

        // The editor's next Run publishes the draft it reloaded.
        PipelineDraftService.PublishResult next = drafts.service(bench).publish("pl", 2, changed, "alice");
        assertThat(next.mutation()).isEqualTo(PipelineDraftMutation.PUBLISHED);
        assertThat(((ServeBlock.Inline) bench.stored("pl").serve()).sync().getFirst().onFullLoad())
                .isEqualTo(OnFullLoad.CLEAR);
    }

    @Test
    void aDraftAlreadyBehindTheDefinitionIsLeftForItsAuthor() {
        String published = publish(draft(1));
        PipelineDraft behind = rebased(drafts.get("pl").orElseThrow(), "0".repeat(64));
        drafts.byId.put("pl", behind);
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));

        service().start("alice", "pl", List.of(new StartDecision(KEY, "clear")), published);

        assertThat(drafts.get("pl")).contains(behind);
        assertThat(((ServeBlock.Inline) bench.stored("pl").serve()).sync().getFirst().onFullLoad())
                .isEqualTo(OnFullLoad.CLEAR);
    }

    @Test
    void aPipelineWithNoDraftHasOnlyItsDefinitionChanged() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.holding("warehouse/orders", new TargetProbe.TargetRows(false, 5L, false));

        service().start("alice", "pl", List.of(new StartDecision(KEY, "clear")), bench.hash("pl"));

        assertThat(drafts.get("pl")).isEmpty();
        assertThat(((ServeBlock.Inline) bench.stored("pl").serve()).sync().getFirst().onFullLoad())
                .isEqualTo(OnFullLoad.CLEAR);
    }

    private PipelineStartService service() {
        return new PipelineStartService(bench.lifecycle, bench.evaluator(),
                new DraftSyncingDefinitionWriter(bench.apply, drafts, bench.auditGate, StartChecksBench.CLOCK));
    }

    /** Publishes a fresh draft the way the editor does, leaving draft and definition in step. */
    private String publish(PipelineDraft draft) {
        drafts.byId.put(draft.pipelineId(), draft);
        PipelineDraftService.PublishResult result = drafts.service(bench).publish("pl", draft.revision(), null, "alice");
        assertThat(result.mutation()).isEqualTo(PipelineDraftMutation.PUBLISHED);
        return result.artifactHash();
    }

    private static PipelineDraft draft(long revision) {
        PipelineDraft.Graph graph = new PipelineDraft.Graph(List.of(
                new PipelineDraft.Node("source-orders", "source", "src", "orders", Map.of(), Map.of()),
                new PipelineDraft.Node("out", "target", "warehouse", "orders", Map.of(), Map.of())),
                List.of(new PipelineDraft.Edge("source-to-target", "source-orders", "out")),
                new PipelineDraft.Viewport(0, 0, 1));
        return new PipelineDraft("pl", 1, revision, PipelineDraft.Mode.DAG, "pl", "", graph, null,
                null, null, null, T0, T0, "alice");
    }

    private static PipelineDraft rebased(PipelineDraft draft, String base) {
        return new PipelineDraft(draft.pipelineId(), draft.schemaVersion(), draft.revision(), draft.mode(), draft.name(),
                draft.description(), draft.graph(), draft.wizard(), base, draft.publishedDraftRevision(),
                draft.publishedArtifactHash(), draft.createdAt(), draft.updatedAt(), draft.updatedBy());
    }

    private static PipelineDraft.Node target(PipelineDraft draft) {
        return draft.graph().nodes().stream().filter(node -> node.type().equals("target")).findFirst().orElseThrow();
    }

    /** Drafts beside the bench's definitions, publishing and changing both together the way the store does. */
    private static final class Drafts implements PipelineDraftStore {
        final Map<String, PipelineDraft> byId = new LinkedHashMap<>();
        private final StartChecksBench.Artifacts artifacts;

        Drafts(StartChecksBench.Artifacts artifacts) {
            this.artifacts = artifacts;
        }

        PipelineDraftService service(StartChecksBench bench) {
            return new PipelineDraftService(this, new ArtifactQueryService(artifacts), bench.auditGate, bench.apply);
        }

        @Override
        public Optional<PipelineDraft> get(String pipelineId) {
            return Optional.ofNullable(byId.get(pipelineId));
        }

        @Override
        public List<PipelineDraft> list() {
            return new ArrayList<>(byId.values());
        }

        @Override
        public PipelineDraftMutation create(PipelineDraft draft) {
            byId.put(draft.pipelineId(), draft);
            return PipelineDraftMutation.CREATED;
        }

        @Override
        public PipelineDraftMutation replace(String pipelineId, long expectedRevision, PipelineDraft replacement) {
            byId.put(pipelineId, replacement);
            return PipelineDraftMutation.REPLACED;
        }

        @Override
        public PipelineDraftMutation delete(String pipelineId, long expectedRevision) {
            byId.remove(pipelineId);
            return PipelineDraftMutation.DELETED;
        }

        @Override
        public PipelineDraftMutation publish(PipelineDraft.Publication publication) {
            PipelineDraft current = byId.get(publication.pipelineId());
            if (current == null || current.revision() != publication.expectedDraftRevision()) {
                return PipelineDraftMutation.REVISION_CONFLICT;
            }
            String stored = artifacts.get(publication.pipelineId()).map(CanonicalHash::of).orElse(null);
            if (!Objects.equals(stored, publication.expectedArtifactHash())
                    || !Objects.equals(current.baseArtifactHash(), publication.expectedArtifactHash())) {
                return PipelineDraftMutation.ARTIFACT_CONFLICT;
            }
            artifacts.saveAll(List.of(publication.artifact()));
            byId.put(current.pipelineId(), new PipelineDraft(current.pipelineId(), current.schemaVersion(),
                    current.revision(), current.mode(), current.name(), current.description(), current.graph(),
                    current.wizard(), publication.publishedArtifactHash(), current.revision(),
                    publication.publishedArtifactHash(), current.createdAt(), publication.publishedAt(),
                    publication.updatedBy()));
            return PipelineDraftMutation.PUBLISHED;
        }

        @Override
        public PipelineDraftMutation changeDefinition(PipelineDraft.DefinitionChange change) {
            Resource stored = artifacts.get(change.pipelineId()).orElse(null);
            if (stored == null || !CanonicalHash.of(stored).equals(change.expectedArtifactHash())) {
                return PipelineDraftMutation.ARTIFACT_CONFLICT;
            }
            artifacts.saveAll(List.of(change.artifact()));
            PipelineDraft current = byId.get(change.pipelineId());
            if (current != null && change.expectedArtifactHash().equals(current.baseArtifactHash())) {
                byId.put(current.pipelineId(), change.draftChange().apply(current));
            }
            return PipelineDraftMutation.REPLACED;
        }
    }
}
