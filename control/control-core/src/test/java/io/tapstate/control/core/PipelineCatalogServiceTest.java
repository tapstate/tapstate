package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineCatalogServiceTest {

    @Test
    void mergesArtifactsAndDraftsAndProjectsEveryLifecycleTransition() {
        List<Resource> resources = new ArrayList<>();
        for (String id : List.of("artifact-stop", "failed", "resuming", "starting", "pausing", "running",
                "paused", "desired-running", "desired-paused", "desired-stopped", "desired-failed",
                "desired-new", "desired-completed", "completed-running-intent", "corrupt-intent", "new")) {
            resources.add(pipeline(id, id.equals("artifact-stop") ? "artifact description" : null));
        }
        resources.add(new DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: unrelated
                connector: mysql
                config: {}
                """));

        MemoryDraftStore draftStore = new MemoryDraftStore();
        draftStore.seed(draft("failed", " ", "draft description", 4));
        draftStore.seed(draft("draft-only", " ", null, 2));
        MemoryDesiredStore desired = new MemoryDesiredStore();
        desired.save(intent("artifact-stop", PipelineState.STOPPED));
        desired.save(intent("failed", PipelineState.RUNNING));
        desired.save(intent("resuming", PipelineState.RUNNING));
        desired.save(intent("starting", PipelineState.RUNNING));
        desired.save(intent("pausing", PipelineState.PAUSED));
        desired.save(intent("running", PipelineState.RUNNING));
        desired.save(intent("desired-running", PipelineState.RUNNING));
        desired.save(intent("desired-paused", PipelineState.PAUSED));
        desired.save(intent("desired-stopped", PipelineState.STOPPED));
        desired.save(intent("desired-failed", PipelineState.FAILED));
        desired.save(intent("desired-new", PipelineState.NEW));
        desired.save(intent("desired-completed", PipelineState.COMPLETED));
        desired.save(intent("completed-running-intent", PipelineState.RUNNING));
        desired.corrupt("corrupt-intent");
        MemoryObservationStore observations = new MemoryObservationStore();
        observations.save(observation("artifact-stop", PipelineState.RUNNING));
        observations.save(observation("failed", PipelineState.FAILED));
        observations.save(observation("resuming", PipelineState.PAUSED));
        observations.save(observation("starting", PipelineState.STOPPED));
        observations.save(observation("pausing", PipelineState.RUNNING));
        observations.save(observation("running", PipelineState.RUNNING));
        observations.save(observation("paused", PipelineState.PAUSED));
        observations.save(observation("completed-running-intent", PipelineState.COMPLETED));

        PipelineCatalogService service = new PipelineCatalogService(
                new ArtifactQueryService(artifactStore(resources)),
                new PipelineDraftService(draftStore), desired,
                new PipelineObservationQueryService(new ArtifactQueryService(artifactStore(resources)), observations));

        List<PipelineCatalogItem> items = service.list();
        Map<String, PipelineCatalogItem> byId = new HashMap<>();
        items.forEach(item -> byId.put(item.id(), item));

        assertThat(items).extracting(PipelineCatalogItem::id).isSorted().doesNotContain("unrelated");
        assertThat(byId).containsOnlyKeys("artifact-stop", "failed", "resuming", "starting", "pausing", "running",
                "paused", "desired-running", "desired-paused", "desired-stopped", "desired-failed",
                "desired-new", "desired-completed", "completed-running-intent", "corrupt-intent", "new",
                "draft-only");
        assertThat(byId.get("failed").name()).isEqualTo("failed");
        assertThat(byId.get("failed").description()).isEqualTo("draft description");
        assertThat(byId.get("failed").mode()).isEqualTo("wizard");
        assertThat(byId.get("failed").hasArtifact()).isTrue();
        assertThat(byId.get("artifact-stop").description()).isEqualTo("artifact description");
        assertThat(byId.get("draft-only").name()).isEqualTo("draft-only");
        assertThat(byId.get("draft-only").description()).isNull();
        assertThat(byId.get("draft-only").hasArtifact()).isFalse();

        assertThat(byId.get("artifact-stop").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.STOPPING);
        assertThat(byId.get("failed").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.FAILED);
        assertThat(byId.get("resuming").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.RESUMING);
        assertThat(byId.get("starting").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.STARTING);
        assertThat(byId.get("pausing").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.PAUSING);
        assertThat(byId.get("running").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.RUNNING);
        assertThat(byId.get("paused").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.PAUSED);
        assertThat(byId.get("desired-running").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.STARTING);
        assertThat(byId.get("desired-paused").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.PAUSING);
        assertThat(byId.get("desired-stopped").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.STOPPING);
        assertThat(byId.get("desired-failed").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.FAILED);
        assertThat(byId.get("desired-new").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.NEW);
        assertThat(byId.get("desired-completed").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.COMPLETED);
        assertThat(byId.get("completed-running-intent").status().state())
                .isEqualTo(PipelineCatalogItem.DisplayState.COMPLETED);
        assertThat(byId.get("corrupt-intent").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.NEW);
        assertThat(byId.get("new").status().state()).isEqualTo(PipelineCatalogItem.DisplayState.NEW);
        assertThat(service.list(0, 2)).extracting(PipelineCatalogItem::id)
                .containsExactly("artifact-stop", "completed-running-intent");
    }

    private static Resource pipeline(String id, String description) {
        Metadata metadata = description == null ? null : new Metadata(Map.of(), description);
        return new PipelineResource(id, metadata, List.of(SourceRef.bare("mysql")), List.of(),
                null, null, null, Map.of());
    }

    private static PipelineDraft draft(String id, String name, String description, long revision) {
        Instant now = Instant.parse("2026-09-29T00:00:00Z");
        return new PipelineDraft(id, 1, revision, PipelineDraft.Mode.WIZARD, name, description, null,
                new PipelineDraft.Wizard(new PipelineDraft.Root("root", "mysql", "orders", List.of(), List.of()),
                        List.of(), List.of(), null), null, null, null, now, now, "test");
    }

    private static DesiredState intent(String id, PipelineState state) {
        return new DesiredState(id, state, "artifact-revision");
    }

    private static Observation observation(String id, PipelineState state) {
        return new Observation(id, state, Map.of(), Map.of(), Map.of());
    }

    private static ArtifactStore artifactStore(List<Resource> resources) {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                throw new UnsupportedOperationException("writes are not exercised by this fixture");
            }

            @Override
            public Optional<Resource> get(String id) {
                return resources.stream().filter(resource -> resource.id().equals(id)).findFirst();
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(resources);
            }
        };
    }

    private static final class MemoryDraftStore implements PipelineDraftStore {
        private final Map<String, PipelineDraft> drafts = new HashMap<>();

        void seed(PipelineDraft draft) {
            drafts.put(draft.pipelineId(), draft);
        }

        @Override
        public Optional<PipelineDraft> get(String pipelineId) {
            return Optional.ofNullable(drafts.get(pipelineId));
        }

        @Override
        public List<PipelineDraft> list() {
            return List.copyOf(drafts.values());
        }

        @Override
        public PipelineDraftMutation create(PipelineDraft draft) {
            throw new UnsupportedOperationException("writes are not exercised by this fixture");
        }

        @Override
        public PipelineDraftMutation replace(String pipelineId, long expectedRevision, PipelineDraft replacement) {
            throw new UnsupportedOperationException("writes are not exercised by this fixture");
        }

        @Override
        public PipelineDraftMutation delete(String pipelineId, long expectedRevision) {
            throw new UnsupportedOperationException("writes are not exercised by this fixture");
        }

        @Override
        public PipelineDraftMutation publish(PipelineDraft.Publication publication) {
            throw new UnsupportedOperationException("writes are not exercised by this fixture");
        }
    }

    private static final class MemoryDesiredStore implements DesiredStore {
        private final Map<String, DesiredState> desired = new HashMap<>();
        private final java.util.Set<String> unreadable = new java.util.HashSet<>();

        void corrupt(String pipelineId) {
            unreadable.add(pipelineId);
        }

        @Override
        public void save(DesiredState state) {
            desired.put(state.pipelineId(), state);
        }

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            if (unreadable.contains(pipelineId)) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                        Map.of("id", pipelineId, "field", "targetState"), null);
            }
            return Optional.ofNullable(desired.get(pipelineId));
        }

        @Override
        public List<String> pipelineIds() {
            return List.copyOf(desired.keySet());
        }

        @Override
        public void delete(String pipelineId) {
            desired.remove(pipelineId);
        }
    }

    private static final class MemoryObservationStore implements ObservationStore {
        private final Map<String, Observation> values = new HashMap<>();

        @Override
        public void save(Observation observation) {
            values.put(observation.pipelineId(), observation);
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(values.get(pipelineId));
        }

        @Override
        public void delete(String pipelineId) {
            values.remove(pipelineId);
        }
    }
}
