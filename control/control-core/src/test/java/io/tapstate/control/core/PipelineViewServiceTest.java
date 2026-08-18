package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineViewServiceTest {

    @Test
    void listsPipelinesInStableIdOrderAndKeepsDeclaredSourceOrder() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(source("orders", "Orders", "mysql"));
        store.seed(source("customers", "Customers", "postgres"));
        store.seed(pipeline("nightly", List.of("customers", "orders")));
        store.seed(pipeline("daily", List.of("orders", "customers")));
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(store), new PipelineRepresentation());

        List<PipelineView> listed = pipelines.list();

        assertThat(listed).extracting(PipelineView::id).containsExactly("daily", "nightly");
        assertThat(listed.getFirst().sources()).extracting(PipelineSourceSummary::id)
                .containsExactly("orders", "customers");
        assertThat(listed.get(1).sources()).extracting(PipelineSourceSummary::id)
                .containsExactly("customers", "orders");
    }

    @Test
    void findsAPipelineWithCanonicalHashAndResolvedSourceSummaries() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(source("orders", "Orders", "mysql"));
        store.seed(pipeline("daily", List.of("orders")));
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(store), new PipelineRepresentation());

        PipelineView view = pipelines.find("daily").orElseThrow();

        assertThat(view.id()).isEqualTo("daily");
        assertThat(view.contentHash()).hasSize(64);
        assertThat(view.sources()).containsExactly(
                new PipelineSourceSummary(
                        "orders", new Metadata(Map.of("team", "sales"), "Orders"), "mysql"));
    }

    @Test
    void doesNotTreatNonPipelineArtifactsAsPipelineViews() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(source("orders", "Orders", "mysql"));
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(store), new PipelineRepresentation());

        assertThat(pipelines.find("orders")).isEmpty();
    }

    @Test
    void rejectsMissingOrWrongKindSourceReferencesInsteadOfDroppingThem() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(pipeline("missing", List.of("absent")));
        store.seed(pipeline("wrong_kind", List.of("not_a_source")));
        store.seed(pipeline("not_a_source", List.of("another_source")));
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(store), new PipelineRepresentation());

        assertThatThrownBy(() -> pipelines.find("missing"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("absent")
                .hasMessageContaining("Source");
        assertThatThrownBy(() -> pipelines.find("wrong_kind"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not_a_source")
                .hasMessageContaining("Source");
    }

    private static SourceResource source(String id, String description, String connector) {
        return new SourceResource(
                id,
                new Metadata(Map.of("team", "sales"), description),
                connector,
                Map.of(),
                null,
                null,
                null,
                null,
                null);
    }

    private static PipelineResource pipeline(String id, List<String> sourceIds) {
        return new PipelineResource(id, null, sourceIds, null, null, null, null, null);
    }

    private static final class ReadOnlyArtifactStore implements ArtifactStore {

        private final Map<String, Resource> resources = new LinkedHashMap<>();

        void seed(Resource resource) {
            resources.put(resource.id(), resource);
        }

        @Override
        public ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
            throw new AssertionError("Pipeline view queries must not write artifacts");
        }

        @Override
        public ArtifactMutation create(Resource artifact) {
            throw new AssertionError("Pipeline view queries must not write artifacts");
        }

        @Override
        public ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
            throw new AssertionError("Pipeline view queries must not write artifacts");
        }

        @Override
        public ArtifactMutation delete(String id, String expectedContentHash) {
            throw new AssertionError("Pipeline view queries must not write artifacts");
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            throw new AssertionError("Pipeline view queries must not write artifacts");
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(resources.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(resources.values());
        }
    }
}
