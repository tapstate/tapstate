package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.PipelineLayoutStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StoredArtifactRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineViewServiceTest {

    @Test
    void viewCatalogReportsTheSyncRenamedCollection() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        var parser = new io.tapstate.core.dsl.DslParser();
        store.seed(parser.parse("""
                version: tapstate/v1
                kind: source
                id: mysql_source
                connector: mysql
                config: { database: acceptance }
                mode: cdc
                tables: [ ACC_COMPOSITE ]
                """));
        store.seed(parser.parse("""
                version: tapstate/v1
                kind: source
                id: atlas-store
                connector: mongodb
                config: { uri: "mongodb://localhost:27017/tapstate_acc" }
                """));
        store.seed(parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: acc_copy_mysql
                source: mysql_source
                serve:
                  from: ACC_COMPOSITE
                  sync:
                    - id: acc_copy_mysql_to_store
                      source: atlas-store
                      rename: { prefix: "mysql_" }
                """));
        ArtifactQueryService artifacts = new ArtifactQueryService(store);
        EmptySchemaStore schemas = new EmptySchemaStore();
        DataBrowserService browser = new DataBrowserService(store, schemas,
                config -> List.of("mysql_ACC_COMPOSITE"),
                (config, collection) -> new io.tapstate.spi.store.DataBrowserTableInfo(null, null, null),
                (config, query) -> { throw new AssertionError("Listing views must not read rows"); },
                (config, request, listener) -> { throw new AssertionError("Listing views must not follow rows"); });
        ViewCatalogService views = new ViewCatalogService(artifacts,
                new PipelineObservationQueryService(artifacts, new InMemoryObservationStore()),
                browser, copyOutputNames(store));

        assertThat(views.list()).singleElement().satisfies(view -> {
            assertThat(view.id()).isEqualTo("acc_copy_mysql:acc_copy_mysql_to_store:ACC_COMPOSITE");
            assertThat(view.kind()).isEqualTo("replica");
            assertThat(view.location().storeId()).isEqualTo("atlas-store");
            assertThat(view.location().database()).isEqualTo("tapstate_acc");
            assertThat(view.location().collection())
                    .as("the view must point to the collection written after the sync rename")
                    .isEqualTo("mysql_ACC_COMPOSITE");
        });
    }

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
    void listAttachesAvailableRuntimeStatusAndLeavesUnobservedPipelinesUnavailable() {
        ReadOnlyArtifactStore artifacts = new ReadOnlyArtifactStore();
        artifacts.seed(source("orders", "Orders", "mysql"));
        artifacts.seed(pipeline("running", List.of("orders")));
        artifacts.seed(pipeline("pending", List.of("orders")));
        InMemoryObservationStore observations = new InMemoryObservationStore();
        observations.save(new Observation("running", PipelineState.RUNNING, Map.of(), Map.of(), Map.of()));

        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(artifacts),
                new PipelineRepresentation(),
                new PipelineObservationQueryService(new ArtifactQueryService(artifacts), observations));

        List<PipelineView> listed = pipelines.list();
        assertThat(listed).extracting(PipelineView::id).containsExactly("pending", "running");
        assertThat(listed.get(0).status()).isNull();
        assertThat(listed.get(1).status())
                .isEqualTo(new PipelineStatus("running", PipelineState.RUNNING, null));
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
    void marksMissingOrWrongKindSourceReferencesAsUnresolved() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(pipeline("missing", List.of("absent")));
        store.seed(pipeline("wrong_kind", List.of("not_a_source")));
        store.seed(pipeline("not_a_source", List.of("another_source")));
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(store), new PipelineRepresentation());

        assertThat(pipelines.find("missing").orElseThrow().sources())
                .containsExactly(PipelineSourceSummary.unresolved("absent"));
        assertThat(pipelines.find("wrong_kind").orElseThrow().sources())
                .containsExactly(PipelineSourceSummary.unresolved("not_a_source"));
    }

    @Test
    void listKeepsHealthyPipelinesVisibleWhenAnotherPipelineHasADanglingSource() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(source("orders", "Orders", "mysql"));
        store.seed(pipeline("healthy", List.of("orders")));
        store.seed(pipeline("broken", List.of("missing")));
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(store), new PipelineRepresentation());

        List<PipelineView> listed = pipelines.list();

        assertThat(listed).extracting(PipelineView::id).containsExactly("broken", "healthy");
        assertThat(listed.getFirst().sources()).containsExactly(PipelineSourceSummary.unresolved("missing"));
        assertThat(listed.get(1).sources()).containsExactly(
                new PipelineSourceSummary("orders", new Metadata(Map.of("team", "sales"), "Orders"), "mysql"));
    }

    @Test
    void listKeepsReadablePipelinesAvailableWhenAStoredArtifactIsUnreadable() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(source("orders", "Orders", "mysql"));
        store.seed(pipeline("daily", List.of("orders")));
        store.seedUnreadable("corrupt", "pipeline", "not: [valid");
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(store), new PipelineRepresentation());

        assertThat(pipelines.list()).extracting(PipelineView::id).containsExactly("daily");
    }

    @Test
    void savesEditorLayoutWithoutChangingThePipelineArtifactOrAcceptingAnUnknownPipeline() {
        ReadOnlyArtifactStore artifacts = new ReadOnlyArtifactStore();
        artifacts.seed(source("orders", "Orders", "mysql"));
        artifacts.seed(pipeline("daily", List.of("orders")));
        PipelineViewService pipelines = new PipelineViewService(
                new ArtifactQueryService(artifacts), new PipelineRepresentation());
        InMemoryPipelineLayoutStore layouts = new InMemoryPipelineLayoutStore();
        PipelineLayoutService service = new PipelineLayoutService(pipelines, layouts);

        String before = pipelines.get("daily").contentHash();
        PipelineLayoutView layout = service.save("daily", Map.of(
                "source:orders", new PipelineLayoutView.NodePosition(120.0, 240.0)),
                new PipelineLayoutView.Viewport(-10.0, 20.0, 0.75));

        assertThat(service.get("daily")).isEqualTo(layout);
        assertThat(layouts.get("daily")).hasValueSatisfying(stored -> {
            assertThat(stored.pipelineId()).isEqualTo(layout.pipelineId());
            assertThat(stored.nodes()).containsEntry(
                    "source:orders", new PipelineLayout.NodePosition(120.0, 240.0));
            assertThat(stored.viewport()).isEqualTo(new PipelineLayout.Viewport(-10.0, 20.0, 0.75));
        });
        assertThat(pipelines.get("daily").contentHash()).isEqualTo(before);
        assertThatThrownBy(() -> service.get("ghost"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(PipelineError.NOT_FOUND);
                    assertThat(error.args()).containsEntry("id", "ghost");
                });
    }

    @ParameterizedTest
    @MethodSource("collectionRenames")
    void viewCatalogUsesTheRenamedCollectionForInlineAndReusableServe(RenameSpec rename, String collection) {
        for (boolean reusable : List.of(false, true)) {
            ReadOnlyArtifactStore store = replicaSources();
            List<SyncElement> sync = List.of(new SyncElement("sink", "atlas-store", null, rename, null));
            ServeBlock serve;
            if (reusable) {
                store.seed(new ServeResource("shared_serve", null, sync, null, null, null));
                serve = new ServeBlock.Use(null, "shared_serve", FromRef.literal("ACC_COMPOSITE"));
            } else {
                serve = new ServeBlock.Inline(null, FromRef.literal("ACC_COMPOSITE"), sync, null, null);
            }
            store.seed(new PipelineResource("copy", null, List.of(SourceRef.bare("mysql_source")),
                    null, null, serve, null, null));
            List<String> statsRequests = new ArrayList<>();

            assertThat(replicaCatalog(store, List.of(collection), statsRequests).list())
                    .as("reusable serve: %s", reusable).singleElement().satisfies(view -> {
                        assertThat(view.id()).isEqualTo("copy:sink:ACC_COMPOSITE");
                        assertThat(view.location()).isEqualTo(
                                new ViewCatalogItem.Location("atlas-store", "tapstate_acc", collection));
                        assertThat(view.documents()).isEqualTo(7L);
                        assertThat(view.sizeBytes()).isEqualTo(42L);
                    });
            assertThat(statsRequests).containsExactly("atlas-store/" + collection);
        }
    }

    private static Stream<Arguments> collectionRenames() {
        return Stream.of(
                Arguments.of(null, "ACC_COMPOSITE"),
                Arguments.of(new RenameSpec(null, RenameCase.LOWER, "Raw_", "_v1"), "Raw_acc_composite_v1"),
                Arguments.of(new RenameSpec(Map.of("ACC_COMPOSITE", "chosen"), RenameCase.UPPER,
                        "ignored_", "_ignored"), "chosen"),
                Arguments.of(new RenameSpec(Map.of(), null, null, "_archive"), "ACC_COMPOSITE_archive"),
                Arguments.of(new RenameSpec(null, RenameCase.PASCAL, null, null), "AccComposite"));
    }

    @Test
    void viewCatalogDistinguishesPipelinesCopyingTheSameTableWithDifferentPrefixes() {
        ReadOnlyArtifactStore store = replicaSources();
        for (String prefix : List.of("mysql_", "ora_", "pg_")) {
            store.seed(new PipelineResource("copy_" + prefix, null, List.of(SourceRef.bare("mysql_source")),
                    null, null, new ServeBlock.Inline(null, FromRef.literal("ACC_COMPOSITE"),
                            List.of(new SyncElement("sink", "atlas-store", null,
                                    new RenameSpec(null, null, prefix, null), null)), null, null), null, null));
        }
        List<String> collections = List.of("mysql_ACC_COMPOSITE", "ora_ACC_COMPOSITE", "pg_ACC_COMPOSITE");
        List<String> statsRequests = new ArrayList<>();

        List<ViewCatalogItem> listed = replicaCatalog(store, collections, statsRequests).list();

        assertThat(listed).extracting(view -> view.location().collection()).containsExactlyElementsOf(collections);
        assertThat(listed).extracting(ViewCatalogItem::id).containsExactly(
                "copy_mysql_:sink:ACC_COMPOSITE", "copy_ora_:sink:ACC_COMPOSITE", "copy_pg_:sink:ACC_COMPOSITE");
        assertThat(statsRequests).containsExactly(
                "atlas-store/mysql_ACC_COMPOSITE", "atlas-store/ora_ACC_COMPOSITE", "atlas-store/pg_ACC_COMPOSITE");
    }

    private static PipelineOutputNames copyOutputNames(ReadOnlyArtifactStore store) {
        return pipeline -> {
            ServeBlock.Inline serve;
            if (pipeline.serve() instanceof ServeBlock.Inline inline) {
                serve = inline;
            } else {
                ServeBlock.Use use = (ServeBlock.Use) pipeline.serve();
                ServeResource resource = (ServeResource) store.get(use.use()).orElseThrow();
                serve = new ServeBlock.Inline(null, use.from(), resource.sync(), null, null);
            }
            return serve.sync().stream().map(sync -> new PipelineOutputNames.Output(sync.id(), "replica",
                    sync.source(), io.tapstate.core.model.TableRename.apply("ACC_COMPOSITE", sync.rename()),
                    sync.id() + ":ACC_COMPOSITE")).toList();
        };
    }

    private static ReadOnlyArtifactStore replicaSources() {
        ReadOnlyArtifactStore store = new ReadOnlyArtifactStore();
        store.seed(new SourceResource("mysql_source", null, "mysql", Map.of(), null,
                List.of(new TableRef.Literal("ACC_COMPOSITE")), null, null));
        store.seed(new SourceResource("atlas-store", null, "mongodb",
                Map.of("uri", "mongodb://localhost:27017/tapstate_acc"), null, null, null, null));
        return store;
    }

    private static ViewCatalogService replicaCatalog(ReadOnlyArtifactStore store,
            List<String> collections, List<String> statsRequests) {
        ArtifactQueryService artifacts = new ArtifactQueryService(store);
        EmptySchemaStore schemas = new EmptySchemaStore();
        DataBrowserService browser = new DataBrowserService(store, schemas, config -> collections,
                (config, collection) -> {
                    statsRequests.add(config.id() + "/" + collection);
                    return new DataBrowserTableInfo(7L, 42L, 6L);
                },
                (config, query) -> { throw new AssertionError("Listing views must not read rows"); },
                (config, request, listener) -> { throw new AssertionError("Listing views must not follow rows"); });
        return new ViewCatalogService(artifacts,
                new PipelineObservationQueryService(artifacts, new InMemoryObservationStore()),
                browser, copyOutputNames(store));
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
                null);
    }

    private static PipelineResource pipeline(String id, List<String> sourceIds) {
        return new PipelineResource(id, null,
                sourceIds.stream().map(sourceId -> (SourceRef) SourceRef.bare(sourceId)).toList(),
                null, null, null, null, null);
    }

    private static final class ReadOnlyArtifactStore implements ArtifactStore {

        private final Map<String, Resource> resources = new LinkedHashMap<>();
        private final List<StoredArtifactRecord> unreadable = new ArrayList<>();

        void seed(Resource resource) {
            resources.put(resource.id(), resource);
        }

        void seedUnreadable(String id, String kind, String canonicalForm) {
            unreadable.add(new StoredArtifactRecord(id, kind, canonicalForm, null, false));
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
            unreadable.forEach(row -> new io.tapstate.core.dsl.DslParser().parse(row.canonicalForm()));
            return List.copyOf(resources.values());
        }

        @Override
        public List<StoredArtifactRecord> listStored() {
            List<StoredArtifactRecord> rows = resources.values().stream()
                    .map(StoredArtifactRecord::of)
                    .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
            rows.addAll(unreadable);
            return List.copyOf(rows);
        }
    }

    private static final class InMemoryPipelineLayoutStore implements PipelineLayoutStore {

        private final Map<String, PipelineLayout> layouts = new LinkedHashMap<>();

        @Override
        public Optional<PipelineLayout> get(String pipelineId) {
            return Optional.ofNullable(layouts.get(pipelineId));
        }

        @Override
        public void save(PipelineLayout layout) {
            layouts.put(layout.pipelineId(), layout);
        }

        @Override
        public void delete(String pipelineId) {
            layouts.remove(pipelineId);
        }
    }

    private static final class InMemoryObservationStore implements ObservationStore {

        private final Map<String, Observation> observations = new LinkedHashMap<>();

        @Override
        public void save(Observation observation) {
            observations.put(observation.pipelineId(), observation);
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(observations.get(pipelineId));
        }

        @Override
        public void delete(String pipelineId) {
            observations.remove(pipelineId);
        }
    }
}
