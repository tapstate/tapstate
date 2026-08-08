package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StorePort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Structure-assert coverage for the store-backed DAG source: it loads a stored pipeline artifact and its
 * referenced source and target artifacts, then hands them to the engine's DAG builder. These tests assert
 * the built graph's vertex and edge topology against an in-memory artifact store, without running a Jet job
 * - the leaves (SRS source vertex, transform port, sink writer) are built but never opened here.
 */
class StoreBackedDagSourceTest {

    @Test
    void builds_a_real_source_transform_sink_dag_from_the_stored_pipeline() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(connectionSupplier("orders_dest"));
        store.artifacts().save(new PipelineResource(
                "p", null,
                List.of("orders_src"),
                List.of(filter("keep_even", "row.id % 2 == 0", FromRef.literal("orders_src"))),
                null,
                serve(FromRef.literal("keep_even"), sync("sync_1", "orders_dest")),
                null, null));
        OpenRingGenerations.forSources(store, "orders_src");

        DAG dag = new StoreBackedDagSource(store).dagFor("p");

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "keep_even", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("orders_src", "keep_even"),
                edge("keep_even", "serve.sync_1"));
    }

    @Test
    void builds_a_job_for_a_source_that_reads_no_chain_of_its_own() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(connectionSupplier("orders_dest"));
        store.artifacts().save(new PipelineResource(
                "p", null,
                List.of("orders_src"),
                null,
                null,
                serve(FromRef.literal("orders_src"), sync("sync_1", "orders_dest")),
                null, null));

        // No chain record: only a read with an incremental tail through the shared ring opens one, so a
        // snapshot-only or srs-disabled read reaches here with none. Its rows come from the snapshot buffer
        // and no capture fills its ring, so demanding a generation of it would refuse to build a job that
        // is perfectly well formed.
        DAG dag = new StoreBackedDagSource(store).dagFor("p");

        assertThat(vertexNames(dag)).contains("orders_src");
    }

    @Test
    void reports_a_coded_error_when_the_pipeline_artifact_is_absent() {
        FakeStorePort store = new FakeStorePort();

        assertThatThrownBy(() -> new StoreBackedDagSource(store).dagFor("ghost"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException coded = (TapstateException) thrown;
                    assertThat(coded.code().code()).isEqualTo("actuation.pipeline-not-found");
                    assertThat(coded.args()).containsEntry("pipeline", "ghost");
                });
    }

    @Test
    void reports_a_coded_error_when_the_artifact_is_not_a_pipeline() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));

        assertThatThrownBy(() -> new StoreBackedDagSource(store).dagFor("orders_src"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException coded = (TapstateException) thrown;
                    assertThat(coded.code().code()).isEqualTo("actuation.not-a-pipeline");
                    assertThat(coded.args()).containsEntry("pipeline", "orders_src");
                    assertThat(coded.args()).containsEntry("kind", "source");
                });
    }

    // ---- fixtures ----------------------------------------------------------------------

    private static SourceResource cdcSource(String id, String table) {
        return new SourceResource(id, null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null, null);
    }

    private static SourceResource connectionSupplier(String id) {
        return new SourceResource(id, null, "mysql", Map.of("host", "d"), null, null, null, null, null);
    }

    private static ServeBlock serve(FromRef from, SyncElement... sync) {
        return new ServeBlock.Inline(null, from, List.of(sync), null, null);
    }

    private static SyncElement sync(String id, String source) {
        return new SyncElement(id, source, null, null, null, null);
    }

    private static Step filter(String id, String expr, FromRef... from) {
        return Step.inline(id, FromClause.list(from), new TransformBody.Filter(expr), null, null);
    }

    private static List<String> vertexNames(DAG dag) {
        List<String> names = new ArrayList<>();
        for (Vertex v : dag) {
            names.add(v.getName());
        }
        return names;
    }

    /** All edges as {@code "src->dest#srcOrd,destOrd"} strings, for order-insensitive assertions. */
    private static List<String> edges(DAG dag) {
        List<String> out = new ArrayList<>();
        for (Vertex v : dag) {
            for (Edge e : dag.getOutboundEdges(v.getName())) {
                out.add(e.getSourceName() + "->" + e.getDestName()
                        + "#" + e.getSourceOrdinal() + "," + e.getDestOrdinal());
            }
        }
        return out;
    }

    private static String edge(String src, String dest) {
        return src + "->" + dest + "#0,0";
    }

    /** In-memory artifact store keyed by top-level id; the other sub-stores are not exercised. */
    private static final class FakeArtifactStore implements ArtifactStore {

        private final Map<String, Resource> byId = new LinkedHashMap<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            for (Resource artifact : artifacts) {
                byId.put(artifact.id(), artifact);
            }
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(byId.values());
        }
    }

    /** A {@link StorePort} exposing only a real artifact store; every other sub-store is out of scope here. */
    private static final class FakeStorePort implements StorePort {

        private final FakeArtifactStore artifacts = new FakeArtifactStore();
        private final InMemorySchemaStore schemas = new InMemorySchemaStore();

        @Override
        public ArtifactStore artifacts() {
            return artifacts;
        }

        @Override
        public StateStore state() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DesiredStore desired() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CatalogStore catalog() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SchemaStore schemas() {
            return schemas;
        }

        @Override
        public ConnectorRegistry connectors() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorCatalogStore connectorCatalog() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorSpecStore connectorSpecs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectionTestResultStore connectionTestResults() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ObservationStore observations() {
            throw new UnsupportedOperationException();
        }

        private final SrsMetaStore meta = new InMemorySrsMetaStore();
        private final InMemoryKeyedStateStore keyedState = new InMemoryKeyedStateStore();

        @Override
        public SrsMetaStore meta() {
            return meta;
        }

        @Override
        public io.tapstate.spi.store.KeyedStateStore keyedState() {
            return keyedState;
        }
    }
}
