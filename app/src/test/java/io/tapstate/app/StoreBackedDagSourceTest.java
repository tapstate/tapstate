package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTester;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StorePort;
import java.time.Duration;
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
                List.of(SourceRef.spec("orders_src", true)),
                List.of(filter("keep_even", "row.id % 2 == 0", FromRef.literal("orders_src"))),
                null,
                serve(FromRef.literal("keep_even"), sync("sync_1", "orders_dest")),
                null, null));
        discovered(store, "orders_src", "orders");
        OpenRingGenerations.forSources(store, "orders_src");

        DAG dag = new StoreBackedDagSource(store).dagFor("p");

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "keep_even", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("orders_src", "keep_even"),
                edge("keep_even", "serve.sync_1"));
    }

    @Test
    void a_view_declared_by_reference_materializes_like_an_inline_one() {
        // The wizard writes this form whenever an author reuses an existing view, so it is not a
        // grammar curiosity: the reference must reach the builder already expanded.
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(connectionSupplier(ViewTargetResolver.STATE_STORE_SOURCE_ID));
        store.artifacts().save(new ViewResource("order_state", null, "order_id", null, null, null));
        store.artifacts().save(new PipelineResource(
                "p", null, List.of(SourceRef.spec("orders_src", true)), null,
                new ViewBlock.Use(null, "order_state", FromRef.literal("orders_src")),
                null, null, null));

        DAG dag = new StoreBackedDagSource(store).dagFor("p");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "view.order_state");
        assertThat(edges(dag)).containsExactly(edge("orders_src", "view.order_state"));
    }

    @Test
    void a_serve_declared_by_reference_writes_like_an_inline_one() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(connectionSupplier("orders_dest"));
        store.artifacts().save(new ServeResource(
                "publish", null, List.of(sync("sync_1", "orders_dest")), null, null, null));
        store.artifacts().save(new PipelineResource(
                "p", null, List.of(SourceRef.spec("orders_src", true)), null, null,
                new ServeBlock.Use(null, "publish", FromRef.literal("orders_src")),
                null, null));
        discovered(store, "orders_src", "orders");

        DAG dag = new StoreBackedDagSource(store).dagFor("p");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "serve.sync_1");
        assertThat(edges(dag)).containsExactly(edge("orders_src", "serve.sync_1"));
    }

    @Test
    void a_view_without_a_managed_store_to_land_in_says_so_by_name() {
        // The store is the deployment's rather than the author's, so its absence is a deployment
        // condition an operator can act on - not an invariant that should crash bare.
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(new PipelineResource(
                "p", null, List.of(SourceRef.spec("orders_src", true)), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "id", null, null),
                null, null, null));

        assertThatThrownBy(() -> new StoreBackedDagSource(store).dagFor("p"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("actuation.view-store-not-configured"));
    }

    @Test
    void a_declared_view_materializes_into_the_managed_state_store() {
        // No serve block anywhere: declaring the view is the whole instruction, and the store it lands
        // in is the deployment's own rather than anything the pipeline names.
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(connectionSupplier(ViewTargetResolver.STATE_STORE_SOURCE_ID));
        store.artifacts().save(new PipelineResource(
                "p", null,
                List.of(SourceRef.spec("orders_src", true)),
                null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "id", null, null),
                null, null, null));

        DAG dag = new StoreBackedDagSource(store).dagFor("p");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "view.order_state");
        assertThat(edges(dag)).containsExactly(edge("orders_src", "view.order_state"));
    }

    @Test
    void a_view_whose_store_is_registered_but_unreachable_says_so_by_its_own_name() {
        // A different condition from "not configured", and deliberately a different code: that one says
        // nobody set the store up, this one says it is set up and not answering. Collapsing them would
        // send an operator to check a configuration that is already correct.
        FakeStorePort store = seededViewPipeline();

        assertThatThrownBy(() -> new StoreBackedDagSource(store, refusing("connection refused")).dagFor("p"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("actuation.view-store-unreachable"));
    }

    @Test
    void a_connector_reporting_a_failed_check_is_what_unreachable_means() {
        // The test above hands in a refusal already formed, so it proves the build asks and propagates --
        // not that a real probe turns a connector's FAILED verdict into this code. Drive the real probe
        // over a tester that answers FAILED, and require the reason to carry what the connector said:
        // an implementation that reports the failure without it sends the operator away with nothing to
        // act on, and would pass an assertion that only looked at the code.
        FakeStorePort store = seededViewPipeline();
        ConnectionTester failing = config -> new ConnectionTestResult(
                config.id(), config.connectorId(), ConnectionTestResult.Outcome.FAILED,
                List.of(new ConnectionTestItem("reachable", ConnectionTestItem.Status.FAILED,
                        "connection refused by mongo:27017", null, null, null)),
                0L);

        assertThatThrownBy(() -> new StoreBackedDagSource(
                store, StoreReachability.probing(failing, Duration.ofSeconds(5))).dagFor("p"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException coded = (TapstateException) e;
                    assertThat(coded.code().code()).isEqualTo("actuation.view-store-unreachable");
                    assertThat(String.valueOf(coded.args().get("reason")))
                            .as("what the connector said, carried through to the operator")
                            .contains("connection refused by mongo:27017");
                });
    }

    @Test
    void a_connector_that_passes_lets_the_pipeline_build() {
        // The real probe's other direction: without this, a probing() that threw unconditionally would
        // satisfy every refusal test above while breaking every working deployment.
        FakeStorePort store = seededViewPipeline();
        ConnectionTester passing = config -> new ConnectionTestResult(
                config.id(), config.connectorId(), ConnectionTestResult.Outcome.PASSED, List.of(), 0L);

        DAG dag = new StoreBackedDagSource(
                store, StoreReachability.probing(passing, Duration.ofSeconds(5))).dagFor("p");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "view.order_state");
    }

    @Test
    void a_store_that_never_answers_is_given_up_on_rather_than_waited_out() {
        // The requirement is a diagnosis, not a hang: a connector's own test carries no deadline, so a
        // store that accepts the connection and then goes quiet would otherwise park the start forever.
        // Asserting the elapsed time is what discriminates -- a probe with no bound still produces this
        // exact exception eventually, and a test that only checked the code would pass on it.
        FakeStorePort store = seededViewPipeline();
        StoreReachability neverAnswers = (id, connectorId, settings) -> {
            try {
                Thread.sleep(Duration.ofMinutes(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        long startedAt = System.nanoTime();
        assertThatThrownBy(() -> new StoreBackedDagSource(
                store, StoreReachability.bounded(neverAnswers, Duration.ofMillis(200))).dagFor("p"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("actuation.view-store-unreachable"));
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                .as("the start gave up on its own rather than waiting for the probe")
                .isLessThan(Duration.ofMinutes(1));
    }

    @Test
    void giving_up_actually_interrupts_the_probe_rather_than_abandoning_it() throws InterruptedException {
        // Giving up on the answer and giving up on the work are different things, and only the second
        // frees the thread. Left un-interrupted, a probe against a store that never replies keeps running
        // for as long as its connector takes -- invisible, because the caller already returned. Asserting
        // the timeout alone cannot see that: it passes identically either way.
        FakeStorePort store = seededViewPipeline();
        java.util.concurrent.CountDownLatch interrupted = new java.util.concurrent.CountDownLatch(1);
        StoreReachability neverAnswers = (id, connectorId, settings) -> {
            try {
                Thread.sleep(Duration.ofMinutes(5));
            } catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
        };

        assertThatThrownBy(() -> new StoreBackedDagSource(
                store, StoreReachability.bounded(neverAnswers, Duration.ofMillis(200))).dagFor("p"))
                .isInstanceOf(TapstateException.class);

        assertThat(interrupted.await(5, java.util.concurrent.TimeUnit.SECONDS))
                .as("the probe was interrupted, not merely left behind")
                .isTrue();
    }

    @Test
    void a_store_that_answers_is_not_refused() {
        // The refusals above are worth nothing if the healthy case does not pass them: a probe wired to
        // reject everything would satisfy both of them and break every working deployment.
        FakeStorePort store = seededViewPipeline();

        DAG dag = new StoreBackedDagSource(store, StoreReachability.assumingReachable()).dagFor("p");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "view.order_state");
    }

    /** A pipeline whose only instruction is a view, with the managed store registered and plain. */
    private FakeStorePort seededViewPipeline() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(connectionSupplier(ViewTargetResolver.STATE_STORE_SOURCE_ID));
        store.artifacts().save(new PipelineResource(
                "p", null, List.of(SourceRef.spec("orders_src", true)), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "id", null, null),
                null, null, null));
        return store;
    }

    /** A reachability check that reports the store unreachable for the given reason. */
    private static StoreReachability refusing(String reason) {
        return (id, connectorId, settings) -> {
            throw new TapstateException(ActuationError.VIEW_STORE_UNREACHABLE,
                    Map.of("store", id, "reason", reason), null);
        };
    }

    @Test
    void builds_a_job_for_a_source_that_reads_no_chain_of_its_own() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("orders_src", "orders"));
        store.artifacts().save(connectionSupplier("orders_dest"));
        store.artifacts().save(new PipelineResource(
                "p", null,
                List.of(SourceRef.spec("orders_src", true)),
                null,
                null,
                serve(FromRef.literal("orders_src"), sync("sync_1", "orders_dest")),
                null, null));
        discovered(store, "orders_src", "orders");

        // No chain record: only a read with an incremental tail through the shared ring opens one, so a
        // snapshot-only or srs-disabled read reaches here with none. Its rows come from the snapshot buffer
        // and no capture fills its ring, so demanding a generation of it would refuse to build a job that
        // is perfectly well formed.
        DAG dag = new StoreBackedDagSource(store).dagFor("p");

        assertThat(vertexNames(dag)).contains("orders_src");
    }

    @Test
    void expands_a_multi_table_source_into_one_source_vertex_per_table() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(new SourceResource("multi_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders"), TableRef.literal("customers")), null, null, null));
        store.artifacts().save(connectionSupplier("orders_dest"));
        store.artifacts().save(new PipelineResource(
                "multi", null, List.of(SourceRef.spec("multi_src", true)), null, null,
                serve(FromRef.literal("multi_src"), sync("sync_1", "orders_dest")), null, null));
        discovered(store, "multi_src", "orders", "customers");

        DAG dag = new StoreBackedDagSource(store).dagFor("multi");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder(
                "multi_src.orders", "multi_src.customers", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("multi_src.orders", "serve.sync_1"),
                "multi_src.customers->serve.sync_1#0,1");
    }

    @Test
    void omitted_tables_expand_to_the_latest_discovered_source_schema() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(new SourceResource("all_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null, null));
        store.schemas.save(new DiscoveredSourceModel("all_src", "mysql", 1L, new SourceModel(List.of(
                new SourceTable("orders", List.of(), List.of(), List.of()),
                new SourceTable("customers", List.of(), List.of(), List.of())))));
        store.artifacts().save(connectionSupplier("all_dest"));
        store.artifacts().save(new PipelineResource(
                "all", null, List.of(SourceRef.spec("all_src", true)), null, null,
                serve(FromRef.literal("all_src"), sync("sync_1", "all_dest")), null, null));

        DAG dag = new StoreBackedDagSource(store).dagFor("all");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("all_src.orders", "all_src.customers", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("all_src.orders", "serve.sync_1"),
                "all_src.customers->serve.sync_1#0,1");
    }

    @Test
    void from_regex_expands_selected_source_tables_instead_of_failing_the_dag_build() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(new SourceResource("players_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC,
                List.of(TableRef.literal("Player"), TableRef.literal("PlayerCard"), TableRef.literal("Orders")),
                null, null, null));
        store.artifacts().save(connectionSupplier("players_dest"));
        store.artifacts().save(new PipelineResource(
                "players", null, List.of(SourceRef.spec("players_src", true)), null, null,
                serve(FromRef.regex("Player.*"), sync("sync_1", "players_dest")), null, null));
        discovered(store, "players_src", "Player", "PlayerCard", "Orders");

        DAG dag = new StoreBackedDagSource(store).dagFor("players");

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder(
                "players_src.Player", "players_src.PlayerCard", "players_src.Orders", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("players_src.Player", "serve.sync_1"),
                "players_src.PlayerCard->serve.sync_1#0,1");
    }

    @Test
    void rejects_an_unqualified_table_selected_by_two_sources() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("src_a", "orders"));
        store.artifacts().save(cdcSource("src_b", "orders"));
        store.artifacts().save(connectionSupplier("dest"));
        store.artifacts().save(new PipelineResource(
                "ambiguous", null, List.of(SourceRef.spec("src_a", true), SourceRef.spec("src_b", true)), null, null,
                serve(FromRef.literal("orders"), sync("sync_1", "dest")), null, null));

        assertThatThrownBy(() -> new StoreBackedDagSource(store).dagFor("ambiguous"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.source-table-ambiguous"));
    }

    @Test
    void rejects_a_qualified_reference_to_a_table_not_selected_by_its_source() {
        FakeStorePort store = new FakeStorePort();
        store.artifacts().save(cdcSource("src_a", "orders"));
        store.artifacts().save(connectionSupplier("dest"));
        store.artifacts().save(new PipelineResource(
                "missing", null, List.of(SourceRef.spec("src_a", true)), null, null,
                serve(FromRef.literal("src_a.customers"), sync("sync_1", "dest")), null, null));

        assertThatThrownBy(() -> new StoreBackedDagSource(store).dagFor("missing"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException coded = (TapstateException) thrown;
                    assertThat(coded.code().code()).isEqualTo("actuation.source-table-not-discovered");
                    assertThat(coded.args()).containsEntry("source", "src_a");
                    assertThat(coded.args()).containsEntry("table", "customers");
                });
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

    /** Persists the source model a production sync start requires before constructing its DAG. */
    private static void discovered(FakeStorePort store, String sourceId, String... tables) {
        List<SourceTable> discovered = new ArrayList<>();
        for (String table : tables) {
            discovered.add(new SourceTable(table, List.of(), List.of(), List.of()));
        }
        store.schemas.save(new DiscoveredSourceModel(
                sourceId, "mysql", 1L, new SourceModel(discovered)));
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
        private final SrsLogStore srsLog = new InMemorySrsLogStore();
        private final InMemoryKeyedStateStore keyedState = new InMemoryKeyedStateStore();
        private final InMemoryNestDeadLetterStore nestDeadLetters = new InMemoryNestDeadLetterStore();

        @Override
        public SrsMetaStore meta() {
            return meta;
        }

        @Override
        public SrsLogStore srsLog() {
            return srsLog;
        }

        @Override
        public io.tapstate.spi.store.KeyedStateStore keyedState() {
            return keyedState;
        }

        @Override
        public io.tapstate.spi.store.NestDeadLetterStore nestDeadLetters() {
            return nestDeadLetters;
        }
    }
}
