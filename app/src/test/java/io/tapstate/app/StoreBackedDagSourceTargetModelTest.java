package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Coverage for how the store-backed DAG source feeds a sink's resolved target models: the write-side target
 * tables resolved from the pipeline sources' discovered models reach the sink binder, so the sink creates
 * each target by that model and keys an upsert on its primary key. A sync start refuses any reaching source
 * whose model has not been discovered, while view-only materialization retains its pre-discovery path.
 */
class StoreBackedDagSourceTargetModelTest {

    @Test
    void a_view_target_is_keyed_by_the_source_tables_that_reach_it() {
        // The sink resolves a target by the table the row came from, so a view - which collapses every
        // upstream table into one collection - must answer to each of those table names. Keyed by the
        // view's own name instead, every lookup misses and the rows land under the source table: the
        // right rows, silently in the wrong collection.
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource(ViewTargetResolver.STATE_STORE_SOURCE_ID, null,
                "mongodb", Map.of("uri", "u"), null, null, null, null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src"), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "order_id", null, null),
                null, null, null));
        List<Map<String, TargetTable>> bound = new ArrayList<>();

        new StoreBackedDagSource(store, mapCapturingBinder(bound)).dagFor("p");

        assertThat(bound).singleElement().satisfies(targets -> {
            assertThat(targets).containsOnlyKeys("orders");
            assertThat(targets.get("orders").name()).isEqualTo("order_state");
        });
    }

    @Test
    void a_view_target_carries_the_key_index_the_collection_is_read_by() {
        // The index travels with the target model rather than being applied out of band, so whoever
        // creates the collection creates its index in the same act.
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource(ViewTargetResolver.STATE_STORE_SOURCE_ID, null,
                "mongodb", Map.of("uri", "u"), null, null, null, null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src"), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "order_id", null, null),
                null, null, null));
        List<TargetTable> bound = new ArrayList<>();

        new StoreBackedDagSource(store, capturingBinder(bound)).dagFor("p");

        assertThat(bound).singleElement().satisfies(target -> {
            assertThat(target.name()).isEqualTo("order_state");
            assertThat(target.indexes()).containsExactly(new TargetIndex(List.of("order_id"), true));
        });
    }

    @Test
    void feeds_the_resolved_target_model_to_the_sink_binder() {
        InMemoryStorePort store = seededPipeline();
        store.schemas().save(discovered("orders_src", "mysql", new SourceTable(
                "orders",
                List.of(new SourceField("id", "INT"), new SourceField("amount", "DECIMAL")),
                List.of("id"),
                List.of())));
        List<TargetTable> bound = new ArrayList<>();

        new StoreBackedDagSource(store, capturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly(new TargetTable("orders", List.of(
                new TargetField("id", "INT", true),
                new TargetField("amount", "DECIMAL", false))));
    }

    @Test
    void refuses_a_sync_start_when_the_source_schema_was_never_discovered() {
        InMemoryStorePort store = seededPipeline();

        assertThatThrownBy(() -> new StoreBackedDagSource(store).validateStart("p"))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-schema-not-discovered")
                .hasMessageContaining("orders_src");
    }

    @Test
    void requires_discovery_only_for_the_source_that_reaches_sync() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("address_src"));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));

        assertThatCode(() -> new StoreBackedDagSource(store).validateStart("p"))
                .doesNotThrowAnyException();
    }

    @Test
    void follows_a_transform_chain_to_the_source_that_reaches_sync() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("keep_recent"),
                Step.inline("keep_recent", FromClause.list(FromRef.literal("address_src")),
                        new TransformBody.Filter("true"), null, null));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));

        assertThatCode(() -> new StoreBackedDagSource(store).validateStart("p"))
                .doesNotThrowAnyException();
    }

    @Test
    void still_requires_every_source_that_reaches_sync_through_a_union() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("merged"),
                Step.inline("merged", FromClause.list(FromRef.literal("orders"), FromRef.literal("PlayerAddress")),
                        new TransformBody.Union(), null, null));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));

        assertThatThrownBy(() -> new StoreBackedDagSource(store).validateStart("p"))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class)
                .hasMessageContaining("actuation.source-schema-not-discovered")
                .hasMessageContaining("orders_src");
    }

    @Test
    void does_not_require_an_undiscovered_view_source_when_sync_reads_another_source() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("address_src"));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src", "address_src"), null,
                new ViewBlock.Inline("orders_view", FromRef.literal("orders_src"), "id", null, null),
                new ServeBlock.Inline(null, FromRef.literal("address_src"), List.of(new SyncElement(
                        "sync_1", "orders_dest", null, null, null, null)), null, null),
                null, null));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));

        assertThatCode(() -> new StoreBackedDagSource(store).validateStart("p"))
                .doesNotThrowAnyException();
    }

    @Test
    void leaves_a_view_only_pipeline_allowed_before_source_schema_discovery() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource(ViewTargetResolver.STATE_STORE_SOURCE_ID, null,
                "mongodb", Map.of("uri", "u"), null, null, null, null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src"), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "id", null, null),
                null, null, null));

        new StoreBackedDagSource(store).validateStart("p");
    }

    @Test
    void gives_each_sync_its_own_renamed_target_model() {
        InMemoryStorePort store = seededPipeline(
                new SyncElement("mongo", "orders_dest", null,
                        new RenameSpec(Map.of("orders", "player_address"), null, null, null), null, null),
                new SyncElement("warehouse", "orders_dest", null,
                        new RenameSpec(null, RenameCase.LOWER, "ods_", null), null, null));
        store.schemas().save(discovered("orders_src", "mysql", new SourceTable(
                "orders", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));
        List<TargetTable> bound = new ArrayList<>();

        new StoreBackedDagSource(store, capturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly(
                new TargetTable("player_address", List.of(new TargetField("id", "INT", true))),
                new TargetTable("ods_orders", List.of(new TargetField("id", "INT", true))));
    }

    @Test
    void renames_with_the_table_of_the_source_the_serve_block_reads() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("address_src"));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));
        Map<String, TargetTable> bound = new LinkedHashMap<>();

        new StoreBackedDagSource(store, capturingMapBinder(bound)).dagFor("p");

        assertThat(bound).containsEntry("PlayerAddress", new TargetTable(
                "player_address", List.of(new TargetField("id", "INT", true))));
    }

    @Test
    void binds_the_model_of_the_source_the_serve_block_reads_not_the_first_discovered_one() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("address_src"));
        store.schemas().save(discovered("orders_src", "mysql", new SourceTable(
                "orders", List.of(new SourceField("total", "DECIMAL")), List.of("total"), List.of())));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));
        Map<String, TargetTable> bound = new LinkedHashMap<>();

        new StoreBackedDagSource(store, capturingMapBinder(bound)).dagFor("p");

        assertThat(bound).containsEntry("PlayerAddress", new TargetTable(
                "player_address", List.of(new TargetField("id", "INT", true))));
    }

    @Test
    void renames_with_the_table_a_serve_block_reaches_through_a_transform_chain() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("keep_recent"),
                Step.inline("keep_recent", FromClause.list(FromRef.literal("address_src")),
                        new TransformBody.Filter("true"), null, null));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));
        Map<String, TargetTable> bound = new LinkedHashMap<>();

        new StoreBackedDagSource(store, capturingMapBinder(bound)).dagFor("p");

        assertThat(bound).containsEntry("PlayerAddress", new TargetTable(
                "player_address", List.of(new TargetField("id", "INT", true))));
    }

    @Test
    void renames_with_the_table_a_serve_block_names_directly() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("PlayerAddress"));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));
        Map<String, TargetTable> bound = new LinkedHashMap<>();

        new StoreBackedDagSource(store, capturingMapBinder(bound)).dagFor("p");

        assertThat(bound).containsEntry("PlayerAddress", new TargetTable(
                "player_address", List.of(new TargetField("id", "INT", true))));
    }

    @Test
    void binds_the_qualified_table_of_a_non_first_source() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("address_src.PlayerAddress"));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));
        Map<String, TargetTable> bound = new LinkedHashMap<>();

        new StoreBackedDagSource(store, capturingMapBinder(bound)).dagFor("p");

        assertThat(bound).containsEntry("PlayerAddress", new TargetTable(
                "player_address", List.of(new TargetField("id", "INT", true))));
    }

    @Test
    void binds_target_models_for_all_sources_when_a_step_merges_several_upstreams() {
        InMemoryStorePort store = seededMultiSourcePipeline(FromRef.literal("merged"),
                Step.inline("merged", FromClause.list(FromRef.literal("orders"), FromRef.literal("PlayerAddress")),
                        new TransformBody.Union(), null, null));
        store.schemas().save(discovered("orders_src", "mysql", new SourceTable(
                "orders", List.of(new SourceField("id", "INT"), new SourceField("total", "DECIMAL")),
                List.of("id"), List.of())));
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT"), new SourceField("street", "VARCHAR")),
                List.of("id"), List.of())));
        Map<String, TargetTable> bound = new LinkedHashMap<>();

        new StoreBackedDagSource(store, capturingMapBinder(bound)).dagFor("p");

        assertThat(bound)
                .containsEntry("orders", new TargetTable("orders", List.of(
                        new TargetField("id", "INT", true),
                        new TargetField("total", "DECIMAL", false))))
                .containsEntry("PlayerAddress", new TargetTable("player_address", List.of(
                        new TargetField("id", "INT", true),
                        new TargetField("street", "VARCHAR", false))));
    }

    // ---- fixtures ----------------------------------------------------------------------

    /**
     * The topology is the only place that knows which node a sink is, so the binder is where that has to
     * be handed over. A sink whose node never arrived opens its connector scoped to nothing and writes
     * every row correctly, so nothing about the data says the identity went missing.
     */
    @Test
    void the_node_a_sync_element_is_names_the_pipeline_and_the_element() {
        InMemoryStorePort store = seededPipeline();
        store.schemas().save(discovered("orders_src", "mysql", new SourceTable(
                "orders",
                List.of(new SourceField("id", "INT"), new SourceField("amount", "DECIMAL")),
                List.of("id"),
                List.of())));
        List<PipelineNode> bound = new ArrayList<>();

        new StoreBackedDagSource(store, nodeCapturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly(new PipelineNode("p", "sync_1"));
    }

    /**
     * An element that declares no id of its own is named by the source it writes to. Left unnamed it
     * would have no node at all, and a sync element without an id is the ordinary shape — the id is only
     * required when a query backend refers to it.
     */
    @Test
    void a_sync_element_with_no_id_is_named_by_the_source_it_writes_to() {
        InMemoryStorePort store = seededPipeline(new SyncElement(null, "orders_dest", null, null, null, null));
        store.schemas().save(discovered("orders_src", "mysql", new SourceTable(
                "orders",
                List.of(new SourceField("id", "INT"), new SourceField("amount", "DECIMAL")),
                List.of("id"),
                List.of())));
        List<PipelineNode> bound = new ArrayList<>();

        new StoreBackedDagSource(store, nodeCapturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly(new PipelineNode("p", "orders_dest"));
    }

    /** A view's sink is a node too, named by the view — the same seam, reached by the other binding. */
    @Test
    void the_node_a_view_sink_is_names_the_pipeline_and_the_view() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource(ViewTargetResolver.STATE_STORE_SOURCE_ID, null,
                "mongodb", Map.of("uri", "u"), null, null, null, null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src"), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders_src"), "order_id", null, null),
                null, null, null));
        List<PipelineNode> bound = new ArrayList<>();

        new StoreBackedDagSource(store, nodeCapturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly(new PipelineNode("p", "order_state"));
    }

    private static StoreBackedDagSource.SinkWriterBinder nodeCapturingBinder(List<PipelineNode> bound) {
        return (connectorId, settings, writeMode, ddl, target, node) -> {
            bound.add(node);
            return (SupplierEx<SinkWriter>) () -> null;
        };
    }

    private static InMemoryStorePort seededPipeline() {
        return seededPipeline(new SyncElement("sync_1", "orders_dest", null, null, null, null));
    }

    private static InMemoryStorePort seededPipeline(SyncElement... syncElements) {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource("orders_dest", null, "mongodb", Map.of("uri", "u"),
                null, null, null, null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src"), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(syncElements), null, null),
                null, null));
        OpenRingGenerations.forSources(store, "orders_src");
        return store;
    }

    /**
     * A two-source pipeline whose sink renames {@code PlayerAddress}. Both selected source tables are bound,
     * while only {@code address_src} carries the renamed table.
     */
    private static InMemoryStorePort seededMultiSourcePipeline(FromRef serveFrom, Step... transforms) {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource("address_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("PlayerAddress")), null, null, null));
        store.artifacts().save(new SourceResource("orders_dest", null, "mongodb", Map.of("uri", "u"),
                null, null, null, null, null));
        store.artifacts().save(pipelineOf(serveFrom, transforms));
        return store;
    }

    /** That pipeline on its own: two sources, and one sink renaming {@code PlayerAddress}. */
    private static PipelineResource pipelineOf(FromRef serveFrom, Step... transforms) {
        return new PipelineResource("p", null, List.of("orders_src", "address_src"),
                transforms.length == 0 ? null : List.of(transforms), null,
                new ServeBlock.Inline(null, serveFrom, List.of(new SyncElement(
                        "sync_1", "orders_dest", null,
                        new RenameSpec(Map.of("PlayerAddress", "player_address"), null, null, null), null, null)),
                        null, null),
                null, null);
    }

    /** A binder that records the target it is handed and returns a sink supplier the build never opens. */
    /**
     * Captures the whole target map rather than a single model. The sink looks a target up by the table a
     * row came from, so which keys the map carries is the load-bearing part - a map holding the right model
     * under the wrong key is indistinguishable from a correct one until rows actually move.
     */
    private static StoreBackedDagSource.SinkWriterBinder mapCapturingBinder(
            List<Map<String, TargetTable>> bound) {
        return new StoreBackedDagSource.SinkWriterBinder() {
            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    io.tapstate.spi.sink.WriteMode writeMode, io.tapstate.spi.sink.DdlPolicy ddl,
                    TargetTable target, PipelineNode node) {
                bound.add(target == null ? Map.of() : Map.of(target.name(), target));
                return (SupplierEx<SinkWriter>) () -> null;
            }

            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    io.tapstate.spi.sink.WriteMode writeMode, io.tapstate.spi.sink.DdlPolicy ddl,
                    Map<String, TargetTable> targets, PipelineNode node) {
                bound.add(targets);
                return (SupplierEx<SinkWriter>) () -> null;
            }
        };
    }

    private static StoreBackedDagSource.SinkWriterBinder capturingBinder(List<TargetTable> bound) {
        return (connectorId, settings, writeMode, ddl, target, node) -> {
            bound.add(target);
            return (SupplierEx<SinkWriter>) () -> null;
        };
    }

    private static StoreBackedDagSource.SinkWriterBinder capturingMapBinder(Map<String, TargetTable> bound) {
        return new StoreBackedDagSource.SinkWriterBinder() {
            @Override
            public SupplierEx<? extends SinkWriter> bind(
                    String connectorId, Map<String, Object> settings, io.tapstate.spi.sink.WriteMode writeMode,
                    io.tapstate.spi.sink.DdlPolicy ddl, TargetTable target, PipelineNode node) {
                if (target != null) {
                    bound.put(target.name(), target);
                }
                return (SupplierEx<SinkWriter>) () -> null;
            }

            @Override
            public SupplierEx<? extends SinkWriter> bind(
                    String connectorId, Map<String, Object> settings, io.tapstate.spi.sink.WriteMode writeMode,
                    io.tapstate.spi.sink.DdlPolicy ddl, Map<String, TargetTable> targets, PipelineNode node) {
                bound.putAll(targets);
                return (SupplierEx<SinkWriter>) () -> null;
            }
        };
    }

    private static DiscoveredSourceModel discovered(String connectionId, String connectorId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, connectorId, 0L, new SourceModel(List.of(table)));
    }
}
