package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Coverage for how the store-backed DAG source feeds a sink's resolved target model: the write-side target
 * table resolved from the pipeline source's discovered model is what reaches the sink binder, so the sink
 * creates the target by that model and keys an upsert on its primary key. When no model has been discovered
 * the sink is bound with no target and falls back to a bare table id.
 */
class StoreBackedDagSourceTargetModelTest {

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
    void binds_a_null_target_when_the_source_schema_was_never_discovered() {
        InMemoryStorePort store = seededPipeline();
        List<TargetTable> bound = new ArrayList<>();

        new StoreBackedDagSource(store, capturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly((TargetTable) null);
    }

    @Test
    void applies_explicit_rename_without_a_discovered_model() {
        InMemoryStorePort store = seededPipeline(new SyncElement(
                "sync_1", "orders_dest", null,
                new RenameSpec(Map.of("orders", "player_address"), null, null, null), null, null));
        List<TargetTable> bound = new ArrayList<>();

        new StoreBackedDagSource(store, capturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly(new TargetTable("player_address", List.of()));
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
    void renames_with_the_table_whose_discovered_model_binds_the_sink() {
        InMemoryStorePort store = seededMultiSourcePipeline();
        store.schemas().save(discovered("address_src", "mysql", new SourceTable(
                "PlayerAddress", List.of(new SourceField("id", "INT")), List.of("id"), List.of())));
        List<TargetTable> bound = new ArrayList<>();

        new StoreBackedDagSource(store, capturingBinder(bound)).dagFor("p");

        assertThat(bound).containsExactly(new TargetTable(
                "player_address", List.of(new TargetField("id", "INT", true))));
    }

    // ---- fixtures ----------------------------------------------------------------------

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
        return store;
    }

    private static InMemoryStorePort seededMultiSourcePipeline() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource("address_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("PlayerAddress")), null, null, null));
        store.artifacts().save(new SourceResource("orders_dest", null, "mongodb", Map.of("uri", "u"),
                null, null, null, null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src", "address_src"), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"), List.of(new SyncElement(
                        "sync_1", "orders_dest", null,
                        new RenameSpec(Map.of("PlayerAddress", "player_address"), null, null, null), null, null)),
                        null, null),
                null, null));
        return store;
    }

    /** A binder that records the target it is handed and returns a sink supplier the build never opens. */
    private static StoreBackedDagSource.SinkWriterBinder capturingBinder(List<TargetTable> bound) {
        return (connectorId, settings, writeMode, ddl, target) -> {
            bound.add(target);
            return (SupplierEx<SinkWriter>) () -> null;
        };
    }

    private static DiscoveredSourceModel discovered(String connectionId, String connectorId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, connectorId, 0L, new SourceModel(List.of(table)));
    }
}
