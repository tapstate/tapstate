package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
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

    // ---- fixtures ----------------------------------------------------------------------

    private static InMemoryStorePort seededPipeline() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null));
        store.artifacts().save(new SourceResource("orders_dest", null, "mongodb", Map.of("uri", "u"),
                null, null, null, null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of("orders_src"), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null, null)), null, null),
                null, null));
        OpenRingGenerations.forSources(store, "orders_src");
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
