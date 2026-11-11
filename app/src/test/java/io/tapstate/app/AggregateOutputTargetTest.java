package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
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
 * What an aggregate step hands the sink behind it, and what it needs of the rows in front of it.
 *
 * <p>The rows it sends are rows of its own. A sink told nothing about them would file them under the
 * name and the key of the table the step read, where every group would overwrite an input row; so the
 * step publishes a table named by itself and keyed by its group names, and the graph is built with it.
 */
class AggregateOutputTargetTest {

    @Test
    void theStepsRowsReachTheSinkAsATableOfItsOwnKeyedByTheGroupNames() {
        Bound bound = bind(pipeline("", "region: $region"), List.of("id"));

        assertThat(bound.targets).containsOnlyKeys("by_region");
        TargetTable target = bound.targets.get("by_region");
        assertThat(target.name()).isEqualTo("by_region");
        assertThat(target.fields()).extracting(TargetField::name).containsExactly("region", "orders", "revenue");
        assertThat(target.fields()).filteredOn(TargetField::primaryKey).extracting(TargetField::name)
                .containsExactly("region");
        assertThat(target.indexes()).containsExactly(new TargetIndex(List.of("region"), true));
        assertThat(target.fields()).filteredOn(field -> field.name().equals("orders")).singleElement()
                .satisfies(field -> assertThat(field.inferredType()).isEqualTo(TapstateType.INT64));
    }

    @Test
    void theStepIsDrawnAsOneVertexBetweenItsInputAndTheSink() {
        DAG dag = bind(pipeline("", "region: $region"), List.of("id")).dag;

        assertThat(dag.getVertex("by_region")).isNotNull();
        assertThat(dag.getInboundEdges("by_region")).hasSize(1);
        assertThat(dag.getOutboundEdges("by_region")).isNotEmpty();
    }

    @Test
    void severalGroupNamesMakeTheKeyInTheOrderTheyAreWritten() {
        TargetTable target = bind(pipeline("", "region: $region\n      status: $status"), List.of("id"))
                .targets.get("by_region");

        assertThat(target.indexes()).containsExactly(new TargetIndex(List.of("region", "status"), true));
    }

    @Test
    void aStepNamingNoKeyOverATableWithoutOneIsRefusedWithItsCode() {
        assertThatThrownBy(() -> bind(pipeline("", "region: $region"), List.of()))
                .isInstanceOfSatisfying(TapstateException.class,
                        refused -> assertThat(refused.code().code()).isEqualTo("aggregate.key-required"));
    }

    @Test
    void aStepNamingItsOwnKeyNeedsNoneOfTheTable() {
        Bound bound = bind(pipeline("key: [ id ]", "region: $region"), List.of());

        assertThat(bound.targets.get("by_region").indexes()).containsExactly(new TargetIndex(List.of("region"), true));
    }

    private record Bound(Map<String, TargetTable> targets, DAG dag) {
    }

    private static String pipeline(String key, String groupBy) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: sales
                source: orders_src
                transforms:
                  - id: by_region
                    from: [ orders ]
                    type: aggregate
                    %s
                    groupBy:
                      %s
                    fields:
                      orders: { count: true }
                      revenue: { sum: $amount }
                serve:
                  from: by_region
                  sync: [ { id: dw, source: target, write_mode: upsert } ]
                """.formatted(key, groupBy);
    }

    private static final String ORDERS_SRC = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: target
            connector: mongodb
            config: { uri: u }
            """;

    private static Bound bind(String pipeline, List<String> tableKey) {
        DslParser parser = new DslParser();
        List<Resource> resources = new ArrayList<>();
        for (String document : List.of(ORDERS_SRC, TARGET, pipeline)) {
            resources.add(parser.parse(document));
        }
        Workspace.of(resources);
        InMemoryStorePort store = new InMemoryStorePort();
        resources.forEach(store.artifacts()::save);
        OpenRingGenerations.forSources(store, "orders_src");
        store.schemas().save(new DiscoveredSourceModel("orders_src", "mysql", 0L,
                new SourceModel(List.of(new SourceTable("orders", List.of(
                        new SourceField("id", "bigint", TapstateType.INT64),
                        new SourceField("region", "varchar", TapstateType.STRING),
                        new SourceField("status", "varchar", TapstateType.STRING),
                        new SourceField("amount", "bigint", TapstateType.INT64)), tableKey, null)))));
        Map<String, Map<String, TargetTable>> captured = new LinkedHashMap<>();
        DAG dag = new StoreBackedDagSource(store, new StoreBackedDagSource.SinkWriterBinder() {
            @Override
            public SupplierEx<SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, TargetTable target, PipelineNode node) {
                return (SupplierEx<SinkWriter>) () -> null;
            }

            @Override
            public SupplierEx<SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, Map<String, TargetTable> targets, PipelineNode node) {
                captured.put(connectorId, targets);
                return (SupplierEx<SinkWriter>) () -> null;
            }
        }).dagFor("sales");
        return new Bound(captured.getOrDefault("mongodb", Map.of()), dag);
    }
}
