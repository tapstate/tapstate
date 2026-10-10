package io.tapstate.app;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlanningFactsTest {

    @ParameterizedTest
    @ValueSource(strings = {"source", "simple", "multi-table", "union", "nest", "join", "js"})
    void activationNeverDrawsBeyondItsFrozenCompilerGeometry(String kind) {
        InMemoryStorePort store = store(kind);
        DagSource.PlannedStart start = source(store).prepareStart("p", "default").plan();
        DagSource.PlanningFacts facts = start.planningFacts().orElseThrow();

        assertThat(facts.plannedStableIds()).containsExactly("a", "b", "c");
        assertThat(store.derivedSchemas().latest("p", "src.orders")).isEmpty();
        assertThat(store.keyedState().load("nest.shape.p", "step")).isEmpty();
        DagSource.PlannedDag actual = start.build(null).planned();
        assertThatCode(() -> DagResourceAnalyzer.requireSameGeometry(facts, actual)).doesNotThrowAnyException();
        assertThat(facts.vertices()).hasSize(actual.dag().vertices().size());
        assertThat(facts.perMemberUpperBounds()).containsOnlyKeys("a", "b", "c");
        for (var vertex : actual.dag()) {
            var reserved = facts.vertices().get(vertex.getName());
            assertThat(vertex.determineLocalParallelism(4)).isLessThanOrEqualTo(reserved.localProcessorSlots());
        }
        if (kind.equals("js")) {
            assertThat(facts.unknownInputs()).containsExactly("buffered-records:step:transform-output");
        } else {
            assertThat(facts.unknownInputs()).isEmpty();
        }
        actual.dag().newVertex("extra", ProcessorMetaSupplier.forceTotalParallelismOne(
                com.hazelcast.jet.core.ProcessorSupplier.of(() -> null), "extra"));
        assertThatThrownBy(() -> DagResourceAnalyzer.requireSameGeometry(facts, actual))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void actualNativeWidthsIncludeRoutersAndStandInsWithoutMultiplyingSourceReaders() {
        DagSource.PlanningFacts facts = source(store("simple")).prepareStart("p", "default").plan()
                .planningFacts().orElseThrow();

        assertThat(facts.shape().nodes().get("src").effective()).isEqualTo(1);
        assertThat(facts.vertices().get("src").effectiveProcessors()).isEqualTo(1);
        assertThat(facts.vertices().get("step").localProcessorSlots()).isEqualTo(2);
        assertThat(facts.vertices().values()).filteredOn(DagSource.VertexResources::writer)
                .singleElement().satisfies(writer -> assertThat(writer.localProcessorSlots()).isEqualTo(2));
        assertThat(facts.perMemberUpperBounds().get("a").processors()).isEqualTo(7);
        assertThat(facts.perMemberUpperBounds().get("a").blockingProcessors()).isEqualTo(3);
        assertThat(facts.perMemberUpperBounds().get("a").writers()).isEqualTo(2);
        assertThat(facts.perMemberUpperBounds().get("a").connectorInstances()).isEqualTo(4);
        assertThat(facts.clusterUpperBound().processors()).isEqualTo(21);
        assertThat(facts.clusterUpperBound().writers()).isEqualTo(6);
        assertThat(facts.edges()).hasSize(4).allSatisfy(edge -> assertThat(edge.queueCapacity()).isEqualTo(1024));
        // One source-to-step edge, one step-to-router edge, two router-to-writer edges, and seven native
        // snapshot queues. The remote sender/receiver queues are real allocated queues too.
        assertThat(facts.perMemberUpperBounds().get("a").edgeQueueRecords()).isEqualTo(51L * 1024);
    }

    @Test
    void oneSnapshotSessionAndTailAreCountedOnceForAMultiTableSource() {
        DagSource.PlanningFacts facts = source(store("multi-table")).prepareStart("p", "default").plan()
                .planningFacts().orElseThrow();

        assertThat(facts.vertices()).containsKeys("src.orders", "src.items");
        assertThat(facts.perMemberUpperBounds().get("a").connectorInstances()).isEqualTo(4);
        assertThat(facts.clusterUpperBound().connectorInstances()).isEqualTo(8);
        assertThat(facts.diagnostics()).contains("connector-owned input allocations are unmeasured",
                "connector pool and connection ceilings are unavailable");
    }

    @Test
    void planningDoesNotCompareAnOldNestShapeBeforeThePendingPurge() {
        InMemoryStorePort store = store("nest");
        store.keyedState().save("nest.shape.p", "step", "old-path".getBytes(StandardCharsets.UTF_8));

        DagSource.PlannedStart start = source(store).prepareStart("p", "default").plan();

        assertThat(start.planningFacts()).isPresent();
        assertThatThrownBy(() -> start.build(null)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code().code()).isEqualTo("nest.state-paths-changed"));
        store.keyedState().dropNamespace("nest.shape.p");
        assertThatCode(() -> start.build(null)).doesNotThrowAnyException();
        assertThat(store.keyedState().load("nest.shape.p", "step")).isPresent();
    }

    @Test
    void aCompilerWithoutTheActualRuntimeCapacitiesSuppliesNoAdmissionFacts() {
        assertThat(new StoreBackedDagSource(store("simple")).prepareStart("p", "default").plan()
                .planningFacts()).isEmpty();
    }

    @Test
    void laterDiscoveryCannotExpandSelectedTablesOrReplaceTheAdmittedColumns() {
        InMemoryStorePort store = store("multi-table");
        store.artifacts().save(new SourceResource("src", null, "mysql", Map.of("host", "h"),
                SourceMode.CDC, null, null, null));
        DagSource.StartPreparation prepared = source(store).prepareStart("p", "default");
        store.schemas().save(new DiscoveredSourceModel("src", "mysql", 2,
                new SourceModel(List.of(new SourceTable("orders", List.of(new SourceField("changed", "varchar")),
                        List.of(), List.of()), table("items"), table("unexpected")))));

        DagSource.PlannedStart planned = prepared.plan();

        assertThat(planned.sourceModels().orElseThrow().get("src").tables())
                .extracting(SourceTable::name).containsExactly("orders", "items");
        assertThat(planned.sourceModels().orElseThrow().get("src").tables().getFirst().fields())
                .extracting(SourceField::name).containsExactly("id", "order_id", "name");
        assertThat(planned.planningFacts().orElseThrow().vertices())
                .containsKeys("src.orders", "src.items").doesNotContainKey("src.unexpected");
        planned.activate();
        assertThat(store.derivedSchemas().pinned("p", "src.orders").orElseThrow().schema())
                .containsOnlyKeys("id", "order_id", "name");
        assertThatCode(() -> planned.build(null)).doesNotThrowAnyException();
    }

    @Test
    void literalSourceOnlySelectionFreezesAbsentDiscoveryWithoutInventingAZeroColumnModel() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null));
        store.artifacts().save(new PipelineResource("p", null, List.of(SourceRef.spec("src", true)),
                null, null, null, null, null));
        OpenRingGenerations.forSources(store, "src");

        DagSource.PlannedStart planned = source(store).prepareStart("p", "default").plan();

        assertThat(planned.sourceModels()).isPresent();
        assertThat(planned.sourceModels().orElseThrow()).doesNotContainKey("src");
        assertThat(planned.planningFacts().orElseThrow().vertices()).containsOnlyKeys("src");
        assertThat(planned.planningFacts().orElseThrow().unknownInputs()).isEmpty();
        assertThatCode(() -> planned.build(null)).doesNotThrowAnyException();
    }

    @Test
    void selectedSourcesComeFromTheActualVerticesRatherThanTheArtifactReferenceList() {
        InMemoryStorePort store = store("simple");
        store.artifacts().save(new SourceResource("unused", null, "mysql", Map.of("host", "u"), SourceMode.CDC,
                List.of(TableRef.literal("other")), null, null));
        store.schemas().save(new DiscoveredSourceModel("unused", "mysql", 1,
                new SourceModel(List.of(table("other")))));
        PipelineResource pipeline = (PipelineResource) store.artifacts().get("p").orElseThrow();
        store.artifacts().save(new PipelineResource(pipeline.id(), pipeline.metadata(),
                List.of(SourceRef.spec("src", true), SourceRef.spec("unused", true)), pipeline.transforms(),
                pipeline.view(), pipeline.serve(), pipeline.settings(), pipeline.experimental()));

        DagSource.PlannedStart planned = source(store).prepareStart("p", "default").plan();
        DagSource.PlanningFacts facts = planned.planningFacts().orElseThrow();

        assertThat(planned.sourceModels().orElseThrow()).containsKeys("src", "unused");
        assertThat(facts.selectedSourceIds()).containsExactly("src");
        assertThat(facts.vertices()).doesNotContainKey("unused");
        DagSource.PlanningFacts incomplete = new DagSource.PlanningFacts(facts.plannedStableIds(), facts.shape(),
                facts.perMemberUpperBounds(), facts.clusterUpperBound(), facts.vertices(), facts.edges(),
                facts.unknownInputs(), facts.diagnostics());
        assertThat(incomplete.unknownInputs()).contains("selected-sources-unproven");
    }

    @Test
    void theActualMigrationChunkChangesTheAdmissionDecisionBeforeActivation() {
        InMemoryStorePort smallStore = store("nest");
        PipelineResource pipeline = (PipelineResource) smallStore.artifacts().get("p").orElseThrow();
        TransformBody.Nest body = (TransformBody.Nest) ((Step.Inline) pipeline.transforms().getFirst()).body();
        String namespace = NestTopology.compile("p", "step", body,
                alias -> new NestTable(alias.equals("o") ? "orders" : "items", List.of("id")))
                .assembler().mapName();
        var small = source(smallStore, NestSettings.defaults().withMigrationBatchSize(namespace, 1))
                .prepareStart("p", "default").plan().planningFacts().orElseThrow();
        InMemoryStorePort largeStore = store("nest");
        var large = source(largeStore, NestSettings.defaults().withMigrationBatchSize(namespace, 512))
                .prepareStart("p", "default").plan().planningFacts().orElseThrow();
        ClusterCapacityDemand demand = small.perMemberUpperBounds().get("a");
        ClusterCapacityLimits ceilings = new ClusterCapacityLimits(Long.MAX_VALUE, Long.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE, demand.bufferedRecords(), Long.MAX_VALUE);

        assertThat(small.unknownInputs()).isEmpty();
        assertThat(large.unknownInputs()).isEmpty();
        assertThat(ceilings.violations(ClusterCapacityDemand.ZERO, demand)).isEmpty();
        assertThat(ceilings.violations(ClusterCapacityDemand.ZERO, large.perMemberUpperBounds().get("a")))
                .singleElement().satisfies(refused -> assertThat(refused.resource()).isEqualTo("buffered-records"));
        assertThat(smallStore.keyedState().load("nest.shape.p", "step")).isEmpty();
        assertThat(largeStore.keyedState().load("nest.shape.p", "step")).isEmpty();
    }

    private static StoreBackedDagSource source(InMemoryStorePort store) {
        return source(store, NestSettings.defaults());
    }

    private static StoreBackedDagSource source(InMemoryStorePort store, NestSettings settings) {
        return new StoreBackedDagSource(store, settings, StoreReachability.assumingReachable(),
                SourcePlacement.anyMember(), () -> List.of("a", "b", "c"), ParallelismBudget.DEFAULTS,
                new DagResourceAnalyzer.Limits(4, 3000, 1024, 1000));
    }

    private static InMemoryStorePort store(String kind) {
        InMemoryStorePort store = new InMemoryStorePort();
        boolean multi = kind.equals("multi-table");
        boolean stateful = kind.equals("nest") || kind.equals("join");
        boolean multipleSources = stateful || kind.equals("union");
        store.artifacts().save(new SourceResource("src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                multi ? List.of(TableRef.literal("orders"), TableRef.literal("items"))
                        : List.of(TableRef.literal("orders")), null, null));
        store.schemas().save(new DiscoveredSourceModel("src", "mysql", 1,
                new SourceModel(multi ? List.of(table("orders"), table("items")) : List.of(table("orders")))));
        if (multipleSources) {
            store.artifacts().save(new SourceResource("child", null, "mysql", Map.of("host", "c"), SourceMode.CDC,
                    List.of(TableRef.literal("items")), null, null));
            store.schemas().save(new DiscoveredSourceModel("child", "mysql", 1,
                    new SourceModel(List.of(table("items")))));
        }
        store.artifacts().save(new SourceResource("dest", null, "mongodb", Map.of("uri", "u"),
                null, null, null, null));
        FromClause input = FromClause.list(FromRef.literal("src"));
        TransformBody body = new TransformBody.Filter("true");
        if (kind.equals("js")) {
            body = new TransformBody.Js("function process(record, ctx) { return record; }");
        }
        if (kind.equals("union")) {
            input = FromClause.list(FromRef.literal("orders"), FromRef.literal("items"));
            body = new TransformBody.Union();
        }
        if (stateful) {
            input = FromClause.aliases(Map.of("o", FromRef.literal("orders"), "i", FromRef.literal("items")));
            body = kind.equals("join") ? new TransformBody.Join(JoinEngine.BUILTIN,
                    "SELECT o.id AS id, i.name AS item_name FROM o LEFT JOIN i ON o.id = i.order_id")
                    : new TransformBody.Nest(null, null, new NestRoot("o", List.of("id"), null, null,
                            List.of(new Embed("i", Map.of("order_id", "id"), EmbedAs.ARRAY, "items",
                                    List.of("id"), null, null, null))));
        }
        boolean sourceOnly = kind.equals("source");
        List<Step> transforms = sourceOnly || multi ? null : List.of(Step.inline("step", input, body,
                new ExecutionSpec(6, new BatchSpec(7, "0ms")), null));
        ServeBlock serve = sourceOnly ? null : new ServeBlock.Inline(null,
                FromRef.literal(multi ? "src" : "step"), List.of(new SyncElement("out", "dest", null, null,
                        null, null, new ExecutionSpec(6, new BatchSpec(11, "0ms")))), null, null);
        store.artifacts().save(new PipelineResource("p", null, multipleSources
                ? List.of(SourceRef.spec("src", true), SourceRef.spec("child", true))
                : List.of(SourceRef.spec("src", true)), transforms, null, serve, null, null));
        OpenRingGenerations.forSources(store, multipleSources ? new String[]{"src", "child"} : new String[]{"src"});
        return store;
    }

    private static SourceTable table(String name) {
        return new SourceTable(name, List.of(new SourceField("id", "bigint", TapstateType.INT64),
                new SourceField("order_id", "bigint", TapstateType.INT64),
                new SourceField("name", "varchar", TapstateType.STRING)), List.of("id"), List.of());
    }
}
