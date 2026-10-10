package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.lifecycle.AwaitedLoad;
import io.tapstate.core.lifecycle.HoldsChangesForLoads;
import io.tapstate.core.lifecycle.LoadLandings;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Structure-assert coverage for the pipeline DAG builder: builds a DAG from a hand-built
 * {@link PipelineResource} with injected fake bindings and asserts the vertex and edge topology,
 * without running a Jet job. All leaves (source, sink, transform ports) are injected doubles, so
 * these tests carry no dependency on the SRS source path.
 */
class PipelineDagBuilderTest {

    @Test
    void sourcePressureMeasurementPreservesPlacementAndOutputRetries() throws Exception {
        Address owner = new Address("127.0.0.1", 5701);
        Address other = new Address("127.0.0.1", 5702);
        ProcessorMetaSupplier placed = new PlacedSource(owner);
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of(SourceRef.bare("orders_src")), null, null,
                serve(FromRef.literal("orders_src"), sync("sync_1", "orders_dest")), null, null);
        DagBindings bindings = new DagBindings(id -> placed,
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> stubWriter(), ref -> List.of("orders_src"));
        ProcessorMetaSupplier assembled = PipelineDagBuilder.build(pipeline, bindings)
                .getVertex("orders_src").getMetaSupplier();
        assertThat(assembled.preferredLocalParallelism()).isEqualTo(1);
        assertThat(assembled.initIsCooperative()).isFalse();
        assertThat(assembled.closeIsCooperative()).isFalse();
        assertThat(assembled.getTags()).containsExactlyEntriesOf(placed.getTags());
        var suppliers = assembled.get(List.of(owner, other));
        assertThat(suppliers.apply(other).get(1)).singleElement()
                .isNotInstanceOf(StageOutputPressureProcessor.class);
        Processor measured = suppliers.apply(owner).get(1).iterator().next();
        assertThat(measured).isInstanceOf(StageOutputPressureProcessor.class);
        assertThat(measured.isCooperative()).isFalse();
        TestOutbox outbox = new TestOutbox(1);
        measured.init(outbox, new TestProcessorContext());
        assertThat(outbox.offer("occupied")).isTrue();
        assertThat(measured.complete()).isFalse();
        assertThat(outbox.queue(0)).containsExactly("occupied");
        outbox.queue(0).clear();
        assertThat(measured.complete()).isTrue();
        assertThat(outbox.queue(0)).containsExactly("source-row");
        measured.close();
    }

    @Test
    void loadGateBindsBeforePressureAndRetriesEmitOneBusinessRow() throws Exception {
        var landed = new java.util.concurrent.atomic.AtomicBoolean();
        var sourceFactory = new GateAwareSourceFactory();
        ProcessorMetaSupplier raw = ProcessorMetaSupplier.of(ProcessorSupplier.of(sourceFactory));
        SinkAckFactory acks = new GateAckFactory(landed);
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of(SourceRef.bare("orders_src")), null, null,
                serve(FromRef.literal("orders_src"), sync("sync_1", "orders_dest")), null, null);
        DagBindings bindings = new DagBindings(id -> raw,
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> stubWriter(), ref -> List.of("orders_src"));
        String sink = "serve.sync_1";
        ExecutionShape shape = new ExecutionShape(1, Map.of(sink, new NodeParallelism(sink, 2,
                NodeParallelism.Origin.EXPLICIT, NodeParallelism.Scope.NATIVE, 1, 2, 2, List.of())),
                Map.of(sink, Map.of("orders", List.of("id"))),
                Map.of(sink, Map.of("orders", new SinkTarget("orders_dest", List.of("id")))));
        DAG dag = PipelineDagBuilder.build(pipeline, bindings, acks,
                new FrontierBinding(Map.of("orders_src", "orders")), shape);
        assertThat(StageWorkDag.singleVertices(dag)).contains("orders_src").doesNotContain(sink);
        Processor measured = openOneSource(dag.getVertex("orders_src").getMetaSupplier());
        GateAwareSource source = sourceFactory.created;
        assertThat(measured).isInstanceOf(StageOutputPressureProcessor.class);
        assertThat(source.gateApplications).isEqualTo(1);
        assertThat(source.awaited).containsExactly(new AwaitedLoad(sink, "orders", List.of(
                SinkProcessor.writerId(sink, 0), SinkProcessor.writerId(sink, 1))));
        TestOutbox outbox = new TestOutbox(1);
        measured.init(outbox, new TestProcessorContext());
        assertThat(measured.complete()).isFalse();
        assertThat(outbox.queue(0)).isEmpty();
        landed.set(true);
        assertThat(outbox.offer("occupied")).isTrue();
        assertThat(measured.complete()).isFalse();
        assertThat(source.acceptedRows).isZero();
        outbox.queue(0).clear();
        assertThat(measured.complete()).isTrue();
        assertThat(measured.complete()).isTrue();
        assertThat(outbox.queue(0)).containsExactly("source-row");
        assertThat(source.acceptedRows).isEqualTo(1);
        measured.close();
    }

    @Test
    void pressureOutsideTheRawHolderHidesItFromALaterLoadGate() throws Exception {
        var sourceFactory = new GateAwareSourceFactory();
        ProcessorMetaSupplier raw = ProcessorMetaSupplier.of(ProcessorSupplier.of(sourceFactory));
        var gate = new LoadGate(SinkAckFactory.NONE, List.of(new AwaitedLoad(
                "serve.sync_1", "orders", List.of(SinkProcessor.writerId("serve.sync_1", 0)))));
        Processor wrong = openOneSource(gate.appliedTo(StageOutputPressureProcessor.wrap(raw)));
        GateAwareSource source = sourceFactory.created;
        assertThat(source.gateApplications).isZero();
        TestOutbox outbox = new TestOutbox(1);
        wrong.init(outbox, new TestProcessorContext());
        assertThat(wrong.complete()).isTrue();
        assertThat(outbox.queue(0)).containsExactly("source-row");
        wrong.close();
    }

    private static Processor openOneSource(ProcessorMetaSupplier meta) throws Exception {
        Address address = new Address("127.0.0.1", 5701);
        ProcessorSupplier supplier = meta.get(List.of(address)).apply(address);
        supplier.init(new TestProcessorSupplierContext());
        return supplier.get(1).iterator().next();
    }

    private static final class GateAwareSourceFactory implements SupplierEx<Processor> {
        private static final long serialVersionUID = 1L;
        private transient GateAwareSource created;
        @Override public Processor getEx() {
            created = new GateAwareSource();
            return created;
        }
    }

    private static final class GateAckFactory implements SinkAckFactory {
        private static final long serialVersionUID = 1L;
        private final java.util.concurrent.atomic.AtomicBoolean landed;
        private GateAckFactory(java.util.concurrent.atomic.AtomicBoolean landed) { this.landed = landed; }
        @Override public SinkAck resolve(com.hazelcast.core.HazelcastInstance member) {
            return SinkAckFactory.NONE.resolve(member);
        }
        @Override public LoadLandings loadLandings(com.hazelcast.core.HazelcastInstance member) {
            return awaited -> landed.get() ? List.of() : awaited;
        }
    }

    /** A source holding the same load-gate contract as an SRS source, without a dependency on that ring. */
    private static final class GateAwareSource extends AbstractProcessor implements Staged, HoldsChangesForLoads {
        private List<AwaitedLoad> awaited;
        private LoadLandings landings;
        private int gateApplications;
        private int acceptedRows;
        private boolean emitted;
        @Override public Stage stage() { return Stage.SOURCE; }
        @Override public void holdChangesUntil(List<AwaitedLoad> awaited, LoadLandings landings) {
            this.awaited = List.copyOf(awaited);
            this.landings = java.util.Objects.requireNonNull(landings);
            gateApplications++;
        }
        @Override public boolean complete() {
            if (emitted) { return true; }
            if (awaited != null && !landings.stillLanding(awaited).isEmpty()) { return false; }
            if (!tryEmit("source-row")) { return false; }
            emitted = true;
            acceptedRows++;
            return true;
        }
    }

    private static final class OutputSource extends AbstractProcessor implements Staged {
        @Override public Stage stage() { return Stage.SOURCE; }
        @Override public boolean isCooperative() { return false; }
        @Override public boolean complete() { return tryEmit("source-row"); }
    }

    private static final class PlacedSource implements ProcessorMetaSupplier, java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final int ownerPort;
        private PlacedSource(Address owner) { ownerPort = owner.getPort(); }
        @Override public int preferredLocalParallelism() { return 1; }
        @Override public boolean initIsCooperative() { return false; }
        @Override public boolean closeIsCooperative() { return false; }
        @Override public Map<String, String> getTags() { return Map.of("binding", "placed"); }
        @Override public Function<Address, ProcessorSupplier> get(List<Address> addresses) {
            assertThat(addresses).extracting(Address::getPort).contains(ownerPort);
            return address -> ProcessorSupplier.of(() -> address.getPort() == ownerPort
                    ? new OutputSource() : new AbstractProcessor() { });
        }
    }

    @Test
    void recordingBusinessMetadataDoesNotChangeTheVertexSchedulingChoice() {
        DAG dag = new StageWorkDag();
        Vertex source = dag.newVertex("source", stubMeta()).localParallelism(3);
        StageWorkDag.measured(dag, source, io.tapstate.core.lifecycle.Stage.SOURCE, true);
        assertThat(source.getLocalParallelism()).isEqualTo(3);
        assertThat(StageWorkDag.singleVertices(dag)).containsExactly("source");
    }

    @Test
    void source_to_serve_without_transforms_is_a_source_then_sink() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                null,
                null,
                serve(FromRef.literal("orders_src"), sync("sync_1", "orders_dest")),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"))));

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "serve.sync_1");
        assertThat(edges(dag)).containsExactly(edge("orders_src", "serve.sync_1"));
        assertThat(StageWorkDag.vertices(dag)).containsExactlyInAnyOrderEntriesOf(
                Map.of("orders_src", "source", "serve.sync_1", "sink"));
        assertThat(StageWorkDag.singleVertices(dag)).containsExactlyInAnyOrder("orders_src", "serve.sync_1");
    }

    @Test
    void one_serve_sink_accepts_multiple_explicit_source_table_references() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("src")),
                null,
                null,
                new ServeBlock.Inline(
                        "serve",
                        FromClause.list(
                                FromRef.literal("src.orders"),
                                FromRef.literal("src.customers")),
                        List.of(sync("sync_1", "orders_dest")),
                        null,
                        null),
                null, null);

        DagBindings bindings = new DagBindings(
                srcId -> stubMeta(),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> stubWriter(),
                ref -> Map.of(
                        FromRef.literal("src.orders"), List.of("src.orders"),
                        FromRef.literal("src.customers"), List.of("src.customers"))
                        .getOrDefault(ref, List.of()),
                sourceId -> List.of("src.orders", "src.customers"),
                viewBlock -> stubWriter());

        DAG dag = PipelineDagBuilder.build(pipeline, bindings);

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder(
                "src.orders", "src.customers", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("src.orders", "serve.sync_1", 0, 0),
                edge("src.customers", "serve.sync_1", 0, 1));
    }

    @Test
    void view_without_serve_is_a_source_then_a_materialization_sink() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                null,
                view("order_state", FromRef.literal("orders_src")),
                null,
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"))));

        assertThat(vertexNames(dag)).containsExactlyInAnyOrder("orders_src", "view.order_state");
        assertThat(edges(dag)).containsExactly(edge("orders_src", "view.order_state"));
    }

    @Test
    void a_transform_chain_feeding_a_view_wires_source_through_step_into_the_materialization() {
        // The shape the quickstart demo ships: one source, one stateless step, and a view as the only
        // output. If this stopped building, the demo would fail on a clean machine and nowhere else.
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(filter("shape_orders", "row.id % 2 == 0", FromRef.literal("orders_src"))),
                view("order_state", FromRef.literal("shape_orders")),
                null, null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("shape_orders"), List.of("shape_orders"))));

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "shape_orders", "view.order_state");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("orders_src", "shape_orders"),
                edge("shape_orders", "view.order_state"));
    }

    @Test
    void view_and_serve_each_get_the_data_rather_than_one_swallowing_the_other() {
        // The parser defaults serve.from to the view's id when a pipeline declares both, so this is
        // the shape a real workspace produces - not a hand-built curiosity.
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                null,
                view("order_state", FromRef.literal("orders_src")),
                serve(FromRef.literal("order_state"), sync("sync_1", "orders_dest")),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"))));

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "view.order_state", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("orders_src", "view.order_state"),
                edge("orders_src", "serve.sync_1", 1, 0));
    }

    @Test
    void stateless_step_wires_source_through_a_transform_vertex_to_sink() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(filter("keep_even", "row.id % 2 == 0", FromRef.literal("orders_src"))),
                null,
                serve(FromRef.literal("keep_even"), sync("sync_1", "orders_dest")),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("keep_even"), List.of("keep_even"))));

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "keep_even", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("orders_src", "keep_even"),
                edge("keep_even", "serve.sync_1"));
    }

    @Test
    void linear_chain_wires_a_vertex_per_step_in_declared_order() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(
                        filter("f", "row.id > 0", FromRef.literal("orders_src")),
                        map("m", FromRef.literal("f")),
                        js("j", FromRef.literal("m"))),
                null,
                serve(FromRef.literal("j"), sync("sync_1", "orders_dest")),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("f"), List.of("f"),
                FromRef.literal("m"), List.of("m"),
                FromRef.literal("j"), List.of("j"))));

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "f", "m", "j", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("orders_src", "f"),
                edge("f", "m"),
                edge("m", "j"),
                edge("j", "serve.sync_1"));
    }

    @Test
    void union_merges_upstreams_into_a_passthrough_vertex_without_a_transform_port() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("a_src"), SourceRef.bare("b_src")),
                List.of(union("u", FromRef.literal("a_src"), FromRef.literal("b_src"))),
                null,
                serve(FromRef.literal("u"), sync("sync_1", "orders_dest")),
                null, null);

        DagBindings bindings = new DagBindings(
                srcId -> stubMeta(),
                step -> {
                    throw new AssertionError("union must not consult transformPorts");
                },
                syncElement -> stubWriter(),
                ref -> Map.of(
                        FromRef.literal("a_src"), List.of("a_src"),
                        FromRef.literal("b_src"), List.of("b_src"),
                        FromRef.literal("u"), List.of("u")).getOrDefault(ref, List.of()));

        DAG dag = PipelineDagBuilder.build(pipeline, bindings);

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("a_src", "b_src", "u", "serve.sync_1");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("a_src", "u", 0, 0),
                edge("b_src", "u", 0, 1),
                edge("u", "serve.sync_1", 0, 0));
    }

    @Test
    void pins_stateless_and_union_vertices_to_a_single_instance_for_ordered_delivery() throws Exception {
        // A sink acks an ordered position stream, so every vertex on the path must be single-instance
        // across the whole cluster: a parallelism-greater-than-one, or one-instance-per-member, transform
        // or union would re-lane events and break that order.
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("a_src"), SourceRef.bare("b_src")),
                List.of(
                        union("u", FromRef.literal("a_src"), FromRef.literal("b_src")),
                        map("m", FromRef.literal("u"))),
                null,
                serve(FromRef.literal("m"), sync("sync_1", "orders_dest")),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("a_src"), List.of("a_src"),
                FromRef.literal("b_src"), List.of("b_src"),
                FromRef.literal("u"), List.of("u"),
                FromRef.literal("m"), List.of("m"))));

        assertThat(TotalParallelismOne.pins(dag.getVertex("u").getMetaSupplier(), 3)).isTrue();
        assertThat(TotalParallelismOne.pins(dag.getVertex("m").getMetaSupplier(), 3)).isTrue();
    }

    @Test
    void multi_ref_stateless_step_merges_all_upstreams_by_fan_in() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("a_src"), SourceRef.bare("b_src")),
                List.of(filter("f", "true", FromRef.literal("a_src"), FromRef.literal("b_src"))),
                null,
                serve(FromRef.literal("f"), sync("sync_1", "orders_dest")),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("a_src"), List.of("a_src"),
                FromRef.literal("b_src"), List.of("b_src"),
                FromRef.literal("f"), List.of("f"))));

        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("a_src", "f", 0, 0),
                edge("b_src", "f", 0, 1),
                edge("f", "serve.sync_1", 0, 0));
    }

    @Test
    void multiple_sync_elements_fan_out_from_the_serve_upstream() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                null,
                null,
                serve(FromRef.literal("orders_src"),
                        sync("sync_1", "dest_a"), sync("sync_2", "dest_b")),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"))));

        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "serve.sync_1", "serve.sync_2");
        assertThat(edges(dag)).containsExactlyInAnyOrder(
                edge("orders_src", "serve.sync_1", 0, 0),
                edge("orders_src", "serve.sync_2", 1, 0));
    }

    /**
     * A join its author wrote no width for runs as one processor for the whole cluster, as any step does: both of
     * its vertices are pinned to one member, and every edge into them - from its sources, and from the join into
     * its projection - is delivered there, since every other member runs only a stand-in that refuses input.
     */
    @Test
    void a_join_run_as_one_processor_pins_both_its_vertices_and_sends_every_edge_into_them_there() {
        DAG dag = PipelineDagBuilder.build(joinPipeline(null), joinBindings());

        for (String vertex : List.of("j", "j:project")) {
            assertThat(dag.getVertex(vertex).getLocalParallelism())
                    .isEqualTo(com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT);
            assertThat(dag.getVertex(vertex).getMetaSupplier().preferredLocalParallelism()).as(vertex).isEqualTo(1);
            assertThat(dag.getInboundEdges(vertex)).isNotEmpty().allSatisfy(edge ->
                    assertThat(edge.getPartitioner().getConstantPartitioningKey())
                            .as("%s -> %s", edge.getSourceName(), vertex).isEqualTo(vertex));
        }
    }

    /**
     * A join run wide runs its width on every member in both of its vertices, each edge into them routed by the
     * key of the state it is about to change; and where its author asked for batches, both take their input in
     * them.
     */
    @Test
    void a_wide_join_runs_its_width_in_both_vertices_and_takes_its_input_in_its_batches() {
        ExecutionShape wide = new ExecutionShape(1, Map.of("j", new NodeParallelism("j", 3,
                NodeParallelism.Origin.EXPLICIT, NodeParallelism.Scope.NATIVE, 1, 3, 3, List.of())), Map.of());

        DAG dag = PipelineDagBuilder.build(joinPipeline(new io.tapstate.core.model.ExecutionSpec(3,
                new io.tapstate.core.model.BatchSpec(8, null))), joinBindings(), null, null, wide);

        for (String vertex : List.of("j", "j:project")) {
            assertThat(dag.getVertex(vertex).getLocalParallelism()).as(vertex).isEqualTo(3);
            assertThat(InputBatches.takesInputInBatches(dag.getVertex(vertex).getMetaSupplier()))
                    .as(vertex).isTrue();
            assertThat(dag.getInboundEdges(vertex)).isNotEmpty().allSatisfy(edge ->
                    assertThat(edge.getPartitioner().getConstantPartitioningKey())
                            .as("%s -> %s", edge.getSourceName(), vertex).isNull());
        }
    }

    /**
     * The drawing says which vertices run at each node's width: a source's own, both of a join's vertices, and a
     * wide sink's router as well as the sink - where a sink's own vertex is all it draws when it runs as one.
     */
    @Test
    void the_drawing_tells_which_vertices_run_at_each_nodes_width() {
        Map<String, NodeParallelism> nodes = new java.util.LinkedHashMap<>();
        nodes.put("j", new NodeParallelism("j", 3, NodeParallelism.Origin.EXPLICIT, NodeParallelism.Scope.NATIVE,
                1, 3, 3, List.of()));
        nodes.put("serve.sync_1", new NodeParallelism("serve.sync_1", 4, NodeParallelism.Origin.NODE_DEFAULT,
                NodeParallelism.Scope.NATIVE, 1, 4, 4, List.of()));
        ExecutionShape wide = new ExecutionShape(1, nodes, Map.of(),
                Map.of("serve.sync_1", Map.of("j", new SinkTarget("orders", List.of("id")))));
        NodeVertices drawn = new NodeVertices();
        NodeVertices narrow = new NodeVertices();

        PipelineDagBuilder.build(joinPipeline(null), joinBindings(), null, null, wide, drawn);
        PipelineDagBuilder.build(joinPipeline(null), joinBindings(), null, null, ExecutionShape.totalOne(), narrow);

        assertThat(drawn.byNode()).containsExactly(
                Map.entry("orders_src", List.of("orders_src")),
                Map.entry("customers_src", List.of("customers_src")),
                Map.entry("j", List.of("j", "j:project")),
                Map.entry("serve.sync_1", List.of("route.serve.sync_1", "serve.sync_1")));
        assertThat(narrow.byNode()).containsEntry("serve.sync_1", List.of("serve.sync_1"));
        // What feeds the sink is the vertex the join emits from, however wide the sink runs: the queues into
        // the sink are counted from it.
        assertThat(drawn.feedingByNode()).containsExactly(Map.entry("serve.sync_1", List.of("j:project")));
        assertThat(narrow.feedingByNode()).containsExactly(Map.entry("serve.sync_1", List.of("j:project")));
    }

    /**
     * A stateless step runs at its node's width in a vertex of its own, and the drawing says so under the step's
     * id - so a reader matching a running vertex to the width its node was planned at finds an ordinary step as
     * it finds a join, and never has to guess from the vertex's name.
     */
    @Test
    void the_drawing_tells_that_a_steps_own_vertex_runs_at_its_width() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(filter("shape_orders", "row.id % 2 == 0", FromRef.literal("orders_src"))),
                view("order_state", FromRef.literal("shape_orders")),
                null, null, null);
        NodeVertices drawn = new NodeVertices();

        PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("shape_orders"), List.of("shape_orders"))),
                null, null, ExecutionShape.totalOne(), drawn);

        assertThat(drawn.byNode()).containsEntry("shape_orders", List.of("shape_orders"));
    }

    private static PipelineResource joinPipeline(io.tapstate.core.model.ExecutionSpec execution) {
        return new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src"), SourceRef.bare("customers_src")),
                List.of(joinStep("j", FromRef.literal("orders_src"), FromRef.literal("customers_src"), execution)),
                null,
                serve(FromRef.literal("j"), sync("sync_1", "orders_dest")),
                null, null);
    }

    private static DagBindings joinBindings() {
        return bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("customers_src"), List.of("customers_src"),
                FromRef.literal("j"), List.of("j"))).withJoin(joinBinding());
    }

    /**
     * The positive control for the case below: with the binding supplied, the join is drawn rather than
     * refused. Source changes keep their own ordinals, and final projection - where the join runs on several
     * processors - meets at the output key.
     */
    @Test
    void join_step_routes_final_projection_by_the_fact_key() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src"), SourceRef.bare("customers_src")),
                List.of(joinStep("j", FromRef.literal("orders_src"), FromRef.literal("customers_src"))),
                null,
                serve(FromRef.literal("j"), sync("sync_1", "orders_dest")),
                null, null);
        ExecutionShape wide = new ExecutionShape(1, Map.of("j", new NodeParallelism("j", 4,
                NodeParallelism.Origin.EXPLICIT, NodeParallelism.Scope.NATIVE, 1, 4, 4, List.of())), Map.of());

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("customers_src"), List.of("customers_src"),
                FromRef.literal("j"), List.of("j"))).withJoin(joinBinding()),
                SinkAckFactory.NONE,
                new FrontierBinding(Map.of("orders_src", "orders", "customers_src", "customers")), wide);

        assertThat(StageWorkDag.singleVertices(dag)).doesNotContain("j", "j:project");
        assertThat(vertexNames(dag))
                .containsExactlyInAnyOrder("orders_src", "customers_src", "j", "j:project", "serve.sync_1",
                        StateStoreCostMetricNames.VERTEX);
        assertThat(edges(dag)).contains(
                edge("orders_src", "j", 0, 0),
                edge("customers_src", "j", 0, 1),
                edge("j", "j:project", 0, 0),
                edge("j:project", "serve.sync_1", 0, 0));
        Edge projection = dag.getInboundEdges("j:project").getFirst();
        assertThat(projection.isDistributed()).isTrue();
        assertThat(projection.getPartitioner()).isNotNull();
        @SuppressWarnings("unchecked")
        com.hazelcast.jet.core.Partitioner<Object> partitioner =
                (com.hazelcast.jet.core.Partitioner<Object>) projection.getPartitioner();
        java.util.concurrent.atomic.AtomicReference<Object> routed = new java.util.concurrent.atomic.AtomicReference<>();
        partitioner.init(key -> {
            routed.set(key);
            return 0;
        });
        partitioner.getPartition(new io.tapstate.runtime.engine.join.JoinUpdate(
                io.tapstate.core.sql.JoinKey.of(List.of(10L)).name(),
                Envelope.insert(1, "j", Map.of("customer", "new"), null)), 17);
        assertThat(routed.get()).isEqualTo(io.tapstate.core.sql.JoinKey.of(List.of(10L)).name());
        partitioner.getPartition(new io.tapstate.runtime.engine.join.JoinUpdate(
                io.tapstate.core.sql.JoinKey.of(List.of(10L)).name(),
                Envelope.delete(1, "j", Map.of("customer", "old"), null)), 17);
        assertThat(routed.get()).isEqualTo(io.tapstate.core.sql.JoinKey.of(List.of(10L)).name());
        SettledPositions word = new SettledPositions(Map.of("customers",
                new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(1, 9), "w9")));
        partitioner.getPartition(word, 17);
        assertThat(routed.get()).isNotNull();

        Edge dimension = dag.getInboundEdges("j").stream()
                .filter(edge -> edge.getDestOrdinal() == 1).findFirst().orElseThrow();
        @SuppressWarnings("unchecked")
        com.hazelcast.jet.core.Partitioner<Object> sourcePartitioner =
                (com.hazelcast.jet.core.Partitioner<Object>) dimension.getPartitioner();
        sourcePartitioner.init(key -> {
            routed.set(key);
            return 0;
        });
        sourcePartitioner.getPartition(word, 17);
        assertThat(routed.get()).isNotNull();
    }

    /**
     * A join draws its own vertex and its own edges, and what it needs to do that - the compiled plan,
     * the driving source's key columns, where its state lives - comes from the assembly root. A builder
     * handed a join and no binding cannot make any of it up, and inventing a default would be a graph
     * that runs and holds its state somewhere nobody chose.
     */
    @Test
    void join_step_without_its_binding_is_a_wiring_mistake() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(joinStep("j", FromRef.literal("orders_src"))),
                null,
                serve(FromRef.literal("j"), sync("sync_1", "orders_dest")),
                null, null);

        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("j"), List.of("j")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no join binding was supplied");
    }

    @Test
    void use_reference_step_is_not_yet_resolved_and_rejected() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(Step.use("u", "shared_filter", FromClause.list(FromRef.literal("orders_src")))),
                null,
                serve(FromRef.literal("u"), sync("sync_1", "orders_dest")),
                null, null);

        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("u"), List.of("u")))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void use_reference_serve_block_is_not_yet_resolved_and_rejected() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                null,
                null,
                new ServeBlock.Use(null, "shared_serve", FromRef.literal("orders_src")),
                null, null);

        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src")))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void reference_that_resolves_to_nothing_is_an_invariant_violation() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(filter("f", "true", FromRef.literal("ghost"))),
                null,
                serve(FromRef.literal("f"), sync("sync_1", "orders_dest")),
                null, null);

        // "ghost" is not in the canned lookup, so it resolves to an empty upstream set.
        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("f"), List.of("f")))))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void reference_to_an_unknown_vertex_key_is_an_invariant_violation() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(filter("f", "true", FromRef.literal("orders_src"))),
                null,
                serve(FromRef.literal("f"), sync("sync_1", "orders_dest")),
                null, null);

        // The resolver returns a key for which no vertex was ever built.
        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, bindings(Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("f"), List.of("no_such_vertex")))))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void a_source_without_vertex_keys_is_an_invariant_violation() {
        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.bare("orders_src")), null, null,
                serve(FromRef.literal("orders_src"), sync("sync_1", "orders_dest")), null, null);
        DagBindings bindings = new DagBindings(
                srcId -> stubMeta(),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> stubWriter(),
                ref -> List.of(),
                sourceId -> List.of());

        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, bindings))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders_src");
    }

    @Test
    void a_null_source_vertex_key_result_is_an_invariant_violation() {
        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.bare("orders_src")), null, null,
                serve(FromRef.literal("orders_src"), sync("sync_1", "orders_dest")), null, null);
        DagBindings bindings = new DagBindings(
                srcId -> stubMeta(),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> stubWriter(),
                ref -> List.of(),
                sourceId -> null);

        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, bindings))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders_src");
    }

    // ---- fixtures ----------------------------------------------------------------------

    /** A binder whose leaves are structural stubs; {@code upstreams} is a canned lookup. */
    private static DagBindings bindings(Map<FromRef, List<String>> upstreams) {
        return new DagBindings(
                srcId -> stubMeta(),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> stubWriter(),
                Function.<FromRef>identity().andThen(ref -> upstreams.getOrDefault(ref, List.of())),
                srcId -> List.of(srcId),
                viewBlock -> stubWriter());
    }

    /** A structurally valid, behaviourally irrelevant vertex supplier for graph-shape assertions. */
    private static ProcessorMetaSupplier stubMeta() {
        return ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity()));
    }

    /** A behaviourally irrelevant sink-writer factory; the builder wraps it but never opens it here. */
    private static SupplierEx<SinkWriter> stubWriter() {
        return NoOpSinkWriter::new;
    }

    private static final class NoOpSinkWriter implements SinkWriter {
        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    private static ViewBlock view(String id, FromRef from) {
        return new ViewBlock.Inline(id, from, "order_id", null);
    }

    private static ServeBlock serve(FromRef from, SyncElement... sync) {
        return new ServeBlock.Inline(null, from, List.of(sync), null, null);
    }

    private static SyncElement sync(String id, String dest) {
        return new SyncElement(id, dest, null, null, null);
    }

    private static Step filter(String id, String expr, FromRef... from) {
        return Step.inline(id, FromClause.list(from), new TransformBody.Filter(expr), null);
    }

    private static Step map(String id, FromRef... from) {
        TransformBody body = new TransformBody.MapProjection(Map.of("out", FieldRule.rename("in")));
        return Step.inline(id, FromClause.list(from), body, null);
    }

    private static Step js(String id, FromRef... from) {
        return Step.inline(id, FromClause.list(from), new TransformBody.Js("emit(row)"), null);
    }

    private static Step union(String id, FromRef... from) {
        return Step.inline(id, FromClause.list(from), new TransformBody.Union(), null);
    }

    private static Step joinStep(String id, FromRef from) {
        TransformBody body = new TransformBody.Join(JoinEngine.BUILTIN, "SELECT 1");
        return Step.inline(id, FromClause.aliases(Map.of("root", from)), body, null);
    }

    /** A join step reading two sources, under the alias names the plan below calls them by. */
    private static Step joinStep(String id, FromRef fact, FromRef dimension) {
        return joinStep(id, fact, dimension, null);
    }

    private static Step joinStep(String id, FromRef fact, FromRef dimension,
            io.tapstate.core.model.ExecutionSpec execution) {
        TransformBody body = new TransformBody.Join(JoinEngine.BUILTIN,
                "SELECT o.id FROM orders o JOIN customers c ON o.cust_id = c.id");
        return Step.inline(id, FromClause.aliases(new java.util.LinkedHashMap<>(
                Map.of("o", fact, "c", dimension))), body, execution, null);
    }

    /**
     * The compiled plan and the driving source's key, as the assembly root would supply them. Built by
     * hand rather than derived: deriving needs the SQL library, which this ring cannot see.
     */
    private static io.tapstate.runtime.engine.join.JoinBinding joinBinding() {
        io.tapstate.core.sql.JoinTree from = new io.tapstate.core.sql.JoinTree.Join(
                new io.tapstate.core.sql.JoinTree.Source("o", "orders"),
                new io.tapstate.core.sql.JoinTree.Source("c", "customers"),
                io.tapstate.core.sql.JoinKind.INNER,
                List.of(new io.tapstate.core.sql.JoinTree.KeyPair(
                        new io.tapstate.core.sql.JoinTree.ColumnRef("o", "cust_id"),
                        new io.tapstate.core.sql.JoinTree.ColumnRef("c", "id"))),
                false);
        io.tapstate.core.sql.JoinPlan plan = new io.tapstate.core.sql.JoinPlan(
                List.of(new io.tapstate.core.sql.OutputField("id",
                        io.tapstate.core.common.TapstateType.INT64, false,
                        new io.tapstate.core.sql.Expr.Column(
                                new io.tapstate.core.sql.JoinTree.ColumnRef("o", "id")))),
                from, Map.of("o", List.of("cust_id", "id"), "c", List.of("id")));
        return new io.tapstate.runtime.engine.join.JoinBinding(
                step -> plan,
                step -> List.of("id"),
                (member, pipelineId, stepId) ->
                        new io.tapstate.runtime.engine.join.MapJoinStores());
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
        return edge(src, dest, 0, 0);
    }

    private static String edge(String src, String dest, int srcOrd, int destOrd) {
        return src + "->" + dest + "#" + srcOrd + "," + destOrd;
    }
}
