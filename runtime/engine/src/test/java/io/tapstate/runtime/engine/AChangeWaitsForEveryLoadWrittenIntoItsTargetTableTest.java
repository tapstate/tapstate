package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.Processors;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.AwaitedLoad;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.HeapNestStores;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * A table's changes wait at its source for every load written into the same target table by a sink that spreads
 * loads over its writers - its own and every other table's - landed at every writer of that sink.
 *
 * <p>Such a sink hands a keyed target table's load rows to whichever writer has room and a change to the writer
 * its key belongs to, so a change could overtake a load row of the same key still being written elsewhere. A row
 * of any table written into that target can carry the key, so waiting for its own table's load alone would let
 * a change of one pass a load row of the other. A table written into a target with no key waits for nothing:
 * every row of it goes to one writer, in the order it was read. Neither does anything reaching only a sink that
 * runs as one processor for the cluster.
 */
class AChangeWaitsForEveryLoadWrittenIntoItsTargetTableTest {

    private static final FrontierBinding FRONTIER = new FrontierBinding(Map.of(
            "a_src", "a", "b_src", "b", "c_src", "c", "d_src", "d"));
    private static final List<String> SPREADING_WRITERS = List.of("serve.s#0", "serve.s#1");

    @Test
    void eachTableWaitsForTheLoadsWrittenIntoItsTargetAtTheWritersOfTheSinkThatSpreadsThem() {
        DAG dag = PipelineDagBuilder.build(pipeline(), bindings(), new NoAcks(), FRONTIER, shape());

        List<AwaitedLoad> intoX = List.of(
                new AwaitedLoad("serve.s", "a", SPREADING_WRITERS),
                new AwaitedLoad("serve.s", "b", SPREADING_WRITERS));
        assertThat(awaitedBy(dag, "a_src"))
                .as("a table written into a target another table is written into too waits for both loads")
                .containsExactlyElementsOf(intoX);
        assertThat(awaitedBy(dag, "b_src")).containsExactlyElementsOf(intoX);
        assertThat(awaitedBy(dag, "c_src"))
                .as("a table alone in its target waits for its own load")
                .containsExactly(new AwaitedLoad("serve.s", "c", SPREADING_WRITERS));
        assertThat(dag.getVertex("d_src").getMetaSupplier())
                .as("a table written into a target with no key goes to one writer in order, and waits for nothing")
                .isNotInstanceOf(LoadGatedSource.class);
    }

    /**
     * A nest's documents are rows of its own and never load rows of the tables it reads, so a sink spreading
     * loads receives no row of theirs: the tables behind the nest wait for nothing there.
     */
    @Test
    void aTableReachingTheSinkOnlyThroughANestWaitsForNothingThere() {
        Embed orders = new Embed("o", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "orders",
                List.of("order_id"), null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, null, List.of(orders)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("c", FromRef.literal("customers"));
        aliases.put("o", FromRef.literal("orders"));
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of(SourceRef.bare("customers"), SourceRef.bare("orders")),
                List.of(Step.inline("customer_doc", FromClause.aliases(aliases), body, null)), null,
                new ServeBlock.Inline("serve", FromRef.literal("customer_doc"),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
        Map<String, NestTable> tables = Map.of(
                "c", new NestTable("customers", List.of("customer_id")),
                "o", new NestTable("orders", List.of("order_id")));
        DagBindings bindings = new DagBindings(
                sourceId -> ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity())),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) SettlingWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> { }));
        ExecutionShape shape = new ExecutionShape(1,
                Map.of("serve.s", new NodeParallelism("serve.s", 2, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 1, 2, 2, List.of())),
                Map.of(),
                Map.of("serve.s", Map.of("customer_doc", new SinkTarget("customer_doc", List.of("customer_id")))));

        DAG dag = PipelineDagBuilder.build(pipeline, bindings, new NoAcks(),
                new FrontierBinding(Map.of("customers", "customers", "orders", "orders")), shape);

        for (String source : List.of("customers", "orders")) {
            assertThat(dag.getVertex(source).getMetaSupplier()).as(source).isNotInstanceOf(LoadGatedSource.class);
        }
    }

    @Test
    void nothingWaitsWhereEverySinkRunsAsOneProcessorForTheCluster() {
        DAG dag = PipelineDagBuilder.build(pipeline(), bindings(), new NoAcks(), FRONTIER, ExecutionShape.totalOne());

        for (String source : List.of("a_src", "b_src", "c_src", "d_src")) {
            assertThat(dag.getVertex(source).getMetaSupplier()).as(source).isNotInstanceOf(LoadGatedSource.class);
        }
    }

    @Test
    void nothingWaitsWhereTheSinksRecordNothingALoadCouldBeSeenLandingIn() {
        DAG dag = PipelineDagBuilder.build(pipeline(), bindings(), null, FRONTIER, shape());

        for (String source : List.of("a_src", "b_src", "c_src", "d_src")) {
            assertThat(dag.getVertex(source).getMetaSupplier()).as(source).isNotInstanceOf(LoadGatedSource.class);
        }
    }

    private static List<AwaitedLoad> awaitedBy(DAG dag, String source) {
        ProcessorMetaSupplier supplier = dag.getVertex(source).getMetaSupplier();
        assertThat(supplier).as("the supplier of %s", source).isInstanceOf(LoadGatedSource.class);
        return ((LoadGatedSource) supplier).gate().awaited();
    }

    /** Four tables into two serve elements: {@code s} spreads loads over its writers, {@code t} is one writer. */
    private static PipelineResource pipeline() {
        return new PipelineResource("p", null,
                List.of(SourceRef.bare("a_src"), SourceRef.bare("b_src"), SourceRef.bare("c_src"),
                        SourceRef.bare("d_src")),
                null, null,
                new ServeBlock.Inline(null,
                        new FromClause.Flow(List.of(FromRef.literal("a_src"), FromRef.literal("b_src"),
                                FromRef.literal("c_src"), FromRef.literal("d_src"))),
                        List.of(new SyncElement("s", "dest_s", null, null, null),
                                new SyncElement("t", "dest_t", null, null, null)),
                        null, null),
                null, null);
    }

    /** {@code s} two writers on one member: {@code a} and {@code b} into keyed {@code x}, and so on. */
    private static ExecutionShape shape() {
        return new ExecutionShape(1,
                Map.of("serve.s", new NodeParallelism("serve.s", 2, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 1, 2, 2, List.of())),
                Map.of(),
                Map.of("serve.s", Map.of(
                        "a", new SinkTarget("x", List.of("id")),
                        "b", new SinkTarget("x", List.of("id")),
                        "c", new SinkTarget("y", List.of("id")),
                        "d", new SinkTarget("z", List.of()))));
    }

    private static DagBindings bindings() {
        Map<FromRef, List<String>> upstreams = Map.of(
                FromRef.literal("a_src"), List.of("a_src"),
                FromRef.literal("b_src"), List.of("b_src"),
                FromRef.literal("c_src"), List.of("c_src"),
                FromRef.literal("d_src"), List.of("d_src"));
        return new DagBindings(
                sourceId -> ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity())),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) SettlingWriter::new,
                ref -> upstreams.getOrDefault(ref, List.of()));
    }

    private static final class SettlingWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    private static final class NoAcks implements SinkAckFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public SinkAck resolve(HazelcastInstance member) {
            return (chain, position) -> { };
        }
    }
}
