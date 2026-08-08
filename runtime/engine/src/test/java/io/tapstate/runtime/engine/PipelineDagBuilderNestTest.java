package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * A nest step no longer stops the builder: it draws the sub-graph its tree compiles to. These are
 * structure assertions over a hand-built pipeline with injected doubles - no job runs - and what they
 * pin is that the graph matches what the compiler decided, edge for edge.
 *
 * <p>The tree is customer with policies (each with claims) and a profile object, so all three edge kinds
 * appear at once: rows arriving from a source, a leaf reaching its parent directly, and a resolver
 * cascading into the level above.
 */
class PipelineDagBuilderNestTest {

    private static Embed embed(String alias, String childField, String parentField, EmbedAs as, String path,
            List<String> arrayKey, Embed... children) {
        return new Embed(alias, Map.of(childField, parentField), as, path, arrayKey, null, null,
                children.length == 0 ? null : List.of(children));
    }

    private static TransformBody.Nest tree() {
        return new TransformBody.Nest(null, null, new NestRoot("customer", List.of("customer_id"), null, null,
                List.of(
                        embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                                List.of("policy_no"),
                                embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims",
                                        List.of("claim_id"))),
                        embed("profile", "customer_id", "customer_id", EmbedAs.OBJECT, "profile",
                                List.of("customer_id")))));
    }

    private static Step nestStep(String id, TransformBody.Nest body, String... aliasToSource) {
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        for (int i = 0; i < aliasToSource.length; i += 2) {
            aliases.put(aliasToSource[i], FromRef.literal(aliasToSource[i + 1]));
        }
        return Step.inline(id, FromClause.aliases(aliases), body, null, null);
    }

    private static DAG buildWith(TransformBody.Nest body, String... aliasToSource) {
        Step step = nestStep("doc", body, aliasToSource);
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of("customers", "policies", "claims", "profiles"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);
        return PipelineDagBuilder.build(pipeline, bindings());
    }

    @Test
    void aNestStepDrawsOneResolverPerNonLeafEmbedAndOneAssembler() {
        DAG dag = buildWith(tree(),
                "customer", "customers", "policy", "policies", "claim", "claims", "profile", "profiles");

        assertThat(vertexNames(dag)).contains("nest:doc:policies", "nest:doc");
        assertThat(vertexNames(dag))
                .describedAs("claims is a leaf, so it reaches its parent directly rather than through a vertex")
                .doesNotContain("nest:doc:policies.claims");
    }

    @Test
    void everyStreamArrivesOnTheOrdinalTheCompilerGaveIt() {
        DAG dag = buildWith(tree(),
                "customer", "customers", "policy", "policies", "claim", "claims", "profile", "profiles");

        assertThat(edges(dag)).contains(
                "policies->nest:doc:policies#0,0",
                "claims->nest:doc:policies#0,1",
                "nest:doc:policies->nest:doc#0,1",
                "customers->nest:doc#0,0",
                "profiles->nest:doc#0,2");
    }

    @Test
    void everyEdgeIntoANestVertexIsPartitionedAndDistributed() {
        DAG dag = buildWith(tree(),
                "customer", "customers", "policy", "policies", "claim", "claims", "profile", "profiles");

        List<Edge> intoNest = new ArrayList<>();
        for (Vertex vertex : dag) {
            if (vertex.getName().startsWith("nest:")) {
                intoNest.addAll(dag.getInboundEdges(vertex.getName()));
            }
        }
        assertThat(intoNest).hasSize(5);
        assertThat(intoNest).allSatisfy(edge -> {
            assertThat(edge.getPartitioner())
                    .describedAs("the row declaring a mapping and the rows asking about it must meet")
                    .isNotNull();
            assertThat(edge.isDistributed()).isTrue();
        });
    }

    @Test
    void whatFollowsTheNestReadsFromTheAssembler() {
        DAG dag = buildWith(tree(),
                "customer", "customers", "policy", "policies", "claim", "claims", "profile", "profiles");

        assertThat(edges(dag))
                .describedAs("the step id names the vertex documents come out of")
                .contains("nest:doc->serve.sync_1#0,0");
    }

    @Test
    void aNestWithNothingEmbeddedIsOneVertexNamedAfterTheStep() {
        TransformBody.Nest empty =
                new TransformBody.Nest(null, null, new NestRoot("customer", List.of("customer_id"), null, null, null));

        DAG dag = buildWith(empty, "customer", "customers");

        assertThat(vertexNames(dag))
                .describedAs("it assembles nothing, so it takes no state, no map and no thread of its own")
                .containsExactlyInAnyOrder("customers", "policies", "claims", "profiles", "doc", "serve.sync_1");
        assertThat(edges(dag)).contains("customers->doc#0,0", "doc->serve.sync_1#0,0");
    }

    @Test
    void sayingWhyRatherThanFailingOnANullWhenNoNestBindingWasSupplied() {
        Step step = nestStep("doc", tree(),
                "customer", "customers", "policy", "policies", "claim", "claims", "profile", "profiles");
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of("customers", "policies", "claims", "profiles"),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);
        DagBindings withoutNest = new DagBindings(
                srcId -> stubMeta(),
                s -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> (SupplierEx<SinkWriter>) NoOpSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()));

        assertThatThrownBy(() -> PipelineDagBuilder.build(pipeline, withoutNest))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no nest binding");
    }

    // ---- doubles ----------------------------------------------------------------------

    private static DagBindings bindings() {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("customer", new NestTable("customers", List.of("customer_id")));
        tables.put("policy", new NestTable("policies", List.of("policy_id")));
        tables.put("claim", new NestTable("claims", List.of("claim_id")));
        tables.put("profile", new NestTable("profiles", List.of("customer_id")));
        return new DagBindings(
                srcId -> stubMeta(),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> (SupplierEx<SinkWriter>) NoOpSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onHeap(), element -> { }));
    }

    private static ProcessorMetaSupplier stubMeta() {
        return ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity()));
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

    private static List<String> vertexNames(DAG dag) {
        List<String> names = new ArrayList<>();
        for (Vertex v : dag) {
            names.add(v.getName());
        }
        return names;
    }

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
}
