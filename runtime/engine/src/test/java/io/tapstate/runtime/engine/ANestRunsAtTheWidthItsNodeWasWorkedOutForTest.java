package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.ExecutionSpec;
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
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * A nest runs at the width its node was worked out to run at, like any step: every vertex of it that keeps
 * state runs that many processors on each member where the node runs wide, and one for the whole cluster where it
 * runs as one - which is what a nest its author wrote no width for does. Where its author asked for batches, each
 * of those vertices takes its input in them.
 */
class ANestRunsAtTheWidthItsNodeWasWorkedOutForTest {

    private static final TransformBody.Nest BODY = new TransformBody.Nest(null, null,
            new NestRoot("c", List.of("customer_id"), null, null, List.of(new Embed("o",
                    Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "orders", List.of("order_id"), null, null,
                    null))));

    @Test
    void aWideNestRunsItsWidthInEveryVertexThatKeepsStateAndTakesItsInputInItsBatches() {
        ExecutionShape wide = new ExecutionShape(1, Map.of("doc", new NodeParallelism("doc", 3,
                NodeParallelism.Origin.EXPLICIT, NodeParallelism.Scope.NATIVE, 1, 3, 3, List.of())), Map.of());

        DAG dag = PipelineDagBuilder.build(pipeline(new ExecutionSpec(3, new BatchSpec(8, null))), bindings(),
                null, null, wide);

        assertThat(keepingState()).isNotEmpty().allSatisfy(name -> {
            Vertex vertex = dag.getVertex(name);
            assertThat(vertex.getLocalParallelism()).as("%s runs the node's width on each member", name)
                    .isEqualTo(3);
            assertThat(InputBatches.takesInputInBatches(vertex.getMetaSupplier()))
                    .as("%s takes its input in the batches its author asked for", name).isTrue();
        });
    }

    @Test
    void aNestItsAuthorWroteNoWidthForRunsEveryVertexThatKeepsStateAsOneProcessor() {
        DAG dag = PipelineDagBuilder.build(pipeline(null), bindings());

        assertThat(keepingState()).isNotEmpty().allSatisfy(name -> {
            Vertex vertex = dag.getVertex(name);
            assertThat(vertex.getLocalParallelism()).as("%s leaves its count to the one-processor pin", name)
                    .isEqualTo(Vertex.LOCAL_PARALLELISM_USE_DEFAULT);
            assertThat(vertex.getMetaSupplier().preferredLocalParallelism())
                    .as("%s runs one processor for the whole cluster", name).isEqualTo(1);
            assertThat(InputBatches.takesInputInBatches(vertex.getMetaSupplier())).isFalse();
        });
    }

    /**
     * A wide nest is held to the member count its width was worked out for: a run that starts on any other count
     * is refused before a single processor runs, like any node run wide, rather than running a number of them
     * nobody worked out.
     */
    @Test
    void aWideNestStartingOnAMemberCountOtherThanItsPlanIsRefusedBeforeAnyProcessorRuns() {
        ExecutionShape forTwo = new ExecutionShape(2, Map.of("doc", new NodeParallelism("doc", 4,
                NodeParallelism.Origin.EXPLICIT, NodeParallelism.Scope.NATIVE, 2, 2, 4, List.of())), Map.of());
        DAG dag = PipelineDagBuilder.build(pipeline(new ExecutionSpec(4, null)), bindings(), null, null, forTwo);
        Config config = new Config();
        config.setClusterName("nest-width-" + System.nanoTime());
        config.getJetConfig().setEnabled(true);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            Throwable failure = catchThrowable(() -> member.getJet().newJob(dag, new JobConfig().setName("p")).join());

            assertThat(failure).isNotNull();
            assertThat(JobFailureRegistry.of(member).get("p")).hasValueSatisfying(recorded ->
                    assertThat(recorded).isInstanceOfSatisfying(TapstateException.class, refused -> {
                        assertThat(refused.code()).isEqualTo(EngineError.MEMBERSHIP_CHANGED_BEFORE_START);
                        assertThat(refused.args()).containsEntry("planned", 2).containsEntry("actual", 1);
                    }));
        } finally {
            member.shutdown();
        }
    }

    private static List<String> keepingState() {
        NestTopology topology = NestTopology.compile("p", "doc", BODY, tables()::get);
        List<String> names = new ArrayList<>();
        topology.vertices().forEach(vertex -> names.add(vertex.name()));
        topology.lookups().forEach(lookup -> names.add(lookup.name()));
        return names;
    }

    private static PipelineResource pipeline(ExecutionSpec execution) {
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("c", FromRef.literal("customers"));
        aliases.put("o", FromRef.literal("orders"));
        Step step = Step.inline("doc", FromClause.aliases(aliases), BODY, execution, null);
        return new PipelineResource("p", null, List.of(SourceRef.bare("customers"), SourceRef.bare("orders")),
                List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal("doc"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null)), null, null),
                null, null);
    }

    private static Map<String, NestTable> tables() {
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable("customers", List.of("customer_id")));
        tables.put("o", new NestTable("orders", List.of("order_id")));
        return tables;
    }

    private static DagBindings bindings() {
        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        for (String table : List.of("customers", "orders")) {
            sources.put(table, ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(Processors.noopP()), table));
        }
        Map<String, NestTable> tables = tables();
        return new DagBindings(
                sources::get,
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) NoWrites::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, HeapNestStores.onHeap(), (from, released) -> { }));
    }

    /** A writer the drawn graph names and never calls. */
    private static final class NoWrites implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            throw new AssertionError("the graph is drawn, never run");
        }

        @Override
        public void close() {
        }
    }
}
