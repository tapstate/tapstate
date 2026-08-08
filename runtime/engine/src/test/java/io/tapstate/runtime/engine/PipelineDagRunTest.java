package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs a DAG the builder assembled from a whole pipeline on an embedded single member: a source, a
 * real stateless filter port and the real generic sink adapter over an injected writer, wired only
 * through the bindings. Proves the built topology is a runnable Jet job, that the filter actually
 * drops rows, and that the sink adapter delivers the surviving rows to its writer - the part a
 * structural assertion cannot see. Every leaf is an injected double, so the test carries no SRS or
 * connector dependency.
 */
class PipelineDagRunTest {

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void built_source_filter_sink_dag_runs_and_the_filter_drops_odd_rows() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of("orders_src"),
                List.of(Step.inline("keep_even",
                        FromClause.list(FromRef.literal("orders_src")),
                        new TransformBody.Filter("row.id % 2 == 0"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_even"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null, null)),
                        null, null),
                null, null);

        SupplierEx<TransformPort> keepEvenIds = () -> event ->
                ((Integer) event.after().get("id")) % 2 == 0 ? List.of(event) : List.of();
        SupplierEx<SinkWriter> intoOut = () -> new CollectingSinkWriter("out");

        DagBindings bindings = new DagBindings(
                sourceId -> insertsSource(List.of(1, 2, 3, 4), "orders"),
                step -> keepEvenIds,
                syncElement -> intoOut,
                ref -> Map.of(
                        FromRef.literal("orders_src"), List.of("orders_src"),
                        FromRef.literal("keep_even"), List.of("keep_even")).getOrDefault(ref, List.of()));

        CollectingSinkWriter.reset("out");
        member.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings)).join();

        assertThat(CollectingSinkWriter.collected("out")).containsExactlyInAnyOrder(2, 4);
    }

    @Test
    void built_union_dag_merges_two_sources_at_runtime() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of("a_src", "b_src"),
                List.of(Step.inline("u",
                        FromClause.list(FromRef.literal("a_src"), FromRef.literal("b_src")),
                        new TransformBody.Union(), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("u"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null, null)),
                        null, null),
                null, null);

        SupplierEx<SinkWriter> intoOut = () -> new CollectingSinkWriter("out");

        DagBindings bindings = new DagBindings(
                sourceId -> insertsSource(
                        sourceId.equals("a_src") ? List.of(1, 2) : List.of(10, 20), sourceId),
                step -> {
                    throw new AssertionError("union must not consult transformPorts");
                },
                syncElement -> intoOut,
                ref -> Map.of(
                        FromRef.literal("a_src"), List.of("a_src"),
                        FromRef.literal("b_src"), List.of("b_src"),
                        FromRef.literal("u"), List.of("u")).getOrDefault(ref, List.of()));

        CollectingSinkWriter.reset("out");
        member.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings)).join();

        assertThat(CollectingSinkWriter.collected("out")).containsExactlyInAnyOrder(1, 2, 10, 20);
    }

    @Test
    void a_built_union_carries_on_the_bound_of_each_stream_it_merges() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of("a_src", "b_src"),
                List.of(Step.inline("u",
                        FromClause.list(FromRef.literal("a_src"), FromRef.literal("b_src")),
                        new TransformBody.Union(), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("u"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null, null)),
                        null, null),
                null, null);

        FrontierBinding frontier = new FrontierBinding(Map.of("a_src", "a", "b_src", "b"));
        ChainAxes axes = frontier.axes();
        SupplierEx<SinkWriter> intoOut = () -> new CollectingSinkWriter("bounds");

        DagBindings bindings = new DagBindings(
                sourceId -> sourceId.equals("a_src")
                        ? boundedSource(List.of(1), "a", axes.axisOf("a"), 7)
                        : boundedSource(List.of(10), "b", axes.axisOf("b"), 9),
                step -> {
                    throw new AssertionError("union must not consult transformPorts");
                },
                syncElement -> intoOut,
                ref -> Map.of(
                        FromRef.literal("a_src"), List.of("a_src"),
                        FromRef.literal("b_src"), List.of("b_src"),
                        FromRef.literal("u"), List.of("u")).getOrDefault(ref, List.of()));

        CollectingSinkWriter.reset("bounds");
        BOUNDS.clear();
        DAG dag = PipelineDagBuilder.build(pipeline, bindings, null, frontier);
        // A bound is not a record, so no sink writer can see one: the probe hangs off the union's spare
        // outbound ordinal, which leaves the graph the builder drew exactly as it drew it.
        Vertex probe = dag.newVertex("probe",
                ProcessorSupplier.of((SupplierEx<Processor>) RecordingBounds::new)).localParallelism(1);
        dag.edge(Edge.from(dag.getVertex("u"), 1).to(probe));
        member.getJet().newJob(dag).join();

        // Each source reads its own table, so neither chain is one both of the union's edges carry - the
        // shape where a union left to the engine's default passes on nothing at all and every frontier
        // behind it stops, with the pipeline still reading RUNNING.
        assertThat(BOUNDS).containsExactlyInAnyOrder(
                bound("a", 7), bound("b", 9));
    }

    /** Every bound that got past the union, as {@code chain:seq}. */
    private static final List<String> BOUNDS = Collections.synchronizedList(new ArrayList<>());

    private static String bound(String chain, long seq) {
        return chain + ":" + seq;
    }

    /** A source that emits its inserts and then one bound on its chain's axis. */
    private static ProcessorMetaSupplier boundedSource(List<Integer> ids, String src, byte axis, long seq) {
        long packed = FrontierOrders.pack(src, new SourceOrder(1, seq));
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new BoundedSource(ids, src, axis, packed)));
    }

    private static final class BoundedSource extends AbstractProcessor {
        private final List<Integer> ids;
        private final String src;
        private final Watermark bound;
        private int next;
        private boolean announced;

        BoundedSource(List<Integer> ids, String src, byte axis, long packed) {
            this.ids = ids;
            this.src = src;
            this.bound = new Watermark(packed, axis);
        }

        @Override
        public boolean complete() {
            while (next < ids.size()) {
                int id = ids.get(next);
                if (!tryEmit(Envelope.insert(id, src, Map.of("id", id), null))) {
                    return false;
                }
                next++;
            }
            if (!announced) {
                if (!tryEmit(bound)) {
                    return false;
                }
                announced = true;
            }
            return true;
        }
    }

    /** Records every bound that reached it, named by the chain whose axis carried it. */
    private static final class RecordingBounds extends AbstractProcessor {
        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            // The spare ordinal carries the union's records too; only its bounds are what this is for.
            return true;
        }

        @Override
        public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            BOUNDS.add(bound(AXES_FOR_PROBE.chainOn(watermark.key()),
                    FrontierOrders.unpack(watermark.timestamp()).seq()));
            return true;
        }
    }

    /** The same numbering the job was built with; the probe reads bounds back through it. */
    private static final ChainAxes AXES_FOR_PROBE =
            new FrontierBinding(Map.of("a_src", "a", "b_src", "b")).axes();

    /** A source that builds insert envelopes on the member from a serializable list of ids. */
    private static ProcessorMetaSupplier insertsSource(List<Integer> ids, String src) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new InsertsSource(ids, src)));
    }

    private static final class InsertsSource extends AbstractProcessor {
        private final List<Integer> ids;
        private final String src;
        private int next;

        InsertsSource(List<Integer> ids, String src) {
            this.ids = ids;
            this.src = src;
        }

        @Override
        public boolean complete() {
            while (next < ids.size()) {
                int id = ids.get(next);
                if (!tryEmit(Envelope.insert(id, src, Map.of("id", id), null))) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }
}
