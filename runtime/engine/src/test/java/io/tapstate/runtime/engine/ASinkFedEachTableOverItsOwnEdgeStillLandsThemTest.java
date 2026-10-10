package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.transform.TransformPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink that goes by bounds and is fed each table over an edge of its own still lands every table.
 *
 * <p>The engine combines a bound across every edge into a vertex before it hands the combined value over, and
 * an edge that never carries a chain never says anything about it - so a sink waiting on that combined value
 * waits, for a chain one edge carries, on an edge that will never speak. Its sources never finish, as none do
 * in a running pipeline, so no edge ever falls silent by ending either: the table is written, and nothing
 * records it as landed for as long as the run lasts.
 *
 * <p>The graph is the smallest that has both halves: one table through a step that runs on every member, which
 * is what makes the sink go by bounds, and a second table straight from its source, into the same serve.
 */
class ASinkFedEachTableOverItsOwnEdgeStillLandsThemTest {

    private static final FrontierBinding FRONTIER =
            new FrontierBinding(Map.of("a_src", "a", "b_src", "b"));
    private static final List<String> LANDED = Collections.synchronizedList(new ArrayList<>());

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
        LANDED.clear();
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void eachTableLandsThoughNoEdgeCarriesBoth() throws InterruptedException {
        PipelineResource pipeline = new PipelineResource("p", null,
                List.of(SourceRef.bare("a_src"), SourceRef.bare("b_src")),
                List.of(Step.inline("a_kept", FromClause.list(FromRef.literal("a_src")),
                        new TransformBody.Filter("true"), null)),
                null,
                new ServeBlock.Inline(null,
                        new FromClause.Flow(List.of(FromRef.literal("a_kept"), FromRef.literal("b_src"))),
                        List.of(new SyncElement("s", "dest", null, null, null)), null, null),
                null, null);
        ExecutionShape shape = new ExecutionShape(1,
                Map.of("a_kept", new NodeParallelism("a_kept", 1, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, 1, 1, 1, List.of())),
                Map.of("a_kept", Map.of("a", List.of("id"))));
        ChainAxes axes = FRONTIER.axes();
        Map<FromRef, List<String>> upstreams = Map.of(
                FromRef.literal("a_src"), List.of("a_src"),
                FromRef.literal("b_src"), List.of("b_src"),
                FromRef.literal("a_kept"), List.of("a_kept"));
        DagBindings bindings = new DagBindings(
                sourceId -> sourceId.equals("a_src")
                        ? endless("a", axes.axisOf("a"))
                        : endless("b", axes.axisOf("b")),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) SettlingWriter::new,
                ref -> upstreams.getOrDefault(ref, List.of()));

        DAG dag = PipelineDagBuilder.build(pipeline, bindings,
                on -> (chain, position) -> LANDED.add(chain + "=" + position.token()), FRONTIER, shape);
        Job job = member.getJet().newJob(dag);
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!(LANDED.contains("a=a3") && LANDED.contains("b=b3")) && System.nanoTime() < deadline) {
                Thread.sleep(25);
            }
        } finally {
            job.cancel();
        }

        assertThat(LANDED).as("what the sink recorded as landed").contains("a=a3", "b=b3");
    }

    /** A source that emits three rows of its table, then a bound covering them, then nothing - and never ends. */
    private static ProcessorMetaSupplier endless(String table, byte axis) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new EndlessSource(table, axis)));
    }

    private static final class EndlessSource extends AbstractProcessor {

        private final String table;
        private final Watermark bound;
        private int next = 1;
        private boolean announced;

        EndlessSource(String table, byte axis) {
            this.table = table;
            this.bound = new Watermark(FrontierOrders.pack(table, new SourceOrder(1, 3)), axis);
        }

        @Override
        public boolean complete() {
            while (next <= 3) {
                Envelope row = Envelope.insert(next, table, Map.of("id", next), null)
                        .withPosition(new ChainPosition(new SourceOrder(1, next), table + next));
                if (!tryEmit(row)) {
                    return false;
                }
                next++;
            }
            if (!announced && tryEmit(bound)) {
                announced = true;
            }
            return false;
        }
    }

    /** A writer whose every write has settled by the time it answers. */
    private static final class SettlingWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
