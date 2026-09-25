package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink one table reaches over more than one path lands that table only as far as the bound the engine
 * combines across its input queues, never as far as the order its rows arrive in suggests.
 *
 * <p>Rows of one table reaching a sink along two paths arrive in whatever order the two paths drain in: a later
 * change can land along one before an earlier change has come along the other. Reading the order they land in
 * as the order they happened - a later position settling closes every earlier one - then says the earlier
 * change is written while the row it produced on the other path is still on its way, and a resume from there
 * never writes that row. Only the bound, combined across every queue into the sink, says what is still coming
 * on all of them.
 *
 * <p>The sink is driven by hand here, with rows and no bound, which is exactly the stretch in which the two
 * readings differ: going by arrival order, settled positions close each other; going by the bound, nothing is
 * landed until a bound says so. The single-path case beside it is the control - there a later position
 * settling does prove the earlier ones, and the sink keeps saying so.
 */
class ASinkReachedByOneTableTwiceGoesByTheBoundTest {

    private static final FrontierBinding FRONTIER = new FrontierBinding(Map.of("orders_src", "orders"));

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
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
    void aSinkTheTableReachesTwiceLandsNothingNoBoundCovers() throws Exception {
        List<String> landed = settleThreeRows(servedFrom("kept", "copied"));

        assertThat(landed)
                .as("rows settled along one path say nothing about the rows still on the other")
                .isEmpty();
    }

    @Test
    void aSinkTheTableReachesOnceClosesAPositionWithTheNextOne() throws Exception {
        List<String> landed = settleThreeRows(servedFrom("kept"));

        assertThat(landed)
                .as("one path keeps the table's order, so a later position settling closes the earlier ones")
                .containsExactly("orders=p1", "orders=p2");
    }

    /** Settles three rows of the table at the pipeline's sink, each its own batch, and answers what landed. */
    private List<String> settleThreeRows(PipelineResource pipeline) throws Exception {
        List<String> landed = new ArrayList<>();
        SinkAck ack = (chain, position) -> landed.add(chain + "=" + position.token());
        DAG dag = PipelineDagBuilder.build(pipeline, bindings(), on -> ack, FRONTIER);
        SinkProcessor sink = resolveOnMember(dag.getVertex("serve.s").getMetaSupplier());
        sink.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());
        for (String position : List.of("p1", "p2", "p3")) {
            TestInbox inbox = new TestInbox();
            inbox.add(Envelope.insert(1L, "orders", Map.of("id", position), null)
                    .withSrcPos(position)
                    .withOrder(new SourceOrder(1, Integer.parseInt(position.substring(1)))));
            for (int i = 0; i < 10_000 && !inbox.isEmpty(); i++) {
                sink.process(0, inbox);
            }
        }
        for (int i = 0; i < 10_000 && !sink.complete(); i++) {
            Thread.onSpinWait();
        }
        return landed;
    }

    /**
     * A pipeline reading one table through two steps that keep every row, serving whichever of them are
     * named. Named both, the table reaches the sink twice.
     */
    private static PipelineResource servedFrom(String... steps) {
        List<FromRef> refs = new ArrayList<>();
        for (String step : steps) {
            refs.add(FromRef.literal(step));
        }
        return new PipelineResource("p", null, List.of(SourceRef.bare("orders_src")),
                List.of(keepAll("kept"), keepAll("copied")), null,
                new ServeBlock.Inline(null, new FromClause.Flow(refs),
                        List.of(new SyncElement("s", "orders_dest", null, null, null)), null, null),
                null, null);
    }

    private static Step keepAll(String id) {
        return Step.inline(id, FromClause.list(FromRef.literal("orders_src")), new TransformBody.Filter("true"), null);
    }

    private static DagBindings bindings() {
        Map<FromRef, List<String>> upstreams = Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("kept"), List.of("kept"),
                FromRef.literal("copied"), List.of("copied"));
        return new DagBindings(
                sourceId -> ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity())),
                step -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                element -> (SupplierEx<SinkWriter>) SettlingWriter::new,
                ref -> upstreams.getOrDefault(ref, List.of()));
    }

    /** Resolves the sink's supplier on the running member down to the one processor it pins. */
    private SinkProcessor resolveOnMember(ProcessorMetaSupplier meta) throws Exception {
        List<Address> addresses = List.of(member.getCluster().getLocalMember().getAddress());
        meta.init(new TestProcessorMetaSupplierContext()
                .setHazelcastInstance(member).setTotalParallelism(1).setLocalParallelism(1));
        ProcessorSupplier supplier = meta.get(addresses).apply(addresses.get(0));
        supplier.init(new TestProcessorSupplierContext().setHazelcastInstance(member));
        return (SinkProcessor) supplier.get(1).iterator().next();
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
