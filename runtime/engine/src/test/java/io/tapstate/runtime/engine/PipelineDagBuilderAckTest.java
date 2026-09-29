package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.function.FunctionEx;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves the builder wires the ack-bearing sink when a {@link SinkAckFactory} is given: the serve sink
 * vertex it builds carries the ack factory, so once resolved on a member it advances the watermark. The
 * ack path itself (the lag-by-one prefix) is covered by SinkProcessorTest; this proves the seam from the
 * builder through to a firing ack, which the no-ack overload does not wire.
 */
class PipelineDagBuilderAckTest {

    private static final String ACK_KEY = "test.sink.ack";
    // Golden persisted keys: changing this encoding requires an explicit retained-state migration.
    private static final String FIRST_WRITER_ID = "sink-c2VydmUuJGZpcnN0";
    private static final String SECOND_WRITER_ID = "sink-c2VydmUuc2Vjb25k";
    private static final String VIEW_WRITER_ID = "sink-dmlldy5vcmRlcnM";

    private static int suffix(String token) {
        return Integer.parseInt(token.replaceAll("\\D+", ""));
    }

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
    void builds_an_ack_bearing_serve_sink_that_advances_the_watermark() throws Exception {
        RecordingAck ack = new RecordingAck();
        member.getUserContext().put(ACK_KEY, ack);
        SinkAckFactory sinkAck = m -> (SinkAck) m.getUserContext().get(ACK_KEY);

        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.bare("orders_src")), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null)), null, null),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, bindings(), sinkAck);

        SinkProcessor sink = resolveOnMember(dag.getVertex("serve.sync_1").getMetaSupplier());
        sink.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());

        // Two positions, each its own batch: p1 opens, p2 closes it (the lag-by-one acked prefix).
        feed(sink, at("orders", "p1"));
        feed(sink, at("orders", "p2"));
        drain(sink);

        assertThat(ack.calls).containsExactly("orders=p1");
    }

    @Test
    void declaresEverySinkWriterForTheStreamsItReceivesBeforeTheDagRuns() {
        PlanningAckFactory sinkAck = new PlanningAckFactory();
        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.bare("orders_src")), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                        List.of(
                                new SyncElement("$first", "first_dest", null, null, null),
                                new SyncElement("second", "second_dest", null, null, null)),
                        null, null),
                null, null);

        PipelineDagBuilder.build(
                pipeline,
                bindings(),
                sinkAck,
                new FrontierBinding(Map.of("orders_src", "orders")));

        assertThat(sinkAck.preparedPlan.get("orders"))
                .containsExactly(FIRST_WRITER_ID, SECOND_WRITER_ID);
        assertThat(sinkAck.scopes).hasSize(2);
        assertThat(sinkAck.scopes).allSatisfy(scope -> {
            assertThat(scope.streams()).containsExactly("orders");
            assertThat(scope.writerIdsByStream().get("orders"))
                    .containsExactly(FIRST_WRITER_ID, SECOND_WRITER_ID);
        });
    }

    @Test
    void retainedWriterProgressFollowsNamedSinksAcrossReassemblyOrder() {
        SyncElement first = new SyncElement("$first", "first_dest", null, null, null);
        SyncElement second = new SyncElement("second", "second_dest", null, null, null);
        PlanningAckFactory initial = buildPlan(List.of(first, second));
        Map<String, Long> retainedProgress = Map.of(
                initial.scopes.get(0).writerId(), 100L,
                initial.scopes.get(1).writerId(), 50L);

        PlanningAckFactory reordered = buildPlan(List.of(second, first));

        assertThat(reordered.scopes)
                .extracting(scope -> retainedProgress.get(scope.writerId()))
                .containsExactly(50L, 100L);
        assertThat(reordered.preparedPlan.get("orders"))
                .containsExactly(SECOND_WRITER_ID, FIRST_WRITER_ID);
    }

    @Test
    void addingAViewDoesNotMoveANamedServeSinkToAnotherPersistedSlot() {
        SyncElement sink = new SyncElement("$first", "first_dest", null, null, null);
        PlanningAckFactory initial = buildPlan(List.of(sink));
        Map<String, Long> retainedProgress = Map.of(initial.scopes.get(0).writerId(), 100L);

        PlanningAckFactory withView = buildPlanWithView(sink);

        assertThat(withView.scopes)
                .extracting(scope -> retainedProgress.get(scope.writerId()))
                .containsExactly(null, 100L);
        assertThat(withView.preparedPlan.get("orders"))
                .containsExactly(VIEW_WRITER_ID, FIRST_WRITER_ID);
    }

    private static PlanningAckFactory buildPlan(List<SyncElement> sync) {
        PlanningAckFactory sinkAck = new PlanningAckFactory();
        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.bare("orders_src")), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders_src"), sync, null, null),
                null, null);
        PipelineDagBuilder.build(
                pipeline,
                bindings(),
                sinkAck,
                new FrontierBinding(Map.of("orders_src", "orders")));
        return sinkAck;
    }

    private static PlanningAckFactory buildPlanWithView(SyncElement sync) {
        PlanningAckFactory sinkAck = new PlanningAckFactory();
        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.bare("orders_src")), null,
                new ViewBlock.Inline("orders", FromRef.literal("orders_src"), "id", null),
                new ServeBlock.Inline(null, FromRef.literal("orders"), List.of(sync), null, null),
                null, null);
        PipelineDagBuilder.build(
                pipeline,
                bindings(),
                sinkAck,
                new FrontierBinding(Map.of("orders_src", "orders")));
        return sinkAck;
    }

    /** Structural stubs for the leaves; the serve sink is the only vertex this test drives. */
    private static DagBindings bindings() {
        return new DagBindings(
                srcId -> ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity())),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> (SupplierEx<SinkWriter>) RecordingWriter::new,
                Function.<FromRef>identity().andThen(ref ->
                        Map.of(FromRef.literal("orders_src"), List.of("orders_src")).getOrDefault(ref, List.of())),
                sourceId -> List.of(sourceId),
                view -> (SupplierEx<SinkWriter>) RecordingWriter::new);
    }

    /**
     * Resolves the meta-supplier down to the one processor it pins, binding the member into the context.
     *
     * <p>The address is read off the running member rather than written down. The vertex is pinned to
     * whichever member owns its name's partition, so a fabricated address is the real one only while the
     * member happens to have taken the port that was guessed - and when it has not, this resolves to the
     * stand-in that stands in for "the vertex is elsewhere", which fails as a cast rather than as anything
     * about a sink.
     */
    private SinkProcessor resolveOnMember(ProcessorMetaSupplier meta) throws Exception {
        List<Address> addresses = List.of(member.getCluster().getLocalMember().getAddress());
        meta.init(new TestProcessorMetaSupplierContext()
                .setHazelcastInstance(member).setTotalParallelism(1).setLocalParallelism(1));
        ProcessorSupplier supplier = meta.get(addresses).apply(addresses.get(0));
        supplier.init(new TestProcessorSupplierContext().setHazelcastInstance(member));
        Processor processor = supplier.get(1).iterator().next();
        return (SinkProcessor) processor;
    }

    /** Feeds one event as its own batch, so each settles in issue order under the single-in-flight bound. */
    private static void feed(SinkProcessor sink, Envelope event) {
        TestInbox inbox = new TestInbox();
        inbox.add(event);
        for (int i = 0; i < 10_000 && !inbox.isEmpty(); i++) {
            sink.process(0, inbox);
        }
    }

    private static void drain(SinkProcessor sink) {
        for (int i = 0; i < 10_000; i++) {
            if (sink.complete()) {
                return;
            }
        }
        throw new AssertionError("sink did not complete");
    }

    private static Envelope at(String src, String pos) {
        return Envelope.insert(1L, src, Map.of("id", pos), null)
                .withSrcPos(pos)
                .withOrder(new SourceOrder(1, Integer.parseInt(pos.replaceAll("\\D+", ""))));
    }

    private static final class RecordingAck implements SinkAck {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void advance(String chain, ChainPosition position) {
            calls.add(chain + "=" + position.token());
        }
    }

    private static final class PlanningAckFactory implements SinkAckFactory {
        private final List<WriterScope> scopes = new ArrayList<>();
        private Map<String, List<String>> preparedPlan = Map.of();

        @Override
        public SinkAck resolve(HazelcastInstance member) {
            return (chain, position) -> { };
        }

        @Override
        public void prepareWriterPlan(Map<String, List<String>> writerIdsByStream) {
            preparedPlan = Map.copyOf(writerIdsByStream);
        }

        @Override
        public SinkAckFactory forWriter(
                String writerId, List<String> streams, Map<String, List<String>> writerIdsByStream) {
            scopes.add(new WriterScope(writerId, List.copyOf(streams), Map.copyOf(writerIdsByStream)));
            return this;
        }
    }

    private record WriterScope(
            String writerId, List<String> streams, Map<String, List<String>> writerIdsByStream)
            implements java.io.Serializable {
    }

    private static final class RecordingWriter implements SinkWriter {
        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
