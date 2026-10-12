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
import io.tapstate.core.model.FromClause;
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

    /**
     * Each chain is expected at exactly the writers the graph routes it to, and the execution writes that down
     * before any of them exists: the first sink the graph draws starts the accounting as the execution starts.
     *
     * <p>Two tables and three sinks, and the shape is chosen to tell the wrong answers apart. The view reads
     * one table and the two serve elements read both, so a set that named every sink for every table would
     * wait on the view for a table it never receives - a table whose progress then never lands - and a set
     * that named only the sinks one table reaches would leave a writer of the other out, so a faster writer
     * could stand for it.
     */
    @Test
    void theFirstSinkStartsARunExpectingEachChainAtTheWritersItReaches() throws Exception {
        RunRecordingAcks sinkAck = new RunRecordingAcks();
        PipelineResource pipeline = new PipelineResource(
                "p", null, List.of(SourceRef.bare("orders_src"), SourceRef.bare("items_src")), null,
                new ViewBlock.Inline("v", FromRef.literal("orders_src"), "id", null),
                new ServeBlock.Inline(null,
                        new FromClause.Flow(List.of(FromRef.literal("orders_src"), FromRef.literal("items_src"))),
                        List.of(new SyncElement("a", "dest_a", null, null, null),
                                new SyncElement("b", "dest_b", null, null, null)), null, null),
                null, null);

        DAG dag = PipelineDagBuilder.build(pipeline, twoSourceBindings(), sinkAck,
                new FrontierBinding(Map.of("orders_src", "orders", "items_src", "items")));

        ProcessorMetaSupplier first = dag.getVertex("view.v").getMetaSupplier();
        assertThat(first).isInstanceOf(WriterRunStart.class);
        assertThat(((WriterRunStart) first).writersByChain()).isEqualTo(Map.of(
                "orders", List.of("view.v#0", "serve.a#0", "serve.b#0"),
                "items", List.of("serve.a#0", "serve.b#0")));
        assertThat(dag.getVertex("serve.a").getMetaSupplier())
                .as("one vertex starts the run; a second would only write the same set again")
                .isNotInstanceOf(WriterRunStart.class);
        assertThat(dag.getVertex("serve.b").getMetaSupplier()).isNotInstanceOf(WriterRunStart.class);

        first.init(new TestProcessorMetaSupplierContext()
                .setHazelcastInstance(member).setTotalParallelism(1).setLocalParallelism(1));

        assertThat(sinkAck.started).containsExactly(((WriterRunStart) first).writersByChain());
    }

    /** Acks that record every run started through them; each resolves to an ack recording nothing. */
    private static final class RunRecordingAcks implements SinkAckFactory {

        private static final long serialVersionUID = 1L;

        private final List<Map<String, List<String>>> started = new ArrayList<>();

        @Override
        public SinkAck resolve(HazelcastInstance on) {
            return new RecordingAck();
        }

        @Override
        public void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain) {
            started.add(writersByChain);
        }
    }

    /** Two sources, each its own vertex, with a view sink and a writer for every serve element. */
    private static DagBindings twoSourceBindings() {
        Map<FromRef, List<String>> upstreams = Map.of(
                FromRef.literal("orders_src"), List.of("orders_src"),
                FromRef.literal("items_src"), List.of("items_src"));
        return new DagBindings(
                srcId -> ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity())),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> (SupplierEx<SinkWriter>) RecordingWriter::new,
                ref -> upstreams.getOrDefault(ref, List.of()),
                sourceId -> List.of(sourceId),
                view -> (SupplierEx<SinkWriter>) RecordingWriter::new);
    }

    /** Structural stubs for the leaves; the serve sink is the only vertex this test drives. */
    private static DagBindings bindings() {
        return new DagBindings(
                srcId -> ProcessorMetaSupplier.of(Processors.mapP(FunctionEx.identity())),
                step -> (SupplierEx<TransformPort>) () -> ev -> List.of(ev),
                syncElement -> (SupplierEx<SinkWriter>) RecordingWriter::new,
                Function.<FromRef>identity().andThen(ref ->
                        Map.of(FromRef.literal("orders_src"), List.of("orders_src")).getOrDefault(ref, List.of())));
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
