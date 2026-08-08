package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The ack-binding meta-supplier: the sink vertex that carries a {@link SinkAck} not as a prebuilt object
 * but as a {@link SinkAckFactory} resolved on the member that runs the vertex - the store the ack writes is
 * not serializable, so only serializable coordinates travel and the store is bound member-side, the same
 * way the source's read-cursor publisher is. These prove the factory is resolved from the running member
 * and the resolved ack drives the watermark; the watermark algorithm itself is covered by SinkProcessorTest.
 */
class SinkAckMetaSupplierTest {

    private static final String ACK_KEY = "test.sink.ack";


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
    void resolves_the_ack_from_the_running_member_and_advances_the_watermark() throws Exception {
        // The store the ack writes lives on the member, bound under a user-context key; the factory travels
        // on the DAG carrying no store, and resolves it member-side - here, a recording ack stand-in.
        RecordingAck ack = new RecordingAck();
        member.getUserContext().put(ACK_KEY, ack);
        SinkAckFactory factory = m -> (SinkAck) m.getUserContext().get(ACK_KEY);

        ProcessorMetaSupplier meta =
                SinkProcessor.metaSupplier(() -> new RecordingWriter(), factory, ContiguousPrefix::new);
        SinkProcessor sink = resolveOnMember(meta);
        sink.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());

        // Two batches: p1 settles first (opens, no ack), p2 settles next and closes p1 (the lag-by-one prefix).
        feed(sink, at("orders", "p1"));
        feed(sink, at("orders", "p2"));
        drain(sink);

        assertThat(ack.calls).containsExactly("orders=p1");
    }

    @Test
    void pins_the_ack_sink_vertex_to_a_single_instance_across_the_cluster() throws Exception {
        ProcessorMetaSupplier meta = SinkProcessor.metaSupplier(
                () -> new RecordingWriter(), member -> (chain, position) -> { }, ContiguousPrefix::new);

        assertThat(TotalParallelismOne.pins(meta, 3)).isTrue();
    }

    @Test
    void rejects_a_null_factory_or_frontier() {
        assertThatThrownBy(() -> SinkProcessor.metaSupplier(
                () -> new RecordingWriter(), null, ContiguousPrefix::new))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SinkProcessor.metaSupplier(
                () -> new RecordingWriter(), member -> (chain, position) -> { }, null))
                .isInstanceOf(NullPointerException.class);
    }

    /** Resolves the meta-supplier down to the one processor it pins, binding the member into the context. */
    private SinkProcessor resolveOnMember(ProcessorMetaSupplier meta) throws Exception {
        List<Address> addresses = List.of(Address.createUnresolvedAddress("127.0.0.1", 5701));
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

    /** An event on chain {@code src} at source position {@code pos}, ordered by that token's suffix. */
    private static Envelope at(String src, String pos) {
        return Envelope.insert(1L, src, Map.of("id", pos), null)
                .withSrcPos(pos)
                .withOrder(new SourceOrder(1, Integer.parseInt(pos.replaceAll("\\D+", ""))));
    }

    /** Records every {@code advance(chain, position)} call as {@code "chain=token"}, in order. */
    private static final class RecordingAck implements SinkAck {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void advance(String chain, ChainPosition position) {
            calls.add(chain + "=" + position.token());
        }
    }

    /** Records nothing but completes synchronously, so each batch settles on the next reap. */
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
