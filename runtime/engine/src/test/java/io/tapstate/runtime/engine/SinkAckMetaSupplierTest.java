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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
                SinkProcessor.metaSupplier("sink", () -> new RecordingWriter(), factory, ContiguousPrefix::new);
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
        ProcessorMetaSupplier meta = SinkProcessor.metaSupplier("sink",
                () -> new RecordingWriter(), member -> (chain, position) -> { }, ContiguousPrefix::new);

        assertThat(TotalParallelismOne.pins(meta, 3)).isTrue();
    }

    @Test
    void rejects_a_null_factory_or_frontier() {
        assertThatThrownBy(() -> SinkProcessor.metaSupplier("sink",
                () -> new RecordingWriter(), null, ContiguousPrefix::new))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SinkProcessor.metaSupplier("sink",
                () -> new RecordingWriter(), member -> (chain, position) -> { }, null))
                .isInstanceOf(NullPointerException.class);
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
    @Test void independentOpensRemainBoundedAndReturnedProcessorsKeepTheirOriginalSlots() throws Exception {
        var firstEntered = new CountDownLatch(1);
        var secondReturned = new CountDownLatch(1); var releaseFirst = new CountDownLatch(1);
        var recorder = new OpeningRecorder(4) {
            @Override protected SinkWriter openOwned(int member, int index) throws Exception {
                if (index == 1) {
                    firstEntered.countDown();
                    assertThat(releaseFirst.await(2, TimeUnit.SECONDS)).isTrue();
                } else if (index == 2) {
                    assertThat(firstEntered.await(2, TimeUnit.SECONDS)).isTrue();
                }
                var opened = super.openOwned(member, index);
                if (index == 2) secondReturned.countDown();
                return opened;
            }
        };
        recorder.expectClosed(4);
        try (var calls = Executors.newSingleThreadExecutor()) {
            var result = calls.submit(() -> openingSupplier(recorder).get(4));
            try {
                assertThat(secondReturned.await(2, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> result.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            } finally { releaseFirst.countDown(); }
            var processors = result.get(2, TimeUnit.SECONDS);
            assertThat(recorder.maximum).hasValue(2);
            assertThat(recorder.members).containsOnly(0);
            for (Processor processor : processors) {
                var sink = (SinkProcessor) processor;
                sink.init(new TestOutbox(new int[] {}, 128), new TestProcessorContext());
                feed(sink, at("src", "t1")); drain(sink);
            }
            assertThat(recorder.received).containsExactly(0, 1, 2, 3);
            for (Processor processor : processors) processor.close();
        }
        awaitClosed(recorder);
    }

    @Test void anOpeningFailureClosesEverySuccessfulIndependentResult() throws Exception {
        var failure = new IllegalStateException("indexed open refused");
        var others = new CountDownLatch(2);
        var recorder = new OpeningRecorder(3) {
            @Override protected SinkWriter openOwned(int member, int index) throws Exception {
                if (index == 1) { assertThat(others.await(2, TimeUnit.SECONDS)).isTrue(); throw failure; }
                var opened = super.openOwned(member, index);
                if (index > 1) others.countDown();
                return opened;
            }
        };
        recorder.expectClosed(3);
        assertThatThrownBy(() -> openingSupplier(recorder).get(4)).isSameAs(failure);
        awaitClosed(recorder);
        assertThat(recorder.writers).hasSize(3);
        assertThat(recorder.closed).hasValue(3);
    }

    @Test void aLateUninterruptibleResultIsClosedInsteadOfEscapingTheFailedOpening() throws Exception {
        var failure = new IllegalStateException("first indexed open refused");
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var returned = new CountDownLatch(1);
        var recorder = new OpeningRecorder(2) {
            @Override protected SinkWriter openOwned(int member, int index) throws Exception {
                if (index == 1) { assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue(); throw failure; }
                if (index == 2) {
                    entered.countDown();
                    while (release.getCount() != 0) { try { release.await(); } catch (InterruptedException ignored) { } }
                }
                if (index == 3) throw failure;
                var opened = super.openOwned(member, index);
                if (index == 2) returned.countDown();
                return opened;
            }
        };
        recorder.expectClosed(2);
        try {
            assertThatThrownBy(() -> openingSupplier(recorder).get(4)).isSameAs(failure);
            assertThat(returned.getCount()).isOne();
            assertThat(recorder.writers.get(0).ended).isTrue();
        } finally { release.countDown(); }
        assertThat(returned.await(2, TimeUnit.SECONDS)).isTrue();
        awaitClosed(recorder);
    }

    @Test void anInterruptedCollectorDoesNotReturnOrLeakLateWriters() throws Exception {
        var entered = new CountDownLatch(2); var release = new CountDownLatch(1);
        var recorder = new OpeningRecorder(2) {
            @Override protected SinkWriter openOwned(int member, int index) throws Exception {
                if (index > 0) {
                    entered.countDown();
                    while (release.getCount() != 0) { try { release.await(); } catch (InterruptedException ignored) { } }
                }
                if (index == 3) throw new java.util.concurrent.CancellationException();
                return super.openOwned(member, index);
            }
        };
        recorder.expectClosed(3);
        var thrown = new AtomicReference<Throwable>();
        var caller = new Thread(() -> {
            try { openingSupplier(recorder).get(4); } catch (Throwable failure) { thrown.set(failure); }
        });
        caller.start();
        try {
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            recorder.writers.get(0).clearInterruptOnClose = true;
            caller.interrupt(); caller.join(2000);
            assertThat(caller.isAlive()).isFalse();
            assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
            assertThat(caller.isInterrupted()).isTrue();
        } finally { release.countDown(); }
        awaitClosed(recorder);
    }

    @Test void aFrontierFailureClosesWarmAndUnclaimedParallelWriters() throws Exception {
        var failure = new IllegalStateException("frontier construction refused");
        var recorder = new OpeningRecorder(2); var calls = new AtomicInteger();
        assertThatThrownBy(() -> openingSupplier(recorder, () -> {
            if (calls.incrementAndGet() == 3) throw failure;
            return null;
        }).get(4)).isSameAs(failure);
        assertThat(recorder.writers).hasSize(4);
        assertThat(recorder.closed).hasValue(4);
    }

    @Test void failurePriorityUsesTheOriginalSlotRatherThanCompletionOrder() throws Exception {
        var first = new IllegalStateException("first slot refused");
        var later = new IllegalStateException("later slot refused");
        var laterFailed = new CountDownLatch(1); var releaseFirst = new CountDownLatch(1); var lastReturned = new CountDownLatch(1);
        var recorder = new OpeningRecorder(2) {
            @Override protected SinkWriter openOwned(int member, int index) throws Exception {
                if (index == 1) { assertThat(releaseFirst.await(2, TimeUnit.SECONDS)).isTrue(); throw first; }
                if (index == 2) { laterFailed.countDown(); throw later; }
                var opened = super.openOwned(member, index);
                if (index == 3) lastReturned.countDown();
                return opened;
            }
        };
        recorder.expectClosed(2);
        try (var calls = Executors.newSingleThreadExecutor()) {
            var result = calls.submit(() -> openingSupplier(recorder).get(4));
            try {
                assertThat(laterFailed.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(lastReturned.await(2, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> result.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            } finally { releaseFirst.countDown(); }
            assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class).hasCause(first);
        }
        awaitClosed(recorder);
        assertThat(first.getSuppressed()).contains(later);
    }

    private static void awaitClosed(OpeningRecorder recorder) throws InterruptedException {
        assertThat(recorder.allClosed.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(recorder.closed.get()).isEqualTo(recorder.writers.size());
    }

    private ProcessorSupplier openingSupplier(OpeningRecorder recorder) throws Exception { return openingSupplier(recorder, null); }

    private ProcessorSupplier openingSupplier(OpeningRecorder recorder, SupplierEx<SinkFrontier> frontier) throws Exception {
        var meta = SinkProcessor.nativeMetaSupplier("sink", recorder, frontier == null ? null : ignored -> null,
                frontier == null ? () -> null : frontier, null, 1, 100, 0, Map.of(), Map.of());
        var address = member.getCluster().getLocalMember().getAddress();
        meta.init(new TestProcessorMetaSupplierContext().setHazelcastInstance(member).setTotalParallelism(4).setLocalParallelism(4));
        var supplier = meta.get(List.of(address)).apply(address);
        supplier.init(new TestProcessorSupplierContext().setHazelcastInstance(member));
        return supplier;
    }

    private static class OpeningRecorder implements IndexedSinkWriterFactory {
        final int concurrency;
        final Map<Integer, OpeningWriter> writers = new ConcurrentHashMap<>();
        final java.util.Set<Integer> members = ConcurrentHashMap.newKeySet();
        final List<Integer> received = new CopyOnWriteArrayList<>();
        final AtomicInteger active = new AtomicInteger(), maximum = new AtomicInteger(), closed = new AtomicInteger();
        volatile CountDownLatch allClosed = new CountDownLatch(0);
        OpeningRecorder(int concurrency) { this.concurrency = concurrency; }
        void expectClosed(int count) { allClosed = new CountDownLatch(count); }
        public SinkWriter getEx() throws Exception { return forWriter(0, 0); }
        public final SinkWriter forWriter(int member, int index) throws Exception {
            if (index != 0) assertThat(writers).containsKey(0);
            members.add(member); int count = active.incrementAndGet(); maximum.accumulateAndGet(count, Math::max);
            try { return openOwned(member, index); } finally { active.decrementAndGet(); }
        }
        protected SinkWriter openOwned(int member, int index) throws Exception {
            var writer = new OpeningWriter(index, this); writers.put(index, writer); return writer;
        }
        public Opening opening() {
            return new Opening() {
                public SinkWriter open(int member, int index) throws Exception { return forWriter(member, index); }
                public int maximumConcurrentRemainingOpens() { return concurrency; }
            };
        }
    }

    private static final class OpeningWriter implements SinkWriter {
        final int index; final OpeningRecorder owner; final AtomicBoolean ended = new AtomicBoolean();
        volatile boolean clearInterruptOnClose;
        OpeningWriter(int index, OpeningRecorder owner) { this.index = index; this.owner = owner; }
        public CompletionStage<WriteResult> write(List<Envelope> rows) {
            owner.received.add(index); return CompletableFuture.completedFuture(new WriteResult(rows.size()));
        }
        public void close() {
            if (!ended.compareAndSet(false, true)) throw new AssertionError("writer closed twice");
            owner.closed.incrementAndGet(); owner.allClosed.countDown();
            if (clearInterruptOnClose) Thread.interrupted();
        }
    }
}
