package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.sink.TargetKeyConstraints;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SinkLaneRouterTest {
    private static final ChainAxes AXES = ChainAxes.assign(List.of("orders"));
    private static final SinkTarget TARGET = new SinkTarget("orders", List.of("id"));
    private static HazelcastInstance first;
    private static HazelcastInstance second;

    @BeforeAll
    static void startMembers() {
        String cluster = "sink-lane-router-" + UUID.randomUUID();
        first = Hazelcast.newHazelcastInstance(config(cluster));
        second = Hazelcast.newHazelcastInstance(config(cluster));
        assertThat(first.getCluster().getMembers()).hasSize(2);
    }

    @AfterAll
    static void stopMembers() {
        if (second != null) second.shutdown();
        if (first != null) first.shutdown();
    }

    @Test
    void unchangedKeysStayPlainAndSnapshotRowsTakeTheSpreadEdge() throws Exception {
        try (var f = fixture(false, 32)) {
            var snapshot = Envelope.read(1, "orders", image(1, "one"), null).withOrder(SourceOrder.snapshotRow(1));
            var ordinary = update(2, 1, "one", 1, "one");
            f.router.process(0, new TestInbox(List.of(snapshot, ordinary)));

            assertThat(f.drain(SinkRouter.SPREAD)).containsExactly(snapshot);
            assertThat(f.drain(SinkRouter.BY_KEY)).containsExactly(ordinary);
            assertThat(f.router.complete()).isTrue();
        }
    }

    @Test
    void anUncertifiedTargetKeepsStableDataFlowingAndRejectsAnActualMove() throws Exception {
        try (var f = fixture(TargetKeyConstraints.unknown(), 8)) {
            var inserted = insert(1, 7, "first");
            var stable = update(2, 7, "first", 7, "second");
            var removed = Envelope.delete(3, "orders", image(7, "second"), null);
            var ordinary = List.of(inserted, stable, removed);
            f.router.process(0, new TestInbox(ordinary));
            assertThat(f.drain(SinkRouter.BY_KEY)).containsExactlyElementsOf(ordinary);
            var move = update(4, 7, "second", 8, "second");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> f.router.process(0, new TestInbox(List.of(move))))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(EngineError.TARGET_KEY_ORDER_UNKNOWN));
            assertThat(f.drain(SinkRouter.BY_KEY)).isEmpty();
        }
    }

    @Test
    void aPartialOrDifferentlyTypedKeyStopsBeforeAnyTargetCommand() throws Exception {
        try (var f = fixture(false, 1)) {
            for (var oldRow : List.<Map<String, Object>>of(Map.of("label", "unknown"), Map.of("id", "01"))) {
                var change = Envelope.update(10, "orders", oldRow, image(1, "one"), null);
                org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        f.router.process(0, new TestInbox(List.of(change))))
                        .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(EngineError.TARGET_KEY_ORDER_UNKNOWN));
                assertThat(f.drain(SinkRouter.BY_KEY)).isEmpty();
            }
        }
    }

    @Test
    void bothKeyOwnersMustConfirmBeforeAMoveAndItsFollowersCanWrite() throws Exception {
        try (var f = fixture(false, 32)) {
            var move = update(10, 1, "one", 2, "one");
            var oldKey = insert(11, 1, "replacement");
            var newKey = update(12, 2, "one", 2, "one");
            var unrelated = update(13, 9, "nine", 9, "nine");
            var input = new TestInbox(List.of(move, oldKey, newKey, unrelated));
            f.router.process(0, input);
            var initial = f.drain(SinkRouter.BY_KEY);
            assertThat(input.size()).isZero();
            assertThat(initial).hasSize(3);
            var oldFence = (SinkLaneCommand.Fence) initial.get(0);
            var newFence = (SinkLaneCommand.Fence) initial.get(1);
            assertThat(oldFence.lane()).isNotEqualTo(newFence.lane());
            assertThat(initial.get(2)).isSameAs(unrelated);

            f.ack(oldFence);
            assertThat(f.drain(SinkRouter.BY_KEY)).isEmpty();
            f.ack(newFence);
            var applied = f.onlyMove();
            assertThat(applied.row()).isSameAs(move);
            assertThat(f.router.complete()).isFalse();
            assertThat(f.drain(SinkRouter.BY_KEY)).isEmpty();

            f.ack(applied);
            assertThat(f.drain(SinkRouter.BY_KEY)).containsExactly(oldKey, newKey);
            assertThat(f.router.complete()).isTrue();
        }
    }

    @Test
    void aReleasedUniqueValueWaitsForItsWriterBeforeTheMoveAndOldKeyReuse() throws Exception {
        try (var f = fixture(true, 32)) {
            var release = update(10, 3, "claimed", 3, "free");
            var move = update(11, 1, "mine", 2, "claimed");
            var reuse = insert(12, 1, "replacement");
            var unrelated = update(13, 9, "nine", 9, "nine");
            f.router.process(0, new TestInbox(List.of(release, move, reuse, unrelated)));
            var initial = f.drain(SinkRouter.BY_KEY);
            assertThat(initial).hasSize(2);
            assertThat(initial.get(1)).isSameAs(unrelated);
            f.ack((SinkLaneCommand.Fence) initial.getFirst());
            var releasing = f.onlyMove();
            assertThat(releasing.row()).isSameAs(release);
            f.router.tryProcess();
            assertThat(f.drain(SinkRouter.BY_KEY)).isEmpty();

            f.ack(releasing);
            var owners = f.drain(SinkRouter.BY_KEY);
            assertThat(owners).hasSize(2).allMatch(SinkLaneCommand.Fence.class::isInstance);
            f.ack((SinkLaneCommand.Fence) owners.getFirst());
            assertThat(f.drain(SinkRouter.BY_KEY)).isEmpty();
            f.ack((SinkLaneCommand.Fence) owners.getLast());
            var migrating = f.onlyMove();
            assertThat(migrating.row()).isSameAs(move);
            f.router.tryProcess();
            assertThat(f.drain(SinkRouter.BY_KEY)).isEmpty();

            f.ack(migrating);
            var reusingFence = (SinkLaneCommand.Fence) f.drain(SinkRouter.BY_KEY).getFirst();
            f.ack(reusingFence);
            var reusing = f.onlyMove();
            assertThat(reusing.row()).isSameAs(reuse);
            f.ack(reusing);
            assertThat(f.router.complete()).isTrue();
        }
    }

    @Test
    void aNarrowOutboxRetriesCommandsAndKeepsBoundsBelowTheUnconfirmedMove() throws Exception {
        try (var f = fixture(false, 1)) {
            var move = update(10, 1, "one", 2, "one");
            var unrelated = update(11, 9, "nine", 9, "nine");
            var input = new TestInbox(List.of(move, unrelated));
            var emitted = new ArrayList<Object>();
            for (int attempt = 0; attempt < 8 && (!input.isEmpty() || emitted.size() < 3); attempt++) {
                f.router.process(0, input);
                f.router.tryProcess();
                emitted.addAll(f.drain(SinkRouter.BY_KEY));
            }
            assertThat(input.size()).isZero();
            assertThat(emitted).hasSize(3);
            assertThat(emitted.get(2)).isSameAs(unrelated);
            var fences = emitted.subList(0, 2).stream().map(SinkLaneCommand.Fence.class::cast).toList();
            long far = FrontierOrders.pack("orders", new SourceOrder(1, 90));
            var mark = new Watermark(far, AXES.axisOf("orders"));
            assertThat(f.out.offer("occupied")).isTrue();
            assertThat(f.router.tryProcessWatermark(0, mark)).isFalse();
            assertThat(f.drain(SinkRouter.SPREAD)).containsExactly("occupied");
            assertThat(f.drain(SinkRouter.BY_KEY)).containsExactly("occupied");
            assertThat(f.router.tryProcessWatermark(0, mark)).isTrue();
            var before = f.drain(SinkRouter.BY_KEY);
            assertThat(before).containsExactly(new Watermark(FrontierOrders.pack("orders", new SourceOrder(1, 10)) - 1,
                    AXES.axisOf("orders")));
            f.drain(SinkRouter.SPREAD);
            fences.forEach(f::ack);
            var applying = f.onlyMove();
            f.ack(applying);
            var after = f.drain(SinkRouter.BY_KEY);
            assertThat(after).containsExactly(mark);
        }
    }

    @Test
    void aCommandAndItsAcknowledgmentCrossActualMemberBoundaries() throws Exception {
        try (var f = fixture(false, 32)) {
            var move = update(10, 1, "one", 2, "one");
            f.router.process(0, new TestInbox(List.of(move)));
            var wire = second.<SinkLaneCommand>getQueue("sink-lane-wire-" + UUID.randomUUID());
            try {
                for (Object item : f.drain(SinkRouter.BY_KEY)) {
                    wire.put((SinkLaneCommand) item);
                    SinkLaneAcks.acknowledge(second, wire.take().reply());
                }
                var applying = f.awaitMove();
                wire.put(applying);
                var read = (SinkLaneCommand.Move) wire.take();
                assertThat(read.row()).isEqualTo(move);
                assertThat(read.lane()).isEqualTo(applying.lane());
                SinkLaneAcks.acknowledge(second, read.reply());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!f.router.complete() && System.nanoTime() < deadline) TimeUnit.MILLISECONDS.sleep(5);
                assertThat(f.router.complete()).isTrue();
            } finally {
                wire.destroy();
            }
        }
    }

    private static Fixture fixture(boolean secondary, int capacity) throws Exception {
        var keys = secondary ? List.of(List.of("id"), List.of("email"), List.of("_id"))
                : List.of(List.of("id"), List.of("_id"));
        return fixture(new TargetKeyConstraints(keys, true, true,
                Map.of("id", TapstateType.INT64, "email", TapstateType.STRING)), capacity);
    }

    private static Fixture fixture(TargetKeyConstraints constraints, int capacity) throws Exception {
        var router = new SinkLaneRouter("copy", Map.of("orders", TARGET),
                Map.of("orders", constraints), AXES, Map.of(0, List.of("orders")));
        var out = new TestOutbox(new int[] {capacity, capacity}, capacity);
        router.openReplies(first);
        router.init(out, new TestProcessorContext());
        return new Fixture(router, out);
    }

    private record Fixture(SinkLaneRouter router, TestOutbox out) implements AutoCloseable {
        List<Object> drain(int ordinal) {
            var rows = new ArrayList<Object>();
            out.drainQueueAndReset(ordinal, rows, false);
            return rows;
        }

        void ack(SinkLaneCommand command) {
            SinkLaneAcks.acknowledge(first, command.reply());
            router.tryProcess();
        }

        SinkLaneCommand.Move onlyMove() {
            var emitted = drain(SinkRouter.BY_KEY);
            assertThat(emitted).hasSize(1);
            return (SinkLaneCommand.Move) emitted.getFirst();
        }

        SinkLaneCommand.Move awaitMove() throws InterruptedException {
            var emitted = new ArrayList<Object>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (emitted.isEmpty() && System.nanoTime() < deadline) {
                router.tryProcess();
                emitted.addAll(drain(SinkRouter.BY_KEY));
                if (emitted.isEmpty()) TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(emitted).hasSize(1);
            return (SinkLaneCommand.Move) emitted.getFirst();
        }

        @Override public void close() { router.close(); }
    }

    private static Envelope update(long seq, int before, String oldEmail, int after, String newEmail) {
        return Envelope.update(seq, "orders", image(before, oldEmail), image(after, newEmail), null)
                .withOrder(new SourceOrder(1, seq));
    }

    private static Envelope insert(long seq, int id, String email) {
        return Envelope.insert(seq, "orders", image(id, email), null).withOrder(new SourceOrder(1, seq));
    }

    private static Map<String, Object> image(int id, String email) {
        return Map.of("id", id, "email", email);
    }

    private static Config config(String cluster) {
        var config = new Config().setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.logging.type", "none");
        config.getNetworkConfig().setPort(15891).setPortAutoIncrement(true).setPortCount(2);
        var join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15891", "127.0.0.1:15892"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        return config;
    }
}
