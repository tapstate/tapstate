package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.io.*;
import java.util.LinkedHashMap;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.FrontierOrders;
import com.hazelcast.jet.core.Watermark;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.*;

class RootStateWriteBatchingTest {
    private static final NestTopology TREE = NestTopology.compile("p", "doc",
            nest("order", List.of("order_id"), embed("customer", "customer_id", "cust_ref",
                    EmbedAs.OBJECT, "customer", null)), tables());

    @Test void independentFreshRootsPersistInBoundedGroupsBeforeTheirOutputs() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.everyChange(), 128);
        f.feed(32, false);
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16);
        assertThat(f.drain()).extracting(e -> e.after().get("order_id"))
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, 32).boxed().toList());
    }

    @Test void independentSnapshotRootsUseBatchReadsOnTheFoldingPolicyToo() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.within(0), 128);
        f.feed(32, false);
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.roots.batchReads).isEqualTo(2);
        assertThat(f.drain()).hasSize(32);
    }

    @Test void freshRootsWaitingForTheirReferencedRowsStillUseBoundedWrites() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.within(50), 128);
        f.customers.remove(List.of("C1"));
        f.feed(32, false);
        assertThat(f.drain()).isEmpty();
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16);
        assertThat(f.roots.count()).isEqualTo(32);
    }

    @Test void foldingWavesResolveDistinctReferencesTogether() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.within(50), 128);
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 32; i++) {
            f.customers.save(List.of("C" + i), row("customer_id", "C" + i, "name", "customer-" + i));
            inbox.add(Envelope.read(i, "order", row("order_id", i, "cust_ref", "C" + i), null)
                    .withOrder(at(i + 1)));
        }
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertThat(f.customers.batchReads).isEqualTo(2);
        assertThat(f.drain()).hasSize(32);
    }

    @Test void aRepeatedSnapshotKeyStillFoldsToItsLastVersionWhenFoldingIsAllowed() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.within(0), 128);
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 3; i++) inbox.add(Envelope.read(i, "order",
                row("order_id", i == 1 ? 2 : 1, "cust_ref", "C1", "version", i), null).withOrder(at(i + 1)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertThat(f.drain()).extracting(e -> e.after().get("version")).containsExactly(2, 1);
    }

    @Test void aSnapshotKeyRepeatedBeyondAWaveStillFoldsToItsLastVersion() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.within(0), 128);
        TestInbox inbox = f.inbox(32, false);
        inbox.add(Envelope.read(32, "order", row("order_id", 0, "cust_ref", "C1", "version", 32), null)
                .withOrder(at(33)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        List<Envelope> documents = f.drain();
        assertThat(documents).hasSize(32).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        assertThat(documents.getFirst().after()).containsEntry("order_id", 0).containsEntry("version", 32);
    }

    @Test void windowHeldAndReleasedIndependentRootsUseBoundedReplacementWrites() throws Exception {
        long[] now = {0};
        Fixture f = new Fixture(NestSendPolicy.within(50), 128, new RecordingStore(), false, () -> now[0]);
        f.feed(32, false);
        assertThat(f.drain()).hasSize(32);
        f.roots.batches.clear();
        f.roots.singleWrites = 0;
        now[0] = 1;
        TestInbox changes = new TestInbox();
        for (int i = 0; i < 32; i++) changes.add(Envelope.update(i, "order",
                row("order_id", i, "cust_ref", "C1", "version", i),
                row("order_id", i, "cust_ref", "C1", "version", i + 100), null).withOrder(at(i + 33)));
        f.assembler.process(0, changes);
        assertThat(changes).isEmpty();
        assertThat(f.drain()).isEmpty();
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16);
        now[0] = 51;
        f.assembler.tryProcess();
        assertThat(f.drain()).extracting(e -> e.after().get("version"))
                .containsExactlyElementsOf(java.util.stream.IntStream.range(100, 132).boxed().toList());
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16, 16, 16);
    }

    @Test void aPartialWindowWritePublishesNoPositionsAndFreshReplayCoversTheUnwrittenSuffix() throws Exception {
        long[] now = {0};
        RecordingStore durable = new RecordingStore();
        Fixture f = new Fixture(NestSendPolicy.within(50), 128, durable, true, () -> now[0]);
        f.feed(32, false);
        f.drainRaw();
        now[0] = 1;
        TestInbox updates = positionedUpdates(32);
        f.assembler.process(0, updates);
        assertThat(updates).isEmpty();
        assertThat(f.drainRaw()).isEmpty();
        durable.failuresLeft = 1;
        now[0] = 51;
        assertThatThrownBy(f.assembler::tryProcess).isInstanceOf(IllegalStateException.class);
        assertThat(f.drainRaw()).isEmpty();

        Fixture replay = new Fixture(NestSendPolicy.within(0), 128, durable, true);
        replay.assembler.process(0, positionedUpdates(32));
        Watermark frontier = new Watermark(FrontierOrders.pack("order", at(64)), replay.axes.axisOf("order"));
        assertThat(replay.assembler.tryProcessWatermark(0, frontier)).isTrue();
        List<Object> items = replay.drainRaw();
        assertThat(items.getLast()).isEqualTo(frontier);
        List<Envelope> documents = items.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).toList();
        assertThat(documents).hasSize(32).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        assertThat(documents).extracting(e -> e.after().get("version"))
                .containsExactlyElementsOf(java.util.stream.IntStream.range(100, 132).boxed().toList());
        assertThat(documents.getLast().positions()).containsEntry("order", new ChainPosition(at(64), "update-31"));
    }

    @Test void dueWindowWritesCountRawFieldsHiddenBySmallEmbeds() throws Exception {
        long[] now = {0};
        Fixture f = new Fixture(NestSendPolicy.within(50), 128, new RecordingStore(), false, () -> now[0]);
        String wide = "x".repeat(5 * 1024 * 1024);
        for (int version = 0; version < 2; version++) {
            now[0] = version;
            TestInbox rows = new TestInbox();
            for (int id = 0; id < 2; id++) rows.add(Envelope.read(version, "order",
                    row("order_id", id, "cust_ref", "C1", "customer", wide, "version", version), null)
                    .withOrder(at(version * 2 + id + 1)));
            f.assembler.process(0, rows);
            assertThat(rows).isEmpty();
            if (version == 0) assertThat(f.drain()).hasSize(2);
            else assertThat(f.drain()).isEmpty();
        }
        f.roots.batches.clear();
        f.roots.singleWrites = 0;
        now[0] = 51;
        f.assembler.tryProcess();
        assertThat(f.drain()).hasSize(2);
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(2);
    }

    private static TestInbox positionedUpdates(int count) {
        TestInbox updates = new TestInbox();
        for (int i = 0; i < count; i++) updates.add(Envelope.update(i, "order",
                row("order_id", i, "cust_ref", "C1", "version", i),
                row("order_id", i, "cust_ref", "C1", "version", i + 100), null).withOrder(at(i + 33))
                .withPositions(Map.of("order", new ChainPosition(at(i + 33), "update-" + i))));
        return updates;
    }

    @Test void aPartialBatchFailurePublishesNoDocuments() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.within(0), 128);
        f.roots.failBatch = true;
        assertThatThrownBy(() -> f.feed(16, false)).isInstanceOf(IllegalStateException.class)
                .hasMessage("state batch failed");
        assertThat(f.drainRaw()).isEmpty();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {16, 256})
    void aFreshProcessorReplaysEveryPositionAfterAPartiallyWrittenWave(int count) throws Exception {
        RecordingStore durable = new RecordingStore();
        durable.failuresLeft = 1;
        Fixture failed = new Fixture(NestSendPolicy.within(0), 1024, durable, true);
        assertThatThrownBy(() -> failed.feed(count, false)).isInstanceOf(IllegalStateException.class);
        assertThat(failed.drainRaw()).isEmpty();
        assertThat(durable.count()).isPositive();

        var previouslyWritten = new java.util.LinkedHashSet<>(durable.durable.keySet());
        Fixture restarted = new Fixture(NestSendPolicy.within(0), 1, durable, true);
        restarted.feed(count, false);
        Watermark frontier = new Watermark(FrontierOrders.pack("order", at(count)), restarted.axes.axisOf("order"));
        List<Envelope> documents = new ArrayList<>();
        boolean accepted = false;
        for (int attempt = 0; attempt < count + 4 && !accepted; attempt++) {
            var downstream = new ArrayList<Object>();
            restarted.out.drainQueueAndReset(0, downstream, false);
            assertThat(downstream).noneMatch(Watermark.class::isInstance);
            downstream.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).forEach(documents::add);
            accepted = restarted.assembler.tryProcessWatermark(0, frontier);
        }
        assertThat(accepted).isTrue();
        var last = new ArrayList<Object>();
        restarted.out.drainQueueAndReset(0, last, false);
        last.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).forEach(documents::add);
        assertThat(last).anySatisfy(item -> assertThat(item).isEqualTo(frontier));
        assertThat(documents).hasSize(count);
        assertThat(documents).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        for (Envelope document : documents) {
            int id = (Integer) document.after().get("order_id");
            assertThat(document.after()).containsEntry("customer", row("customer_id", "C1", "name", "Ada"));
            if (!previouslyWritten.contains(List.of(id)) || !document.positions().isEmpty()) {
                assertThat(document.positions()).containsEntry("order", new ChainPosition(at(id + 1), "offset-" + id));
            }
            assertThat(durable.load(List.of(id))).isNotNull();
        }
        // A transported watermark cannot invent a token for an already-sent persisted duplicate.
        // The unwritten suffix supplies candidates; an entirely persisted retry may require new data.
        if (previouslyWritten.size() < count) {
            assertThat(documents).anySatisfy(document -> assertThat(document.positions())
                    .containsEntry("order", new ChainPosition(at(count), "offset-" + (count - 1))));
        }
        assertThat(durable.count()).isEqualTo(count);
    }

    @Test void rawFieldsHiddenByAnEmbedStillCountAgainstTheWriteWaveBudget() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.within(0), 128);
        String wide = "x".repeat(5 * 1024 * 1024);
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 2; i++) inbox.add(Envelope.read(i, "order",
                row("order_id", i, "cust_ref", "C1", "customer", wide), null).withOrder(at(i + 1)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertThat(f.drain()).hasSize(2);
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(2);
    }

    @Test void aFailedFastSnapshotWaveKeepsInputsForFreshReplay() throws Exception {
        RecordingStore durable = new RecordingStore();
        durable.failuresLeft = 1;
        Fixture failed = new Fixture(NestSendPolicy.everyChange(), 128, durable, true);
        TestInbox queued = failed.inbox(16, false);
        assertThatThrownBy(() -> failed.assembler.process(0, queued)).isInstanceOf(IllegalStateException.class);
        assertThat(queued.size()).isEqualTo(16);
        assertThat(failed.drainRaw()).isEmpty();
        Fixture replay = new Fixture(NestSendPolicy.everyChange(), 128, durable, true);
        replay.assembler.process(0, queued);
        assertThat(queued).isEmpty();
        List<Envelope> documents = replay.drain();
        assertThat(documents).hasSize(16).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        assertThat(documents).anySatisfy(document -> assertThat(document.positions())
                .containsEntry("order", new ChainPosition(at(16), "offset-15")));
        assertThat(durable.count()).isEqualTo(16);
    }

    @Test void backpressureConsumesOnlyTheAcknowledgedFirstWave() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.everyChange(), 1);
        TestInbox queued = f.inbox(32, false);
        f.assembler.process(0, queued);
        assertThat(queued.size()).isEqualTo(16);
        assertThat(f.roots.batches).containsExactly(16);
        List<Envelope> documents = new ArrayList<>();
        for (int attempt = 0; attempt < 80; attempt++) {
            documents.addAll(f.drain());
            if (queued.isEmpty()) f.assembler.tryProcess();
            else f.assembler.process(0, queued);
        }
        documents.addAll(f.drain());
        assertThat(queued).isEmpty();
        assertThat(documents).hasSize(32).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        assertThat(f.roots.batches).containsExactly(16, 16);
        assertThat(f.roots.singleWrites).isZero();
    }

    @Test void wideReferencedRowsDoNotCreateAnUnboundedAppendOutputWave() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.everyChange(), 1);
        f.customers.save(List.of("C1"), row("customer_id", "C1", "name", "x".repeat(5 * 1024 * 1024)));
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 16; i++) inbox.add(Envelope.read(i, "order",
                row("order_id", i, "cust_ref", "C1"), null).withOrder(at(i + 1)));
        f.assembler.process(0, inbox);
        // The ordinary append path can hold one emitted and one blocked oversized document.
        // A wave must not consume all sixteen roots behind that same one-slot outbox.
        assertThat(inbox.size()).isEqualTo(14);
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(2);
    }

    @Test void appendStillEmitsEveryVersionOfOneRoot() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.everyChange(), 128);
        f.feed(16, true);
        assertThat(f.drain()).extracting(e -> e.after().get("version"))
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, 16).boxed().toList());
        assertThat(f.roots.singleWrites).isEqualTo(16);
    }

    @Test void backpressureDoesNotRepeatAcknowledgedRootWrites() throws Exception {
        Fixture f = new Fixture(NestSendPolicy.everyChange(), 1);
        f.feed(16, false);
        List<Envelope> all = new ArrayList<>();
        for (int attempt = 0; attempt < 32; attempt++) {
            all.addAll(f.drain());
            f.assembler.tryProcess();
        }
        all.addAll(f.drain());
        assertThat(all).hasSize(16);
        assertThat(all).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        assertThat(f.roots.batches).containsExactly(16);
        assertThat(f.roots.singleWrites).isZero();
    }

    private static final class Fixture {
        final RecordingStore roots;
        final Rows customers = new Rows();
        final ChainAxes axes = ChainAxes.assign(List.of("order", "customer"));
        final TestOutbox out;
        final AssemblerProcessor assembler;
        Fixture(NestSendPolicy policy, int capacity) throws Exception {
            this(policy, capacity, new RecordingStore(), false);
        }
        Fixture(NestSendPolicy policy, int capacity, RecordingStore durable, boolean bounded) throws Exception {
            this(policy, capacity, durable, bounded, NestClock.SYSTEM);
        }
        Fixture(NestSendPolicy policy, int capacity, RecordingStore durable, boolean bounded, NestClock clock) throws Exception {
            roots = durable;
            out = new TestOutbox(capacity);
            customers.save(List.of("C1"), row("customer_id", "C1", "name", "Ada"));
            assembler = new AssemblerProcessor(TREE.assembler(), TREE.slots(), roots, "doc", bounded ? axes : null,
                    bounded ? Map.of(0, List.of("order"), TREE.lookups().getFirst().touchOrdinal(), List.of("customer")) : null,
                    ReplayFloor.NONE, NestSettings.defaults(), clock, policy,
                    new HeapNestStore<>(), (from, released) -> {}, Map.of(TREE.lookups().getFirst().mapName(), customers));
            assembler.init(out, new TestProcessorContext());
        }
        TestInbox inbox(int count, boolean sameKey) {
            TestInbox inbox = new TestInbox();
            for (int i = 0; i < count; i++) inbox.add(Envelope.read(i, "order",
                    row("order_id", sameKey ? 1 : i, "cust_ref", "C1", "version", i), null).withOrder(at(i + 1))
                    .withPositions(Map.of("order", new ChainPosition(at(i + 1), "offset-" + i))));
            return inbox;
        }
        void feed(int count, boolean sameKey) {
            TestInbox inbox = inbox(count, sameKey);
            assembler.process(0, inbox);
            assertThat(inbox).isEmpty();
        }
        List<Object> drainRaw() {
            var items = new ArrayList<Object>();
            out.drainQueueAndReset(0, items, false);
            return items;
        }
        List<Envelope> drain() {
            return drainRaw().stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).toList();
        }
    }
    private static final class Rows implements NestStore<Map<String, Object>> {
        private final HeapNestStore<Map<String, Object>> delegate = new HeapNestStore<>();
        int batchReads;
        @Override public Map<String, Object> load(Object key) { return delegate.load(key); }
        @Override public Map<Object, Map<String, Object>> loadAll(java.util.Collection<Object> keys) {
            batchReads++;
            return delegate.loadAll(keys);
        }
        @Override public void save(Object key, Map<String, Object> value) { delegate.save(key, value); }
        @Override public void remove(Object key) { delegate.remove(key); }
        @Override public long count() { return delegate.count(); }
    }
    private static final class RecordingStore implements NestStore<RootAssembly> {
        final Map<Object, byte[]> durable = new LinkedHashMap<>();
        final List<Integer> batches = new ArrayList<>();
        int singleWrites;
        int singleReads;
        int batchReads;
        boolean failBatch;
        int failuresLeft;
        @Override public RootAssembly load(Object key) { singleReads++; return read(key); }
        @Override public Map<Object, RootAssembly> loadAll(java.util.Collection<Object> keys) {
            batchReads++;
            Map<Object, RootAssembly> result = new LinkedHashMap<>();
            for (Object key : keys) { RootAssembly value = read(key); if (value != null) result.put(key, value); }
            return result;
        }
        private RootAssembly read(Object key) {
            if (!durable.containsKey(key)) return null;
            try (var input = new ObjectInputStream(new ByteArrayInputStream(durable.get(key)))) {
                return (RootAssembly) input.readObject();
            } catch (IOException | ClassNotFoundException failure) { throw new AssertionError(failure); }
        }
        private void persist(Object key, RootAssembly state) {
            var bytes = new ByteArrayOutputStream();
            try (var output = new ObjectOutputStream(bytes)) { output.writeObject(state); }
            catch (IOException failure) { throw new AssertionError(failure); }
            durable.put(key, bytes.toByteArray());
        }
        @Override public void save(Object key, RootAssembly state) { singleWrites++; persist(key, state); }
        @Override public void saveAll(Map<Object, RootAssembly> states, Set<Object> firstFilings) {
            batches.add(states.size());
            if (failBatch || failuresLeft > 0) {
                if (failuresLeft > 0) failuresLeft--;
                var first = states.entrySet().iterator().next();
                persist(first.getKey(), first.getValue());
                throw new IllegalStateException("state batch failed");
            }
            states.forEach(this::persist);
        }
        @Override public void remove(Object key) { durable.remove(key); }
        @Override public long count() { return durable.size(); }
    }
}
