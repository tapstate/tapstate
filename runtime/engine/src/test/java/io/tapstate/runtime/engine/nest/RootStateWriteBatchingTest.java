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
    private static final NestTopology ARRAY_TREE = NestTopology.compile("p", "doc",
            nest("order", List.of("order_id"),
                    embed("item", "order_id", "order_id", EmbedAs.ARRAY, "lines", List.of("line_no")),
                    embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null)), tables());

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void guardedTouchOrderingChangesCallCountsButNotTheKeysReadOrWritten(boolean touchesFirst) throws Exception {
        Fixture f = new Fixture(NestSendPolicy.everyChange(), 256);
        TestInbox roots = new TestInbox();
        TestInbox touches = new TestInbox();
        for (int id = 0; id < 8; id++) {
            roots.add(Envelope.insert(id, "order", row("order_id", id, "cust_ref", "C1"), null)
                    .withOrder(at(id + 1)));
            touches.add(new NestTouch(List.of(id), id, Map.of(), true, false));
        }
        int touchOrdinal = TREE.lookups().getFirst().touchOrdinal();
        if (touchesFirst) f.assembler.process(touchOrdinal, touches);
        f.assembler.process(0, roots);
        if (!touchesFirst) f.assembler.process(touchOrdinal, touches);

        assertThat(roots).isEmpty();
        assertThat(touches).isEmpty();
        assertThat(f.roots.singleReads).isEqualTo(touchesFirst ? 0 : 8);
        assertThat(f.roots.batchReads).isEqualTo(touchesFirst ? 1 : 0);
        assertThat(f.roots.singleReads + f.roots.batchReadKeys.stream().mapToInt(List::size).sum())
                .isEqualTo(8);
        assertThat(f.roots.singleWrites).isEqualTo(8);
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.drain()).hasSize(8).extracting(e -> e.after().get("order_id"))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void arrayChildrenBeforeRootsUseBoundedReadsAndWrites(boolean folding) throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), folding);
        TestInbox lines = lineRows(32);
        f.assembler.process(1, lines);
        assertThat(lines).isEmpty();
        assertThat(f.drainRaw()).isEmpty();
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.roots.batchReads).isEqualTo(2);
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16);
        f.feed(32, false);
        assertArrayDocuments(f.drain(), 32);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void arrayRootReadsPersistExistingStatesInBoundedBatches(boolean folding) throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), folding);
        f.assembler.process(1, lineRows(32));
        f.roots.singleReads = 0;
        f.roots.batchReads = 0;
        f.roots.singleWrites = 0;
        f.roots.batches.clear();
        f.feed(32, false);
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.roots.batchReads).isEqualTo(2);
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16);
        assertArrayDocuments(f.drain(), 32);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
            "false,1", "true,1", "false,15", "true,15",
            "false,16", "true,16", "false,64", "true,64"})
    void knownAbsentRootsRemainInTheBoundedWriteWave(boolean folding, int absentCount) throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), folding);
        wakeAbsent(f, absentCount);
        // Exclude the guarded-touch preparation from root READ measurements.
        f.roots.batchReads = 0; f.roots.batchReadKeys.clear();
        f.roots.singleReads = 0;
        f.feed(64, false);
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.roots.batchReads).isEqualTo(4 - absentCount / 16);
        assertThat(f.roots.batchReadKeys).allSatisfy(keys -> {
            assertThat(keys.size()).isLessThanOrEqualTo(16);
            assertThat(keys).allSatisfy(key -> assertThat((Integer) ((List<?>) key).getFirst())
                    .isGreaterThanOrEqualTo(absentCount));
        });
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16, 16, 16);
        assertEmptyArrayDocuments(f.drain(), 64);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void knownAbsenceAndExistingChildStateShareOneWave(boolean folding) throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), folding);
        TestInbox children = new TestInbox();
        for (Object item : lineRows(16)) {
            if ((Integer) ((Envelope) item).after().get("order_id") % 2 == 1) children.add(item);
        }
        f.assembler.process(1, children);
        TestInbox wakes = new TestInbox();
        for (int id = 0; id < 16; id += 2) wakes.add(new NestTouch(List.of(id), 0, Map.of(), true, false));
        f.assembler.process(ARRAY_TREE.lookups().getFirst().touchOrdinal(), wakes);
        f.roots.singleReads = 0; f.roots.batchReadKeys.clear();
        f.roots.singleWrites = 0; f.roots.batches.clear();
        f.feed(16, false);
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.roots.batchReadKeys).hasSize(1).first().asList().hasSize(8);
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16);
        assertThat(f.drain()).hasSize(16).allSatisfy(document -> {
            int id = (Integer) document.after().get("order_id");
            assertThat((List<?>) document.after().get("lines")).hasSize(id % 2 == 1 ? 2 : 0);
            assertThat(document.after()).containsEntry("customer", row("customer_id", "C1", "name", "Ada"));
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void knownAbsentWaveFailureRetainsInputsAndReplaysBeforeTheWatermark(boolean folding) throws Exception {
        RecordingStore durable = new RecordingStore();
        var policy = folding ? NestSendPolicy.within(0) : NestSendPolicy.everyChange();
        Fixture failed = new Fixture(ARRAY_TREE, policy, 128, durable, true, NestClock.SYSTEM);
        wakeAbsent(failed, 16);
        durable.failuresLeft = 1;
        TestInbox inbox = failed.inbox(16, false);
        assertThatThrownBy(() -> failed.assembler.process(0, inbox))
                .isInstanceOf(IllegalStateException.class).hasMessage("state batch failed");
        assertThat(inbox.size()).isEqualTo(16);
        assertThat(failed.drainRaw()).isEmpty();
        Fixture replay = new Fixture(ARRAY_TREE, policy, 1, durable, true, NestClock.SYSTEM);
        replay.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        Watermark frontier = new Watermark(FrontierOrders.pack("order", at(16)), replay.axes.axisOf("order"));
        List<Envelope> documents = new ArrayList<>();
        boolean accepted = false;
        for (int attempt = 0; attempt < 20 && !accepted; attempt++) {
            List<Object> downstream = replay.drainRaw();
            assertThat(downstream).noneMatch(Watermark.class::isInstance);
            downstream.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).forEach(documents::add);
            accepted = replay.assembler.tryProcessWatermark(0, frontier);
        }
        assertThat(accepted).isTrue();
        List<Object> last = replay.drainRaw();
        assertThat(last.getLast()).isEqualTo(frontier);
        last.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).forEach(documents::add);
        assertEmptyArrayDocuments(documents, 16);
    }

    @Test void knownAbsentInputsStillRespectTheWaveByteBudget() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        wakeAbsent(f, 2);
        f.roots.batchReads = 0; f.roots.batchReadKeys.clear();
        f.roots.singleReads = 0;
        String wide = "x".repeat(5 * 1024 * 1024);
        TestInbox inbox = new TestInbox();
        for (int id = 0; id < 2; id++) inbox.add(Envelope.read(id, "order",
                row("order_id", id, "cust_ref", "C1", "wide", wide), null).withOrder(at(id + 1)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.roots.batchReads).isZero();
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(2);
        assertThat(f.drain()).hasSize(2);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void knownAbsenceDoesNotChangeRepeatedRootVersionSemantics(boolean folding) throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), folding);
        wakeAbsent(f, 1);
        TestInbox inbox = f.inbox(32, false);
        inbox.add(Envelope.read(99, "order", row("order_id", 0, "cust_ref", "C1", "version", 99), null)
                .withOrder(at(100)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        List<Envelope> documents = f.drain();
        assertThat(documents).hasSize(folding ? 32 : 33);
        assertThat(documents.stream().filter(e -> e.after().get("order_id").equals(0))
                .map(e -> e.after().get("version")).toList())
                .containsExactlyElementsOf(folding ? List.of(99) : List.of(0, 99));
    }

    private static void wakeAbsent(Fixture f, int count) {
        TestInbox wakes = new TestInbox();
        for (int id = 0; id < count; id++) wakes.add(new NestTouch(List.of(id), 0, Map.of(), false, true));
        f.assembler.process(ARRAY_TREE.lookups().getFirst().touchOrdinal(), wakes);
        assertThat(wakes).isEmpty();
        assertThat(f.drainRaw()).isEmpty();
    }

    private static void assertEmptyArrayDocuments(List<Envelope> documents, int count) {
        assertThat(documents).hasSize(count);
        assertThat(documents).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        assertThat(documents).allSatisfy(document -> {
            assertThat((List<?>) document.after().get("lines")).isEmpty();
            assertThat(document.after()).containsEntry("customer", row("customer_id", "C1", "name", "Ada"));
        });
    }

    @Test void rootPrefetchFailureKeepsEveryInputAndMakesNoWrites() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.assembler.process(1, lineRows(16));
        f.roots.singleWrites = 0;
        f.roots.batches.clear();
        f.roots.failReadBatch = true;
        TestInbox inbox = f.inbox(16, false);
        assertThatThrownBy(() -> f.assembler.process(0, inbox))
                .isInstanceOf(IllegalStateException.class).hasMessage("state read batch failed");
        assertThat(inbox.size()).isEqualTo(16);
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.drain()).isEmpty();
        f.roots.failReadBatch = false;
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertArrayDocuments(f.drain(), 16);
    }

    @Test void prefetchedArrayRootsKeepRepeatedVersionsOnAppend() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.assembler.process(1, lineRows(2));
        TestInbox inbox = f.inbox(2, false);
        inbox.add(Envelope.read(3, "order", row("order_id", 0, "cust_ref", "C1", "version", 99), null)
                .withOrder(at(100)));
        f.assembler.process(0, inbox);
        List<Envelope> documents = f.drain();
        assertThat(documents).extracting(e -> e.after().get("version")).containsExactly(0, 1, 99);
        assertThat(documents).allSatisfy(e -> assertThat((List<?>) e.after().get("lines")).hasSize(2));
    }

    @Test void prefetchedArrayRootsKeepSameDrainFoldingBeyondTheReadAhead() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), true);
        f.assembler.process(1, lineRows(32));
        f.roots.singleWrites = 0;
        TestInbox inbox = f.inbox(32, false);
        inbox.add(Envelope.read(99, "order", row("order_id", 0, "cust_ref", "C1", "version", 99), null)
                .withOrder(at(100)));
        f.assembler.process(0, inbox);
        List<Envelope> documents = f.drain();
        assertThat(documents).hasSize(32);
        assertThat(documents.getFirst().after()).containsEntry("order_id", 0).containsEntry("version", 99);
        assertThat(f.roots.singleWrites).isEqualTo(32);
    }

    @Test void rootReadAheadStopsBeforeCdcAndDoesNotCacheItsKey() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.assembler.process(1, lineRows(4));
        f.roots.batchReadKeys.clear();
        TestInbox inbox = f.inbox(2, false);
        inbox.add(Envelope.insert(3, "order", row("order_id", 2, "cust_ref", "C1"), null).withOrder(at(3)));
        inbox.add(Envelope.read(4, "order", row("order_id", 3, "cust_ref", "C1"), null).withOrder(at(4)));
        f.assembler.process(0, inbox);
        assertThat(f.roots.batchReadKeys).containsExactly(List.of(List.of(0), List.of(1)));
        assertArrayDocuments(f.drain(), 4);
    }

    @Test void absentArrayRootsAreNotFetchedAgainAfterPrefetch() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.feed(32, false);
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.roots.batchReads).isEqualTo(2);
        assertThat(f.roots.batchReadKeys).allSatisfy(keys -> assertThat(keys).hasSize(16));
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16);
        assertThat(f.drain()).hasSize(32).allSatisfy(e -> assertThat((List<?>) e.after().get("lines")).isEmpty());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void arrayRootBackpressureKeepsTheNextWaveFresh(boolean knownAbsent) throws Exception {
        Fixture f = new Fixture(ARRAY_TREE, NestSendPolicy.everyChange(), 1, new RecordingStore(), false, NestClock.SYSTEM);
        if (knownAbsent) wakeAbsent(f, 32);
        else f.assembler.process(1, lineRows(32));
        f.roots.batches.clear();
        TestInbox inbox = f.inbox(32, false);
        f.assembler.process(0, inbox);
        assertThat(inbox.size()).isEqualTo(16);
        assertThat(f.roots.batches).containsExactly(16);
        List<Envelope> documents = new ArrayList<>();
        for (int attempt = 0; attempt < 20; attempt++) {
            documents.addAll(f.drain());
            f.assembler.tryProcess();
        }
        TestInbox changedChild = new TestInbox();
        changedChild.add(Envelope.read(100, "item", row("item_id", 37L, "order_id", 18, "line_no", 1, "value", 99), null)
                .withOrder(at(100)));
        f.assembler.process(1, changedChild);
        assertThat(changedChild).isEmpty();
        for (int attempt = 0; attempt < 40; attempt++) {
            f.assembler.process(0, inbox);
            documents.addAll(f.drain());
        }
        assertThat(inbox).isEmpty();
        assertThat(documents).hasSize(32).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        Envelope changed = documents.stream().filter(e -> e.after().get("order_id").equals(18)).findFirst().orElseThrow();
        assertThat((List<?>) changed.after().get("lines")).anySatisfy(line ->
                assertThat(((Map<?, ?>) line).get("value")).isEqualTo(99));
    }

    @Test void wideRootStateStopsFurtherReadAheadProbesInTheDrain() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        String wide = "x".repeat(5 * 1024 * 1024);
        for (int id = 0; id < 4; id++) {
            TestInbox line = new TestInbox();
            line.add(Envelope.read(id, "item", row("item_id", id, "order_id", id, "line_no", 1, "wide", wide), null)
                    .withOrder(at(id + 1)));
            f.assembler.process(1, line);
        }
        f.roots.singleReads = 0;
        f.roots.batchReads = 0;
        f.feed(4, false);
        assertThat(f.roots.batchReads).isEqualTo(1);
        // Only one complete 5 MiB assembly fits the retained 8 MiB prefetch budget.
        assertThat(f.roots.singleReads).isEqualTo(3);
        assertThat(f.drain()).hasSize(4).allSatisfy(e -> assertThat((List<?>) e.after().get("lines")).hasSize(1));
    }

    @Test void rootReadAheadDoesNotReportALaterMissingRowBeforeEarlierRows() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.assembler.process(1, lineRows(2));
        f.roots.singleWrites = 0;
        TestInbox inbox = f.inbox(2, false);
        inbox.add(Envelope.read(3, "order", null, null).withOrder(at(3)));
        assertThatThrownBy(() -> f.assembler.process(0, inbox))
                .isInstanceOf(IllegalStateException.class);
        assertThat(inbox.size()).isEqualTo(1);
        assertThat(f.roots.singleWrites).isZero();
        assertArrayDocuments(f.drain(), 2);
    }

    @Test void rootReadAheadPreservesTheExistingFoldingDrainLimit() throws Exception {
        int count = DrainFolding.MAX_KEYS_HELD * 2 + 1;
        Fixture f = new Fixture(ARRAY_TREE, NestSendPolicy.within(0), count * 2,
                new RecordingStore(), false, NestClock.SYSTEM);
        f.feed(count, false);
        assertThat(f.roots.batchReadKeys).allSatisfy(keys -> assertThat(keys.size()).isLessThanOrEqualTo(16));
        assertThat(f.roots.singleWrites).isEqualTo(1);
        assertThat(f.roots.batches).hasSize(32).allMatch(size -> size == 16);
        assertThat(f.drain()).hasSize(count).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
    }

    @Test void wideRootInputsUseOrdinaryReadsRatherThanExceedingTheInputBudget() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        String wide = "x".repeat(5 * 1024 * 1024);
        TestInbox inbox = new TestInbox();
        for (int id = 0; id < 2; id++) inbox.add(Envelope.read(id, "order",
                row("order_id", id, "cust_ref", "C1", "wide", wide), null).withOrder(at(id + 1)));
        f.assembler.process(0, inbox);
        assertThat(f.roots.batchReads).isZero();
        assertThat(f.roots.singleReads).isEqualTo(2);
        assertThat(f.drain()).hasSize(2);
    }

    @Test void prefetchedPersistedRootsStillRejectAnOlderSourceEpoch() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.assembler.process(1, lineRows(2));
        TestInbox current = new TestInbox();
        var newer = new io.tapstate.core.event.SourceOrder(2, 1);
        for (int id = 0; id < 2; id++) current.add(Envelope.read(id, "order",
                row("order_id", id, "cust_ref", "C1", "version", 42), null)
                .withPositions(Map.of("order", new ChainPosition(newer, "newest-" + id))));
        f.assembler.process(0, current);
        assertThat(f.drain()).hasSize(2).allSatisfy(document -> {
            int id = (Integer) document.after().get("order_id");
            assertThat(document.positions()).containsEntry("order", new ChainPosition(newer, "newest-" + id));
        });
        f.roots.singleReads = 0;
        f.roots.batchReads = 0;
        f.feed(2, false);
        assertThat(f.roots.batchReads).isEqualTo(1);
        assertThat(f.roots.singleReads).isZero();
        assertThat(f.drain()).hasSize(2).allSatisfy(document -> {
            assertThat(document.after()).containsEntry("version", 42);
            // Already delivered positions are not reintroduced by an older replay.
            assertThat(document.positions()).doesNotContainKey("order");
        });
    }

    @Test void partialChildWaveFailureKeepsInputsAndReplaysEveryLine() throws Exception {
        var durable = new RecordingStore(); durable.failuresLeft = 1;
        Fixture failed = arrayFixture(durable, false);
        TestInbox lines = lineRows(16);
        assertThatThrownBy(() -> failed.assembler.process(1, lines))
                .isInstanceOf(IllegalStateException.class).hasMessage("state batch failed");
        assertThat(lines.size()).isEqualTo(32);
        assertThat(failed.drainRaw()).isEmpty();
        Fixture replay = arrayFixture(durable, false);
        replay.assembler.process(1, lines);
        assertThat(lines).isEmpty();
        assertThat(replay.drainRaw()).isEmpty();
        replay.feed(16, false);
        assertArrayDocuments(replay.drain(), 16);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void partialOwnedRootWriteFailureKeepsTheWaveForReplay(boolean folding) throws Exception {
        RecordingStore durable = new RecordingStore();
        Fixture failed = new Fixture(ARRAY_TREE, folding ? NestSendPolicy.within(0) : NestSendPolicy.everyChange(),
                128, durable, true, NestClock.SYSTEM);
        failed.assembler.process(1, lineRows(16));
        durable.failuresLeft = 1;
        TestInbox inbox = failed.inbox(16, false);
        assertThatThrownBy(() -> failed.assembler.process(0, inbox))
                .isInstanceOf(IllegalStateException.class).hasMessage("state batch failed");
        assertThat(inbox.size()).isEqualTo(16);
        assertThat(failed.drainRaw()).isEmpty();
        Fixture replay = new Fixture(ARRAY_TREE, folding ? NestSendPolicy.within(0) : NestSendPolicy.everyChange(),
                1, durable, true, NestClock.SYSTEM);
        replay.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        Watermark frontier = new Watermark(FrontierOrders.pack("order", at(16)), replay.axes.axisOf("order"));
        List<Envelope> documents = new ArrayList<>();
        boolean accepted = false;
        for (int attempt = 0; attempt < 20 && !accepted; attempt++) {
            List<Object> downstream = replay.drainRaw();
            assertThat(downstream).noneMatch(Watermark.class::isInstance);
            downstream.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).forEach(documents::add);
            accepted = replay.assembler.tryProcessWatermark(0, frontier);
        }
        assertThat(accepted).isTrue();
        List<Object> finalItems = replay.drainRaw();
        assertThat(finalItems.getLast()).isEqualTo(frontier);
        finalItems.stream().filter(Envelope.class::isInstance).map(Envelope.class::cast).forEach(documents::add);
        assertThat(documents).hasSize(16).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        assertThat(documents).allSatisfy(document -> {
            assertThat((List<?>) document.after().get("lines")).hasSize(2);
            assertThat(document.after()).containsEntry("customer", row("customer_id", "C1", "name", "Ada"));
        });
        for (Envelope document : documents) {
            int id = (Integer) document.after().get("order_id");
            if (id > 0) assertThat(document.positions())
                    .containsEntry("order", new ChainPosition(at(id + 1), "offset-" + id))
                    .containsEntry("item", new ChainPosition(at(id * 2L + 2), "line-" + (id * 2L + 2)));
        }
        Watermark childFrontier = new Watermark(FrontierOrders.pack("item", at(32)), replay.axes.axisOf("item"));
        assertThat(replay.assembler.tryProcessWatermark(1, childFrontier)).isTrue();
        assertThat(replay.drainRaw()).containsExactly(childFrontier);
    }

    @Test void ownedRootFoldingWavesStillReadReferencesInBatches() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), true);
        f.assembler.process(1, lineRows(32));
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 32; i++) {
            f.customers.save(List.of("C" + i), row("customer_id", "C" + i, "name", "customer-" + i));
            inbox.add(Envelope.read(i, "order", row("order_id", i, "cust_ref", "C" + i), null).withOrder(at(i + 1)));
        }
        f.customers.batchReads = 0;
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertThat(f.customers.batchReads).isEqualTo(2);
        assertThat(f.drain()).hasSize(32).allSatisfy(document -> {
            int id = (Integer) document.after().get("order_id");
            assertThat(document.after()).containsEntry("customer", row("customer_id", "C" + id, "name", "customer-" + id));
        });
    }

    @Test void ownedRootWavesCountHiddenPersistentFieldsAndSplitBeforeWriting() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.assembler.process(1, lineRows(4));
        f.roots.batches.clear(); f.roots.batchBytes.clear(); f.roots.singleWrites = 0;
        String wide = "x".repeat(3 * 1024 * 1024);
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 4; i++) inbox.add(Envelope.read(i, "order",
                row("order_id", i, "cust_ref", "C1", "lines", wide), null).withOrder(at(i + 1)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertArrayDocuments(f.drain(), 4);
        assertThat(f.roots.batches).containsExactly(2, 2);
        assertThat(f.roots.batchBytes).allMatch(bytes -> bytes <= 8L * 1024 * 1024);
    }

    @Test void anOwnedStateGrowingBeyondTheBudgetIsSavedAlone() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        String wide = "x".repeat(5 * 1024 * 1024);
        TestInbox children = new TestInbox();
        children.add(Envelope.read(1, "item", row("item_id", 1, "order_id", 0, "line_no", 1, "wide", wide), null)
                .withOrder(at(1)));
        f.assembler.process(1, children);
        f.roots.batches.clear(); f.roots.singleWrites = 0;
        TestInbox inbox = new TestInbox();
        inbox.add(Envelope.read(1, "order", row("order_id", 0, "cust_ref", "C1", "lines", wide), null).withOrder(at(1)));
        inbox.add(Envelope.read(2, "order", row("order_id", 1, "cust_ref", "C1"), null).withOrder(at(2)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(2);
        assertThat(StateWaveSize.of(f.roots.read(List.of(0)))).isGreaterThan(8L * 1024 * 1024);
        assertThat(f.drain()).hasSize(2);
    }

    @Test void staleOwnedRootReplayResolvesTheHeldReference() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), true);
        f.assembler.process(1, lineRows(2));
        f.feed(2, false); f.drain();
        TestInbox older = new TestInbox();
        for (int id = 0; id < 2; id++) older.add(Envelope.read(id, "order",
                row("order_id", id, "cust_ref", "absent", "version", -1), null).withOrder(at(0)));
        f.assembler.process(0, older);
        assertThat(older).isEmpty();
        assertThat(f.drain()).hasSize(2).allSatisfy(document -> assertThat(document.after())
                .containsEntry("customer", row("customer_id", "C1", "name", "Ada")));
    }

    @Test void wideOwnedReferencesStopTheOutputWaveAndUseSingletonWrites() throws Exception {
        Fixture f = new Fixture(ARRAY_TREE, NestSendPolicy.everyChange(), 1, new RecordingStore(), false, NestClock.SYSTEM);
        f.assembler.process(1, lineRows(16));
        f.roots.batches.clear(); f.roots.singleWrites = 0;
        f.customers.save(List.of("C1"), row("customer_id", "C1", "name", "x".repeat(5 * 1024 * 1024)));
        TestInbox inbox = f.inbox(16, false);
        f.assembler.process(0, inbox);
        assertThat(inbox.size()).isEqualTo(14);
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(2);
    }

    @Test void ownedRootCdcKeepsOrdinarySingleWrites() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.assembler.process(1, lineRows(4));
        f.feed(4, false); f.drain();
        f.roots.batches.clear(); f.roots.singleWrites = 0;
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 4; i++) inbox.add(Envelope.update(i, "order",
                row("order_id", i, "cust_ref", "C1"), row("order_id", i, "cust_ref", "C1", "version", 99), null)
                .withOrder(at(i + 100)));
        f.assembler.process(0, inbox);
        assertThat(inbox).isEmpty();
        assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(4);
        assertThat(f.drain()).hasSize(4).allSatisfy(e -> assertThat(e.after()).containsEntry("version", 99));
    }

    private static Fixture arrayFixture(RecordingStore store, boolean folding) throws Exception {
        return new Fixture(ARRAY_TREE, folding ? NestSendPolicy.within(0) : NestSendPolicy.everyChange(),
                128, store, false, NestClock.SYSTEM);
    }

    @Test void existingRootChildSnapshotsUseBoundedPersistenceWithoutLosingLines() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), true);
        f.feed(32, false); f.drain();
        f.roots.singleWrites = 0; f.roots.batches.clear();
        TestInbox lines = lineRows(32);
        f.assembler.process(1, lines);
        assertThat(lines).isEmpty();
        assertArrayDocuments(f.drain(), 32);
        assertThat(f.roots.singleWrites).isZero();
        assertThat(f.roots.batches).containsExactly(16, 16);
    }

    @Test void existingChildBatchFailureReplaysFromOriginalPositionsAndPreservesTheFrontier() throws Exception {
        RecordingStore durable = new RecordingStore();
        Fixture f = new Fixture(ARRAY_TREE, NestSendPolicy.within(0), 1, durable, true, NestClock.SYSTEM);
        TestInbox roots = f.inbox(32, false);
        for (int i = 0; i < 40; i++) { f.assembler.process(0, roots); f.drainRaw(); }
        assertThat(roots).isEmpty();
        durable.batches.clear(); durable.singleWrites = 0; durable.failuresLeft = 1;
        assertThatThrownBy(() -> f.assembler.process(1, lineRows(32))).isInstanceOf(IllegalStateException.class);
        assertThat(f.drainRaw()).isEmpty();
        durable.failuresLeft = 0;
        Fixture replay = new Fixture(ARRAY_TREE, NestSendPolicy.within(0), 1, durable, true, NestClock.SYSTEM);
        TestInbox original = lineRows(32);
        List<Envelope> docs = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            if (original.isEmpty()) replay.assembler.tryProcess(); else replay.assembler.process(1, original);
            docs.addAll(replay.drain());
        }
        assertThat(original).isEmpty();
        assertThat(docs).hasSize(32).extracting(e -> e.after().get("order_id")).doesNotHaveDuplicates();
        for (Envelope doc : docs) {
            int id = (Integer) doc.after().get("order_id");
            assertThat(doc.after().get("lines")).isEqualTo(List.of(
                    row("item_id", id * 2L + 1, "order_id", id, "line_no", 1),
                    row("item_id", id * 2L + 2, "order_id", id, "line_no", 2)));
            assertThat(doc.after().get("customer")).isEqualTo(row("customer_id", "C1", "name", "Ada"));
            // The durable prefix has already cleared its held positions; replay still emits its content.
            if (id > 0) assertThat(doc.positions()).containsEntry("item", new ChainPosition(at(id * 2L + 2), "line-" + (id * 2L + 2)));
        }
        Watermark bound = new Watermark(FrontierOrders.pack("item", at(64)), replay.axes.axisOf("item"));
        assertThat(replay.assembler.tryProcessWatermark(1, bound)).isTrue();
        assertThat(replay.drainRaw()).containsExactly(bound);
    }

    @Test void mixedChildReadAndCdcDrainKeepsTheOrdinaryWritePath() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), true);
        f.feed(4, false); f.drain(); f.roots.singleWrites = 0; f.roots.batches.clear();
        TestInbox lines = lineRows(4);
        lines.add(Envelope.update(100, "item", row("item_id", 1L, "order_id", 0, "line_no", 1),
                row("item_id", 1L, "order_id", 0, "line_no", 1, "value", 99), null).withOrder(at(100)));
        f.assembler.process(1, lines);
        assertThat(lines).isEmpty(); assertThat(f.roots.batches).isEmpty();
        assertThat(f.roots.singleWrites).isEqualTo(4);
        assertThat(f.drain()).hasSize(4);
    }

    @Test void guardedTouchBatchFailureKeepsInputsAndALaterRealEditStillRedraws() throws Exception {
        RecordingStore durable = new RecordingStore(); Fixture before = arrayFixture(durable, true);
        before.feed(32, false); before.drain();
        Fixture replay = arrayFixture(durable, true); durable.failReadBatch = true;
        TestInbox words = new TestInbox();
        for (int i=0;i<32;i++) words.add(new NestTouch(List.of(i),1,Map.of(),true,false));
        assertThatThrownBy(()->replay.assembler.process(ARRAY_TREE.lookups().getFirst().touchOrdinal(),words))
                .isInstanceOf(IllegalStateException.class).hasMessage("state read batch failed");
        assertThat(words.size()).isEqualTo(32);assertThat(replay.drain()).isEmpty();
        durable.failReadBatch=false;replay.assembler.process(ARRAY_TREE.lookups().getFirst().touchOrdinal(),words);
        assertThat(words).isEmpty(); assertThat(replay.drain()).isEmpty();
        replay.customers.save(List.of("C1"),row("customer_id","C1","name","Updated"));
        TestInbox edit = new TestInbox();edit.add(new NestTouch(List.of(0),2,Map.of(),false,false));
        replay.assembler.process(ARRAY_TREE.lookups().getFirst().touchOrdinal(),edit);
        assertThat(replay.drain()).singleElement().satisfies(e->assertThat(((Map<?,?>)e.after().get("customer")).get("name")).isEqualTo("Updated"));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void existingChildWritesRespectWideStateAndWideReferenceOutput(boolean wideReference) throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), true);
        f.feed(2, false); f.drain();
        String wide = "x".repeat(5 * 1024 * 1024);
        if (wideReference) f.customers.save(List.of("C1"), row("customer_id", "C1", "name", wide));
        TestInbox lines = new TestInbox();
        for (int id=0;id<2;id++) lines.add(Envelope.read(id,"item",
                wideReference ? row("item_id",id,"order_id",id,"line_no",1)
                        : row("item_id",id,"order_id",id,"line_no",1,"payload",wide),null).withOrder(at(100+id)));
        f.roots.singleWrites=0;f.roots.batches.clear();f.assembler.process(1,lines);
        assertThat(lines).isEmpty();assertThat(f.roots.batches).isEmpty();assertThat(f.roots.singleWrites).isEqualTo(2);
        assertThat(f.drain()).hasSize(2).allSatisfy(e->assertThat((List<?>)e.after().get("lines")).hasSize(1));
    }

    @Test void wideGuardedReadAheadFallsBackWithoutRereadingItsRetainedPrefix() throws Exception {
        RecordingStore durable=new RecordingStore();Fixture original=arrayFixture(durable,true);
        original.feed(2,false);original.drain();
        TestInbox lines=new TestInbox();String wide="x".repeat(5*1024*1024);
        for(int id=0;id<2;id++)lines.add(Envelope.read(id,"item",row("item_id",id,"order_id",id,"line_no",1,"payload",wide),null).withOrder(at(100+id)));
        original.assembler.process(1,lines);original.drain();
        Fixture replay=arrayFixture(durable,true);durable.singleReads=0;durable.batchReads=0;durable.singleWrites=0;durable.batches.clear();
        TestInbox words=new TestInbox();for(int id=0;id<2;id++)words.add(new NestTouch(List.of(id),1,Map.of(),true,false));
        replay.assembler.process(ARRAY_TREE.lookups().getFirst().touchOrdinal(),words);
        assertThat(words).isEmpty();assertThat(replay.drain()).isEmpty();
        assertThat(durable.batchReads).isEqualTo(1);assertThat(durable.singleReads).isEqualTo(1);
        assertThat(durable.singleWrites).isZero();assertThat(durable.batches).isEmpty();
    }

    @Test void guardedTouchesAfterRestartReadRootStatesInBoundedBatches() throws Exception {
        RecordingStore durable = new RecordingStore();
        Fixture before = arrayFixture(durable, true);
        before.feed(32, false); before.drain();
        Fixture resumed = arrayFixture(durable, true);
        durable.singleReads = 0; durable.batchReads = 0;
        durable.singleWrites = 0; durable.batches.clear();
        TestInbox words = new TestInbox();
        for (int id = 0; id < 32; id++) words.add(new NestTouch(List.of(id), 1, Map.of(), true, false));
        resumed.assembler.process(ARRAY_TREE.lookups().getFirst().touchOrdinal(), words);
        assertThat(words).isEmpty();
        assertThat(resumed.drain()).isEmpty();
        assertThat(durable.singleReads).isZero();
        assertThat(durable.batchReads).isEqualTo(2);
        assertThat(durable.singleWrites).isZero();
        assertThat(durable.batches).isEmpty();
    }

    @Test void existingRootsDoNotCauseOneWaveProbeForEveryAppendRow() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        f.feed(32, false); f.drain(); f.roots.batchReads = 0;
        f.assembler.process(1, lineRows(32));
        assertThat(f.roots.batchReads).isLessThanOrEqualTo(1);
        assertThat(f.drain()).hasSize(64);
    }

    @Test void aSingleParentDrainIsScannedForBatchingOnlyOnce() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        int[] scans = {0};
        TestInbox inbox = new TestInbox();
        var observed = (com.hazelcast.jet.core.Inbox) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{com.hazelcast.jet.core.Inbox.class}, (proxy, method, args) -> {
                    if (method.getName().equals("iterator")) scans[0]++;
                    return method.invoke(inbox, args);
                });
        for (int i = 0; i < 100; i++) inbox.add(Envelope.read(i, "item",
                row("item_id", i, "order_id", 1, "line_no", i), null).withOrder(at(i + 1)));
        f.assembler.process(1, observed);
        assertThat(inbox).isEmpty();
        assertThat(scans[0]).isEqualTo(1);
    }

    @Test void childWaveRetainsTheLatestVersionOfARepeatedLine() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        TestInbox lines = lineRows(2);
        lines.add(Envelope.read(100, "item", row("item_id", 1L, "order_id", 0, "line_no", 1, "value", 99), null)
                .withOrder(at(100)));
        lines.add(Envelope.read(1, "item", row("item_id", 1L, "order_id", 0, "line_no", 1, "value", -1), null)
                .withOrder(at(1)));
        f.assembler.process(1, lines);
        assertThat(f.drain()).isEmpty();
        f.feed(2, false);
        var documents = f.drain();
        var first = documents.stream().filter(value -> value.after().get("order_id").equals(0)).findFirst().orElseThrow();
        var children = (List<?>) first.after().get("lines");
        assertThat(children).hasSize(2);
        assertThat(((Map<?, ?>) children.getFirst()).get("value")).isEqualTo(99);
    }

    @Test void retainedChildStateCountsAgainstTheWholeWriteWaveBudget() throws Exception {
        Fixture f = arrayFixture(new RecordingStore(), false);
        String wide = "x".repeat(5 * 1024 * 1024);
        for (int id = 0; id < 2; id++) {
            var initial = new TestInbox();
            initial.add(Envelope.read(id, "item", row("item_id", id, "order_id", id, "line_no", 1, "wide", wide), null)
                    .withOrder(at(id + 1)));
            f.assembler.process(1, initial);
        }
        f.roots.batches.clear();
        var additions = new TestInbox();
        for (int id = 0; id < 2; id++) additions.add(Envelope.read(id, "item",
                row("item_id", id + 2, "order_id", id, "line_no", 2), null).withOrder(at(id + 3)));
        f.assembler.process(1, additions);
        assertThat(additions).isEmpty();
        assertThat(f.roots.batches).containsExactly(1, 1);
        assertThat(f.drain()).isEmpty();
    }

    private static TestInbox lineRows(int orders) {
        TestInbox rows = new TestInbox();
        for (int id = 0; id < orders; id++) for (int line = 1; line <= 2; line++) {
            long offset = id * 2L + line;
            rows.add(Envelope.read(offset, "item", row("item_id", offset, "order_id", id, "line_no", line), null)
                    .withOrder(at(offset)).withPositions(Map.of("item", new ChainPosition(at(offset), "line-" + offset))));
        }
        return rows;
    }

    private static void assertArrayDocuments(List<Envelope> documents, int count) {
        assertThat(documents).hasSize(count);
        for (Envelope document : documents) {
            int id = (Integer) document.after().get("order_id");
            assertThat(document.after()).containsEntry("customer", row("customer_id", "C1", "name", "Ada"));
            assertThat(document.after().get("lines")).isEqualTo(List.of(
                    row("item_id", id * 2L + 1, "order_id", id, "line_no", 1),
                    row("item_id", id * 2L + 2, "order_id", id, "line_no", 2)));
            assertThat(document.positions()).containsEntry("item", new ChainPosition(at(id * 2L + 2), "line-" + (id * 2L + 2)));
        }
    }

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
        final ChainAxes axes;
        final TestOutbox out;
        final AssemblerProcessor assembler;
        Fixture(NestSendPolicy policy, int capacity) throws Exception {
            this(policy, capacity, new RecordingStore(), false);
        }
        Fixture(NestSendPolicy policy, int capacity, RecordingStore durable, boolean bounded) throws Exception {
            this(policy, capacity, durable, bounded, NestClock.SYSTEM);
        }
        Fixture(NestSendPolicy policy, int capacity, RecordingStore durable, boolean bounded, NestClock clock) throws Exception {
            this(TREE, policy, capacity, durable, bounded, clock);
        }
        Fixture(NestTopology tree, NestSendPolicy policy, int capacity, RecordingStore durable, boolean bounded, NestClock clock) throws Exception {
            roots = durable;
            axes = ChainAxes.assign(tree == ARRAY_TREE ? List.of("order", "item", "customer") : List.of("order", "customer"));
            Map<Integer, List<String>> inbound = new LinkedHashMap<>();
            inbound.put(0, List.of("order"));
            if (tree == ARRAY_TREE) inbound.put(1, List.of("item"));
            inbound.put(tree.lookups().getFirst().touchOrdinal(), List.of("customer"));
            out = new TestOutbox(capacity);
            customers.save(List.of("C1"), row("customer_id", "C1", "name", "Ada"));
            assembler = new AssemblerProcessor(tree.assembler(), tree.slots(), roots, "doc", bounded ? axes : null,
                    bounded ? inbound : null,
                    ReplayFloor.NONE, NestSettings.defaults(), clock, policy,
                    new HeapNestStore<>(), (from, released) -> {}, Map.of(tree.lookups().getFirst().mapName(), customers));
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
        final List<Long> batchBytes = new ArrayList<>();
        final List<List<Object>> batchReadKeys = new ArrayList<>();
        boolean failReadBatch;
        int singleWrites;
        int singleReads;
        int batchReads;
        boolean failBatch;
        int failuresLeft;
        @Override public RootAssembly load(Object key) { singleReads++; return read(key); }
        @Override public Map<Object, RootAssembly> loadAll(java.util.Collection<Object> keys) {
            batchReads++;
            batchReadKeys.add(new ArrayList<>(keys));
            if (failReadBatch) throw new IllegalStateException("state read batch failed");
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
            batchBytes.add(states.values().stream().mapToLong(StateWaveSize::of).sum());
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
