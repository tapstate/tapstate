package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A hand-over goes into the parking area in pieces the deployment sizes, rather than as one value however
 * wide the tree is.
 *
 * <p>What it buys is not the write itself - a document of that width is already one entry on both sides -
 * but the second hand-over. Appending to one value means reading the whole of it back and writing the whole
 * of it out again, so a key moved repeatedly pays for everything parked before it, every time. In pieces,
 * what is rewritten is one piece.
 *
 * <p>The size is read from the settings the nest already carries and from nowhere else. A second place to
 * configure a nest is a deployment that has set half of a pair of numbers that only mean anything together.
 *
 * <p>A hand-over that fits in one piece stays one entry, which is what nearly all of them are. That is a
 * property worth pinning rather than an accident: it is what makes the common case cost exactly what it did
 * before, and what lets an entry written by an older build still be read.
 */
class AHandOverIsWrittenInBatchesTheDeploymentSizesTest {

    private static final TransformBody.Nest TREE = nest(new NestRoot("customer", List.of("customer_id"),
            null, true,
            List.of(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final String NAMESPACE = TOPOLOGY.assembler().mapName();

    private static final int OWN_ROWS = 0;
    private static final int POLICIES = 1;
    private static final int DEPARTURES = 2;

    private final NestBinding.NestStores stores = HeapNestStores.onHeap();
    private final HeapNestStore<RootAssembly> documents = new HeapNestStore<>();
    private final TestOutbox out = new TestOutbox(512);

    @Test
    void aRecoveredHandoverCountsItsStoredRowsBeforeAcceptingAnotherMove() {
        HeapNestStore<ParkedSubtree> parking = new HeapNestStore<>();
        ParkedSubtree.At address = ParkedSubtree.At.ofRoot(List.of("C2"));
        NestMigrationWriter.publish(address, new CountingRows(5), parking, 2, 5);
        ParkedSubtree original = parking.load(address);

        assertThatThrownBy(() -> NestMigrationWriter.publish(address, new CountingRows(1), parking, 2, 5))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(NestError.MIGRATION_PARKING_LIMIT_EXCEEDED);
                    assertThat(failure.args()).containsEntry("changes", 6L).containsEntry("limit", 5L);
                });

        assertThat(parking.load(address)).isEqualTo(original);
    }

    @Test
    void aMissingPromisedPieceRefusesAnAppendWithoutReplacingTheHeader() {
        HeapNestStore<ParkedSubtree> parking = new HeapNestStore<>();
        ParkedSubtree.At address = ParkedSubtree.At.ofRoot(List.of("C2"));
        ParkedSubtree original = new ParkedSubtree(List.of(new CountingRows(1).iterator().next()), 1);
        parking.save(address, original);

        assertThatThrownBy(() -> NestMigrationWriter.publish(address, new CountingRows(1), parking, 2, 50))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(NestError.MIGRATION_HANDOVER_UNAVAILABLE);
                    assertThat(failure.args()).containsEntry("piece", 1);
                });

        assertThat(parking.load(address)).isEqualTo(original);
    }

    @Test
    void aMissingPromisedPieceIsNotSilentlySkippedByTheReceivingDocument() throws Exception {
        NestStore<ParkedSubtree> parking = stores.forParking(TOPOLOGY.assembler());
        ParkedSubtree.At address = ParkedSubtree.At.ofRoot(List.of("C2"));
        ParkedSubtree original = new ParkedSubtree(List.of(new CountingRows(1).iterator().next()), 1);
        parking.save(address, original);
        AssemblerProcessor gaining = assembler(twoPerBatch());

        assertThatThrownBy(() -> feed(gaining, OWN_ROWS, renamed("C1", "C2")))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(NestError.MIGRATION_HANDOVER_UNAVAILABLE));

        assertThat(parking.load(address)).isEqualTo(original);
    }

    @Test
    void boundedChunksPublishEveryRowInItsOriginalOrder() {
        HeapNestStore<ParkedSubtree> parking = new HeapNestStore<>();
        ParkedSubtree.At address = ParkedSubtree.At.ofRoot(List.of("C2"));
        NestMigrationWriter.publish(address, new CountingRows(21), parking, 2, 50);
        ParkedSubtree first = parking.load(address);
        List<NestElement> rows = new ArrayList<>(first.changes());
        assertThat(first.changes()).hasSizeLessThanOrEqualTo(2);
        for (int piece = 1; piece <= first.batches(); piece++) {
            ParkedSubtree next = parking.load(address.piece(piece));
            assertThat(next.changes()).hasSizeLessThanOrEqualTo(2);
            rows.addAll(next.changes());
        }
        assertThat(rows).extracting(row -> row.fields().get("policy_no"))
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, 21)
                        .mapToObj(index -> "PN-" + index).toList());
    }

    @Test
    void aHandoverViewDoesNotCopyRowsThatHaveNotBeenConsumed() {
        RootAssembly donor = new RootAssembly();
        ElementRef parent = new ElementRef(List.of("policies"), null, List.of("PN-1"), List.of("P1"));
        donor.applyElement(parent, row("policy_id", "P1"), at(1), Map.of());
        ElementRef first = new ElementRef(List.of("policies", "claims"), List.of("P1"), List.of("K1"), null);
        ElementRef later = new ElementRef(List.of("policies", "claims"), List.of("P1"), List.of("K2"), null);
        donor.applyElement(first, row("claim_id", "K1", "value", "first"), at(2), Map.of());
        donor.applyElement(later, row("claim_id", "K2", "value", "before"), at(3), Map.of());

        Iterable<NestElement> handover = donor.subtreeForHandover(parent);
        donor.applyElement(later, row("claim_id", "K2", "value", "after"), at(4), Map.of());

        Iterator<NestElement> rows = handover.iterator();
        assertThat(rows.next().fields()).containsEntry("claim_id", "K1");
        assertThat(rows.next().fields()).as("a later row is copied only as its writer consumes it")
                .containsEntry("claim_id", "K2").containsEntry("value", "after");
        assertThat(rows.hasNext()).isFalse();
    }

    @Test
    void aFailedChunkWriteDoesNotPullRowsBeyondTheHeadAndCurrentChunk() {
        CountingRows rows = new CountingRows(21);
        NestStore<ParkedSubtree> refuses = new NestStore<>() {
            @Override public ParkedSubtree load(Object key) { return null; }
            @Override public void save(Object key, ParkedSubtree value) {
                throw new IllegalStateException("parking write refused");
            }
            @Override public void remove(Object key) { }
            @Override public long count() { return 0; }
        };

        assertThatThrownBy(() -> NestMigrationWriter.publish(ParkedSubtree.At.ofRoot(List.of("C2")),
                rows, refuses, 2, 50)).isInstanceOf(IllegalStateException.class);

        assertThat(rows.produced).as("the writer retains one identifying chunk and one current chunk")
                .isLessThanOrEqualTo(4);
    }

    @Test
    void aParkingLimitStopsBeforeMaterializingTheRestOfTheInput() {
        CountingRows rows = new CountingRows(21);
        HeapNestStore<ParkedSubtree> parking = new HeapNestStore<>();
        ParkedSubtree.At address = ParkedSubtree.At.ofRoot(List.of("C2"));

        assertThatThrownBy(() -> NestMigrationWriter.publish(address, rows, parking, 2, 3))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(NestError.MIGRATION_PARKING_LIMIT_EXCEEDED);
                    assertThat(failure.args()).containsEntry("changes", 4L).containsEntry("limit", 3L);
                });

        assertThat(rows.produced).isEqualTo(4);
        assertThat(parking.load(address)).as("an incomplete handover has no collectible identifying entry")
                .isNull();
    }

    private static final class CountingRows implements Iterable<NestElement> {
        private final int count;
        private int produced;
        private CountingRows(int count) { this.count = count; }
        @Override public Iterator<NestElement> iterator() {
            return new Iterator<>() {
                @Override public boolean hasNext() { return produced < count; }
                @Override public NestElement next() {
                    int ordinal = produced++;
                    return new NestElement(new ElementRef(List.of("policies"), null,
                            List.of("PN-" + ordinal), null), row("policy_no", "PN-" + ordinal),
                            at(ordinal + 1L), Map.of());
                }
            };
        }
    }

    @Test
    void aTreeWiderThanOneBatchIsParkedAsSeveralEntries() throws Exception {
        AssemblerProcessor losing = assembler(twoPerBatch());
        customer(losing, "C1");
        policies(losing, "C1", 5);

        feed(losing, DEPARTURES, renamed("C1", "C2"));

        assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                .describedAs("five changes at two a batch is three entries, not one value holding all five")
                .isEqualTo(3);
    }

    @Test
    void everythingParkedInPiecesArrivesWhenItIsCollected() throws Exception {
        AssemblerProcessor losing = assembler(twoPerBatch());
        AssemblerProcessor gaining = assembler(twoPerBatch());
        customer(losing, "C1");
        policies(losing, "C1", 5);

        feed(losing, DEPARTURES, renamed("C1", "C2"));
        feed(gaining, OWN_ROWS, renamed("C1", "C2"));

        assertThat(policiesOf("C2"))
                .describedAs("a piece left behind is rows gone with nothing anywhere reporting it")
                .hasSize(5);
        assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                .describedAs("and every piece is let go of, not just the one naming the rest")
                .isZero();
    }

    /** The common case, pinned: nothing about the shape of a small hand-over changes. */
    @Test
    void aTreeThatFitsInOneBatchStaysOneEntry() throws Exception {
        AssemblerProcessor losing = assembler(twoPerBatch());
        customer(losing, "C1");
        policies(losing, "C1", 2);

        feed(losing, DEPARTURES, renamed("C1", "C2"));

        assertThat(stores.forParking(TOPOLOGY.assembler()).count()).isEqualTo(1);
    }

    /**
     * The discrimination against a batch size that is read from anywhere but the nest's own settings: with
     * the number raised, the same tree has to become fewer entries. A hard-coded size passes the test above
     * and fails this one.
     */
    @Test
    void theSizeIsTheOneTheDeploymentSetForThisNamespace() throws Exception {
        AssemblerProcessor losing = assembler(NestSettings.defaults().withMigrationBatchSize(NAMESPACE, 4));
        customer(losing, "C1");
        policies(losing, "C1", 5);

        feed(losing, DEPARTURES, renamed("C1", "C2"));

        assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                .describedAs("five changes at four a batch is two entries")
                .isEqualTo(2);
    }

    /**
     * A second hand-over onto an address that already holds one appends rather than replaces. Two moves of
     * one key before either was collected are two lots of rows, and taking only the later one drops whatever
     * the earlier carried.
     */
    @Test
    void asecondHandOverOntoTheSameAddressKeepsWhatTheFirstLeft() throws Exception {
        AssemblerProcessor losing = assembler(twoPerBatch());
        AssemblerProcessor gaining = assembler(twoPerBatch());
        customer(losing, "C1");
        policies(losing, "C1", 0, 3);
        feed(losing, DEPARTURES, renamed("C1", "C2"));

        // The same key vacated a second time, with rows of its own - different rows, so that a lot dropped
        // shows as rows missing rather than being hidden by the other lot carrying the same element keys.
        policies(losing, "C1", 3, 2);
        feed(losing, DEPARTURES, renamed("C1", "C2"));

        feed(gaining, OWN_ROWS, renamed("C1", "C2"));
        assertThat(policiesOf("C2"))
                .describedAs("both lots travelled; the second hand-over did not overwrite the first")
                .hasSize(5);
    }

    // ---- harness ------------------------------------------------------------------------

    private static NestSettings twoPerBatch() {
        return NestSettings.defaults().withMigrationBatchSize(NAMESPACE, 2);
    }

    private AssemblerProcessor assembler(NestSettings settings) throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(),
                documents, "doc", null, null, ReplayFloor.NONE, settings, NestClock.SYSTEM,
                NestSendPolicy.within(0), stores.forParking(TOPOLOGY.assembler()));
        processor.init(out, new TestProcessorContext());
        return processor;
    }

    private void feed(AssemblerProcessor processor, int ordinal, Envelope event) {
        TestInbox inbox = new TestInbox();
        inbox.queue().add(event);
        processor.process(ordinal, inbox);
        out.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    private void customer(AssemblerProcessor processor, String customerId) {
        feed(processor, OWN_ROWS, Envelope.insert(1, "customer",
                row("customer_id", customerId, "name", "n"), null).withOrder(at(1)));
    }

    private void policies(AssemblerProcessor processor, String customerId, int count) {
        policies(processor, customerId, 0, count);
    }

    /** {@code count} policies numbered from {@code from}, so two lots never share an element key. */
    private void policies(AssemblerProcessor processor, String customerId, int from, int count) {
        for (int i = from; i < from + count; i++) {
            feed(processor, POLICIES, Envelope.insert(2, "policy",
                    row("policy_id", "P" + i, "customer_id", customerId, "policy_no", "PN-" + i), null)
                    .withOrder(at(2 + i)));
        }
    }

    private static Envelope renamed(String was, String is) {
        return Envelope.update(9, "customer",
                row("customer_id", was, "name", "n"),
                row("customer_id", is, "name", "n"), null).withOrder(at(90));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> policiesOf(String customerId) {
        RootAssembly assembly = documents.load(List.of(customerId));
        if (assembly == null) {
            return List.of();
        }
        Map<String, Object> document = assembly.render(TOPOLOGY.slots()).orElse(new LinkedHashMap<>());
        return (List<Map<String, Object>>) document.getOrDefault("policies", List.of());
    }
}
