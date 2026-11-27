package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
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
    void aSecondHandOverOntoTheSameAddressKeepsWhatTheFirstLeft() throws Exception {
        AssemblerProcessor losing = assembler(twoPerBatch());
        AssemblerProcessor gaining = assembler(twoPerBatch());
        customer(losing, "C1");
        policies(losing, "C1", 0, 3);
        feed(losing, DEPARTURES, renamed("C1", "C2"));

        // A distinct root occupies the old key before its own departure.
        customer(losing, "C1", 91);
        policy(losing, "C1", 3, 92);
        policy(losing, "C1", 4, 93);
        Envelope secondMove = renamed("C1", "C2", 94);
        feed(losing, DEPARTURES, secondMove);

        feed(gaining, OWN_ROWS, secondMove);
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
        customer(processor, customerId, 1);
    }

    private void customer(AssemblerProcessor processor, String customerId, long sequence) {
        feed(processor, OWN_ROWS, Envelope.insert(sequence, "customer",
                row("customer_id", customerId, "name", "n"), null).withOrder(at(sequence)));
    }

    private void policies(AssemblerProcessor processor, String customerId, int count) {
        policies(processor, customerId, 0, count);
    }

    /** {@code count} policies numbered from {@code from}, so two lots never share an element key. */
    private void policies(AssemblerProcessor processor, String customerId, int from, int count) {
        for (int i = from; i < from + count; i++) {
            policy(processor, customerId, i, 2 + i);
        }
    }

    private void policy(AssemblerProcessor processor, String customerId, int index, long sequence) {
        feed(processor, POLICIES, Envelope.insert(sequence, "policy",
                row("policy_id", "P" + index, "customer_id", customerId, "policy_no", "PN-" + index), null)
                .withOrder(at(sequence)));
    }

    private static Envelope renamed(String was, String is) {
        return renamed(was, is, 90);
    }

    private static Envelope renamed(String was, String is, long sequence) {
        return Envelope.update(sequence, "customer",
                row("customer_id", was, "name", "n"),
                row("customer_id", is, "name", "n"), null).withOrder(at(sequence));
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
