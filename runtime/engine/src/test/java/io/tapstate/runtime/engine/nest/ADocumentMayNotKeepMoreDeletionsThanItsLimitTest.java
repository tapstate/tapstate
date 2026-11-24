package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The bound on what one document may keep to say its elements were deleted, and the sweep that keeps it far
 * from being reached.
 *
 * <p>These two belong in one place because the limit only means what it says when the sweep is what stands
 * in front of it. A record of a deletion is kept until a restart could no longer replay the deletion, so a
 * healthy pipeline drops them continuously however much deleting it does, and the only way to accumulate
 * them is a durable position that has stopped advancing. Weigh the count without dropping first and the
 * limit reports something else entirely - that this vertex has been busy - which is both a failed job for no
 * reason and the real condition going unreported.
 *
 * <p>Failed rather than dropped once it is genuinely reached. Every other bound here can be answered by
 * giving something up, and this one cannot: what would be given up is the record that stops a deleted row
 * coming back at the next restart, and nothing downstream could ever notice that it did.
 */
class ADocumentMayNotKeepMoreDeletionsThanItsLimitTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private static final int ROOT_ROWS = 0;
    private static final int FROM_POLICIES = 1;

    private static final Object CUSTOMER_1 = List.of("c1");

    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();
    private final SettableFloor floor = new SettableFloor();
    private final TestOutbox outbox = new TestOutbox(1024);

    private AssemblerProcessor assembler(NestSettings settings) throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), store,
                "doc", null, null, floor, settings);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    private NestSettings allowing(long deletions) {
        return NestSettings.defaults().withTombstoneLimit(TOPOLOGY.assembler().mapName(), deletions);
    }

    @Test
    void keepingMoreThanTheLimitWithNothingThatMayBeDroppedFailsTheJob() throws Exception {
        AssemblerProcessor processor = assembler(allowing(2));
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2"), policy(4, "c1", "p3"));

        assertThatThrownBy(() -> feed(processor, FROM_POLICIES,
                policyGone(5, "c1", "p1"), policyGone(6, "c1", "p2"), policyGone(7, "c1", "p3")))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("TS0490022")
                .hasMessageContaining("tombstones=3")
                .hasMessageContaining("limit=2");
    }

    @Test
    void keepingExactlyTheLimitIsAllowed() throws Exception {
        AssemblerProcessor processor = assembler(allowing(3));
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2"), policy(4, "c1", "p3"));

        assertThatCode(() -> feed(processor, FROM_POLICIES,
                policyGone(5, "c1", "p1"), policyGone(6, "c1", "p2"), policyGone(7, "c1", "p3")))
                .doesNotThrowAnyException();
    }

    /**
     * The one that decides what this limit is about. Everything here is past the limit and every one of them
     * may be dropped, so a vertex that weighed the count as it stood would fail a pipeline whose frontier is
     * perfectly healthy and merely deletes a lot.
     */
    @Test
    void whatMayBeDroppedIsDroppedBeforeTheCountIsWeighed() throws Exception {
        AssemblerProcessor processor = assembler(allowing(2));
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2"), policy(4, "c1", "p3"));
        floor.at("policy", at(99));

        assertThatCode(() -> feed(processor, FROM_POLICIES,
                policyGone(5, "c1", "p1"), policyGone(6, "c1", "p2"), policyGone(7, "c1", "p3")))
                .doesNotThrowAnyException();
        assertThat(store.load(CUSTOMER_1).tombstones())
                .describedAs("nothing can replay them, so none of them is kept")
                .isZero();
    }

    /** Records of deletion are dropped when the vertex has nothing arriving, long before any limit. */
    @Test
    void whatCanNoLongerBeReplayedIsDroppedWhenNothingIsArriving() throws Exception {
        AssemblerProcessor processor = assembler(NestSettings.defaults());
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policyGone(3, "c1", "p1"));
        assertThat(store.load(CUSTOMER_1).tombstones()).isEqualTo(1);

        floor.at("policy", at(9));
        processor.tryProcess();

        assertThat(store.load(CUSTOMER_1).tombstones()).isZero();
    }

    @Test
    void aRecordWhoseDeletionCouldStillArriveAgainSurvivesTheSweep() throws Exception {
        AssemblerProcessor processor = assembler(NestSettings.defaults());
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policyGone(3, "c1", "p1"));

        floor.at("policy", at(3));
        processor.tryProcess();

        assertThat(store.load(CUSTOMER_1).tombstones())
                .describedAs("the deletion sits at the floor, so a resume still delivers it")
                .isEqualTo(1);
    }

    @Test
    void theFloorIsReadBesideTheFlowRatherThanForEveryChange() throws Exception {
        AssemblerProcessor processor = assembler(NestSettings.defaults());
        feed(processor, ROOT_ROWS, customer(1, "c1"));

        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policyGone(3, "c1", "p1"));

        assertThat(floor.reads)
                .describedAs("a document below its limit weighs a count, never a crossing to the durable plane")
                .isZero();
    }

    /**
     * At the limit is within it, so there is nothing to drop and nothing to ask. Without this the boundary
     * between the two checks is invisible: refusing to return here would still fail at the same count, and
     * the only difference - a crossing to the durable plane made by every document sitting exactly at its
     * limit, on every drain - shows up in nothing a test asserts about documents.
     */
    @Test
    void aDocumentExactlyAtItsLimitWeighsOnlyACount() throws Exception {
        AssemblerProcessor processor = assembler(allowing(3));
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2"), policy(4, "c1", "p3"));

        feed(processor, FROM_POLICIES,
                policyGone(5, "c1", "p1"), policyGone(6, "c1", "p2"), policyGone(7, "c1", "p3"));

        assertThat(floor.reads).isZero();
    }

    @Test
    void aNamespaceIsBoundedByItsOwnLimitAndNotAnother() throws Exception {
        AssemblerProcessor processor = assembler(NestSettings.defaults().withTombstoneLimit("other-nest", 1));
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2"));

        assertThatCode(() -> feed(processor, FROM_POLICIES, policyGone(4, "c1", "p1"), policyGone(5, "c1", "p2")))
                .describedAs("a limit set on some other namespace bounds that one, not this")
                .doesNotThrowAnyException();
    }

    private void feed(AssemblerProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        outbox.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Envelope customer(long seq, String id) {
        return Envelope.insert(seq, "customer", row("customer_id", id, "name", "n" + seq), null)
                .withOrder(at(seq));
    }

    private static Envelope policy(long seq, String customerId, String policyId) {
        return Envelope.insert(seq, "policy",
                        row("customer_id", customerId, "policy_id", policyId, "policy_no", "PN-" + policyId),
                        null)
                .withOrder(at(seq));
    }

    private static Envelope policyGone(long seq, String customerId, String policyId) {
        return Envelope.delete(seq, "policy",
                        row("customer_id", customerId, "policy_id", policyId, "policy_no", "PN-" + policyId),
                        null)
                .withOrder(at(seq));
    }

    /** A floor whose answers the test sets, counting how often it was asked. */
    private static final class SettableFloor implements ReplayFloor {

        private static final long serialVersionUID = 1L;

        private final Map<String, SourceOrder> floors = new LinkedHashMap<>();
        private transient int reads;

        private void at(String chain, SourceOrder order) {
            floors.put(chain, order);
        }

        @Override
        public Optional<SourceOrder> of(String chain) {
            reads++;
            return Optional.ofNullable(floors.get(chain));
        }
    }
}
