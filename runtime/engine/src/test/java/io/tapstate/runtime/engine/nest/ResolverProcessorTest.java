package io.tapstate.runtime.engine.nest;

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
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * One resolver vertex at work. The tree is customer to policies to claims, so the vertex under test is
 * the one for policies: policy rows arrive on ordinal 0 and declare which customer they hang from, claim
 * rows arrive on ordinal 1 and ask that same question of the key they name.
 *
 * <p>Which of the two an item is comes from the ordinal alone. The tests drive the processor directly
 * rather than through a generic harness because half of what is being pinned here is sequencing - what
 * happens when a child outruns its parent, and what happens when the parent turns out to be deleted.
 */
class ResolverProcessorTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestVertex POLICIES =
            NestTopology.compile("p", "doc", TREE, tables()).vertexAt(List.of("policies"));

    private static final int OWN_ROWS = 0;
    private static final int CLAIMS = 1;

    private final HeapNestStore<ResolverState> store = new HeapNestStore<>();
    private final List<NestElement> dead = new ArrayList<>();
    private final ResolverProcessor processor = new ResolverProcessor(POLICIES, store, dead::add);
    private final TestOutbox outbox = new TestOutbox(128);

    @BeforeEach
    void init() throws Exception {
        processor.init(outbox, new TestProcessorContext());
    }

    private static Envelope event(Op op, String src, long seq, Object... pairs) {
        Map<String, Object> fields = row(pairs);
        Envelope built = op == Op.DELETE
                ? Envelope.delete(seq, src, fields, null)
                : Envelope.insert(seq, src, fields, null);
        return built.withOrder(new SourceOrder(1L, seq));
    }

    private static Envelope policy(long seq, String policyId, String customerId) {
        return event(Op.INSERT, "policy", seq,
                "policy_id", policyId, "customer_id", customerId, "policy_no", "PN-" + policyId);
    }

    private static Envelope claim(long seq, String claimId, String policyId) {
        return event(Op.INSERT, "claim", seq, "claim_id", claimId, "policy_id", policyId);
    }

    private List<KeyedElement> feed(int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        List<Object> drained = new ArrayList<>();
        outbox.drainQueueAndReset(0, drained, false);
        return drained.stream().map(KeyedElement.class::cast).toList();
    }

    @Test
    void aRowOfItsOwnEmbedTravelsOnAsAnElementOfTheDocument() {
        List<KeyedElement> out = feed(OWN_ROWS, policy(1, "P1", "C1"));

        assertThat(out).hasSize(1);
        KeyedElement sent = out.get(0);
        assertThat(sent.key())
                .describedAs("routed one level up, by the customer it hangs from")
                .isEqualTo(List.of("C1"));
        assertThat(sent.element().ref().pathId()).containsExactly("policies");
        assertThat(sent.element().ref().elementKey())
                .describedAs("the document shows the business key, not the one children point at")
                .containsExactly("PN-P1");
        assertThat(sent.element().ref().identity()).isEqualTo(List.of("P1"));
        assertThat(sent.element().ref().parentIdentity())
                .describedAs("at depth one the parent is the root itself, so there is no element above")
                .isNull();
        assertThat(sent.element().deletion()).isFalse();
    }

    @Test
    void aChildThatOutrunsItsParentWaitsAndLeavesWithItWhenItArrives() {
        assertThat(feed(CLAIMS, claim(2, "CL1", "P1")))
                .describedAs("nothing can be said about where it belongs yet")
                .isEmpty();

        List<KeyedElement> out = feed(OWN_ROWS, policy(3, "P1", "C1"));

        assertThat(out).hasSize(2);
        assertThat(out).allSatisfy(sent -> assertThat(sent.key()).isEqualTo(List.of("C1")));
        assertThat(out).extracting(sent -> sent.element().ref().pathId())
                .containsExactly(List.of("policies"), List.of("policies", "claims"));
        assertThat(out.get(1).element().ref().parentIdentity())
                .describedAs("the claim hangs under the policy element, named by the key it joined on")
                .isEqualTo(List.of("P1"));
    }

    @Test
    void aChildArrivingAfterItsParentIsRoutedOnTheSpot() {
        feed(OWN_ROWS, policy(1, "P1", "C1"));

        List<KeyedElement> out = feed(CLAIMS, claim(2, "CL1", "P1"));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).key()).isEqualTo(List.of("C1"));
        assertThat(out.get(0).element().ref().elementKey()).containsExactly("CL1");
        assertThat(store.count()).isEqualTo(1);
    }

    @Test
    void aChildOfADeletedParentGoesToTheDeadLetterRatherThanNowhere() {
        feed(OWN_ROWS, policy(1, "P1", "C1"));
        feed(OWN_ROWS, event(Op.DELETE, "policy", 5,
                "policy_id", "P1", "customer_id", "C1", "policy_no", "PN-P1"));

        assertThat(feed(CLAIMS, claim(6, "CL1", "P1")))
                .describedAs("it can never reach a document, so it is not held either")
                .isEmpty();
        assertThat(dead).hasSize(1);
        assertThat(dead.get(0).ref().elementKey()).containsExactly("CL1");
    }

    @Test
    void deletingTheParentReleasesWhatWasWaitingIntoTheDeadLetter() {
        feed(CLAIMS, claim(2, "CL1", "P1"));

        List<KeyedElement> out = feed(OWN_ROWS, event(Op.DELETE, "policy", 5,
                "policy_id", "P1", "customer_id", "C1", "policy_no", "PN-P1"));

        assertThat(out)
                .describedAs("the deletion of the policy element itself still travels to the document")
                .hasSize(1);
        assertThat(out.get(0).element().deletion()).isTrue();
        assertThat(dead)
                .describedAs("those children were waiting for a row now known to be gone")
                .hasSize(1);
        assertThat(dead.get(0).ref().elementKey()).containsExactly("CL1");
    }

    @Test
    void writesTheStateBackOncePerKeyHoweverManyEventsTouchedItInOneDrain() throws Exception {
        CountingStore counting = new CountingStore();
        ResolverProcessor folding = new ResolverProcessor(POLICIES, counting, dead::add);
        TestOutbox out = new TestOutbox(128);
        folding.init(out, new TestProcessorContext());

        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(List.of(
                policy(1, "P1", "C1"), policy(2, "P1", "C1"), policy(3, "P1", "C1"), policy(4, "P2", "C1")));
        folding.process(OWN_ROWS, inbox);

        assertThat(counting.saves)
                .describedAs("four events touched two keys, so the store is written twice, not four times")
                .isEqualTo(2);
    }

    /** A store that counts what it is asked to write, so drain-folding can be measured rather than assumed. */
    private static final class CountingStore implements NestStore<ResolverState> {

        private final Map<Object, ResolverState> entries = new java.util.LinkedHashMap<>();
        private int saves;

        @Override
        public ResolverState load(Object key) {
            return entries.get(key);
        }

        @Override
        public void save(Object key, ResolverState state) {
            saves++;
            entries.put(key, state);
        }

        @Override
        public void remove(Object key) {
            entries.remove(key);
        }

        @Override
        public long count() {
            return entries.size();
        }
    }

    @Test
    void refusesToRunOnAnEventThatCarriesNoOrder() {
        Envelope orderless = Envelope.insert(1L, "policy",
                Map.of("policy_id", "P1", "customer_id", "C1", "policy_no", "PN"), null);

        assertThatThrownBy(() -> feed(OWN_ROWS, orderless))
                .describedAs("guessing an order would silently reorder data, so it crashes bare")
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("no order");
    }

    @Test
    void isNotCooperativeBecauseItsStoreMayBlock() {
        assertThat(processor.isCooperative()).isFalse();
    }

    @Test
    void refusesToBeBuiltOnTheAssemblerVertex() {
        NestVertex assembler = NestTopology.compile("p", "doc", TREE, tables()).assembler();

        assertThatThrownBy(() -> new ResolverProcessor(assembler, store, dead::add))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
