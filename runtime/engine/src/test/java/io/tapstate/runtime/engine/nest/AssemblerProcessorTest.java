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
import io.tapstate.core.event.ChainPosition;
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
 * The vertex that holds whole documents. The tree is a customer with policies (each with claims) and a
 * profile object, so the assembler sees three kinds of arrival: its own root rows on ordinal 0, elements
 * cascading up from the policies resolver on ordinal 1, and profile rows arriving directly on ordinal 2
 * because a leaf hanging off the root has no resolver to pass through.
 *
 * <p>What goes out is the document as it stands, not the change that got it there.
 */
class AssemblerProcessorTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))),
            embed("profile", "customer_id", "customer_id", EmbedAs.OBJECT, "profile", List.of("customer_id")));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private static final int ROOT_ROWS = 0;
    private static final int FROM_POLICIES = 1;
    private static final int PROFILE = 2;

    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();
    private final AssemblerProcessor processor =
            new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), store, "doc");
    private final TestOutbox outbox = new TestOutbox(128);

    @BeforeEach
    void init() throws Exception {
        processor.init(outbox, new TestProcessorContext());
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Envelope customer(long seq, String id, String name) {
        return Envelope.insert(seq, "customer", row("customer_id", id, "name", name), null).withOrder(at(seq));
    }

    private static Envelope profileRow(long seq, String customerId, String tier) {
        return Envelope.insert(seq, "profile", row("customer_id", customerId, "tier", tier), null)
                .withOrder(at(seq));
    }

    /** A policy element as the policies resolver would have routed it: keyed by the customer it hangs from. */
    private static KeyedElement policyElement(long seq, String customerId, String policyId) {
        ElementRef ref = new ElementRef(List.of("policies"), null, List.of("PN-" + policyId), List.of(policyId));
        return new KeyedElement(List.of(customerId),
                new NestElement(ref, row("policy_id", policyId, "policy_no", "PN-" + policyId), at(seq),
                        Map.of("policy", new ChainPosition(at(seq), null))));
    }

    /** A claim element as the claims resolver would have routed it: it hangs under a policy, not the root. */
    private static KeyedElement claimElement(long seq, String customerId, String policyId, String claimId,
            String token) {
        ElementRef ref = new ElementRef(
                List.of("policies", "claims"), List.of(policyId), List.of(claimId), null);
        return new KeyedElement(List.of(customerId),
                new NestElement(ref, row("claim_id", claimId, "policy_id", policyId), at(seq),
                        Map.of("claim", new ChainPosition(at(seq), token))));
    }

    private List<Envelope> feed(int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        List<Object> drained = new ArrayList<>();
        outbox.drainQueueAndReset(0, drained, false);
        return drained.stream().map(Envelope.class::cast).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> arrayAt(Envelope document, String field) {
        return (List<Map<String, Object>>) document.after().get(field);
    }

    @Test
    void aRootRowOnItsOwnIsAlreadyADocument() {
        List<Envelope> out = feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).src()).isEqualTo("doc");
        assertThat(out.get(0).after()).containsEntry("customer_id", "C1").containsEntry("name", "Ada");
        assertThat(arrayAt(out.get(0), "policies"))
                .describedAs("an array embed with nothing in it renders empty rather than missing")
                .isEmpty();
        assertThat(out.get(0).after())
                .describedAs("an object embed with nothing in it omits its field rather than rendering null")
                .doesNotContainKey("profile");
    }

    @Test
    void anAssembledDocumentGoesOutAsAWholeRowAnUpsertCanApply() {
        List<Envelope> out = feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).op())
                .describedAs("the resend unit is the whole document and a sink applies it by upserting; a "
                        + "change offering no before image is the one shape such a sink cannot apply, and it "
                        + "matches nothing rather than failing, so nothing is written and nothing is reported")
                .isEqualTo(Op.INSERT);
        assertThat(out.get(0).before())
                .describedAs("there is no before image to give: what goes out is the state, not the change")
                .isNull();
    }

    @Test
    void anElementArrivingBeforeItsRootProducesNoDocumentAtAll() {
        assertThat(feed(FROM_POLICIES, policyElement(2, "C1", "P1")))
                .describedAs("a document with no root fields is a ghost that nothing later removes")
                .isEmpty();

        List<Envelope> out = feed(ROOT_ROWS, customer(3, "C1", "Ada"));

        assertThat(out).hasSize(1);
        assertThat(arrayAt(out.get(0), "policies"))
                .describedAs("the element was kept and appears as soon as there is a document to put it in")
                .hasSize(1);
    }

    @Test
    void anElementLandsWhereItsPathSaysItBelongs() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        List<Envelope> out = feed(FROM_POLICIES, policyElement(2, "C1", "P1"));

        assertThat(arrayAt(out.get(0), "policies")).hasSize(1);
        assertThat(arrayAt(out.get(0), "policies").get(0)).containsEntry("policy_no", "PN-P1");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> objectAt(Envelope document, String field) {
        return (Map<String, Object>) document.after().get(field);
    }

    @Test
    void aLeafHangingOffTheRootArrivesDirectlyAndStillLands() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        List<Envelope> out = feed(PROFILE, profileRow(2, "C1", "gold"));

        assertThat(out).hasSize(1);
        assertThat(objectAt(out.get(0), "profile")).containsEntry("tier", "gold");
    }

    @Test
    void rendersOneDocumentPerDrainHoweverManyElementsTouchedIt() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        List<Envelope> out = feed(FROM_POLICIES,
                policyElement(2, "C1", "P1"), policyElement(3, "C1", "P2"), policyElement(4, "C1", "P3"));

        assertThat(out)
                .describedAs("three elements of one document render it once, not three times")
                .hasSize(1);
        assertThat(arrayAt(out.get(0), "policies")).hasSize(3);
    }

    @Test
    void twoDocumentsTouchedInOneDrainEachComeOutOnce() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"), customer(2, "C2", "Grace"));

        List<Envelope> out = feed(FROM_POLICIES, policyElement(3, "C1", "P1"), policyElement(4, "C2", "P2"));

        assertThat(out).hasSize(2);
        assertThat(out).extracting(document -> document.after().get("customer_id"))
                .containsExactly("C1", "C2");
    }

    @Test
    void aDeletedRootGoesOutAsADeletionCarryingOnlyItsKey() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        Envelope deletion = Envelope.delete(5, "customer", row("customer_id", "C1", "name", "Ada"), null)
                .withOrder(at(5));
        List<Envelope> out = feed(ROOT_ROWS, deletion);

        assertThat(out).hasSize(1);
        assertThat(out.get(0).op())
                .describedAs("the sink has a document to remove, so this one thing still goes out")
                .isEqualTo(Op.DELETE);
        assertThat(out.get(0).before()).containsExactly(Map.entry("customer_id", "C1"));
        assertThat(out.get(0).after()).isNull();
    }

    /**
     * Reclaiming the entry outright is cheaper than keeping a tombstone, and it is not allowed here. The
     * two conditions that make it safe - the deletion being below the replay floor, and nothing pending -
     * both need a reading of how far the sink has durably got, which this vertex sits upstream of and has
     * no way to obtain. Reclaiming without that reading loses the tombstone, and then a replayed insert
     * from inside the window brings a deleted document back after a restart. Late reclamation costs
     * memory; early reclamation is wrong, so until the reading exists the entry stays.
     */
    @Test
    void aDeletedRootKeepsItsEntryBecauseNothingHereCanTellWhenReclaimingItIsSafe() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));
        List<Object> key = List.of("C1");
        assertThat(store.load(key)).isNotNull();

        feed(ROOT_ROWS, Envelope.delete(5, "customer", row("customer_id", "C1", "name", "Ada"), null)
                .withOrder(at(5)));

        assertThat(store.load(key))
                .describedAs("the entry is what carries the tombstone a replayed insert has to lose to")
                .isNotNull();
    }

    /**
     * A change held for an ancestor that has not arrived has been taken off the stream and put where no
     * sink can see it, so what this vertex reports as its bound has to stay below it. The position is
     * read off the arriving change and can only be kept if it is handed to the state that holds it -
     * drop it there and the bound silently rises past a change that a restart neither replays nor finds.
     */
    @Test
    void aChildWaitingForItsPolicyKeepsThePositionTheFrontierMustStayBelow() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));
        List<Object> key = List.of("C1");

        feed(FROM_POLICIES, claimElement(5, "C1", "P1", "CL1", "t5"));

        assertThat(store.load(key).lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(5), "t5")));

        feed(FROM_POLICIES, policyElement(6, "C1", "P1"));

        assertThat(store.load(key).lowestHeldByChain())
                .describedAs("the parent arrived, the child is in the document, nothing is held back")
                .isEmpty();
    }

    @Test
    void anElementOfANeverSeenRootStillProducesNoDeletion() {
        assertThat(feed(FROM_POLICIES, policyElement(2, "C9", "P9")))
                .describedAs("nothing was ever emitted for it, so there is nothing to tell a sink to remove")
                .isEmpty();
    }

    /**
     * The easier half of the frontier's debt to miss. This element is not waiting for anything - it is
     * attached exactly where it belongs, and looks for all the world like it has been dealt with - but
     * nothing went out, because a document with no root is a ghost downstream. Let the bound past it and a
     * restart neither replays the change nor finds it in a state that did not survive.
     */
    @Test
    void anElementOfARootThatHasNotArrivedGoesOnHoldingTheFrontierBack() {
        feed(FROM_POLICIES, policyElement(2, "C9", "P9"));

        assertThat(store.load(List.of("C9")).lowestHeldByChain())
                .containsExactly(Map.entry("policy", new ChainPosition(at(2), null)));
    }

    @Test
    void aDeletionGoingOutForARootReleasesNothingThatArrivedBeneathIt() {
        feed(FROM_POLICIES, policyElement(2, "C9", "P9"));

        List<Envelope> out = feed(ROOT_ROWS,
                Envelope.delete(3, "customer", row("customer_id", "C9"), null).withOrder(at(3)));

        assertThat(out).singleElement()
                .satisfies(sent -> assertThat(sent.op()).isEqualTo(Op.DELETE));
        assertThat(store.load(List.of("C9")).lowestHeldByChain())
                .describedAs("what goes out for a deleted root is its key, which carries no element with it")
                .containsExactly(Map.entry("policy", new ChainPosition(at(2), null)));
    }

    @Test
    void aDocumentGoesOutSayingHowFarEachChainItDrewOnHasGot() {
        // A document is the only thing that ever leaves here, so it is the only place a chain that ran
        // through a nest can be reported at all. Saying nothing leaves every such chain unackable.
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        List<Envelope> out = feed(FROM_POLICIES,
                policyElement(4, "C1", "P1"), claimElement(9, "C1", "P1", "CL1", "t9"));

        assertThat(out).singleElement().satisfies(document -> assertThat(document.positions())
                .describedAs("the highest of each chain, because the document carries every one of them")
                .containsOnly(
                        Map.entry("policy", new ChainPosition(at(4), null)),
                        Map.entry("claim", new ChainPosition(at(9), "t9"))));
    }

    @Test
    void aDocumentSaysNothingOfWhatAnEarlierOneAlreadyCarriedOut() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        List<Envelope> out = feed(ROOT_ROWS, customer(2, "C1", "Ada Lovelace"));

        assertThat(out).singleElement().satisfies(document -> assertThat(document.positions())
                .describedAs("covering a position twice would have a sink ack it twice over")
                .containsExactly(Map.entry("customer", new ChainPosition(at(2), null))));
    }

    @Test
    void aDocumentCoveringOnlyAnElementNobodyCanSeeYetSaysNothingAtAll() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));

        // The claim hangs under a policy that has not arrived, so it is taken off the stream and put where
        // no sink can see it. The document that goes out is the customer as it already stood.
        List<Envelope> out = feed(FROM_POLICIES, claimElement(9, "C1", "P404", "CL1", "t9"));

        assertThat(out).singleElement()
                .satisfies(document -> assertThat(document.positions()).isEmpty());
        assertThat(store.load(List.of("C1")).lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(9), "t9")));
    }

    @Test
    void aDeletedRootsKeyRowSaysHowFarItsOwnChainGotAndNothingMore() {
        feed(ROOT_ROWS, customer(1, "C1", "Ada"));
        feed(FROM_POLICIES, claimElement(9, "C1", "P404", "CL1", "t9"));

        List<Envelope> out = feed(ROOT_ROWS,
                Envelope.delete(12, "customer", row("customer_id", "C1"), null).withOrder(at(12)));

        assertThat(out).singleElement().satisfies(sent -> {
            assertThat(sent.op()).isEqualTo(Op.DELETE);
            assertThat(sent.positions())
                    .describedAs("a key row carries no element, and a root deleted for good would "
                            + "otherwise pin its own chain for as long as the job runs")
                    .containsExactly(Map.entry("customer", new ChainPosition(at(12), null)));
        });
        assertThat(store.load(List.of("C1")).lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(9), "t9")));
    }

    @Test
    void isNotCooperativeBecauseItsStoreMayBlock() {
        assertThat(processor.isCooperative()).isFalse();
    }

    @Test
    void refusesToBeBuiltOnAResolverVertex() {
        NestVertex resolver = TOPOLOGY.vertexAt(List.of("policies"));

        assertThatThrownBy(() -> new AssemblerProcessor(resolver, TOPOLOGY.slots(), store, "doc"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
