package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tracking;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The whole of a move that crosses documents: the element arrives in one, and what hung beneath it arrives
 * with it rather than being left in the document it came from.
 *
 * <p>Neither side can do this alone. The document letting the element go is the only place those rows exist
 * - nothing will send them again - and the document gaining it is the only place they now belong, and the
 * two are reached by different keys and may be worked by different members. So the rows are read out on one
 * side, left where both can reach them, and taken in on the other.
 *
 * <p>The pair is deliberately driven through a real resolver rather than hand-built, because which of the
 * two halves is sent first is part of what makes this land, and a test that built them itself would be
 * asserting its own choice rather than the code's.
 */
class AnElementWithASubtreeReachesTheDocumentThatGainsItTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            tracking(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private static final int OWN_ROWS = 0;
    private static final int CLAIMS = 1;
    private static final int FROM_POLICIES = 1;

    private final NestBinding.NestStores stores = HeapNestStores.onHeap();
    private final HeapNestStore<RootAssembly> documents = new HeapNestStore<>();
    private final TestOutbox resolverOut = new TestOutbox(256);
    private final TestOutbox assemblerOut = new TestOutbox(256);

    @Test
    void theClaimsUnderAMovedPolicyArriveWithIt() throws Exception {
        ResolverProcessor policies = policies();
        AssemblerProcessor assembler = assembler();
        root(assembler, "C1");
        root(assembler, "C2");
        upward(assembler, through(policies, OWN_ROWS, policy(2, "P1", "C1")));
        upward(assembler, through(policies, CLAIMS, claim(3, "K1", "P1")));

        upward(assembler, through(policies, OWN_ROWS, policyMoved(4, "P1", "C1", "C2")));

        assertThat(policiesOf("C1"))
                .describedAs("the document it left keeps nothing of it")
                .isEmpty();
        List<Map<String, Object>> gained = policiesOf("C2");
        assertThat(gained).hasSize(1);
        assertThat(claimsUnder(gained.get(0)))
                .describedAs("what hung under the policy travelled with it")
                .hasSize(1);
    }

    /**
     * The discrimination that stops this from passing on an implementation that simply drops the subtree:
     * without the hand-over the element still arrives, so asserting only that the new document has the
     * policy would be green either way. What says the rows were carried is that they are <em>there</em>.
     */
    @Test
    void aPolicyWithNoClaimsNeedsNoHandOverAndStillMoves() throws Exception {
        ResolverProcessor policies = policies();
        AssemblerProcessor assembler = assembler();
        root(assembler, "C1");
        root(assembler, "C2");
        upward(assembler, through(policies, OWN_ROWS, policy(2, "P1", "C1")));

        upward(assembler, through(policies, OWN_ROWS, policyMoved(3, "P1", "C1", "C2")));

        assertThat(policiesOf("C1")).isEmpty();
        assertThat(policiesOf("C2")).hasSize(1);
        assertThat(claimsUnder(policiesOf("C2").get(0))).isEmpty();
    }

    /**
     * The two halves are routed by different keys and may be worked by different members, so the arriving
     * half can perfectly well run before anything has been parked for it - and it is the only one that will
     * ever ask. Looking once would leave those rows sitting there and the document that should have them
     * never asking again.
     *
     * <p>Driven as the two members it really is: one assembler takes the arrival and finds nothing, another
     * takes the departure and parks, and the first is then given a turn with nothing coming in. Both share
     * the stores, because that is what a distributed map is.
     */
    @Test
    void aHandOverParkedAfterTheElementArrivedIsStillCollected() throws Exception {
        ResolverProcessor policies = policies();
        AssemblerProcessor gaining = assembler();
        AssemblerProcessor losing = assembler();
        root(gaining, "C1");
        root(gaining, "C2");
        upward(gaining, through(policies, OWN_ROWS, policy(2, "P1", "C1")));
        upward(gaining, through(policies, CLAIMS, claim(3, "K1", "P1")));

        List<Object> pair = through(policies, OWN_ROWS, policyMoved(4, "P1", "C1", "C2"));
        assertThat(pair).describedAs("a move sends two halves").hasSize(2);
        upward(gaining, List.of(pair.get(1)));

        assertThat(claimsUnder(policiesOf("C2").get(0)))
                .describedAs("nothing had been parked when the element arrived")
                .isEmpty();

        upward(losing, List.of(pair.get(0)));
        gaining.tryProcess();

        assertThat(claimsUnder(policiesOf("C2").get(0)))
                .describedAs("the second look is what makes a hand-over land when the halves are worked "
                        + "in the order neither of them chose")
                .hasSize(1);
    }

    /** Nothing is left behind in the parking area once the document that was owed it has taken it. */
    @Test
    void whatWasHandedOverIsNotKeptOnceItHasLanded() throws Exception {
        ResolverProcessor policies = policies();
        AssemblerProcessor assembler = assembler();
        root(assembler, "C1");
        root(assembler, "C2");
        upward(assembler, through(policies, OWN_ROWS, policy(2, "P1", "C1")));
        upward(assembler, through(policies, CLAIMS, claim(3, "K1", "P1")));

        upward(assembler, through(policies, OWN_ROWS, policyMoved(4, "P1", "C1", "C2")));

        assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                .describedAs("a hand-over that landed is not a hand-over still owed")
                .isZero();
    }

    @Test
    void aDelayedParentRecreationIncludesTheNewerChildInTheRenderedDocument() throws Exception {
        ResolverProcessor policies = policies();
        AssemblerProcessor assembler = assembler();
        root(assembler, "C1");
        upward(assembler, through(policies, OWN_ROWS, policy(io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ, "P1", "C1")));
        upward(assembler, through(policies, CLAIMS, Envelope.insert(0, "claim",
                row("claim_id", "K1", "policy_id", "P1", "value", 0), null)
                .withOrder(at(io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ))));
        upward(assembler, through(policies, OWN_ROWS, Envelope.delete(1, "policy",
                row("policy_id", "P1", "customer_id", "C1", "policy_no", "PN-P1"), null).withOrder(at(1))));
        upward(assembler, through(policies, CLAIMS, Envelope.insert(3, "claim",
                row("claim_id", "K1", "policy_id", "P1", "value", 1), null).withOrder(at(3))));
        upward(assembler, through(policies, OWN_ROWS, policy(2, "P1", "C1")));
        assertThat(policiesOf("C1")).hasSize(1);
        assertThat(claimsUnder(policiesOf("C1").getFirst())).singleElement().satisfies(claim ->
                assertThat(claim).containsEntry("value", 1));
    }

    private ResolverProcessor policies() throws Exception {
        ResolverProcessor processor = new ResolverProcessor(TOPOLOGY.vertexAt(List.of("policies")),
                new HeapNestStore<>(), (from, released) -> { });
        processor.init(resolverOut, new TestProcessorContext());
        return processor;
    }

    private AssemblerProcessor assembler() throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(),
                documents, "doc", null, null, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.within(0), stores.forParking(TOPOLOGY.assembler()));
        processor.init(assemblerOut, new TestProcessorContext());
        return processor;
    }

    private List<Object> through(ResolverProcessor processor, int ordinal, Envelope event) {
        TestInbox inbox = new TestInbox();
        inbox.queue().add(event);
        processor.process(ordinal, inbox);
        List<Object> routed = new ArrayList<>();
        resolverOut.drainQueueAndReset(0, routed, false);
        return routed;
    }

    private void upward(AssemblerProcessor processor, List<Object> routed) {
        if (routed.isEmpty()) {
            return;
        }
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(routed);
        processor.process(FROM_POLICIES, inbox);
        assemblerOut.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    private void root(AssemblerProcessor processor, String customerId) {
        TestInbox inbox = new TestInbox();
        inbox.queue().add(Envelope.insert(1, "customer", row("customer_id", customerId), null).withOrder(at(1)));
        processor.process(OWN_ROWS, inbox);
        assemblerOut.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> policiesOf(String customerId) {
        RootAssembly assembly = documents.load(List.of(customerId));
        assertThat(assembly).describedAs("a document exists for %s", customerId).isNotNull();
        Map<String, Object> document = assembly.render(TOPOLOGY.slots()).orElse(new LinkedHashMap<>());
        return (List<Map<String, Object>>) document.getOrDefault("policies", List.of());
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> claimsUnder(Map<String, Object> policy) {
        Object claims = policy.get("claims");
        return claims == null ? List.of() : (List<Map<String, Object>>) claims;
    }

    private static Envelope policy(long seq, String policyId, String customerId) {
        return Envelope.insert(seq, "policy", row("policy_id", policyId, "customer_id", customerId,
                "policy_no", "PN-" + policyId), null).withOrder(at(seq));
    }

    private static Envelope policyMoved(long seq, String policyId, String was, String is) {
        return Envelope.update(seq, "policy",
                row("policy_id", policyId, "customer_id", was, "policy_no", "PN-" + policyId),
                row("policy_id", policyId, "customer_id", is, "policy_no", "PN-" + policyId), null)
                .withOrder(at(seq));
    }

    private static Envelope claim(long seq, String claimId, String policyId) {
        return Envelope.insert(seq, "claim", row("claim_id", claimId, "policy_id", policyId), null)
                .withOrder(at(seq));
    }
}
