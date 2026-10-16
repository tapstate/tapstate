package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tracking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 * The assembler's half of the same rule the resolver has: a document may not give up a subtree before that
 * subtree is somewhere the document gaining it can reach.
 *
 * <p>A drain stores every document it touched as it ends, whether it ended by running out of items or by
 * throwing. Detach first and the rows are out of the document in memory; if parking them then fails, the
 * document stored is the one without them. They are in no document and in no parking area, and the replay
 * that would move them again resumes above the change that moved them - so nothing looks for them ever
 * again, with the pipeline running and no count out of place.
 */
class AFailedHandoffLeavesTheDocumentWholeTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            tracking(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private static final int OWN_ROWS = 0;
    private static final int CLAIMS = 1;
    private static final int FROM_POLICIES = 1;

    private final HeapNestStore<RootAssembly> documents = new HeapNestStore<>();
    private final TestOutbox resolverOut = new TestOutbox(256);
    private final TestOutbox assemblerOut = new TestOutbox(256);

    @Test
    void aFailedRootHandoverLeavesAllRowsInTheDonor() throws Exception {
        NestTopology tracked = NestTopology.compile("p", "doc", nest(new NestRoot("customer",
                List.of("customer_id"), null, true, List.of(embed("policy", "customer_id", "customer_id",
                        EmbedAs.ARRAY, "policies", List.of("policy_no"))))), tables());
        AssemblerProcessor processor = new AssemblerProcessor(tracked.assembler(), tracked.slots(), documents,
                "doc", null, null, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.within(0), new RefusesToSave());
        processor.init(assemblerOut, new TestProcessorContext());
        RootAssembly donor = new RootAssembly();
        donor.applyRoot(row("customer_id", "C1"), at(1));
        donor.applyElement(new ElementRef(List.of("policies"), null, List.of("PN-1"), null),
                row("policy_id", "P1", "policy_no", "PN-1"), at(2), Map.of());
        documents.save(List.of("C1"), donor);
        TestInbox move = new TestInbox();
        move.add(Envelope.update(3, "customer", row("customer_id", "C1"), row("customer_id", "C2"), null)
                .withOrder(at(3)));

        assertThatThrownBy(() -> processor.process(2, move)).isInstanceOf(IllegalStateException.class);

        assertThat((List<?>) documents.load(List.of("C1")).render(tracked.slots()).orElseThrow().get("policies"))
                .as("parking did not accept the handover, so the donor must retain every row").hasSize(1);
    }

    @Test
    void aFailedParkingWriteLeavesTheSubtreeInTheDocumentItCameFrom() throws Exception {
        ResolverProcessor policies = policies();
        AssemblerProcessor assembler = assembler(new RefusesToSave());
        root(assembler, "C1");
        root(assembler, "C2");
        upward(assembler, through(policies, OWN_ROWS, policy(2, "P1", "C1")));
        upward(assembler, through(policies, CLAIMS, claim(3, "K1", "P1")));

        List<Object> move = through(policies, OWN_ROWS, policyMoved(4, "P1", "C1", "C2"));
        assertThatThrownBy(() -> upward(assembler, move))
                .describedAs("the parking write is what fails; the drain's own persistence still runs")
                .isInstanceOf(IllegalStateException.class);

        assertThat(claimsUnder(policiesOf("C1")))
                .describedAs("nothing was parked, so this document is the only place those rows are. Gone "
                        + "from here they are nowhere, and a replay resumes above the change that moved them")
                .hasSize(1);
    }

    /** The discrimination: with a parking area that accepts the write, the subtree does leave. */
    @Test
    void aParkingWriteThatLandsDoesTakeTheSubtreeOut() throws Exception {
        NestBinding.NestStores stores = HeapNestStores.onHeap();
        ResolverProcessor policies = policies();
        AssemblerProcessor assembler = assembler(stores.forParking(TOPOLOGY.assembler()));
        root(assembler, "C1");
        root(assembler, "C2");
        upward(assembler, through(policies, OWN_ROWS, policy(2, "P1", "C1")));
        upward(assembler, through(policies, CLAIMS, claim(3, "K1", "P1")));

        upward(assembler, through(policies, OWN_ROWS, policyMoved(4, "P1", "C1", "C2")));

        assertThat(policiesOf("C1"))
                .describedAs("the element left, and what hung beneath it left with it")
                .isEmpty();
    }

    // ---- harness ------------------------------------------------------------------------

    private ResolverProcessor policies() throws Exception {
        ResolverProcessor processor = new ResolverProcessor(TOPOLOGY.vertexAt(List.of("policies")),
                new HeapNestStore<>(), (from, released) -> { });
        processor.init(resolverOut, new TestProcessorContext());
        return processor;
    }

    private AssemblerProcessor assembler(NestStore<ParkedSubtree> parking) throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(),
                documents, "doc", null, null, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.within(0), parking);
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
        inbox.queue().add(Envelope.insert(1, "customer", row("customer_id", customerId), null)
                .withOrder(at(1)));
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
    private static List<Map<String, Object>> claimsUnder(List<Map<String, Object>> policies) {
        assertThat(policies).describedAs("the element itself is still here").hasSize(1);
        Object claims = policies.get(0).get("claims");
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

    /** A parking area that reads but refuses every write, as one whose member is unreachable would. */
    private static final class RefusesToSave implements NestStore<ParkedSubtree> {

        private final HeapNestStore<ParkedSubtree> entries = new HeapNestStore<>();

        @Override
        public ParkedSubtree load(Object key) {
            return entries.load(key);
        }

        @Override
        public void save(Object key, ParkedSubtree state) {
            throw new IllegalStateException("parking write refused");
        }

        @Override
        public void remove(Object key) {
            entries.remove(key);
        }

        @Override
        public long count() {
            return entries.count();
        }
    }
}
