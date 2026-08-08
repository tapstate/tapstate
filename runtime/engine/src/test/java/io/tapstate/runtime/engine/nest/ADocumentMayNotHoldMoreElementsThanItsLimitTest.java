package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The third of the three quantities a nest is limited by, and the only one that is per document rather
 * than per namespace: how much one document has absorbed. It is the one true bound on memory of the three
 * - a document is assembled whole, so however wide it has grown is what has to fit at once - where the
 * other two are bounds on how much is kept and how much is stored.
 *
 * <p>What is counted is what the document holds, at every depth: an element hanging three levels down
 * occupies the assembly exactly as one hanging off the root does, and counting only the top would let a
 * deep tree grow without limit under a limit that reads as set.
 *
 * <p>What is not counted is what the document is only remembering: a deleted element leaves a record
 * behind so that a replay cannot resurrect it, and that record is released once the replay window has
 * moved past the deletion. Counting those would make the limit depend on how far behind the replay window
 * is, which is not something whoever set the limit can see or reason about - a document would fail for
 * being churned rather than for being wide.
 */
class ADocumentMayNotHoldMoreElementsThanItsLimitTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private static final NestVertex ASSEMBLER = TOPOLOGY.assembler();

    private static final int ROOT_ROWS = 0;
    private static final int FROM_POLICIES = 1;

    /** Two elements fit in one document; the third is what must stop the job. */
    private static final long LIMIT = 2L;

    private static final NestSettings SETTINGS =
            NestSettings.defaults().withElementLimit(ASSEMBLER.mapName(), LIMIT);

    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();

    @Test
    void aDocumentPastItsLimitFailsTheJobSayingWhatItHoldsAndWhatItMay() throws Exception {
        AssemblerProcessor processor = assembler();
        feed(processor, ROOT_ROWS, customer(1, "c1"));

        assertThatThrownBy(() -> feed(processor, FROM_POLICIES,
                policy(2, "c1", "p1"), policy(3, "c1", "p2"), policy(4, "c1", "p3")))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).args())
                .isEqualTo(Map.of("rootKey", "{customer_id=c1}", "elements", LIMIT + 1, "limit", LIMIT));
    }

    @Test
    void aDocumentFilledExactlyToItsLimitKeepsRunning() throws Exception {
        AssemblerProcessor processor = assembler();
        feed(processor, ROOT_ROWS, customer(1, "c1"));

        assertThatCode(() -> feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2")))
                .describedAs("the limit is what may be held, not the first count that is refused")
                .doesNotThrowAnyException();
    }

    /**
     * The limit is per document, so two documents of the allowed width are two allowed documents. Counting
     * across the nest instead would fail the second one for what the first is holding.
     */
    @Test
    void oneDocumentReachingItsLimitDoesNotSpendAnothersRoom() throws Exception {
        AssemblerProcessor processor = assembler();
        feed(processor, ROOT_ROWS, customer(1, "c1"), customer(2, "c2"));

        assertThatCode(() -> feed(processor, FROM_POLICIES,
                policy(3, "c1", "p1"), policy(4, "c1", "p2"),
                policy(5, "c2", "p3"), policy(6, "c2", "p4")))
                .doesNotThrowAnyException();
    }

    /** An element three levels down occupies the document as much as one hanging off the root. */
    @Test
    void whatHangsBeneathAnElementIsCountedTogetherWithIt() throws Exception {
        AssemblerProcessor processor = assembler();
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2"));

        assertThatThrownBy(() -> feed(processor, FROM_POLICIES, claim(4, "c1", "p1", "k1")))
                .describedAs("a claim under a policy is one more element of that document")
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.root-fanout-limit-exceeded");
    }

    /**
     * The one that says which of two readings this is. A deleted element is still in the assembly, held as
     * a record of the deletion, so a count of what is there counts it and a count of what the document
     * holds does not.
     */
    @Test
    void anElementDeletedFromADocumentStopsCountingTowardsItsWidth() throws Exception {
        AssemblerProcessor processor = assembler();
        feed(processor, ROOT_ROWS, customer(1, "c1"));
        feed(processor, FROM_POLICIES, policy(2, "c1", "p1"), policy(3, "c1", "p2"));
        feed(processor, FROM_POLICIES, policyDeleted(4, "c1", "p1"));

        assertThatCode(() -> feed(processor, FROM_POLICIES, policy(5, "c1", "p3")))
                .describedAs("the deleted policy left room behind it, whatever it is still remembered by")
                .doesNotThrowAnyException();
    }

    private AssemblerProcessor assembler() throws Exception {
        AssemblerProcessor processor =
                new AssemblerProcessor(ASSEMBLER, TOPOLOGY.slots(), store, "doc", SETTINGS);
        processor.init(new TestOutbox(256), new TestProcessorContext());
        return processor;
    }

    private static void feed(AssemblerProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static Envelope customer(long seq, String id) {
        return Envelope.insert(seq, "customer", row("customer_id", id, "name", "n" + id), null)
                .withOrder(at(seq));
    }

    private static KeyedElement policy(long seq, String customerId, String policyId) {
        return new KeyedElement(List.of(customerId), new NestElement(
                new ElementRef(List.of("policies"), null, List.of("PN-" + policyId), List.of(policyId)),
                row("policy_id", policyId, "policy_no", "PN-" + policyId), at(seq),
                Map.of("policy", new ChainPosition(at(seq), null))));
    }

    private static KeyedElement policyDeleted(long seq, String customerId, String policyId) {
        return new KeyedElement(List.of(customerId), new NestElement(
                new ElementRef(List.of("policies"), null, List.of("PN-" + policyId), List.of(policyId)),
                null, at(seq), Map.of("policy", new ChainPosition(at(seq), null))));
    }

    private static KeyedElement claim(long seq, String customerId, String policyId, String claimId) {
        return new KeyedElement(List.of(customerId), new NestElement(
                new ElementRef(List.of("policies", "claims"), List.of(policyId), List.of(claimId), null),
                row("claim_id", claimId, "policy_id", policyId), at(seq),
                Map.of("claim", new ChainPosition(at(seq), null))));
    }
}
