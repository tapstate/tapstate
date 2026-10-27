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
 * The second quantity that fails a run rather than being absorbed by the layer behind the memory: how much
 * one key is holding for something that has not arrived.
 *
 * <p>It needs its own limit because none of the others reach it. What is kept in memory is bounded by a
 * count of entries, and everything waiting here waits <em>inside</em> one entry - a level with a single key
 * is within any budget however long that key's queue has grown. The width of a document does not reach it
 * either: what waits for an ancestor is in no document, and what a document absorbed under an absent root
 * is counted there by element while it is held here by change.
 *
 * <p><b>It fails the job rather than letting the waiting go.</b> Reaching a limit says nothing about
 * whether the parent is coming - only that a lot arrived before it did - so releasing on that evidence
 * would drop rows that were going to be part of a document, which is the harm the time limit's three-layer
 * judgement exists to avoid. Failing says the same thing out loud and leaves the rows where they are.
 */
class AKeyMayNotHoldMorePendingChangesThanItsLimitTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestVertex ASSEMBLER = TOPOLOGY.assembler();
    private static final NestVertex POLICIES = TOPOLOGY.vertexAt(List.of("policies"));

    private static final int OWN_ROWS = 0;
    private static final int FROM_BENEATH = 1;

    /** Two changes may wait under one key; the third is what must stop the job. */
    private static final long LIMIT = 2L;

    private static final NestSettings SETTINGS = NestSettings.defaults()
            .withPendingLimit(POLICIES.mapName(), LIMIT)
            .withPendingLimit(ASSEMBLER.mapName(), LIMIT);

    private final HeapNestStore<ResolverState> resolverStore = new HeapNestStore<>();
    private final HeapNestStore<RootAssembly> assemblerStore = new HeapNestStore<>();

    @Test
    void aKeyHoldingMoreThanItsLimitFailsTheJobSayingWhatItHoldsAndWhatItMay() throws Exception {
        ResolverProcessor processor = resolver();

        assertThatThrownBy(() -> feed(processor, FROM_BENEATH,
                claim(1, "k1", "p1"), claim(2, "k2", "p1"), claim(3, "k3", "p1")))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).args())
                .isEqualTo(Map.of("namespace", POLICIES.mapName(), "key", "[p1]",
                        "pending", LIMIT + 1, "limit", LIMIT));
    }

    @Test
    void aKeyHoldingExactlyItsLimitKeepsRunning() throws Exception {
        ResolverProcessor processor = resolver();

        assertThatCode(() -> feed(processor, FROM_BENEATH, claim(1, "k1", "p1"), claim(2, "k2", "p1")))
                .describedAs("the limit is what may be held, not the first count that is refused")
                .doesNotThrowAnyException();
    }

    @Test
    void aDeletedKeyKeepsTheSameCapacityAndRetainsItsChildrenOnOverflow() throws Exception {
        ResolverProcessor processor=resolver();
        feed(processor,OWN_ROWS,policy(1,"p1","c1"));
        feed(processor,OWN_ROWS,Envelope.delete(2,"policy",row("policy_id","p1","customer_id","c1","policy_no","PN-p1"),null).withOrder(at(2)));
        assertThatThrownBy(() -> feed(processor,FROM_BENEATH,claim(3,"k1","p1"),claim(4,"k2","p1"),claim(5,"k3","p1")))
                .isInstanceOfSatisfying(TapstateException.class,error -> {
                    org.assertj.core.api.Assertions.assertThat(error.code()).isEqualTo(NestError.PENDING_LIMIT_EXCEEDED);
                    org.assertj.core.api.Assertions.assertThat(error.args()).containsEntry("pending",3L).containsEntry("limit",LIMIT);
                });
        org.assertj.core.api.Assertions.assertThat(resolverStore.load(List.of("p1")).pending()).isEqualTo(3);
    }

    /** Per key, so two keys holding what each is allowed are two allowed keys. */
    @Test
    void oneKeyHoldingAllItMayDoesNotSpendAnothersRoom() throws Exception {
        ResolverProcessor processor = resolver();

        assertThatCode(() -> feed(processor, FROM_BENEATH,
                claim(1, "k1", "p1"), claim(2, "k2", "p1"),
                claim(3, "k3", "p2"), claim(4, "k4", "p2")))
                .doesNotThrowAnyException();
    }

    /**
     * What the limit is about is what is still stuck, not how much has gone through: the parent arriving
     * sends every child that was waiting on its way, and the room they took comes back with them.
     */
    @Test
    void whatAParentReleasedIsNoLongerHeldAgainstTheLimit() throws Exception {
        ResolverProcessor processor = resolver();
        feed(processor, FROM_BENEATH, claim(1, "k1", "p1"), claim(2, "k2", "p1"));

        feed(processor, OWN_ROWS, policy(3, "p1", "c1"));

        assertThatCode(() -> feed(processor, FROM_BENEATH, claim(4, "k4", "p1"), claim(5, "k5", "p1")))
                .describedAs("four changes have passed through a key that may hold two")
                .doesNotThrowAnyException();
    }

    @Test
    void aDocumentHoldingMoreThanItsLimitForARootThatHasNotArrivedFailsTheJob() throws Exception {
        AssemblerProcessor processor = assembler();

        assertThatThrownBy(() -> feed(processor, FROM_BENEATH,
                policyOf("c1", "p1", 1), policyOf("c1", "p2", 2), policyOf("c1", "p3", 3)))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.pending-limit-exceeded");
    }

    /**
     * The other bucket, and the one a count of what the document absorbed would miss: a row whose ancestor
     * has not arrived is parked outside the tree rather than attached to it.
     */
    @Test
    void aDocumentHoldingMoreThanItsLimitForAnAncestorThatHasNotArrivedFailsTheJob() throws Exception {
        AssemblerProcessor processor = assembler();
        feed(processor, OWN_ROWS, customer(1, "c1"));

        assertThatThrownBy(() -> feed(processor, FROM_BENEATH,
                claimOf("c1", "p1", "k1", 2), claimOf("c1", "p1", "k2", 3), claimOf("c1", "p1", "k3", 4)))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.pending-limit-exceeded");
    }

    @Test
    void whatWentOutWithADocumentIsNoLongerHeldAgainstTheLimit() throws Exception {
        AssemblerProcessor processor = assembler();
        feed(processor, OWN_ROWS, customer(1, "c1"));

        assertThatCode(() -> feed(processor, FROM_BENEATH,
                policyOf("c1", "p1", 2), policyOf("c1", "p2", 3), policyOf("c1", "p3", 4)))
                .describedAs("the root is here, so every change went out in a document and is held no longer")
                .doesNotThrowAnyException();
    }

    /** Nothing is let go of here, so nothing reaches it: what this is about is the count, not the release. */
    private static final NestDeadLetter UNUSED = (from, released) -> {
        throw new AssertionError("a limit on how much may wait does not release what is waiting");
    };

    private ResolverProcessor resolver() throws Exception {
        ResolverProcessor processor =
                new ResolverProcessor(POLICIES, resolverStore, UNUSED, SETTINGS);
        processor.init(new TestOutbox(256), new TestProcessorContext());
        return processor;
    }

    private AssemblerProcessor assembler() throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(
                ASSEMBLER, TOPOLOGY.slots(), assemblerStore, "doc", SETTINGS);
        processor.init(new TestOutbox(256), new TestProcessorContext());
        return processor;
    }

    private static void feed(ResolverProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static void feed(AssemblerProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static Envelope policy(long seq, String policyId, String customerId) {
        return Envelope.insert(seq, "policy", row("policy_id", policyId, "customer_id", customerId,
                "policy_no", "PN-" + policyId), null).withOrder(at(seq));
    }

    private static Envelope claim(long seq, String claimId, String policyId) {
        return Envelope.insert(seq, "claim", row("claim_id", claimId, "policy_id", policyId), null)
                .withOrder(at(seq));
    }

    private static Envelope customer(long seq, String id) {
        return Envelope.insert(seq, "customer", row("customer_id", id, "name", "n" + id), null)
                .withOrder(at(seq));
    }

    private static KeyedElement policyOf(String customerId, String policyId, long seq) {
        return new KeyedElement(List.of(customerId), new NestElement(
                new ElementRef(List.of("policies"), null, List.of("PN-" + policyId), List.of(policyId)),
                row("policy_id", policyId, "policy_no", "PN-" + policyId), at(seq),
                Map.of("policy", new ChainPosition(at(seq), null))), seq);
    }

    private static KeyedElement claimOf(String customerId, String policyId, String claimId, long seq) {
        return new KeyedElement(List.of(customerId), new NestElement(
                new ElementRef(List.of("policies", "claims"), List.of(policyId), List.of(claimId), null),
                row("claim_id", claimId, "policy_id", policyId), at(seq),
                Map.of("claim", new ChainPosition(at(seq), null))), seq);
    }
}
