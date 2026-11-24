package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
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
 * The last of the structural key families: the row that changes key is the <b>root</b>, so the document
 * itself changes identity and the whole tree goes with it.
 *
 * <p>It is not a bigger version of an element moving. An element leaves a document that goes on existing;
 * a root leaves nothing behind at all - the key it was filed under names a row the source no longer has,
 * so that document has to be removed downstream, and everything it held belongs to a key that has never
 * been seen. The rows beneath are exactly the ones nothing will ever send again: the source edited one
 * row and considers the rest untouched.
 *
 * <p><b>The two halves are on different partitions by construction.</b> Old key and new key are different
 * values of the vertex's own partition key, so the instance holding the document being emptied and the
 * instance holding the one being filled are, in the normal case, two different instances - which is why
 * the pair is driven here as two processors sharing their stores rather than as one. A single-instance
 * test cannot tell "the tree was carried across" from "the tree happened to already be local".
 */
class ARootRowThatChangesItsKeyTakesItsDocumentWithItTest {

    private static final TransformBody.Nest TREE = nest(new NestRoot("customer", List.of("customer_id"),
            null, true,
            List.of(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    /** The root's own rows, its children, and the twin edge carrying every root row keyed by where it was. */
    private static final int OWN_ROWS = 0;
    private static final int POLICIES = 1;
    private static final int DEPARTURES = 2;

    private final NestBinding.NestStores stores = HeapNestStores.onHeap();
    private final HeapNestStore<RootAssembly> documents = new HeapNestStore<>();
    private final TestOutbox out = new TestOutbox(256);

    /**
     * The edge layout the rest of this rests on. Asserted rather than assumed: the ordinal is the only thing
     * telling a processor which kind of arrival it is looking at, so a compiler that stopped appending the
     * twin last would silently point every case below at the wrong handling.
     */
    @Test
    void aTrackedRootCompilesATwinEdgeCarryingWhereEachRowWas() {
        List<NestInbound> edges = TOPOLOGY.assembler().inbound();

        assertThat(edges).hasSize(3);
        assertThat(edges.get(DEPARTURES).carriesDepartures()).isTrue();
        assertThat(edges.get(DEPARTURES).pathId())
                .describedAs("it is the root's own stream, not an embed's")
                .isEmpty();
        assertThat(edges.get(OWN_ROWS).carriesDepartures()).isFalse();
    }

    @Test
    void theKeyItLeftIsRemovedDownstream() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");
        policy(losing, "P1", "C1");

        List<Object> emitted = renamed(losing, gaining, "C1", "C2");

        assertThat(emitted)
                .describedAs("the old document is gone from the source, so the sink is told to remove it")
                .anyMatch(item -> item instanceof Envelope event
                        && event.op() == Op.DELETE
                        && "C1".equals(keyOf(event)));
    }

    /**
     * The discrimination against today's shape: without the twin edge being handled, the root row is simply
     * applied a second time under its new key and the old document is never touched - so a test asserting
     * only that C2 exists would be green on an implementation that leaves C1 sitting there whole.
     */
    @Test
    void theKeyItLeftHoldsNothingAfterwards() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");
        policy(losing, "P1", "C1");
        assertThat(policiesOf("C1")).describedAs("it really was there first").hasSize(1);

        renamed(losing, gaining, "C1", "C2");

        assertThat(documents.load(List.of("C1")))
                .satisfiesAnyOf(
                        assembly -> assertThat(assembly).isNull(),
                        assembly -> assertThat(assembly.render(TOPOLOGY.slots()))
                                .describedAs("nothing renders under the key the source no longer has")
                                .isEmpty());
    }

    @Test
    void theNewKeyGainsTheWholeTree() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");
        policy(losing, "P1", "C1");

        renamed(losing, gaining, "C1", "C2");

        assertThat(policiesOf("C2"))
                .describedAs("the rows beneath the root travelled; nothing will ever send them again")
                .hasSize(1);
    }

    /**
     * The arriving half may perfectly well run before anything has been parked for it - the two are routed
     * by different keys and worked by different members, so neither chooses. Looking once would leave the
     * tree sitting in the parking area and the document that should have it never asking again.
     */
    @Test
    void theTreeStillLandsWhenTheNewKeyIsFilledFirst() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");
        policy(losing, "P1", "C1");

        Envelope moved = customerRenamed(9, "C1", "C2");
        feed(gaining, OWN_ROWS, moved);
        assertThat(policiesOf("C2"))
                .describedAs("nothing had been parked when the root row arrived")
                .isEmpty();

        feed(losing, DEPARTURES, moved);
        gaining.tryProcess();

        assertThat(policiesOf("C2"))
                .describedAs("the second look is what makes it land when the halves are worked in the "
                        + "order neither of them chose")
                .hasSize(1);
    }

    /**
     * Every root row travels the twin edge, not only the ones that moved - an edge cannot filter. A tracked
     * root whose key did not change must therefore come out exactly as an untracked one would: one document,
     * no deletion, nothing parked. Without this, turning the switch on would delete a document on every
     * ordinary update.
     */
    @Test
    void anOrdinaryUpdateOfATrackedRootIsNotAMove() throws Exception {
        AssemblerProcessor assembler = assembler();
        customer(assembler, "C1");
        policy(assembler, "P1", "C1");

        Envelope edited = Envelope.update(7, "customer",
                row("customer_id", "C1", "name", "before"),
                row("customer_id", "C1", "name", "after"), null).withOrder(at(7));
        List<Object> emitted = new ArrayList<>();
        emitted.addAll(feed(assembler, OWN_ROWS, edited));
        emitted.addAll(feed(assembler, DEPARTURES, edited));

        assertThat(emitted).noneMatch(item -> item instanceof Envelope event && event.op() == Op.DELETE);
        assertThat(policiesOf("C1"))
                .describedAs("the document is untouched, elements and all")
                .hasSize(1);
        assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                .describedAs("nothing was handed anywhere")
                .isZero();
    }

    /** Nothing is left in the parking area once the key that was owed the tree has taken it. */
    @Test
    void whatWasHandedOverIsNotKeptOnceItHasLanded() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");
        policy(losing, "P1", "C1");

        renamed(losing, gaining, "C1", "C2");

        assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                .describedAs("a hand-over that landed is not a hand-over still owed")
                .isZero();
    }

    /**
     * A root whose key changes while it holds nothing still changes key. The old document goes and the new
     * one is built from the row itself - there is simply nothing to carry between them.
     */
    @Test
    void aRootHoldingNothingStillMoves() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");

        List<Object> emitted = renamed(losing, gaining, "C1", "C2");

        assertThat(emitted).anyMatch(item -> item instanceof Envelope event
                && event.op() == Op.DELETE && "C1".equals(keyOf(event)));
        assertThat(documents.load(List.of("C2")))
                .describedAs("the new key exists even with nothing beneath it")
                .isNotNull();
    }

    /**
     * What the source has cascaded follows the rename by itself. The child row carries the new value, so it
     * is routed to the new key like any other row and needs nothing from the move at all - which is what
     * makes the case beneath it mean something rather than being the only behaviour there is.
     */
    @Test
    void aChildThatFollowedTheRenameLandsUnderTheNewKey() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");
        renamed(losing, gaining, "C1", "C2");

        policy(gaining, "P2", "C2");

        assertThat(policiesOf("C2")).hasSize(1);
    }

    /**
     * The source that did not cascade. The child row goes on naming a key the source no longer has, so it
     * arrives at that key, finds no root there, and is shown to nobody - which is the faithful reading of a
     * row the source itself has left orphaned, not a failure of the move.
     *
     * <p>Written down as a case because it is a promise about what does <em>not</em> happen: nothing guesses
     * that these rows meant to follow, and nothing revives the key they name.
     */
    @Test
    void aChildStillPointingAtTheKeyThatWentIsShownToNobody() throws Exception {
        AssemblerProcessor losing = assembler();
        AssemblerProcessor gaining = assembler();
        customer(losing, "C1");
        renamed(losing, gaining, "C1", "C2");

        List<Object> emitted = new ArrayList<>();
        TestInbox inbox = new TestInbox();
        inbox.queue().add(Envelope.insert(3, "policy",
                row("policy_id", "P3", "customer_id", "C1", "policy_no", "PN-P3"), null).withOrder(at(3)));
        losing.process(POLICIES, inbox);
        out.drainQueueAndReset(0, emitted, false);

        assertThat(policiesOf("C2"))
                .describedAs("nothing guesses that a row still naming the old key meant to follow")
                .isEmpty();
        assertThat(emitted)
                .describedAs("and the key the source no longer has is not revived to carry it")
                .noneMatch(item -> item instanceof Envelope event && event.op() != Op.DELETE
                        && "C1".equals(keyOf(event)));
    }

    @Test
    void aReplayedDepartureCannotTakeTheSubtreeOfTheRootThatReusedTheKey() throws Exception {
        AssemblerProcessor leaving = assembler();
        AssemblerProcessor arriving = assembler();
        customer(leaving, "C1");
        policy(leaving, "P1", "C1");
        renamed(leaving, arriving, "C1", "C2");
        assertThat(policiesOf("C2")).extracting(row -> row.get("policy_no")).containsExactly("PN-P1");

        feed(leaving, OWN_ROWS, Envelope.insert(10, "customer",
                row("customer_id", "C1", "name", "replacement"), null).withOrder(at(10)));
        feed(leaving, POLICIES, Envelope.insert(11, "policy",
                row("policy_id", "P2", "customer_id", "C1", "policy_no", "PN-P2"), null).withOrder(at(11)));
        assertThat(policiesOf("C1")).extracting(row -> row.get("policy_no")).containsExactly("PN-P2");

        AssemblerProcessor reopened = assembler();
        Envelope moved = customerRenamed(9, "C1", "C2");
        List<Object> replay = feed(reopened, DEPARTURES, moved);
        List<Object> arrival = feed(reopened, OWN_ROWS, moved, moved);

        assertAll(
                () -> assertThat(arrival).as("the collected document is available immediately on replay")
                        .anyMatch(item -> item instanceof Envelope event && "C2".equals(keyOf(event))
                                && event.positions().equals(moved.positions())),

                () -> assertThat(policiesOf("C1")).extracting(row -> row.get("policy_no"))
                        .as("the old departure cannot detach children from the replacement root")
                        .containsExactly("PN-P2"),
                () -> assertThat(policiesOf("C2")).extracting(row -> row.get("policy_no")).containsExactly("PN-P1"),
                () -> assertThat(replay).noneMatch(item -> item instanceof Envelope event
                        && event.op() == Op.DELETE && "C1".equals(keyOf(event))),
                () -> assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                        .as("the old departure has no tree left to park")
                        .isZero());
    }

    @Test
    void anArrivalReplayedBeforeTheDepartureStillWaitsForItsTree() throws Exception {
        var leaving = assembler();
        customer(leaving, "C1");
        policy(leaving, "P1", "C1");
        var moved = customerRenamed(9, "C1", "C2");
        assertThat(feed(assembler(), OWN_ROWS, moved)).isEmpty();

        var reopened = assembler();
        assertThat(feed(reopened, OWN_ROWS, moved)).isEmpty();
        feed(leaving, DEPARTURES, moved);
        reopened.tryProcess();
        assertThat(policiesOf("C2")).extracting(row -> row.get("policy_no")).containsExactly("PN-P1");
    }

    @Test
    void repeatingTheDepartureLeavesLaterChildrenAtTheVacantKey() throws Exception {
        var leaving = assembler();
        customer(leaving, "C1");
        policy(leaving, "P1", "C1");
        renamed(leaving, assembler(), "C1", "C2");

        feed(leaving, POLICIES, Envelope.insert(10, "policy",
                row("policy_id", "P2", "customer_id", "C1", "policy_no", "PN-P2"), null).withOrder(at(10)));
        var reopened = assembler();
        feed(reopened, DEPARTURES, customerRenamed(9, "C1", "C2"));
        feed(reopened, OWN_ROWS, Envelope.insert(11, "customer",
                row("customer_id", "C1", "name", "replacement"), null).withOrder(at(11)));

        assertThat(policiesOf("C1")).extracting(row -> row.get("policy_no")).containsExactly("PN-P2");
        assertThat(policiesOf("C2")).extracting(row -> row.get("policy_no")).containsExactly("PN-P1");
        assertThat(stores.forParking(TOPOLOGY.assembler()).count()).isZero();
    }

    /** Drives both halves of one rename, departure first, and answers with everything that went downstream. */
    private List<Object> renamed(AssemblerProcessor losing, AssemblerProcessor gaining, String was, String is)
            throws Exception {
        Envelope moved = customerRenamed(9, was, is);
        List<Object> emitted = new ArrayList<>(feed(losing, DEPARTURES, moved));
        emitted.addAll(feed(gaining, OWN_ROWS, moved));
        return emitted;
    }

    private AssemblerProcessor assembler() throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(),
                documents, "doc", null, null, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.within(0), stores.forParking(TOPOLOGY.assembler()));
        processor.init(out, new TestProcessorContext());
        return processor;
    }

    private List<Object> feed(AssemblerProcessor processor, int ordinal, Envelope... events) {
        TestInbox inbox = new TestInbox(List.of(events));
        processor.process(ordinal, inbox);
        List<Object> emitted = new ArrayList<>();
        out.drainQueueAndReset(0, emitted, false);
        return emitted;
    }

    private void customer(AssemblerProcessor processor, String customerId) {
        feed(processor, OWN_ROWS,
                Envelope.insert(1, "customer", row("customer_id", customerId, "name", "n"), null)
                        .withOrder(at(1)));
    }

    private void policy(AssemblerProcessor processor, String policyId, String customerId) {
        feed(processor, POLICIES, Envelope.insert(2, "policy",
                row("policy_id", policyId, "customer_id", customerId, "policy_no", "PN-" + policyId), null)
                .withOrder(at(2)));
    }

    private static Envelope customerRenamed(long seq, String was, String is) {
        return Envelope.update(seq, "customer",
                row("customer_id", was, "name", "n"),
                row("customer_id", is, "name", "n"), null).withOrder(at(seq));
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

    private static Object keyOf(Envelope event) {
        Map<String, Object> row = event.after() != null ? event.after() : event.before();
        return row == null ? null : row.get("customer_id");
    }
}
