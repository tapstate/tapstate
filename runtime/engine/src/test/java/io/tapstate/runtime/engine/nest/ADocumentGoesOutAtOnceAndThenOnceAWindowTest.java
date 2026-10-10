package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * How often a document goes out. Assembling means the unit that goes out is the whole document, so a root
 * fed by several tables is rewritten in full for every change to any of them - and a root that is hot pays
 * that per change. The window is what bounds it: the first change goes out at once, and while that window
 * is open the ones behind it are folded into what goes out when it runs out.
 *
 * <p>The two halves are asserted apart because they fail apart. A window that never lets go turns a live
 * document into a stale one; a window that lets go every time is a knob that reads as configured and does
 * nothing. What discriminates is that the same feed produces one document with a window and two without.
 *
 * <p><b>Nothing here is event time.</b> The window is wall-clock: what it is rationing is writes to a sink
 * and to a store, which happen in wall-clock time whatever the events say. A replay of a day of changes
 * through a 50ms window is therefore folded by exactly as much as its arrival rate deserves, rather than
 * by what the timestamps in it happen to be.
 */
class ADocumentGoesOutAtOnceAndThenOnceAWindowTest {

    @Test
    void expiredWindowsStopReadingDocumentsWhenTheOutboxRefuses() throws Exception {
        int documents = DrainFolding.MAX_KEYS_HELD * 2 + 3;
        CountingStore counted = new CountingStore();
        TestOutbox limited = new TestOutbox(1);
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), counted,
                "doc", null, null, io.tapstate.runtime.engine.ReplayFloor.NONE, NestSettings.defaults(),
                clock, NestSendPolicy.within(WINDOW));
        processor.init(limited, new TestProcessorContext());
        for (int index = 0; index < documents; index++) {
            TestInbox first = new TestInbox();
            first.add(customer(index + 1L, "C" + index, "before"));
            processor.process(ROOT_ROWS, first);
            limited.drainQueueAndReset(0, new ArrayList<>(), false);
            TestInbox change = new TestInbox();
            change.add(customer(documents + index + 1L, "C" + index, "after"));
            processor.process(ROOT_ROWS, change);
        }
        clock.advance(WINDOW + 1);
        counted.loads = 0;
        limited.block();

        processor.tryProcess();

        assertThat(counted.loads).as("only one expired document may be staged before backpressure is observed")
                .isLessThanOrEqualTo(1);
        limited.unblock();
        limited.reset();
        List<Envelope> emitted = new ArrayList<>();
        for (int turns = 0; turns <= documents && emitted.size() < documents; turns++) {
            processor.tryProcess();
            limited.drainQueueAndReset(0, emitted, false);
        }
        assertThat(emitted).hasSize(documents).allSatisfy(document ->
                assertThat(document.after()).containsEntry("name", "after"));
        assertThat(emitted).extracting(document -> document.after().get("customer_id"))
                .containsExactlyElementsOf(java.util.stream.IntStream.range(0, documents)
                        .mapToObj(index -> "C" + index).toList());
    }

    private static final class CountingStore implements NestStore<RootAssembly> {
        private final HeapNestStore<RootAssembly> delegate = new HeapNestStore<>();
        private int loads;
        @Override public RootAssembly load(Object key) { loads++; return delegate.load(key); }
        @Override public void save(Object key, RootAssembly state) { delegate.save(key, state); }
        @Override public void remove(Object key) { delegate.remove(key); }
        @Override public long count() { return delegate.count(); }
    }

    private static final long WINDOW = 50L;

    /** Policies have claims beneath them, so the policies edge is a cascade rather than a leaf's own rows. */
    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    /** The same tree with an append root: every version of the document is a record of its own. */
    private static final TransformBody.Nest APPEND_TREE = nest(new NestRoot("customer",
            List.of("customer_id"), "append", null,
            List.of(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims",
                            List.of("claim_id"))))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestTopology APPEND = NestTopology.compile("p", "doc", APPEND_TREE, tables());

    private static final int ROOT_ROWS = 0;
    private static final int FROM_POLICIES = 1;

    private final Ticking clock = new Ticking();
    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();
    private final TestOutbox outbox = new TestOutbox(128);

    /** A clock a test moves itself, so what is asserted is the window rather than how long a test took. */
    private static final class Ticking implements NestClock {

        private static final long serialVersionUID = 1L;

        private long now = 1_000L;

        @Override
        public long millis() {
            return now;
        }

        void advance(long millis) {
            now += millis;
        }
    }

    private AssemblerProcessor throttled(long window) throws Exception {
        return started(TOPOLOGY, NestSendPolicy.within(window));
    }

    private AssemblerProcessor started(NestTopology topology, NestSendPolicy sending) throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(topology.assembler(), topology.slots(), store,
                "doc", null, null, io.tapstate.runtime.engine.ReplayFloor.NONE, NestSettings.defaults(),
                clock, sending);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Envelope customer(long seq, String id, String name) {
        return Envelope.insert(seq, "customer", row("customer_id", id, "name", name), null).withOrder(at(seq));
    }

    private static Envelope customerGone(long seq, String id) {
        return Envelope.delete(seq, "customer", row("customer_id", id), null).withOrder(at(seq));
    }

    private static KeyedElement policyElement(long seq, String customerId, String policyId) {
        ElementRef ref = new ElementRef(List.of("policies"), null, List.of("PN-" + policyId), List.of(policyId));
        return new KeyedElement(List.of(customerId),
                new NestElement(ref, row("policy_id", policyId, "policy_no", "PN-" + policyId), at(seq),
                        Map.of("policy", new ChainPosition(at(seq), null))), seq);
    }

    private List<Envelope> feed(AssemblerProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        return drained();
    }

    private List<Envelope> idle(AssemblerProcessor processor) {
        processor.tryProcess();
        return drained();
    }

    private List<Envelope> drained() {
        List<Object> out = new ArrayList<>();
        outbox.drainQueueAndReset(0, out, false);
        return out.stream().map(Envelope.class::cast).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> policiesOf(Envelope document) {
        return (List<Map<String, Object>>) document.after().get("policies");
    }

    @Test
    void theFirstChangeToADocumentGoesOutAtOnce() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);

        assertThat(feed(processor, ROOT_ROWS, customer(1, "C1", "Ada")))
                .describedAs("a document nobody has been writing to pays no latency for the window: it is "
                        + "the leading edge that opens one, not something that waits for one")
                .hasSize(1);
    }

    @Test
    void aChangeInsideTheWindowIsHeldBackRatherThanSent() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));

        clock.advance(WINDOW - 1);

        assertThat(feed(processor, FROM_POLICIES, policyElement(2, "C1", "P1")))
                .describedAs("inside the window the change is folded into what goes out at the end of it, "
                        + "which is the whole point: the resend unit is the document, not the change")
                .isEmpty();
    }

    @Test
    void whatTheWindowHeldBackGoesOutWhenItRunsOut() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));
        clock.advance(WINDOW - 1);
        feed(processor, FROM_POLICIES, policyElement(2, "C1", "P1"));

        assertThat(idle(processor))
                .describedAs("the window has not run out yet, so nothing is due")
                .isEmpty();

        clock.advance(2);

        assertThat(idle(processor))
                .describedAs("nothing else will arrive to carry it: a change folded into a window that "
                        + "nobody flushes is a document left stale on the sink with every count green")
                .hasSize(1);
    }

    @Test
    void theDocumentThatGoesOutAtTheEndOfAWindowCarriesEveryChangeItHeld() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));
        clock.advance(1);
        feed(processor, FROM_POLICIES, policyElement(2, "C1", "P1"));
        clock.advance(1);
        feed(processor, FROM_POLICIES, policyElement(3, "C1", "P2"));

        clock.advance(WINDOW);
        List<Envelope> out = idle(processor);

        assertThat(out).hasSize(1);
        assertThat(policiesOf(out.get(0)))
                .describedAs("folding is safe only because what goes out is the state and not the change: "
                        + "one document at the end of the window has to hold both policies, or the window "
                        + "has skipped a version rather than merged it")
                .hasSize(2);
    }

    @Test
    void aWindowThatRunsOutWithNothingHeldBackSendsNothing() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));

        clock.advance(WINDOW * 10);

        assertThat(idle(processor))
                .describedAs("a window closing is not itself a change; re-sending the last document each "
                        + "time one runs out would turn an idle nest into a steady write load")
                .isEmpty();
    }

    @Test
    void aChangeAfterTheWindowHasRunOutGoesOutAtOnceAgain() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));
        clock.advance(WINDOW * 10);
        idle(processor);

        assertThat(feed(processor, FROM_POLICIES, policyElement(2, "C1", "P1")))
                .describedAs("a root that changes once an hour must pay nothing for the window at all - "
                        + "leading edge again, not a wait for the next boundary")
                .hasSize(1);
    }

    @Test
    void aDeletedRootGoesOutAtOnceEvenInsideAWindow() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));
        clock.advance(1);

        List<Envelope> out = feed(processor, ROOT_ROWS, customerGone(2, "C1"));

        assertThat(out)
                .describedAs("holding a deletion back is not the same as holding a version back: the key it "
                        + "names can be gone before the window ends, and what was folded into it then names "
                        + "nothing")
                .hasSize(1);
        assertThat(out.get(0).op()).isEqualTo(Op.DELETE);
    }

    @Test
    void theWindowIsOverOnceADeletionHasGoneOut() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));
        clock.advance(1);
        feed(processor, ROOT_ROWS, customerGone(2, "C1"));
        clock.advance(1);

        assertThat(feed(processor, ROOT_ROWS, customer(3, "C1", "Ada")))
                .describedAs("the deletion ended the window rather than being sent inside it, so the row "
                        + "that brings the document back is a leading edge of its own")
                .hasSize(1);
    }

    @Test
    void everyChangeToAnAppendRootGoesOutOnItsOwn() throws Exception {
        AssemblerProcessor processor = started(APPEND, NestSendPolicy.everyChange());

        List<Envelope> out = feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"), customer(2, "C1", "Adah"));

        assertThat(out)
                .describedAs("under append every send is a new record, so folding two versions into one is "
                        + "not a saved write but a lost row - and one drain carrying both is exactly where "
                        + "that happens without a window being involved at all")
                .hasSize(2);
    }

    @Test
    void aBusyVertexStillLetsGoOfWhatTheWindowRanOutOn() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));
        clock.advance(1);
        feed(processor, FROM_POLICIES, policyElement(2, "C1", "P1"));
        clock.advance(WINDOW);

        // Another root arriving, and no idle turn anywhere: a vertex fed steadily is never asked to make
        // progress with an empty inbox, which is exactly when the sweep would be the only thing running.
        List<Envelope> out = feed(processor, ROOT_ROWS, customer(3, "C2", "Bo"));

        assertThat(out)
                .describedAs("C2's own leading edge, and C1's window running out - a nest under load would "
                        + "otherwise hold every folded document until the load stopped")
                .hasSize(2);
    }

    @Test
    void aRunThatEndsSendsWhatItsWindowsWereStillHolding() throws Exception {
        AssemblerProcessor processor = throttled(WINDOW);
        feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"));
        clock.advance(1);
        feed(processor, FROM_POLICIES, policyElement(2, "C1", "P1"));

        // Not one millisecond further on: what ends this is the inputs running out, not the window.
        processor.complete();

        assertThat(drained())
                .describedAs("no drain and no idle turn is coming once the inputs are done, so a version "
                        + "left folded here is one nobody will ever see - and a finite run would end with "
                        + "the sink holding the version before it and nothing reported")
                .hasSize(1);
    }

    @Test
    void changesToAnUpsertRootInOneDrainStillGoOutAsOneDocument() throws Exception {
        AssemblerProcessor processor = throttled(0);

        assertThat(feed(processor, ROOT_ROWS, customer(1, "C1", "Ada"), customer(2, "C1", "Adah")))
                .describedAs("the control for the test above: with no window at all an upsert root still "
                        + "folds a drain into one document, so that test is pinning the append rule rather "
                        + "than the window")
                .hasSize(1);
    }
}
