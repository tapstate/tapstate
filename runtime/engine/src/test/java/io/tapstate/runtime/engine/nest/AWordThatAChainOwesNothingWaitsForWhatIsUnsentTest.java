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
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.SettledPositions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * That word of a chain having got past changes with nothing to deliver for them is held here while this
 * level is still holding a lower position on that chain, and goes on once it is not.
 *
 * <p><b>It is the one thing about that word that can lose data.</b> What it says is true where it was said:
 * those rows are durable and no record about them is coming. What it cannot see is a document sitting in a
 * window here holding a <em>lower</em> position on the same chain - and that document is durable nowhere,
 * because what put it in the window changes nothing in the state, only what has to be drawn again. Let past
 * it, the word has a sink ack above a change that a crash then leaves neither delivered nor replayable: the
 * document stays at its previous version for ever, the job running, every count healthy, and no assertion
 * about any document able to see it.
 *
 * <p><b>Nothing else would catch it.</b> Released too early, every document produced is identical, every
 * position eventually written down is identical, and the two implementations differ only in what a crash
 * inside one window costs. So the order the two things leave in is what is asserted, and it is asserted at
 * the only moment they are ever both outstanding.
 *
 * <p>The chain held here is the root's own rather than a pointed-at one, and the mechanism does not know
 * the difference: the guard asks what this level holds lowest on the chain the word names, whichever chain
 * that is. Standing a root chain in for it is what lets the case be a processor and two feeds rather than a
 * job - and the position being held has to come from a document in a window either way, which is what the
 * second feed builds.
 */
class AWordThatAChainOwesNothingWaitsForWhatIsUnsentTest {

    private static final long WINDOW = 50L;

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private static final int ROOT_ROWS = 0;

    /** The chain the root's rows arrive on, which is the one this case has the level hold. */
    private static final String CUSTOMER = "customer";

    private final Ticking clock = new Ticking();
    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();
    private final TestOutbox outbox = new TestOutbox(128);

    @Test
    void theWordWaitsWhileADocumentBelowItIsUnsentAndFollowsItOut() throws Exception {
        AssemblerProcessor processor = started();

        assertThat(feed(processor, ROOT_ROWS, customer(1, "C1", "Ada")))
                .describedAs("the first change goes out at once and opens the window")
                .hasSize(1);

        assertThat(feed(processor, ROOT_ROWS, customer(2, "C1", "Grace")))
                .describedAs("the second is folded into the open window, so the level is now holding "
                        + "position 2 on this chain in a document that has not gone out")
                .isEmpty();

        feed(processor, ROOT_ROWS, settledAt(3));

        assertThat(idle(processor))
                .describedAs("and the word about position 3 may not go past it. Sent now, a sink would "
                        + "ack above the change held in that window - which a crash leaves neither "
                        + "delivered nor replayable, since nothing would draw that document again")
                .isEmpty();

        clock.advance(WINDOW + 1);

        assertThat(idle(processor))
                .describedAs("once the window runs out the document goes out, and the word follows it "
                        + "rather than the other way round: what it says only becomes safe to act on "
                        + "when there is nothing beneath it left to write")
                .satisfiesExactly(
                        first -> assertThat(first).isInstanceOf(Envelope.class),
                        second -> assertThat(second)
                                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories
                                        .type(SettledPositions.class))
                                .extracting(word -> word.positions().get(CUSTOMER))
                                .isEqualTo(new ChainPosition(at(3), null)));
    }

    /**
     * The control: with nothing held, the same word goes straight on. Without it the case above passes on
     * an implementation that never sends the word at all, which is the state this whole change exists to
     * end - and "held for now" and "dropped for ever" produce the same empty outbox.
     */
    @Test
    void theWordGoesStraightOnWhenNothingBelowItIsUnsent() throws Exception {
        AssemblerProcessor processor = started();

        feed(processor, ROOT_ROWS, settledAt(3));

        assertThat(idle(processor))
                .describedAs("no document is waiting on a window, so there is nothing for the word to "
                        + "sit behind")
                .singleElement()
                .isInstanceOf(SettledPositions.class);
    }

    private AssemblerProcessor started() throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), store,
                "doc", null, null, ReplayFloor.NONE, NestSettings.defaults(), clock,
                NestSendPolicy.within(WINDOW));
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    /**
     * Word that the chain the root arrives on got past {@code seq} with nothing to deliver for it. Fed on
     * the root's own ordinal because the ordinal is not what tells this word apart - the item is - and a
     * fixture that had to draw the lookup's edge to say so would be asserting the wiring rather than this.
     */
    private static SettledPositions settledAt(long seq) {
        return new SettledPositions(Map.of(CUSTOMER, new ChainPosition(at(seq), null)));
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Envelope customer(long seq, String id, String name) {
        return Envelope.insert(seq, CUSTOMER, row("customer_id", id, "name", name), null).withOrder(at(seq));
    }

    private List<Object> feed(AssemblerProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        return drained();
    }

    private List<Object> idle(AssemblerProcessor processor) {
        processor.tryProcess();
        return drained();
    }

    private List<Object> drained() {
        List<Object> out = new ArrayList<>();
        outbox.drainQueueAndReset(0, out, false);
        return out;
    }

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
}
