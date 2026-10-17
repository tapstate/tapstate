package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A whole document parked while its root row changes key has to keep the durable frontier below the change
 * that started the move, for as long as it sits there.
 *
 * <p>The exclusion already exists for a subtree between two documents, and this is the same bucket - but it
 * is reached by a different path and so is witnessed on its own. Here the rows in flight are <em>the whole
 * document</em>: the key that held them has been deleted and the key that will hold them has not taken them.
 * Let the frontier past the rename and a restart resumes above it, so nothing replays the rename, nothing
 * collects what was parked, and an entire document is gone from both keys with the job RUNNING and every
 * count reading healthy.
 *
 * <p>The second test is what stops the first from being satisfied by holding forever, which would pin the
 * frontier on every rename ever made. The third says the hold is released by the landing rather than by this
 * instance being the one that landed it - which for a root move is not an edge case but the normal shape,
 * since old key and new key are by construction different values of the partition key.
 */
class ARootMoveInFlightKeepsTheFrontierBelowItTest {

    private static final String CUSTOMERS = "customer";
    private static final String POLICIES = "policy";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(CUSTOMERS, POLICIES));

    private static final int ROOT_ROWS = 0;
    private static final int FROM_POLICIES = 1;
    private static final int DEPARTURES = 2;

    /** The twin carries the same stream as the edge it twins, so it promises on the same chain. */
    private static final Map<Integer, List<String>> CHAINS = Map.of(
            ROOT_ROWS, List.of(CUSTOMERS), FROM_POLICIES, List.of(POLICIES), DEPARTURES, List.of(CUSTOMERS));

    private static final TransformBody.Nest TREE = nest(new NestRoot("customer", List.of("customer_id"),
            null, true,
            List.of(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    /** Far above anything that arrives, so a level passing its edge's bound straight on is unmistakable. */
    private static final long FAR_ABOVE = FrontierOrders.pack(CUSTOMERS, new SourceOrder(1, 900));

    /** Where the rename is read as having happened, and the position the hold has to stay below. */
    private static final long MOVED_AT = 200L;

    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();
    private final NestBinding.NestStores stores = HeapNestStores.onHeap();
    private final TestOutbox outbox = new TestOutbox(128);

    @Test
    void theBoundStaysBelowARenameWhoseDocumentIsStillParked() throws Exception {
        AssemblerProcessor losing = assembler();
        givenACustomerHoldingAPolicy("C1", losing);

        feed(losing, DEPARTURES, renamed("C1", "C2"));

        assertThat(boundsAfter(losing, FAR_ABOVE))
                .describedAs("that document is under no key at all while it sits parked; a frontier past "
                        + "the rename means a restart resumes above it, nothing replays it, and the whole "
                        + "document is gone from both keys with the job RUNNING")
                .containsExactly(FrontierOrders.pack(CUSTOMERS, at(MOVED_AT)) - 1);
    }

    /**
     * The other order, and the one the cases around it do not reach: the arriving half is worked first and
     * finds nothing parked, so what it knows - that this key is owed a document - is held in memory and
     * nowhere else. The rows it needs are still in the document the departure has yet to empty, so nothing
     * durable is missing; what is missing is anything to look again. Let the frontier past the change and a
     * restart resumes above it, so the departure is never replayed, the arrival never asks again, and the
     * document that was moved is under neither key with the job RUNNING and no count out of place.
     *
     * <p>Hard to notice because a move usually is worked the other way round, and every case that starts
     * with the departure holds the bound correctly.
     */
    @Test
    void theBoundStaysBelowAMoveWhoseArrivingHalfIsStillOwedTheDocument() throws Exception {
        AssemblerProcessor gaining = assembler();

        feed(gaining, ROOT_ROWS, renamed("C1", "C2"));

        assertThat(boundsAfter(gaining, FAR_ABOVE))
                .describedAs("this instance is owed a document and only it knows so; a bound past the "
                        + "change that made it owed is a promise nothing can keep after a restart")
                .containsExactly(FrontierOrders.pack(CUSTOMERS, at(MOVED_AT)) - 1);
    }

    @Test
    void anOwedCustomerMoveDoesNotHoldAnUnrelatedPolicyChain() throws Exception {
        AssemblerProcessor gaining = assembler();
        feed(gaining, ROOT_ROWS, renamed("C1", "C2"));
        long policyBound = FrontierOrders.pack(POLICIES, at(900));

        gaining.tryProcessWatermark(FROM_POLICIES, new Watermark(policyBound, AXES.axisOf(POLICIES)));

        assertThat(boundsIn(drained()))
                .as("a customer-only debt has no policy position to withhold")
                .containsExactly(policyBound);
        assertThat(boundsAfter(gaining, FAR_ABOVE))
                .as("the same pending debt still holds its own customer chain")
                .containsExactly(FrontierOrders.pack(CUSTOMERS, at(MOVED_AT)) - 1);
    }

    @Test
    void theBoundGoesPastOnceTheKeyThatWasOwedItHasTakenIt() throws Exception {
        AssemblerProcessor processor = assembler();
        givenACustomerHoldingAPolicy("C1", processor);
        feed(processor, DEPARTURES, renamed("C1", "C2"));
        boundsAfter(processor, FAR_ABOVE);

        feed(processor, ROOT_ROWS, renamed("C1", "C2"));

        // Where it ends up rather than how many steps it took: the root's chain arrives on two edges, so a
        // bound raised on one of them and then the other is raised twice, and the intermediate value is a
        // real promise this level could make at that moment.
        assertThat(boundsAfter(processor, FAR_ABOVE + 1))
                .describedAs("the hold is released by the document landing, not by time passing")
                .endsWith(FAR_ABOVE + 1);
    }

    /**
     * Two instances, one parking area between them - the shape a running pipeline always has here, because
     * the key being emptied and the key being filled are different values of the very column this vertex is
     * partitioned by. Nothing further arrives to prompt the recount, deliberately: what a level may promise
     * depends on what it holds as well as on what its edges said, and only the second of those turns up as a
     * message.
     */
    @Test
    void theHoldIsReleasedByTheLandingWhereverTheDocumentWasTakenIn() throws Exception {
        AssemblerProcessor losing = assembler(store);
        AssemblerProcessor gaining = assembler(new HeapNestStore<>());
        givenACustomerHoldingAPolicy("C1", losing);
        feed(losing, DEPARTURES, renamed("C1", "C2"));
        assertThat(boundsAfter(losing, FAR_ABOVE))
                .containsExactly(FrontierOrders.pack(CUSTOMERS, at(MOVED_AT)) - 1);

        feed(gaining, ROOT_ROWS, renamed("C1", "C2"));
        losing.tryProcess();

        assertThat(boundsIn(drained()))
                .describedAs("the document is under a key again, and the instance that let it go says so on "
                        + "its own rather than waiting for a bound that may never come")
                .containsExactly(FAR_ABOVE);
    }

    private void givenACustomerHoldingAPolicy(String customerId, AssemblerProcessor processor) {
        feed(processor, ROOT_ROWS, customer(1, customerId));
        feed(processor, FROM_POLICIES, Envelope.insert(2, "policy",
                row("policy_id", "P1", "customer_id", customerId, "policy_no", "PN-P1"), null)
                .withOrder(at(2)));
    }

    private AssemblerProcessor assembler() throws Exception {
        return assembler(store);
    }

    private AssemblerProcessor assembler(NestStore<RootAssembly> documents) throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), documents,
                "doc", AXES, CHAINS, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.within(0), stores.forParking(TOPOLOGY.assembler()));
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Envelope customer(long seq, String id) {
        return Envelope.insert(seq, "customer", row("customer_id", id, "name", "n"), null).withOrder(at(seq));
    }

    private static Envelope renamed(String was, String is) {
        return Envelope.update(MOVED_AT, "customer",
                row("customer_id", was, "name", "n"),
                row("customer_id", is, "name", "n"), null).withOrder(at(MOVED_AT));
    }

    private void feed(AssemblerProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        drained();
    }

    /**
     * The root's chain arrives on two edges - its own and its twin - so both have to have promised before
     * this level may say anything about it. That is the ordinary shape and not a quirk of the test: the twin
     * is drawn from the same upstream vertex, so a bound reaching one reaches both.
     */
    private List<Long> boundsAfter(AssemblerProcessor processor, long arriving) {
        processor.tryProcessWatermark(ROOT_ROWS, new Watermark(arriving, AXES.axisOf(CUSTOMERS)));
        processor.tryProcessWatermark(DEPARTURES, new Watermark(arriving, AXES.axisOf(CUSTOMERS)));
        return boundsIn(drained());
    }

    private static List<Long> boundsIn(List<Object> drained) {
        List<Long> bounds = new ArrayList<>();
        for (Object item : drained) {
            if (item instanceof Watermark bound) {
                bounds.add(bound.timestamp());
            }
        }
        return bounds;
    }

    private List<Object> drained() {
        List<Object> out = new ArrayList<>();
        outbox.drainQueueAndReset(0, out, false);
        return out;
    }
}
