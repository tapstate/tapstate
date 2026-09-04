package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.sql.Expr;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The join as the substrate sees it. Two things here belong to the vertex rather than to the driver
 * underneath it, and both are silent when they are wrong.
 *
 * <ul>
 *   <li><b>A change offered again must not be taken in again.</b> The substrate re-offers a delivery
 *       whose processing answered "not yet", and a join that absorbed it a second time would index the
 *       row twice and publish it twice - which an idempotent sink hides, right up until the duplicate
 *       is in the reverse index and the next dimension change sends the row twice for ever.
 *   <li><b>The rest of a recompute must go out when nothing is arriving.</b> A stream that has caught
 *       up stops delivering, so a vertex that only pushed on arrival would leave a fan-out half sent,
 *       with the job running and nothing reported.
 *   <li><b>A delivery must reach the driver whole.</b> The driver reads the fact mirror once per batch
 *       it is handed; a vertex that passed items down one at a time would turn that into one read per
 *       row while every other assertion here stayed green.
 * </ul>
 */
class JoinProcessorTest {

    private static final String STREAM = "order_state";
    private static final int FACT = 0;
    private static final int DIMENSION = 1;

    private CountingJoinStores stores;
    private JoinDriver driver;
    private JoinProcessor processor;
    private TestOutbox outbox;

    @BeforeEach
    void buildVertex() throws Exception {
        stores = new CountingJoinStores(2);
        driver = new JoinDriver(plan(), List.of("id"), STREAM, stores, 4);
        processor = new JoinProcessor(driver, Map.of(FACT, "o", DIMENSION, "c"));
        outbox = new TestOutbox(new int[] {2}, 2);
        processor.init(outbox, new TestProcessorContext());
    }

    @Test
    @DisplayName("a fact row arriving is published with its dimension row joined in")
    void aFactRowIsPublishedJoined() {
        assertThat(offer(DIMENSION, insert(Map.of("id", 1L, "name", "Ada")))).isTrue();

        assertThat(offer(FACT, insert(Map.of("id", 10L, "cust_id", 1L)))).isTrue();

        assertThat(published()).containsExactly(Map.of("order_id", 10L, "customer_name", "Ada"));
    }

    /**
     * The one that says the batching underneath is actually reached. Six fact rows arrive in one
     * delivery, none of them carrying a before image, so each has to ask the mirror what its key used
     * to hold - which is the read a first load makes most of. A vertex that hands them down one at a
     * time asks six times for one key each; every other case in this class passes either way.
     */
    @Test
    @DisplayName("a delivery reaches the driver whole, so its mirror reads are one ask and not six")
    void aDeliveryIsAbsorbedAsOneBatch() {
        offer(DIMENSION, insert(Map.of("id", 1L, "name", "Ada")));
        Envelope[] arriving = new Envelope[6];
        for (int i = 0; i < arriving.length; i++) {
            arriving[i] = insert(Map.of("id", (long) i, "cust_id", 1L));
        }
        stores.forgetCounts();

        offerUntilTaken(FACT, arriving);

        assertThat(stores.singleReads).as("not one round trip per arriving row").isZero();
        assertThat(stores.batchReads)
                .as("the delivery's keys asked for once, however many times it was offered")
                .isEqualTo(1);
        assertThat(stores.keysRead).isEqualTo(6);
        assertThat(published()).hasSize(6);
    }

    /**
     * The one that catches a change taken in twice. The outbox is deliberately too small for the whole
     * fan-out, so the vertex answers "not yet" and is offered the same change again - which is exactly
     * what the substrate does.
     */
    @Test
    @DisplayName("a delivery offered again is not taken into the state a second time")
    void aRetriedChangeIsAbsorbedOnce() {
        offer(DIMENSION, insert(Map.of("id", 1L, "name", "Ada")));
        for (long id = 0; id < 5; id++) {
            offerUntilTaken(FACT, insert(Map.of("id", id, "cust_id", 1L)));
        }
        forgetWhatWasPublished();

        int offers = offerUntilTaken(DIMENSION,
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace")));

        assertThat(offers).as("it really was offered more than once").isGreaterThan(1);
        List<Map<String, Object>> rows = published();
        assertThat(rows).as("five rows, once each - not five per offer").hasSize(5);
        assertThat(rows).allSatisfy(row -> assertThat(row).containsEntry("customer_name", "Grace"));
        assertThat(rows).extracting(row -> row.get("order_id"))
                .containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L);
    }

    /**
     * The idle call. Nothing is arriving, which is the ordinary state of a stream that has caught up,
     * and what is left of the fan-out has to finish anyway.
     */
    @Test
    @DisplayName("what is left of a recompute goes out when nothing is arriving")
    void theRestOfARecomputeGoesOutWhenNothingArrives() {
        offer(DIMENSION, insert(Map.of("id", 1L, "name", "Ada")));
        for (long id = 0; id < 5; id++) {
            offerUntilTaken(FACT, insert(Map.of("id", id, "cust_id", 1L)));
        }
        forgetWhatWasPublished();

        // One offer, which fills the outbox and leaves the rest of the fan-out queued. Nothing is
        // offered again after this - the stream has caught up, which is the case being made.
        assertThat(offer(DIMENSION,
                update(Map.of("id", 1L, "name", "Ada"), Map.of("id", 1L, "name", "Grace"))))
                .as("the outbox holds two, so five rows cannot go out in one call").isFalse();
        assertThat(driver.hasPending()).isTrue();
        drainOutbox();

        int idleCalls = 0;
        while (!processor.tryProcess()) {
            drainOutbox();
            if (++idleCalls > 100) {
                throw new AssertionError("the idle call never finished the recompute");
            }
        }

        assertThat(idleCalls).as("it took more than one idle call, so this is not a no-op")
                .isGreaterThan(0);
        assertThat(published()).extracting(row -> row.get("order_id"))
                .containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    @DisplayName("an edge on an ordinal naming no source is a wiring mistake and says so")
    void anUnknownOrdinalIsRefused() {
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> offer(7, insert(Map.of("id", 1L))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("names no source");
    }

    /** Offers {@code items} as one delivery, and says whether all of it went out in that one call. */
    private boolean offer(int ordinal, Envelope... items) {
        TestInbox inbox = new TestInbox(List.of(items));
        processor.process(ordinal, inbox);
        return inbox.isEmpty();
    }

    /**
     * Offers one delivery over and over, the way the substrate does with what a vertex has not
     * finished with, until it has all gone out. Bounded, because a vertex that never finishes is a
     * hang rather than a failure, and a hang in a suite reads as an infrastructure problem rather
     * than as the case that caused it.
     *
     * @return how many times it had to be offered
     */
    private int offerUntilTaken(int ordinal, Envelope... items) {
        TestInbox inbox = new TestInbox(List.of(items));
        for (int offers = 1; offers <= 100; offers++) {
            processor.process(ordinal, inbox);
            if (inbox.isEmpty()) {
                return offers;
            }
            drainOutbox();
        }
        throw new AssertionError("the vertex never took the delivery in");
    }

    private final List<Map<String, Object>> collected = new ArrayList<>();

    /** Drops what has been published so far, so a case asserts on what it caused rather than on all of it. */
    private void forgetWhatWasPublished() {
        drainOutbox();
        collected.clear();
    }

    private void drainOutbox() {
        List<Object> taken = new ArrayList<>();
        outbox.drainQueueAndReset(0, taken, false);
        for (Object item : taken) {
            Envelope event = (Envelope) item;
            collected.add(event.op() == Op.DELETE ? event.before() : event.after());
        }
    }

    private List<Map<String, Object>> published() {
        drainOutbox();
        return List.copyOf(collected);
    }

    private static Envelope insert(Map<String, Object> after) {
        return Envelope.insert(1L, "src", after, null);
    }

    private static Envelope update(Map<String, Object> before, Map<String, Object> after) {
        return Envelope.update(1L, "src", before, after, null);
    }

    private static JoinPlan plan() {
        JoinTree from = new JoinTree.Join(
                new JoinTree.Source("o", "orders"),
                new JoinTree.Source("c", "customers"),
                JoinKind.LEFT,
                List.of(new JoinTree.KeyPair(new JoinTree.ColumnRef("o", "cust_id"),
                        new JoinTree.ColumnRef("c", "id"))),
                false);
        return new JoinPlan(List.of(
                new OutputField("order_id", TapstateType.INT64, false,
                        new Expr.Column(new JoinTree.ColumnRef("o", "id"))),
                new OutputField("customer_name", TapstateType.STRING, true,
                        new Expr.Column(new JoinTree.ColumnRef("c", "name")))),
                from,
                Map.of("o", List.of("cust_id", "id"), "c", List.of("id", "name")));
    }
}
