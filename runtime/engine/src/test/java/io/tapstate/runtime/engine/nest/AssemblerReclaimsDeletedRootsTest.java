package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Forgetting a root that was deleted. What is left behind after a deletion is the record that it happened,
 * and it has to stay for as long as the change that created the root could be delivered again - drop it too
 * early and a replayed insert builds the root back with nothing left to say it was removed.
 *
 * <p>So the record is dropped only once the deletion sits below where a restart would resume, and only once
 * the assembly is holding nothing else of its own. Both are read beside the flow rather than per change:
 * this is an optimisation that reclaims memory, and it is always safe to do it later than possible.
 */
class AssemblerReclaimsDeletedRootsTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private static final int ROOT_ROWS = 0;
    private static final int FROM_POLICIES = 1;

    private static final Object CUSTOMER_1 = List.of("c1");

    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();
    private final CountingFloor floor = new CountingFloor();
    private final AssemblerProcessor processor =
            new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), store, "doc", floor);
    private final TestOutbox outbox = new TestOutbox(128);

    private void init() throws Exception {
        processor.init(outbox, new TestProcessorContext());
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Envelope customer(long seq, String id) {
        return Envelope.insert(seq, "customer", row("customer_id", id, "name", "n" + seq), null)
                .withOrder(at(seq));
    }

    private static Envelope customerGone(long seq, String id) {
        return Envelope.delete(seq, "customer", row("customer_id", id, "name", "n" + seq), null)
                .withOrder(at(seq));
    }

    /** A policy row, which hangs straight off the root: a leaf embed passes through no resolver. */
    private static Envelope policyRow(long seq, String customerId, String policyId) {
        return Envelope.insert(seq, "policy",
                        row("customer_id", customerId, "policy_id", policyId, "policy_no", "PN-" + policyId),
                        null)
                .withOrder(at(seq));
    }

    private void feed(int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        outbox.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    @Test
    void keeps_what_it_knows_of_a_deleted_root_while_the_deletion_could_still_arrive_again() throws Exception {
        init();
        feed(ROOT_ROWS, customer(1, "c1"), customerGone(2, "c1"));

        floor.at("customer", at(2));
        processor.tryProcess();

        assertThat(store.load(CUSTOMER_1))
                .as("the deletion sits at the floor, so a resume still delivers it")
                .isNotNull();
    }

    @Test
    void forgets_a_deleted_root_once_nothing_can_deliver_its_deletion_again() throws Exception {
        init();
        feed(ROOT_ROWS, customer(1, "c1"), customerGone(2, "c1"));

        floor.at("customer", at(3));
        processor.tryProcess();

        assertThat(store.load(CUSTOMER_1))
                .as("everything up to the deletion has been acked, so nothing can build the root back")
                .isNull();
    }

    @Test
    void keeps_a_deleted_root_that_is_still_holding_something_of_its_own() throws Exception {
        init();
        feed(ROOT_ROWS, customer(1, "c1"), customerGone(2, "c1"));
        feed(FROM_POLICIES, policyRow(4, "c1", "p1"));

        floor.at("customer", at(3));
        processor.tryProcess();

        assertThat(store.load(CUSTOMER_1))
                .as("an element absorbed after the deletion has been shown to nobody yet")
                .isNotNull();
    }

    @Test
    void forgets_nothing_while_the_floor_is_not_known() throws Exception {
        init();
        feed(ROOT_ROWS, customer(1, "c1"), customerGone(2, "c1"));

        processor.tryProcess();

        assertThat(store.load(CUSTOMER_1))
                .as("a floor that cannot be read must leave the record alone, never drop it")
                .isNotNull();
    }

    @Test
    void keeps_a_root_that_came_back_after_it_was_deleted() throws Exception {
        init();
        // Two drains, not one: a root deleted and re-created within a single drain never sends a key row,
        // so nothing was ever put down to forget and the test would pass without exercising anything.
        feed(ROOT_ROWS, customer(1, "c1"), customerGone(2, "c1"));
        feed(ROOT_ROWS, customer(3, "c1"));

        floor.at("customer", at(9));
        processor.tryProcess();

        assertThat(store.load(CUSTOMER_1))
                .as("the root is present again, so there is no deletion left to forget")
                .isNotNull();
    }

    @Test
    void reads_the_floor_beside_the_flow_rather_than_for_every_change() throws Exception {
        init();
        feed(ROOT_ROWS, customer(1, "c1"), customerGone(2, "c1"), customer(3, "c2"), customerGone(4, "c2"));

        assertThat(floor.reads)
                .as("a crossing to the durable plane per change would cost more than the change does")
                .isZero();

        processor.tryProcess();

        assertThat(floor.reads).as("and it is read once the operator has nothing else to do").isPositive();
    }

    /** A floor whose answers are set by the test, counting how often it was asked. */
    private static final class CountingFloor implements ReplayFloor {

        private static final long serialVersionUID = 1L;

        private final Map<String, SourceOrder> floors = new LinkedHashMap<>();
        private transient int reads;

        private void at(String chain, SourceOrder order) {
            floors.put(chain, order);
        }

        @Override
        public Optional<SourceOrder> of(String chain) {
            reads++;
            return Optional.ofNullable(floors.get(chain));
        }
    }
}
