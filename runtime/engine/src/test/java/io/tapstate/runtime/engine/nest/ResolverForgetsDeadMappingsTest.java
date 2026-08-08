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
import io.tapstate.core.event.Op;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Dropping the tombstone a deleted mapping leaves behind. A deletion does not remove the entry, because
 * rows beneath it may still be on their way and would otherwise wait for a parent that no longer exists;
 * the tombstone answers them. It occupies a key while it sits there, so it is worth dropping - but only
 * once the deletion that created it can no longer be delivered a second time.
 */
class ResolverForgetsDeadMappingsTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestVertex POLICIES =
            NestTopology.compile("p", "doc", TREE, tables()).vertexAt(List.of("policies"));

    private static final int OWN_ROWS = 0;

    private static final Object POLICY_1 = List.of("p1");

    private final HeapNestStore<ResolverState> store = new HeapNestStore<>();
    private final CountingFloor floor = new CountingFloor();
    private final ResolverProcessor processor =
            new ResolverProcessor(POLICIES, store, element -> { }, floor);
    private final TestOutbox outbox = new TestOutbox(128);

    @BeforeEach
    void init() throws Exception {
        processor.init(outbox, new TestProcessorContext());
    }

    private static Envelope policy(Op op, long seq, String policyId, String customerId) {
        Map<String, Object> fields =
                row("policy_id", policyId, "customer_id", customerId, "policy_no", "PN-" + policyId);
        Envelope built = op == Op.DELETE
                ? Envelope.delete(seq, "policy", fields, null)
                : Envelope.insert(seq, "policy", fields, null);
        return built.withOrder(new SourceOrder(1L, seq));
    }

    private void feed(int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
        outbox.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    @Test
    void keeps_a_tombstone_while_the_deletion_that_made_it_could_still_arrive_again() throws Exception {
        feed(OWN_ROWS, policy(Op.INSERT, 1, "p1", "c1"), policy(Op.DELETE, 2, "p1", "c1"));

        floor.at("policy", new SourceOrder(1L, 2));
        processor.tryProcess();

        assertThat(store.load(POLICY_1))
                .as("the deletion sits at the floor, so a resume still delivers it")
                .isNotNull();
    }

    @Test
    void forgets_a_tombstone_once_nothing_can_deliver_its_deletion_again() throws Exception {
        feed(OWN_ROWS, policy(Op.INSERT, 1, "p1", "c1"), policy(Op.DELETE, 2, "p1", "c1"));

        floor.at("policy", new SourceOrder(1L, 3));
        processor.tryProcess();

        assertThat(store.load(POLICY_1))
                .as("everything up to the deletion has been acked, so no child can ask about it again")
                .isNull();
    }

    @Test
    void forgets_nothing_while_the_floor_is_not_known() throws Exception {
        feed(OWN_ROWS, policy(Op.INSERT, 1, "p1", "c1"), policy(Op.DELETE, 2, "p1", "c1"));

        processor.tryProcess();

        assertThat(store.load(POLICY_1))
                .as("a floor that cannot be read must leave the tombstone alone, never drop it")
                .isNotNull();
    }

    @Test
    void keeps_a_mapping_that_was_declared_again_after_it_was_deleted() throws Exception {
        // Two drains, so the tombstone is actually put down and then found live again, rather than the
        // whole thing collapsing inside one batch and testing nothing.
        feed(OWN_ROWS, policy(Op.INSERT, 1, "p1", "c1"), policy(Op.DELETE, 2, "p1", "c1"));
        feed(OWN_ROWS, policy(Op.INSERT, 3, "p1", "c2"));

        floor.at("policy", new SourceOrder(1L, 9));
        processor.tryProcess();

        assertThat(store.load(POLICY_1))
                .as("the mapping is live again, so there is no tombstone left to drop")
                .isNotNull();
    }

    @Test
    void reads_the_floor_beside_the_flow_rather_than_for_every_change() throws Exception {
        feed(OWN_ROWS, policy(Op.INSERT, 1, "p1", "c1"), policy(Op.DELETE, 2, "p1", "c1"),
                policy(Op.INSERT, 3, "p2", "c1"), policy(Op.DELETE, 4, "p2", "c1"));

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
