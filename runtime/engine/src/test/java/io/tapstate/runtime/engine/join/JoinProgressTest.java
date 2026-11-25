package io.tapstate.runtime.engine.join;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.SettledPositions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JoinProgressTest {

    @Test
    void anUnmatchedInnerFactReportsItsDurablePositionWithoutPublishingARow() throws Exception {
        JoinPlan left = JoinProjectionTest.plan();
        JoinTree.Join from = (JoinTree.Join) left.from();
        JoinPlan inner = new JoinPlan(left.outputFields(),
                new JoinTree.Join(from.left(), from.right(), JoinKind.INNER, from.on(), false), left.readColumns());
        MapJoinStores stores = new MapJoinStores();
        JoinProcessor processor = new JoinProcessor(new JoinDriver(inner, List.of("id"), "joined", stores),
                Map.of(0, "o", 1, "c"));
        TestOutbox outbox = new TestOutbox(new int[] {1}, 1);
        processor.init(outbox, new TestProcessorContext());
        Envelope fact = Envelope.read(1, "orders", Map.of("id", 10L, "customer_id", 1L), null)
                .withOrder(SourceOrder.snapshotRow(1));
        TestInbox inbox = new TestInbox(List.of(fact));

        processor.process(0, inbox);
        List<Object> emitted = new ArrayList<>();
        outbox.drainQueueAndReset(0, emitted, false);

        assertThat(inbox).isEmpty();
        assertThat(stores.fact(JoinKey.of(List.of(10L)).name())).containsEntry("customer_id", 1L);
        assertThat(emitted).containsExactly(new SettledPositions(fact.positions()));
    }

    @Test
    void aDimensionLoadedBeforeAnyFactsStillReportsItsPosition() throws Exception {
        MapJoinStores stores = new MapJoinStores();
        JoinDriver driver = new JoinDriver(JoinProjectionTest.plan(), List.of("id"), "joined", stores);
        JoinProcessor processor = new JoinProcessor(driver, Map.of(0, "o", 1, "c"));
        TestOutbox outbox = new TestOutbox(new int[] {1}, 1);
        processor.init(outbox, new TestProcessorContext());
        Envelope customer = Envelope.read(1, "customers", Map.of("id", 1L, "name", "Ada"), null)
                .withOrder(SourceOrder.snapshotRow(1));
        TestInbox inbox = new TestInbox(List.of(customer));

        processor.process(1, inbox);
        List<Object> emitted = new ArrayList<>();
        outbox.drainQueueAndReset(0, emitted, false);

        assertThat(inbox).isEmpty();
        assertThat(stores.dimensionRow("c", JoinKey.of(List.of(1L)).name()))
                .containsEntry("name", "Ada");
        assertThat(emitted).containsExactly(new SettledPositions(customer.positions()));
    }

    @Test
    void refreshedRowsAndDeletesKeepThePositionOfTheChangeThatRequestedThem() {
        MapJoinStores stores = new MapJoinStores();
        JoinDriver driver = new JoinDriver(JoinProjectionTest.plan(), List.of("id"), "joined", stores);
        JoinProjection projection = new JoinProjection(JoinProjectionTest.plan(), List.of("id"), "joined", stores);
        Envelope fact = Envelope.insert(1, "orders", Map.of("id", 10L, "customer_id", 1L), null)
                .withPosition(new ChainPosition(new SourceOrder(1, 3), "order-3"));
        driver.absorb(List.of(new SourceChange("o", fact)));
        List<JoinUpdate> delayed = new ArrayList<>();
        assertThat(driver.drainUpdates(delayed::add)).isTrue();
        Envelope customer = Envelope.insert(2, "customers", Map.of("id", 1L, "name", "Ada"), null)
                .withPosition(new ChainPosition(new SourceOrder(1, 7), "customer-7"));
        driver.absorb(List.of(new SourceChange("c", customer)));
        List<JoinUpdate> fanout = new ArrayList<>();
        assertThat(driver.drainUpdates(fanout::add)).isTrue();

        assertThat(projection.refresh(delayed)).singleElement().satisfies(row -> {
            assertThat(row.after()).containsEntry("customer_name", "Ada");
            assertThat(row.positions()).isEqualTo(fact.positions());
        });
        assertThat(projection.refresh(fanout)).singleElement()
                .satisfies(row -> assertThat(row.positions()).isEqualTo(customer.positions()));

        Envelope deleted = Envelope.delete(3, "orders", fact.after(), null)
                .withPosition(new ChainPosition(new SourceOrder(1, 9), "order-9"));
        driver.absorb(List.of(new SourceChange("o", deleted)));
        List<JoinUpdate> removals = new ArrayList<>();
        assertThat(driver.drainUpdates(removals::add)).isTrue();
        assertThat(projection.refresh(removals)).singleElement().satisfies(row -> {
            assertThat(row.op()).isEqualTo(Op.DELETE);
            assertThat(row.positions()).isEqualTo(deleted.positions());
        });
    }

    @Test
    void aDimensionSettlementAndItsBoundCannotOvertakeARefusedFanout() throws Exception {
        MapJoinStores stores = new MapJoinStores(2);
        JoinDriver driver = new JoinDriver(JoinProjectionTest.plan(), List.of("id"), "joined", stores, 2);
        driver.absorb(List.of(new SourceChange("c", Envelope.insert(0, "customers",
                Map.of("id", 1L, "name", "Ada"), null))));
        for (long id = 10; id < 15; id++) {
            driver.absorb(List.of(new SourceChange("o", Envelope.insert(0, "orders",
                    Map.of("id", id, "customer_id", 1L), null))));
        }
        assertThat(driver.drainUpdates(ignored -> true)).isTrue();
        ChainAxes axes = ChainAxes.assign(List.of("orders", "customers"));
        JoinProcessor processor = new JoinProcessor(driver, Map.of(0, "o", 1, "c"),
                new LevelBounds(Map.of(0, List.of("orders"), 1, List.of("customers")),
                        axes, LevelBounds.HOLDS_NOTHING));
        TestOutbox outbox = new TestOutbox(new int[] {1}, 1);
        processor.init(outbox, new TestProcessorContext());
        Envelope customer = Envelope.update(1, "customers", Map.of("id", 1L, "name", "Ada"),
                Map.of("id", 1L, "name", "Grace"), null)
                .withPosition(new ChainPosition(new SourceOrder(1, 7), "customer-7"));
        TestInbox inbox = new TestInbox(List.of(customer));
        Watermark bound = new Watermark(FrontierOrders.pack("customers", customer.position().order()),
                axes.axisOf("customers"));
        List<Object> emitted = new ArrayList<>();

        processor.process(1, inbox);
        assertThat(inbox).isNotEmpty();
        assertThat(processor.tryProcessWatermark(1, bound)).isFalse();
        for (int attempt = 0; attempt < 20 && !inbox.isEmpty(); attempt++) {
            outbox.drainQueueAndReset(0, emitted, false);
            processor.process(1, inbox);
        }
        outbox.drainQueueAndReset(0, emitted, false);
        assertThat(inbox).isEmpty();
        assertThat(emitted).hasSize(6);
        assertThat(emitted.subList(0, 5)).allSatisfy(item -> {
            Envelope row = ((JoinUpdate) item).event();
            assertThat(row.after()).containsEntry("customer_name", "Grace");
            assertThat(row.positions()).isEqualTo(customer.positions());
        });
        assertThat(emitted.getLast()).isEqualTo(new SettledPositions(customer.positions()));

        // No orders bound has arrived: customers must not wait for an unrelated input to speak.
        assertThat(processor.tryProcessWatermark(1, bound)).isTrue();
        List<Object> bounds = new ArrayList<>();
        outbox.drainQueueAndReset(0, bounds, false);
        assertThat(bounds).containsExactly(bound);
        assertThat(processor.tryProcessWatermark(bound)).isTrue();
        assertThat(outbox.queue(0)).isEmpty();
    }
}
