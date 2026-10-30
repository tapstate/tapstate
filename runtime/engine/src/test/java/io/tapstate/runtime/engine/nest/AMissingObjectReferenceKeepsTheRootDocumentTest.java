package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AMissingObjectReferenceKeepsTheRootDocumentTest {

    @Test
    void aMissingCustomerOmitsOnlyItsObjectBranch() throws Exception {
        Map<String, NestTable> tables = Map.of(
                "o", new NestTable("ACC_ORDERS", List.of("ORDER_ID")),
                "l", new NestTable("ACC_LINES", List.of("ORDER_ID", "LINE_NO")),
                "c", new NestTable("ACC_CUSTOMERS", List.of("CUSTOMER_ID")));
        NestTopology topology = NestTopology.compile("acc_nest_mysql", "order_doc",
                nest("o", List.of("ORDER_ID"),
                        embed("l", "ORDER_ID", "ORDER_ID", EmbedAs.ARRAY, "lines", List.of("LINE_NO")),
                        embed("c", "CUSTOMER_ID", "CUSTOMER_ID", EmbedAs.OBJECT, "customer", null)),
                tables::get);

        HeapNestStore<Map<String, Object>> customers = new HeapNestStore<>();
        Map<String, Object> existingCustomer = row("CUSTOMER_ID", 1, "NAME", "Ada");
        customers.save(List.of(1), existingCustomer);
        AssemblerProcessor assembler = new AssemblerProcessor(
                topology.assembler(), topology.slots(), new HeapNestStore<>(), "order_doc", null, null,
                ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM, NestSendPolicy.everyChange(),
                new HeapNestStore<>(), (from, released) -> {
                    throw new AssertionError("unexpected dead letter: " + released);
                }, Map.of(topology.lookups().getFirst().mapName(), customers));
        TestOutbox outbox = new TestOutbox(128);
        assembler.init(outbox, new TestProcessorContext());

        Map<String, Object> line = row("ORDER_ID", 25, "LINE_NO", 1, "SKU", "widget");
        TestInbox lines = new TestInbox();
        lines.queue().add(Envelope.insert(1L, "ACC_LINES", line, null).withOrder(at(1)));
        assembler.process(topology.assembler().inboundFor(List.of("lines")).ordinal(), lines);
        assertThat(lines).isEmpty();

        TestInbox orders = new TestInbox();
        orders.queue().add(Envelope.insert(2L, "ACC_ORDERS",
                row("ORDER_ID", 10, "CUSTOMER_ID", 1), null).withOrder(at(2)));
        orders.queue().add(Envelope.insert(3L, "ACC_ORDERS",
                row("ORDER_ID", 25, "CUSTOMER_ID", 999), null).withOrder(at(3)));
        assembler.process(topology.assembler().inboundFor(List.of()).ordinal(), orders);
        assertThat(orders).isEmpty();
        assertThat(assembler.complete()).isTrue();

        List<Object> emitted = new ArrayList<>();
        outbox.drainQueueAndReset(0, emitted, false);
        Map<Object, Map<String, Object>> documents = new LinkedHashMap<>();
        for (Object item : emitted) {
            Map<String, Object> document = ((Envelope) item).after();
            documents.put(document.get("ORDER_ID"), document);
        }

        assertThat(documents.get(10))
                .as("the existing customer is looked up correctly and missing lines render as an empty array")
                .isEqualTo(row("ORDER_ID", 10, "CUSTOMER_ID", 1,
                        "lines", List.of(), "customer", existingCustomer));
        assertThat(documents)
                .as("a missing N:1 customer must omit only the customer object, not the root order document")
                .containsKey(25);
        assertThat(documents.get(25))
                .isEqualTo(row("ORDER_ID", 25, "CUSTOMER_ID", 999, "lines", List.of(line)));
    }
}
