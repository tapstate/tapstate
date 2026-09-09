package io.tapstate.runtime.engine.join;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JoinProjectionProcessorTest {
    @Test
    void backpressureDoesNotRepeatOrLoseABatchedProjection() throws Exception {
        CountingJoinStores stores = new CountingJoinStores(4);
        stores.putDimensionRow("c", JoinKey.of(List.of(1L)).name(), Map.of("id", 1L, "name", "Ada"));
        List<Envelope> arrivals = new ArrayList<>();
        for (long id = 10; id < 13; id++) {
            stores.putFact(JoinKey.of(List.of(id)).name(), Map.of("id", id, "customer_id", 1L));
            arrivals.add(Envelope.insert(1, "joined", Map.of("order_id", id), null));
        }
        JoinProjectionProcessor processor = new JoinProjectionProcessor(
                new JoinProjection(JoinProjectionTest.plan(), List.of("id"), "joined", stores));
        TestOutbox outbox = new TestOutbox(new int[] {1}, 1);
        processor.init(outbox, new TestProcessorContext());
        TestInbox inbox = new TestInbox(arrivals);
        List<Object> emitted = new ArrayList<>();

        processor.process(0, inbox);
        assertThat(inbox.isEmpty()).as("one slot cannot accept three rows").isFalse();
        for (int attempt = 0; attempt < 4 && !inbox.isEmpty(); attempt++) {
            outbox.drainQueueAndReset(0, emitted, false);
            processor.process(0, inbox);
        }
        outbox.drainQueueAndReset(0, emitted, false);

        assertThat(inbox.isEmpty()).isTrue();
        assertThat(emitted).extracting(item -> ((Envelope) item).after().get("order_id"))
                .containsExactly(10L, 11L, 12L);
        assertThat(stores.batchReads).isEqualTo(1);
        assertThat(stores.singleReads).isZero();

        stores.putDimensionRow("c", JoinKey.of(List.of(1L)).name(), Map.of("id", 1L, "name", "Grace"));
        processor.process(0, new TestInbox(List.of(arrivals.getFirst())));
        List<Object> next = new ArrayList<>();
        outbox.drainQueueAndReset(0, next, false);
        assertThat(next).singleElement().satisfies(item ->
                assertThat(((Envelope) item).after()).containsEntry("customer_name", "Grace"));
        assertThat(stores.batchReads).isEqualTo(2);
    }
}
