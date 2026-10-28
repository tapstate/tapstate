package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.SettledPositions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LookupRowsBatchingTest {
    private static final NestLookup LOOKUP = NestTopology.compile("p", "doc",
            nest("order", List.of("order_id"),
                    embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null)),
            tables()).lookups().getFirst();

    @Test
    void customersArrivingBeforeOrdersReadEmptyReferencesAndWriteRowsInBoundedBatches() throws Exception {
        CountingStore<Map<String, Object>> rows = new CountingStore<>();
        CountingStore<Set<Object>> refs = new CountingStore<>();
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = processor(rows, refs, outbox);
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 16; i++) inbox.add(customer(i, "name-" + i));

        processor.process(LookupProcessor.ROWS, inbox);

        assertThat(inbox.isEmpty()).isTrue();
        assertThat(rows.count()).isEqualTo(16);
        assertThat(refs.count()).isZero();
        assertThat(refs.readBatches).as("one refs query per customer is the reported bottleneck").hasSize(1);
        assertThat(refs.readBatches.getFirst()).hasSize(16 * NestLookup.BUCKETS);
        assertThat(rows.writeBatches).as("row writes must also reach the batch seam").hasSize(1);
        assertThat(rows.writeBatches.getFirst()).hasSize(16);
        assertThat(outbox.queue(0)).allMatch(SettledPositions.class::isInstance);
    }

    @Test
    void repeatedKeysKeepTheirIndividualWritesAndWakeOrder() throws Exception {
        CountingStore<Map<String, Object>> rows = new CountingStore<>();
        CountingStore<Set<Object>> refs = new CountingStore<>();
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = processor(rows, refs, outbox);
        TestInbox registration = new TestInbox();
        registration.add(Envelope.insert(1, "order", row("order_id", 1L, "cust_ref", 100), null).withOrder(at(1)));
        processor.process(LookupProcessor.REGISTRATIONS, registration);
        outbox.queue(0).clear();
        TestInbox inbox = new TestInbox();
        inbox.add(customer(100, "first"));
        inbox.add(customer(100, "second"));

        processor.process(LookupProcessor.ROWS, inbox);

        assertThat(rows.writtenNames).containsExactly("first", "second");
        assertThat(rows.load(List.of(100))).containsEntry("name", "second");
        assertThat(outbox.queue(0).stream().filter(NestTouch.class::isInstance)).hasSize(2);
    }

    @Test
    void aRegistrationDuringBackpressureInvalidatesPrefetchedRefsWithoutLosingFirstFiling() throws Exception {
        CountingStore<Map<String, Object>> rows = new CountingStore<>();
        CountingStore<Set<Object>> refs = new CountingStore<>();
        TestOutbox outbox = new TestOutbox(1);
        LookupProcessor processor = processor(rows, refs, outbox);
        TestInbox registrations = new TestInbox();
        registrations.add(order(1, 1));
        registrations.add(order(2, 2));
        processor.process(LookupProcessor.REGISTRATIONS, registrations);
        TestInbox inbox = new TestInbox();
        for (int i = 1; i <= 3; i++) inbox.add(customer(i, "name-" + i));

        processor.process(LookupProcessor.ROWS, inbox);
        assertThat(inbox.size()).isEqualTo(1);
        assertThat(rows.count()).isEqualTo(3);
        outbox.queue(0).clear();
        TestInbox late = new TestInbox();
        late.add(order(3, 3));
        processor.process(LookupProcessor.REGISTRATIONS, late);
        for (int i = 0; i < 4; i++) {
            outbox.queue(0).clear();
            if (processor.tryProcess()) break;
        }
        outbox.queue(0).clear();
        assertThat(processor.complete()).isFalse();

        processor.process(LookupProcessor.ROWS, inbox);

        assertThat(inbox.isEmpty()).isTrue();
        assertThat(rows.writeBatches).hasSize(1);
        assertThat(refs.readBatches).hasSize(2);
        assertThat(outbox.queue(0)).singleElement().satisfies(item -> {
            assertThat(item).isInstanceOf(NestTouch.class);
            NestTouch touch = (NestTouch) item;
            assertThat(touch.key()).isEqualTo(List.of(3L));
            assertThat(touch.firstFiling()).isTrue();
        });
    }

    @Test
    void moreThanOneChunkKeepsBothTheInputAndReferenceKeyBatchesBounded() throws Exception {
        CountingStore<Map<String, Object>> rows = new CountingStore<>();
        CountingStore<Set<Object>> refs = new CountingStore<>();
        LookupProcessor processor = processor(rows, refs, new TestOutbox(1024));
        TestInbox inbox = new TestInbox();
        for (int i = 0; i < 40; i++) inbox.add(customer(i, "name-" + i));

        processor.process(LookupProcessor.ROWS, inbox);

        assertThat(inbox.isEmpty()).isTrue();
        assertThat(rows.count()).isEqualTo(40);
        assertThat(rows.writeBatches).hasSize(3).allSatisfy(batch -> assertThat(batch).hasSizeLessThanOrEqualTo(16));
        assertThat(refs.readBatches).hasSize(3).allSatisfy(batch -> assertThat(batch).hasSizeLessThanOrEqualTo(1024));
    }

    @Test
    void aPartiallyWrittenBatchRetainsEveryInputAndCanBeRetriedWithoutLosingRows() throws Exception {
        CountingStore<Map<String, Object>> rows = new CountingStore<>();
        rows.failAfterFirstWrite = true;
        CountingStore<Set<Object>> refs = new CountingStore<>();
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = processor(rows, refs, outbox);
        TestInbox registrations = new TestInbox();
        registrations.add(order(1, 1));
        registrations.add(order(2, 2));
        processor.process(LookupProcessor.REGISTRATIONS, registrations);
        TestInbox inbox = new TestInbox();
        inbox.add(customer(1, "first"));
        inbox.add(customer(2, "second"));

        assertThatThrownBy(() -> processor.process(LookupProcessor.ROWS, inbox))
                .isInstanceOf(IllegalStateException.class).hasMessage("injected partial write failure");
        assertThat(rows.count()).isEqualTo(1);
        assertThat(inbox.size()).isEqualTo(2);
        assertThat(outbox.queue(0)).isEmpty();

        rows.failAfterFirstWrite = false;
        processor.process(LookupProcessor.ROWS, inbox);
        assertThat(rows.count()).isEqualTo(2);
        assertThat(inbox.isEmpty()).isTrue();
        assertThat(outbox.queue(0).stream().filter(NestTouch.class::isInstance)
                .map(NestTouch.class::cast).map(NestTouch::key)).containsExactly(List.of(1L), List.of(2L));
    }

    @Test
    void aFailedRowBatchDoesNotConsumeTheInputsOrPublishTheirWakes() throws Exception {
        CountingStore<Map<String, Object>> rows = new CountingStore<>();
        rows.failBatch = true;
        CountingStore<Set<Object>> refs = new CountingStore<>();
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = processor(rows, refs, outbox);
        TestInbox inbox = new TestInbox();
        inbox.add(customer(1, "first"));
        inbox.add(customer(2, "second"));

        assertThatThrownBy(() -> processor.process(LookupProcessor.ROWS, inbox))
                .isInstanceOf(IllegalStateException.class).hasMessage("injected durable batch failure");
        assertThat(inbox.size()).isEqualTo(2);
        assertThat(outbox.queue(0)).isEmpty();
    }

    private static LookupProcessor processor(CountingStore<Map<String, Object>> rows,
            CountingStore<Set<Object>> refs, TestOutbox outbox) throws Exception {
        LookupProcessor processor = new LookupProcessor(LOOKUP, rows, refs, 100L, null);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    private static Envelope customer(int id, String name) {
        return Envelope.insert(id, "customer", row("customer_id", id, "name", name), null).withOrder(at(id));
    }

    private static Envelope order(long id, int customer) {
        return Envelope.insert(id, "order", row("order_id", id, "cust_ref", customer), null).withOrder(at(id));
    }

    private static final class CountingStore<S> implements NestStore<S> {
        private final Map<Object, S> states = new LinkedHashMap<>();
        private final List<List<Object>> readBatches = new ArrayList<>();
        private final List<List<Object>> writeBatches = new ArrayList<>();
        private final List<String> writtenNames = new ArrayList<>();
        private boolean failBatch;
        private boolean failAfterFirstWrite;

        @Override public S load(Object key) { return states.get(key); }
        @Override public Map<Object, S> loadAll(Collection<Object> keys) {
            readBatches.add(List.copyOf(keys));
            return NestStore.super.loadAll(keys);
        }
        @Override public void save(Object key, S state) {
            states.put(key, state);
            if (state instanceof Map<?, ?> row && row.get("name") instanceof String name) writtenNames.add(name);
        }
        @Override public void saveAll(Map<Object, S> batch, Set<Object> firstFilings) {
            writeBatches.add(List.copyOf(batch.keySet()));
            if (failBatch) throw new IllegalStateException("injected durable batch failure");
            if (failAfterFirstWrite) {
                Map.Entry<Object, S> first = batch.entrySet().iterator().next();
                save(first.getKey(), first.getValue());
                throw new IllegalStateException("injected partial write failure");
            }
            NestStore.super.saveAll(batch, firstFilings);
        }
        @Override public void remove(Object key) { states.remove(key); }
        @Override public long count() { return states.size(); }
    }
}
