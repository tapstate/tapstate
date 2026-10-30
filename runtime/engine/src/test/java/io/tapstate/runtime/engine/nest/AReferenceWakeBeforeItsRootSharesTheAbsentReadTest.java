package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class AReferenceWakeBeforeItsRootSharesTheAbsentReadTest {

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc",
            nest("order", List.of("order_id"),
                    embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null)), tables());
    private static final NestLookup CUSTOMERS = TOPOLOGY.lookups().getFirst();
    private static final Map<String, Object> CUSTOMER = row("customer_id", "C1", "name", "Ada");

    @ParameterizedTest
    @CsvSource({"true, false", "false, true"})
    void aWakeThatOvertakesItsRootDoesNotAddASecondStateRead(boolean onlyIfWaiting, boolean firstFiling)
            throws Exception {
        Fixture f = new Fixture();
        f.customers.save(List.of("C1"), CUSTOMER);
        f.wake(onlyIfWaiting, firstFiling);
        assertThat(f.roots.reads).isEqualTo(1);
        assertThat(f.drain()).isEmpty();

        f.order();
        assertThat(f.roots.reads).isEqualTo(1);
        assertThat(f.drain()).extracting(Envelope::after).containsExactly(
                row("order_id", 1, "cust_ref", "C1", "customer", CUSTOMER));

        f.wake(onlyIfWaiting, firstFiling);
        assertThat(f.roots.reads).isEqualTo(1);
        assertThat(f.drain()).isEmpty();
    }

    @Test
    void anAbsentReadStopsBeingReusableWhenTheRootArrivesAndWaits() throws Exception {
        Fixture f = new Fixture();
        f.wake(true, false);
        f.order();
        assertThat(f.roots.reads).isEqualTo(1);
        assertThat(f.drain()).isEmpty();

        f.customers.save(List.of("C1"), CUSTOMER);
        f.wake(false, true);
        assertThat(f.roots.reads).isEqualTo(2);
        assertThat(f.drain()).extracting(Envelope::after).containsExactly(
                row("order_id", 1, "cust_ref", "C1", "customer", CUSTOMER));
    }

    private static final class Fixture {
        private final CountingStore roots = new CountingStore();
        private final HeapNestStore<Map<String, Object>> customers = new HeapNestStore<>();
        private final TestOutbox output = new TestOutbox(128);
        private final AssemblerProcessor assembler;

        private Fixture() throws Exception {
            assembler = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), roots,
                    "doc", null, null, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                    NestSendPolicy.everyChange(), new HeapNestStore<>(), (from, released) -> { },
                    Map.of(CUSTOMERS.mapName(), customers));
            assembler.init(output, new TestProcessorContext());
        }

        private void order() {
            feed(0, Envelope.insert(2L, "order", row("order_id", 1, "cust_ref", "C1"), null)
                    .withOrder(at(2)));
        }

        private void wake(boolean onlyIfWaiting, boolean firstFiling) {
            feed(CUSTOMERS.touchOrdinal(), new NestTouch(List.of(1), 1L, Map.of(), onlyIfWaiting, firstFiling));
        }

        private void feed(int ordinal, Object item) {
            TestInbox inbox = new TestInbox();
            inbox.add(item);
            assembler.process(ordinal, inbox);
            assertThat(inbox).isEmpty();
        }

        private List<Envelope> drain() {
            List<Object> sent = new ArrayList<>();
            output.drainQueueAndReset(0, sent, false);
            return sent.stream().map(Envelope.class::cast).toList();
        }
    }

    private static final class CountingStore implements NestStore<RootAssembly> {
        private final HeapNestStore<RootAssembly> delegate = new HeapNestStore<>();
        private int reads;

        @Override public RootAssembly load(Object key) { reads++; return delegate.load(key); }
        @Override public void save(Object key, RootAssembly state) { delegate.save(key, state); }
        @Override public void remove(Object key) { delegate.remove(key); }
        @Override public long count() { return delegate.count(); }
    }
}
