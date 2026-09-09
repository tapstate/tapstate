package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.SinkProcessor;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

/**
 * Measures what a send costs and what the window takes off it, which is the pair the published limits are
 * worked out from. The limits themselves are requirement-side algebra - "your data shape asks for this
 * much" - and algebra is not a measurement; these are the two numbers underneath it that can be wrong.
 *
 * <p><b>What is measured here is a ratio and a count, not a rate.</b> How many documents a second this
 * machine can push through a socket says nothing about anyone else's; how much of a root's change rate
 * survives the window, and how many documents one round trip carries, are properties of the design and
 * travel. The costs that are properties of an endpoint - what one durable write costs, what a cold read
 * costs - are measured against a real endpoint elsewhere and are not re-derived here.
 *
 * <p>This is an instrument, not a regression test: wall-clock numbers do not belong in a build gate. Its
 * name keeps it out of the default surefire selection, so it costs a build nothing and is run
 * deliberately:
 *
 * <pre>{@code mvn -o test -pl runtime/engine -am -Dtest=NestSendCostBench -Dsurefire.failIfNoSpecifiedTests=false}</pre>
 */
class NestSendCostBench {

    private static final long WINDOW_MILLIS = NestSettings.DEFAULT_SEND_WINDOW_MILLIS;

    /** How long each rate is driven for. Long enough that the window boundaries stop being noise. */
    private static final long RUN_MILLIS = 2_000L;

    /** Change rates to walk, in changes per second against a single root. */
    private static final List<Integer> RATES = List.of(50, 100, 200, 400, 800);

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    @Test
    void whatTheWindowTakesOffAHotRoot() throws Exception {
        System.out.println();
        System.out.printf("nest send window = %d ms, one root, %d ms per rate%n", WINDOW_MILLIS, RUN_MILLIS);
        System.out.println("  rate/s   changes     sends   changes per send   state writes   per change");
        for (int rate : RATES) {
            Measured measured = drive(rate);
            System.out.printf("  %6d   %7d   %7d   %16.1f   %12d   %10.2f%n", rate, measured.changes,
                    measured.sends, measured.changes / (double) measured.sends, measured.writes,
                    measured.writes / (double) measured.changes);
        }
        System.out.println();
        System.out.println("  A root changing slower than the window is not folded at all: every change is");
        System.out.println("  its own leading edge. Above that, sends flatten at 1000/window per second and");
        System.out.println("  the ratio is whatever the rate divided by that comes to.");
        System.out.println();
        System.out.println("  The state writes are the point of the last two columns. The window rations");
        System.out.println("  sending and reaches nothing else: a state that skipped a change would be wrong");
        System.out.println("  rather than stale, so every change is written through whatever is sent.");
        System.out.println("  The count is state writes = changes + windows flushed, slightly above one per");
        System.out.println("  change rather than below it - each flush stores the document again once what");
        System.out.println("  it carried has been released. So the state side costs marginally MORE with a");
        System.out.println("  window than without, and the limits worked out from it do not move.");
    }

    /**
     * How many sends {@code changesPerSecond} produces against one root over {@link #RUN_MILLIS}. The
     * assembler is driven the way the engine drives it - a drain per change, and a turn with nothing
     * arriving in between - so what is being measured includes the sweep that ends a window.
     */
    private Measured drive(int changesPerSecond) throws Exception {
        CountingStore store = new CountingStore();
        TestOutbox outbox = new TestOutbox(4096);
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), store,
                "doc", null, null, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.within(WINDOW_MILLIS));
        processor.init(outbox, new TestProcessorContext());

        long gapNanos = TimeUnit.SECONDS.toNanos(1) / changesPerSecond;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RUN_MILLIS);
        long changes = 0;
        long sends = 0;
        long seq = 0;
        while (System.nanoTime() < deadline) {
            long due = System.nanoTime() + gapNanos;
            TestInbox inbox = new TestInbox();
            inbox.queue().add(customer(++seq, "C1", "name-" + seq));
            processor.process(0, inbox);
            changes++;
            sends += drain(outbox);
            while (System.nanoTime() < due) {
                processor.tryProcess();
                sends += drain(outbox);
                LockSupport.parkNanos(100_000L);
            }
        }
        processor.complete();
        sends += drain(outbox);
        return new Measured(changes, sends, store.writes);
    }

    @Test
    void howManyDocumentsOneRoundTripCarries() throws Exception {
        System.out.println();
        System.out.println("sink, one write in flight: how many documents one write call carries");
        System.out.println("  queued   write calls   largest batch");
        for (int queued : List.of(1, 16, 256, 4_096)) {
            CountingWriter writer = new CountingWriter();
            SinkProcessor sink = new SinkProcessor(writer, 1, 1_024);
            sink.init(new TestOutbox(16), new TestProcessorContext());
            TestInbox inbox = new TestInbox();
            for (int i = 0; i < queued; i++) {
                inbox.queue().add(customer(i + 1L, "C" + i, "name"));
            }
            while (!inbox.isEmpty()) {
                sink.process(0, inbox);
            }
            System.out.printf("  %6d   %11d   %13d%n", queued, writer.calls, writer.largest);
        }
        System.out.println();
        System.out.println("  One round trip is paid per batch, not per document, so the cost of the sink");
        System.out.println("  side falls as the backlog grows - which is the opposite of the shape a");
        System.out.println("  per-document reading of the published limit would suggest.");
    }

    private static int drain(TestOutbox outbox) {
        List<Object> out = new ArrayList<>();
        outbox.drainQueueAndReset(0, out, false);
        return out.size();
    }

    private static Envelope customer(long seq, String id, String name) {
        Map<String, Object> fields = new LinkedHashMap<>(row("customer_id", id, "name", name));
        return Envelope.insert(seq, "customer", fields, null).withOrder(new SourceOrder(1L, seq));
    }

    private record Measured(long changes, long sends, long writes) {
    }

    /**
     * A store that counts what was written through it. Counting rather than timing: how long one durable
     * write takes belongs to an endpoint, but how many of them one change costs belongs to the design and
     * is the same on every machine.
     */
    private static final class CountingStore implements NestStore<RootAssembly> {

        private final Map<Object, RootAssembly> entries = new LinkedHashMap<>();
        private long writes;

        @Override
        public RootAssembly load(Object key) {
            return entries.get(key);
        }

        @Override
        public void save(Object key, RootAssembly state) {
            writes++;
            entries.put(key, state);
        }

        @Override
        public void remove(Object key) {
            entries.remove(key);
        }

        @Override
        public long count() {
            return entries.size();
        }
    }

    /** A target that answers at once: what is being counted is how the batches were formed, not latency. */
    private static final class CountingWriter implements SinkWriter {

        private int calls;
        private int largest;

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            calls++;
            largest = Math.max(largest, records.size());
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }
}
