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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * On the ordinary path a nest vertex reaches for one thing that is not on its own partition, and it is
 * named here.
 *
 * <p>Keeping state in a distributed map is only worth anything while the vertex and the entries it needs
 * are on the same member. One reach across the cluster per event is the shape this whole design was chosen
 * over - it was rejected by name - and it costs nothing visible: every document still assembles, every
 * count is still right, and the job is merely slower on a cluster nobody is running yet.
 *
 * <p>So rather than asserting that no vertex ever reaches across, which stopped being true the day a
 * document could point at a row it does not hold, this writes down the whole of what may: the rows another
 * vertex filed, read and never written. Read, because the entry is a row's fields and a document only
 * renders them; never written, because one writer per entry is what lets the reader tolerate an old value
 * instead of racing for a current one. Anything else arriving on this path - a second namespace, or a
 * write through this one - is the rejected shape coming back, and this is what says so.
 *
 * <p>What is kept between documents is deliberately outside this. It is reached only while a subtree is
 * moving from one key to another, which is rare, and it is addressed by where the subtree is rather than
 * by either key - so it belongs to neither partition and is not on the ordinary path at all. The case
 * below shows it is not touched there.
 */
class TheOnlyStateANestReachesAcrossForIsARowItPointsAtTest {

    /** Orders that point at a customer: the root holds a row it does not carry. */
    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null));

    private NestTopology topology;

    @BeforeEach
    void compileTheTree() {
        topology = NestTopology.compile("p", "doc", TREE, tables());
    }

    @Test
    void whatIsKeptBetweenDocumentsIsNotReachedForOnTheOrdinaryPath() {
        Recording<RootAssembly> own = new Recording<>();
        Recording<ParkedSubtree> parked = new Recording<>();
        Recording<Map<String, Object>> pointedAt = new Recording<>();
        drain(own, parked, pointedAt);

        assertThat(own.touched())
                .describedAs("the assembler kept something for this change, so there is something to place")
                .isNotEmpty();
        assertThat(parked.touched())
                .describedAs("nothing is moving between documents, so what is kept between them is not on "
                        + "this path at all - it is addressed by where a subtree is rather than by either "
                        + "key, and reaching it here would be reaching across for every ordinary change")
                .isEmpty();
    }

    @Test
    void theRowsItPointsAtAreTheOneThingItReachesAcrossFor() {
        Recording<RootAssembly> own = new Recording<>();
        Recording<ParkedSubtree> parked = new Recording<>();
        Recording<Map<String, Object>> pointedAt = new Recording<>();
        pointedAt.delegate.save(NestKeys.valuesOf(row("customer_id", "C-1"), List.of("customer_id")),
                row("customer_id", "C-1", "name", "Ada"));
        pointedAt.reads.clear();
        pointedAt.writes.clear();

        drain(own, parked, pointedAt);

        assertThat(pointedAt.reads)
                .describedAs("the document points at a row, so rendering it went and got one - without "
                        + "that this case would be asserting about a path nothing walked")
                .isNotEmpty();
        assertThat(pointedAt.reads)
                .describedAs("and what it went and got is addressed by something that is not a key of this "
                        + "vertex, which is what makes it a reach at all. Without this the case would pass "
                        + "just as well on a fixture where the two sat together, and would be saying "
                        + "nothing about what may be reached across for")
                .doesNotContainAnyElementsOf(own.touched());
        assertThat(pointedAt.writes)
                .describedAs("and only ever read. One writer per entry is what lets this reader take an "
                        + "old value rather than race for the current one, and a write from here is that "
                        + "argument gone - silently, since the value written would be the right one")
                .isEmpty();
    }

    private void drain(Recording<RootAssembly> own, Recording<ParkedSubtree> parked,
            Recording<Map<String, Object>> pointedAt) {
        AssemblerProcessor processor = assembler(own, parked,
                Map.of(topology.lookups().get(0).mapName(), pointedAt));
        TestOutbox outbox = new TestOutbox(128);
        try {
            processor.init(outbox, new TestProcessorContext());
        } catch (Exception cause) {
            throw new IllegalStateException("could not start the assembler", cause);
        }
        TestInbox inbox = new TestInbox();
        inbox.queue().add(Envelope.insert(1L, "src", row("order_id", "O-1", "customer_id", "C-1"), null)
                .withOrder(new SourceOrder(1L, 1L)));
        processor.process(topology.assembler().inboundFor(List.of()).ordinal(), inbox);
    }

    private AssemblerProcessor assembler(Recording<RootAssembly> own, Recording<ParkedSubtree> parked,
            Map<String, NestStore<Map<String, Object>>> pointedAt) {
        return new AssemblerProcessor(topology.assembler(), topology.slots(), own, "doc", null, null,
                ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.everyChange(), parked, (from, released) -> { }, pointedAt);
    }

    /** A store that answers like any other and remembers what it was read for and written to. */
    private static final class Recording<S> implements NestStore<S> {

        private static final long serialVersionUID = 1L;

        private final transient Set<Object> reads = new LinkedHashSet<>();
        private final transient Set<Object> writes = new LinkedHashSet<>();
        private final HeapNestStore<S> delegate = new HeapNestStore<>();

        Set<Object> touched() {
            Set<Object> all = new LinkedHashSet<>(reads);
            all.addAll(writes);
            return all;
        }

        @Override
        public S load(Object key) {
            reads.add(key);
            return delegate.load(key);
        }

        @Override
        public Map<Object, S> loadAll(Collection<Object> keys) {
            reads.addAll(keys);
            return delegate.loadAll(keys);
        }

        @Override
        public void save(Object key, S state) {
            writes.add(key);
            delegate.save(key, state);
        }

        @Override
        public void remove(Object key) {
            writes.add(key);
            delegate.remove(key);
        }

        @Override
        public long count() {
            return delegate.count();
        }
    }
}
