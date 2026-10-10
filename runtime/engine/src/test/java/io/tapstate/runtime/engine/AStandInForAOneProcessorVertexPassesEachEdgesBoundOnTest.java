package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.SourceOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What stands in for a vertex that runs one processor for the whole cluster, on each member it does not run on.
 *
 * <p>It takes no input, and everything reading the vertex waits for a bound from it as from the processor itself. So
 * it passes on what each edge promised for the chains that edge was compiled to carry, as the processor does; it says
 * nothing for an edge compiled to carry none, as the processor does not either; and it never repeats the bound the
 * engine combined across every edge, which on a vertex fed one table per edge never comes for any table at all.
 */
class AStandInForAOneProcessorVertexPassesEachEdgesBoundOnTest {

    private static final ChainAxes AXES = new FrontierBinding(Map.of("orders", "orders", "logs", "logs")).axes();

    @Test
    void eachEdgesBoundIsPassedOnForTheTableThatEdgeCarries() throws Exception {
        TestOutbox outbox = new TestOutbox(128);
        Processor standIn = standIn(Map.of(0, List.of("orders"), 1, List.of("logs")), outbox);

        assertThat(standIn.tryProcessWatermark(0, bound("orders", 5))).isTrue();
        assertThat(standIn.tryProcessWatermark(1, bound("logs", 3))).isTrue();

        assertThat(emitted(outbox))
                .as("each table's bound, on its own, the moment the one edge carrying it has promised it")
                .containsExactly(bound("orders", 5), bound("logs", 3));
    }

    @Test
    void anEdgeCompiledToCarryNoTableIsNotAnsweredForAndDoesNotEndTheRun() throws Exception {
        TestOutbox outbox = new TestOutbox(128);
        // As a level that files away pointed-at rows answers only for the edge bringing them.
        Processor standIn = standIn(Map.of(0, List.of("orders")), outbox);

        assertThat(standIn.tryProcessWatermark(1, bound("logs", 3))).isTrue();

        assertThat(emitted(outbox)).as("nothing said for an edge the vertex answers for no table on").isEmpty();
    }

    @Test
    void theBoundTheEngineCombinedAcrossEveryEdgeIsNotRepeated() throws Exception {
        TestOutbox outbox = new TestOutbox(128);
        Processor standIn = standIn(Map.of(0, List.of("orders"), 1, List.of("logs")), outbox);

        assertThat(standIn.tryProcessWatermark(bound("orders", 5))).isTrue();

        assertThat(emitted(outbox)).isEmpty();
    }

    @Test
    void aRowRoutedToTheStandInEndsTheRunNamingTheVertex() throws Exception {
        Processor standIn = standIn(Map.of(0, List.of("orders")), new TestOutbox(128));
        TestInbox inbox = new TestInbox();
        inbox.add("a row");

        assertThatThrownBy(() -> standIn.process(0, inbox))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'merge'");
    }

    private static Processor standIn(Map<Integer, List<String>> chainsByOrdinal, TestOutbox outbox) throws Exception {
        Processor standIn = new TotalOne.StandIns("merge", AXES, chainsByOrdinal).get(1).iterator().next();
        standIn.init(outbox, new TestProcessorContext());
        return standIn;
    }

    private static Watermark bound(String table, long seq) {
        return new Watermark(FrontierOrders.pack(table, new SourceOrder(1, seq)), AXES.axisOf(table));
    }

    private static List<Object> emitted(TestOutbox outbox) {
        List<Object> items = new ArrayList<>();
        outbox.drainQueueAndReset(0, items, false);
        return items;
    }
}
