package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Every change of one row is routed alike, wherever in the change its key is read from, so every change of it reaches
 * one processor and is applied there in the order it was read.
 *
 * <p>An insert and an update carry the row's key in their later image; a removal has no later image and carries it
 * in its earlier one. A key of several columns is read column by column and encoded with each value behind its
 * length, so the same values in the same columns route alike and no two rows share a route because their values run
 * together. Into a sink, a row goes by the table it lands in and its key there, never by the stream it came in on:
 * two source tables written into one target table meet on one writer for the same key. A table with no key goes by
 * the table alone.
 *
 * <p>Two changes cannot be routed by their row and end the run with a code instead: one whose key cannot be read,
 * and one that moves its row from one key to another - there is no processor that would see every change of both
 * keys in order.
 */
class EveryChangeOfARowIsRoutedByTheRowItChangesTest {

    private static final List<String> REGION_AND_ID = List.of("region", "id");

    @Test
    void anInsertAnUpdateAndARemovalOfOneRowOfACompositeKeyRouteAlike() {
        Map<String, List<String>> keys = Map.of("orders", REGION_AND_ID);
        Object inserted = RoutingKeys.keyOf("step", keys, Envelope.insert(1L, "orders", row("eu", 7, "new"), null));
        Object updated = RoutingKeys.keyOf("step", keys,
                Envelope.update(2L, "orders", row("eu", 7, "new"), row("eu", 7, "paid"), null));
        Object removed = RoutingKeys.keyOf("step", keys, Envelope.delete(3L, "orders", row("eu", 7, "paid"), null));

        assertThat(updated).as("the update's later image carries the same key").isEqualTo(inserted);
        assertThat(removed).as("the removal's earlier image carries the same key").isEqualTo(inserted);
    }

    @Test
    void rowsThatShareOnlyPartOfACompositeKeyRouteApart() {
        Map<String, List<String>> keys = Map.of("orders", REGION_AND_ID);

        assertThat(RoutingKeys.keyOf("step", keys, Envelope.insert(1L, "orders", row("eu", 7, "new"), null)))
                .as("the same id in another region is another row")
                .isNotEqualTo(RoutingKeys.keyOf("step", keys,
                        Envelope.insert(1L, "orders", row("us", 7, "new"), null)));
        Map<String, Object> runTogether = new LinkedHashMap<>(Map.of("region", "e", "id", "u7"));
        Map<String, Object> apart = new LinkedHashMap<>(Map.of("region", "eu", "id", "7"));
        assertThat(RoutingKeys.keyOf("step", keys, Envelope.insert(1L, "orders", runTogether, null)))
                .as("values whose characters run together are still two different keys")
                .isNotEqualTo(RoutingKeys.keyOf("step", keys, Envelope.insert(1L, "orders", apart, null)));
    }

    @Test
    void twoSourceTablesWrittenIntoOneTargetTableMeetOnOneWriterForOneKey() {
        Map<String, SinkTarget> targets = Map.of(
                "eu_orders", new SinkTarget("orders", REGION_AND_ID),
                "us_orders", new SinkTarget("orders", REGION_AND_ID),
                "notes", new SinkTarget("orders", REGION_AND_ID));

        Object fromOne = RoutingKeys.sinkKeyOf("sink", targets,
                Envelope.insert(1L, "eu_orders", row("eu", 7, "new"), null));
        Object fromTheOther = RoutingKeys.sinkKeyOf("sink", targets,
                Envelope.delete(2L, "us_orders", row("eu", 7, "new"), null));

        assertThat(fromTheOther).as("one row of the target, whichever stream brought its change").isEqualTo(fromOne);
    }

    @Test
    void aTableWithNoKeyGoesByTheTableAloneAndAnotherTableIsNotHeldToIt() {
        Map<String, SinkTarget> targets = Map.of(
                "events", new SinkTarget("events", List.of()),
                "orders", new SinkTarget("orders", REGION_AND_ID));

        Object first = RoutingKeys.sinkKeyOf("sink", targets, Envelope.insert(1L, "events", row("eu", 1, "a"), null));
        Object second = RoutingKeys.sinkKeyOf("sink", targets, Envelope.insert(2L, "events", row("us", 2, "b"), null));
        Object schema = RoutingKeys.sinkKeyOf("sink", targets, Envelope.ddl(3L, "events", Map.of()));
        Object other = RoutingKeys.sinkKeyOf("sink", targets, Envelope.insert(4L, "orders", row("eu", 1, "a"), null));

        assertThat(second).as("every row of a keyless table goes one way").isEqualTo(first);
        assertThat(schema).as("and so does a change of its schema").isEqualTo(first);
        assertThat(other).as("a keyed table's rows are routed by their own key").isNotEqualTo(first);
    }

    @Test
    void aChangeWhoseKeyCannotBeReadEndsTheRunWithACode() {
        Map<String, Object> noRegion = new LinkedHashMap<>(Map.of("id", 7, "status", "new"));

        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> RoutingKeys.keyOf("step", Map.of("orders", REGION_AND_ID),
                        Envelope.insert(1L, "orders", noRegion, null)));

        assertThat(refused.code()).isEqualTo(EngineError.ROUTING_KEY_MISSING);
        assertThat(refused.args()).containsEntry("node", "step").containsEntry("stream", "orders")
                .containsEntry("columns", "region, id");
    }

    @Test
    void aChangeThatMovesItsRowToAnotherKeyEndsTheRunWithACode() {
        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> RoutingKeys.sinkKeyOf("sink", Map.of("orders", new SinkTarget("orders", REGION_AND_ID)),
                        Envelope.update(1L, "orders", row("eu", 7, "new"), row("us", 7, "new"), null)));

        assertThat(refused.code()).isEqualTo(EngineError.KEY_CHANGE_ON_PARALLEL_NODE);
        assertThat(refused.args()).containsEntry("node", "sink").containsEntry("stream", "orders");
    }

    @Test
    void anUpdateWithoutAWholeEarlierKeyIsRoutedByItsLaterOne() {
        // A change whose earlier image does not carry the whole key says nothing about the key it had, so it is
        // not a move: it goes where the row's later image says, like an insert of that row.
        Map<String, List<String>> keys = Map.of("orders", REGION_AND_ID);
        Map<String, Object> partial = new LinkedHashMap<>(Map.of("id", 7));

        assertThat(RoutingKeys.keyOf("step", keys,
                        Envelope.update(1L, "orders", partial, row("eu", 7, "paid"), null)))
                .isEqualTo(RoutingKeys.keyOf("step", keys, Envelope.insert(1L, "orders", row("eu", 7, "new"), null)));
    }

    private static Map<String, Object> row(String region, int id, String status) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("region", region);
        row.put("id", id);
        row.put("status", status);
        return row;
    }
}
