package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The round trip through a run's statistics, where a reading is a bare number under a flat name and what
 * it is about has to ride in the name. Splitting that name back into the dimensions it was taken with is
 * the whole of what these cover, and the case they exist for is the ordinary one: a schema-qualified table
 * has a dot in it, so a split that assumes the last dot separates the parts quietly reports a table called
 * {@code orders} for one called {@code sales.orders} — and then two tables of different schemas share a
 * single reading with nothing saying so.
 */
class ADeliveryNameSurvivesATableWithADotInItTest {

    @Test
    void splits_an_operation_from_a_plain_table() {
        JetDeliveryGauge.Delivered delivered = JetDeliveryGauge.deliveredOf("recordsOut.i.orders");

        assertThat(delivered).isEqualTo(new JetDeliveryGauge.Delivered("i", "orders"));
    }

    @Test
    void keeps_the_whole_of_a_schema_qualified_table() {
        JetDeliveryGauge.Delivered delivered = JetDeliveryGauge.deliveredOf("recordsOut.u.sales.orders");

        // Everything past the first dot is the table. The operation is one of a closed set of symbols with
        // no dot in any of them, which is why it is the part that goes in front.
        assertThat(delivered).isEqualTo(new JetDeliveryGauge.Delivered("u", "sales.orders"));
    }

    @Test
    void keeps_the_whole_of_a_table_qualified_twice_over() {
        JetDeliveryGauge.Delivered delivered =
                JetDeliveryGauge.deliveredOf("recordsOut.d.warehouse.sales.orders");

        assertThat(delivered).isEqualTo(new JetDeliveryGauge.Delivered("d", "warehouse.sales.orders"));
    }

    @Test
    void reads_nothing_out_of_a_name_that_is_not_one_of_these() {
        // A job's statistics carry every reading anything in the run publishes, this sink's among them, so
        // recognising only its own is what keeps the others from being read as deliveries.
        assertThat(JetDeliveryGauge.deliveredOf("frontierGap.orders")).isNull();
        assertThat(JetDeliveryGauge.deliveredOf("recordsOut.")).isNull();
        assertThat(JetDeliveryGauge.deliveredOf("recordsOut.i")).isNull();
        assertThat(JetDeliveryGauge.deliveredOf("recordsOut.i.")).isNull();
        assertThat(JetDeliveryGauge.deliveredOf("recordsOut..orders")).isNull();
    }

    @Test
    void reads_a_table_out_of_a_newest_settled_reading_dots_and_all() {
        assertThat(JetDeliveryGauge.reachedTableOf("outEventTime.orders")).isEqualTo("orders");
        assertThat(JetDeliveryGauge.reachedTableOf("outEventTime.sales.orders")).isEqualTo("sales.orders");
        assertThat(JetDeliveryGauge.reachedTableOf("outEventTime.")).isNull();
        assertThat(JetDeliveryGauge.reachedTableOf("recordsOut.i.orders")).isNull();
    }

    @Test
    void a_stream_or_table_with_nothing_waiting_any_more_is_set_back_to_zero() {
        // A run's statistics keep whatever was last set under a name: a writer whose orders queue emptied would
        // otherwise go on reading as having orders waiting.
        Map<String, Long> readings = JetDeliveryGauge.waitingReadings(Map.of("sales.customers", 3L),
                Map.of("sales.orders", 9L), Set.of("sinkQueued.sales.orders", "sinkQueued.sales.customers"));

        assertThat(readings).containsOnly(
                Map.entry("sinkQueued.sales.orders", 0L),
                Map.entry("sinkQueued.sales.customers", 3L),
                Map.entry("sinkInFlight.sales.orders", 9L));
        assertThat(SinkWaitingMetricNames.streamOfQueued("sinkQueued.sales.orders")).isEqualTo("sales.orders");
        assertThat(SinkWaitingMetricNames.tableOfInFlight("sinkInFlight.sales.orders")).isEqualTo("sales.orders");
        assertThat(SinkWaitingMetricNames.streamOfQueued("sinkInFlight.sales.orders")).isNull();
    }

    @Test
    void tells_the_two_kinds_of_reading_apart() {
        // They ride the same collection, and a prefix that was a prefix of the other would have each read
        // as the other's.
        assertThat(JetDeliveryGauge.deliveredOf("outEventTime.orders")).isNull();
        assertThat(JetDeliveryGauge.reachedTableOf("recordsOut.i.orders")).isNull();
    }
}
