package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.FrontierStall;
import io.tapstate.core.lifecycle.FrontierStallPressure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When a chain whose durable position has stopped moving is worth saying out loud. Nothing else about
 * such a pipeline moves — it goes on running with a zero error count, short queues and its usual
 * throughput, while the source keeps its log from the pinned position onwards and that log has a
 * retention. The condition is reported at its two edges, and what it carries has to say which of the two
 * pins it is, because they are worked on from opposite ends.
 */
class FrontierStallWatchTest {

    @Test
    void aDelayedForgetCannotDeleteTheReplacementsWindow() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);
        watch.saw("orders", Map.of("old", 90_000L), Map.of());
        Runnable cleanup = watch.captureForgetPipelinesOutside(List.of());
        watch.saw("orders", Map.of("new", 90_000L), Map.of());
        cleanup.run();
        watch.saw("orders", Map.of("new", 90_000L), Map.of());
        assertThat(watch.watching()).containsExactly("orders/new");
        assertThat(alert.crossed).containsExactly("orders/old", "orders/new");
    }

    private static final FrontierStallPressure ONE_MINUTE =
            new FrontierStallPressure(Duration.ofMinutes(1));

    @Test
    void reports_a_chain_pinned_past_the_threshold() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("order_items", 90_000L), Map.of("order_items", 0L));

        assertThat(alert.crossed).containsExactly("orders/order_items");
    }

    @Test
    void says_nothing_about_a_chain_pinned_for_less_than_the_threshold() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("order_items", 30_000L), Map.of("order_items", 0L));

        // Being pinned is not the fault - a child row waiting for its parent is pinned, and that is a
        // normal shape lasting seconds. Only being pinned for a long time is worth an operator's attention.
        assertThat(alert.crossed).isEmpty();
        assertThat(alert.cleared).isEmpty();
    }

    @Test
    void treats_reaching_the_threshold_exactly_as_reaching_it() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("order_items", 60_000L), Map.of());

        // The threshold is met from below by a continuous quantity, so the boundary belongs on the
        // reported side. Which side it falls on is otherwise decided by whichever pass happens to sample
        // it, and a chain sitting exactly on the line would report or not depending on the tick.
        assertThat(alert.crossed).containsExactly("orders/order_items");
    }

    @Test
    void reports_a_crossing_once_however_long_it_goes_on() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("order_items", 90_000L), Map.of());
        watch.saw("orders", Map.of("order_items", 150_000L), Map.of());
        watch.saw("orders", Map.of("order_items", 210_000L), Map.of());

        // A position pinned for hours is one thing that happened, not one per pass. Reporting it every
        // pass would bury the record it is written to under the same line.
        assertThat(alert.crossed).containsExactly("orders/order_items");
    }

    @Test
    void reports_a_chain_that_started_advancing_again() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("order_items", 90_000L), Map.of());
        watch.saw("orders", Map.of("order_items", 2_000L), Map.of());

        // The chain is still pinned - it holds something above its bound - but its position moved two
        // seconds ago, so the duration restarted. Never withdrawing the alarm would leave whoever read it
        // believing a chain that recovered is still stuck.
        assertThat(alert.cleared).containsExactly("orders/order_items");
    }

    @Test
    void does_not_call_a_chain_recovered_when_it_stops_being_reported() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("order_items", 90_000L), Map.of());
        watch.saw("orders", Map.of(), Map.of());

        // A chain that vanished either caught up or belongs to a job that stopped, and from here those are
        // the same reading while meaning opposite things: a stopped job's durable position is pinned
        // exactly where it was, and the retention window goes on being consumed with nothing running.
        // Calling that recovery is the one belief this alarm exists to prevent.
        assertThat(alert.cleared).isEmpty();
        assertThat(watch.watching()).isEmpty();
    }

    @Test
    void carries_the_distance_so_the_two_pins_are_told_apart() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("held", 90_000L, "starved", 90_000L),
                Map.of("held", 0L, "starved", 4_000L));

        // Both chains are pinned for the same time and the fix for each is at the opposite end of the
        // pipeline: one is held back by changes still pending upstream, the other has a bound running on
        // past positions the sink was never given. An alarm that carried only the duration would send an
        // operator looking without saying where. Order is not part of the claim - one pass's chains come
        // out of a map, and pinning an order here would fail on a rehash rather than on a defect.
        assertThat(alert.starved).containsExactlyInAnyOrder("held=false", "starved=true");
    }

    @Test
    void keeps_a_chain_with_no_distance_apart_from_one_at_zero() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("orders", Map.of("order_items", 90_000L), Map.of());

        // No distance is reported for a chain that never advanced at all, and that absence is not a zero:
        // zero means a frontier sitting exactly on its bound, which is a chain that has been advancing.
        // Defaulting the absence to zero would file the worst case under the milder one.
        assertThat(alert.gaps).containsExactly("order_items=absent");
    }

    @Test
    void judges_each_pipeline_s_chains_on_their_own() {
        RecordingAlert alert = new RecordingAlert();
        FrontierStallWatch watch = new FrontierStallWatch(ONE_MINUTE, alert);

        watch.saw("one", Map.of("order_items", 90_000L), Map.of());
        watch.saw("two", Map.of("order_items", 90_000L), Map.of());

        // A chain is named for the table it carries, so two pipelines reading the same table report the
        // same chain name. Held under one name they would be each other's previous reading, and the second
        // pipeline's stall would be swallowed as "already reported".
        assertThat(alert.crossed).containsExactly("one/order_items", "two/order_items");
    }

    /** Records which chains were reported crossed and cleared, and what each crossing carried. */
    private static final class RecordingAlert implements FrontierStallAlert {

        private final List<String> crossed = new ArrayList<>();
        private final List<String> cleared = new ArrayList<>();
        private final List<String> starved = new ArrayList<>();
        private final List<String> gaps = new ArrayList<>();

        @Override
        public void crossed(String pipelineId, FrontierStall stall) {
            crossed.add(pipelineId + "/" + stall.chain());
            starved.add(stall.chain() + "=" + stall.starvedOfPositions());
            gaps.add(stall.chain() + "=" + (stall.gap().isEmpty()
                    ? "absent" : Long.toString(stall.gap().getAsLong())));
        }

        @Override
        public void cleared(String pipelineId, FrontierStall stall) {
            cleared.add(pipelineId + "/" + stall.chain());
        }
    }
}
