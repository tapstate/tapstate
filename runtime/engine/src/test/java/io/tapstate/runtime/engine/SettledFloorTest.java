package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkFrontier.ChainEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * How far a sink may say it has landed when what reaches it was assembled from several chains at once.
 * Nothing that arrives here says by itself how far a chain has travelled — a document is drawn from
 * changes of several chains, and the events of one chain reach the sink out of their own order — so the
 * frontier is the meeting of two separate things: the entries the sink has proven durable, and the bound
 * the engine combined across every input queue for that chain. The highest entry at or below that bound
 * is what may be acked, and nothing above it.
 */
class SettledFloorTest {

    private static final ChainAxes AXES = ChainAxes.assign(List.of("orders", "lines"));

    @Test
    void a_routed_writer_covers_sibling_positions_without_acknowledging_their_tokens() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = SettledFloor.forRoutedWriter(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1), acked);
        floor.bound(boundAt("orders", 3), acked);

        assertThat(floor.gaps()).containsExactly(entry("orders", 0L));
        assertThat(floor.stalls()).isEmpty();
        assertThat(acked.calls).containsExactly("orders=w1");

        // A later local row still needs the upstream bound to cover it. Accounting for siblings must
        // neither hide that hold nor acknowledge the row before the bound reaches it.
        floor.settled(entries("orders", 5), acked);
        assertThat(floor.stalls()).containsOnlyKeys("orders");
        assertThat(acked.calls).containsExactly("orders=w1");
        floor.bound(boundAt("orders", 5), acked);
        assertThat(floor.stalls()).isEmpty();
        assertThat(acked.calls).containsExactly("orders=w1", "orders=w5");
    }

    @Test
    void advances_to_the_highest_settled_entry_at_or_below_the_bound() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 2), acked);

        // 3 is settled but the bound only reaches 2: everything at or below 2 is proven to have arrived,
        // while a change of another instance at 3 may still be in flight.
        assertThat(acked.calls).containsExactly("orders=w2");
    }

    @Test
    void says_nothing_until_a_bound_arrives() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2, 3), acked);

        // Settled is not the same as covered: what is durable here says nothing about what is still on its
        // way to here. Acking the highest settled entry is exactly the frontier lying.
        assertThat(acked.calls).isEmpty();
    }

    /**
     * The shape a parting raise arrives in. Once every queue behind an edge has finished there is nothing
     * left to take a lowest of, and the engine offers the highest value any of those queues ever reported -
     * a maximum where every other step took a minimum, and one that can land far above anything this sink
     * was ever given. An instance that finished while still holding a change it never sent is exactly the
     * case such a bound would carry a frontier over.
     *
     * <p>What stops it is not that the raise fails to arrive - whether it is handed to a processor at all is
     * the engine's scheduling to decide, and it has been observed both ways. It is that a position is only
     * ever advanced to because it settled here, so a bound above everything settled moves the frontier no
     * further than the last batch that actually arrived. The value being unreachable is what
     * {@link SettledFloor#gaps()} then reports as a distance, which is a reading rather than a hazard.
     */
    @Test
    void a_bound_far_above_everything_settled_reaches_only_what_settled() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 100), acked);

        assertThat(acked.calls)
                .describedAs("a frontier may only reach a position that arrived and settled here, whatever "
                        + "value a bound carries")
                .containsExactly("orders=w3");
    }

    @Test
    void advances_nothing_when_the_bound_is_below_every_settled_entry() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 5, 6), acked);
        floor.bound(boundAt("orders", 4), acked);

        assertThat(acked.calls).isEmpty();
    }

    @Test
    void a_bound_that_climbs_advances_the_frontier_without_another_batch() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 1), acked);
        floor.bound(boundAt("orders", 3), acked);

        // The last batch of a run settles before the bound that covers it arrives. Waiting for a further
        // batch to notice would leave the durable position short of what is provably durable, for as long
        // as the source stays quiet.
        assertThat(acked.calls).containsExactly("orders=w1", "orders=w3");
    }

    @Test
    void tracks_each_chain_against_its_own_bound() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2), acked);
        floor.settled(entries("lines", 7, 8), acked);
        floor.bound(boundAt("orders", 2), acked);
        floor.bound(boundAt("lines", 7), acked);

        // Two chains are two independent promises: one running ahead never carries the other with it.
        assertThat(acked.calls).containsExactly("orders=w2", "lines=w7");
    }

    @Test
    void never_acks_the_same_entry_twice() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2), acked);
        floor.bound(boundAt("orders", 2), acked);
        floor.bound(boundAt("orders", 2), acked);
        floor.settled(entries("orders", 3), acked);

        // A repeated bound and a batch that adds nothing beneath it are both silence, not a second write
        // of the same position.
        assertThat(acked.calls).containsExactly("orders=w2");
    }

    @Test
    void never_lets_a_later_low_entry_pull_the_frontier_back() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 5), acked);
        floor.bound(boundAt("orders", 9), acked);
        floor.settled(entries("orders", 2), acked);

        // A batch settling beneath what was already acked can only mean batches settled out of order,
        // which the single write in flight rules out. It is never rewritten backwards: a durable position
        // that moves down is one a restart replays from, past changes that were already acked away.
        assertThat(acked.calls).containsExactly("orders=w5");
    }

    @Test
    void carries_a_snapshot_row_through_with_no_token_of_its_own() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        Envelope snapshotRow = Envelope.read(1L, "orders", Map.of("id", 1), null)
                .withOrder(SourceOrder.snapshotRow(1));
        floor.settled(floor.positions(List.of(snapshotRow)), acked);
        floor.bound(new Watermark(FrontierOrders.pack("orders", SourceOrder.snapshotRow(1)),
                AXES.axisOf("orders")), acked);

        // A snapshot row is ordered but is not a spot in a change stream, so it carries no token. Taking
        // part is decided on the order; what gets persisted for it is the chain's cdc start, which is the
        // ack binding's to resolve. Skipping it because the token is absent stalls a snapshot-only run's
        // frontier at nothing at all.
        assertThat(acked.calls).containsExactly("orders=<no token>");
    }

    @Test
    void an_event_with_no_order_takes_no_part() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        Envelope synthetic = Envelope.insert(1L, "orders", Map.of("id", 1), null);
        assertThat(floor.positions(List.of(synthetic))).isEmpty();
    }

    @Test
    void takes_every_position_of_a_batch_rather_than_its_highest() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(floor.positions(List.of(event("orders", 1), event("orders", 2), event("orders", 3))),
                acked);
        floor.bound(boundAt("orders", 2), acked);

        // Reducing a batch to its highest position would leave the frontier stuck behind a bound that
        // falls inside a batch: with only 3 kept, a bound at 2 finds nothing to advance to.
        assertThat(acked.calls).containsExactly("orders=w2");
    }

    @Test
    void keeps_what_it_holds_for_a_chain_within_its_capacity_line() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, 8);

        for (int seq = 1; seq <= 100; seq++) {
            floor.settled(entries("orders", seq), acked);
        }

        // Entries are only dropped when the frontier passes them, so a bound that stalls - one dangling
        // reference is enough - grows this at the event rate inside the sink's own process, where the
        // guards over the assembled state cannot see it.
        assertThat(floor.held("orders")).isLessThanOrEqualTo(8);
    }

    @Test
    void thinning_keeps_the_highest_it_holds_however_many_entries_it_thinned() {
        // Every length, because where the last entry falls against the thinning decides whether keeping the
        // highest is really being exercised: one length alone passes on the arithmetic rather than the rule.
        for (int highest = 9; highest <= 40; highest++) {
            RecordingAck acked = new RecordingAck();
            SettledFloor floor = new SettledFloor(AXES, 8);

            for (int seq = 1; seq <= highest; seq++) {
                floor.settled(entries("orders", seq), acked);
            }
            floor.bound(boundAt("orders", highest), acked);

            // Thinning loses precision, never correctness: a dropped entry only makes the floor lower, so
            // the frontier is more conservative. Dropping the highest costs an advance that is provably
            // safe, and a frontier that never reaches what is already durable is what burns a retention
            // window while every indicator reads healthy.
            assertThat(acked.calls).as("highest settled position %d", highest)
                    .containsExactly("orders=w" + highest);
        }
    }

    @Test
    void a_thinned_chain_still_advances_to_an_entry_at_or_below_the_bound() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, 8);

        for (int seq = 1; seq <= 100; seq++) {
            floor.settled(entries("orders", seq), acked);
        }
        floor.bound(boundAt("orders", 50), acked);

        assertThat(acked.calls).hasSize(1);
        int reached = Integer.parseInt(acked.calls.get(0).replaceAll("\\D+", ""));
        assertThat(reached).isLessThanOrEqualTo(50);
    }

    @Test
    void reports_how_far_a_chain_s_bound_runs_ahead_of_what_it_reached() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2), acked);
        floor.bound(boundAt("orders", 5), acked);

        // The bound reached 5, the highest entry at or below it was 2, and the three positions between
        // them are what the frontier could have covered and did not.
        assertThat(floor.gaps()).containsExactly(entry("orders", 3L));
    }

    @Test
    void reports_no_gap_for_a_frontier_its_bound_is_holding_back() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        for (int seq = 1; seq <= 10; seq++) {
            floor.settled(entries("orders", seq), acked);
        }
        floor.bound(boundAt("orders", 3), acked);

        // Seven entries sit settled above the bound, and none of them is a gap: the frontier is exactly
        // where its bound lets it be. This is the reading that tells the two stalls apart - a frontier
        // pinned by real pending upstream reports nothing here however long it sits, so a gap that grows
        // can only be the other one, and the two are worked on from opposite ends.
        assertThat(floor.gaps()).containsExactly(entry("orders", 0L));
    }

    @Test
    void widens_the_gap_as_a_bound_climbs_with_nothing_settling_beneath_it() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1), acked);
        floor.bound(boundAt("orders", 1), acked);
        List<Long> widening = new ArrayList<>();
        for (int reached : new int[] {10, 20, 30}) {
            floor.bound(boundAt("orders", reached), acked);
            widening.add(floor.gaps().get("orders"));
        }

        // Nothing new settles, so every bound finds the same entry to advance to and the distance from it
        // grows. A frontier starved of positions to advance to looks exactly like this and is the only
        // thing that does; without it the starvation is indistinguishable from being held back.
        assertThat(widening).containsExactly(9L, 19L, 29L);
    }

    @Test
    void reports_nothing_for_a_chain_that_has_never_reached_a_position() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 5, 6), acked);
        floor.bound(boundAt("orders", 4), acked);

        // A gap is measured from the position the frontier reached, and this chain has reached none. A
        // zero here would read as a frontier keeping up with its bound, which is the opposite of the case.
        assertThat(floor.gaps()).isEmpty();
    }

    @Test
    void reports_nothing_for_a_chain_no_bound_has_arrived_for() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1, 2), acked);

        assertThat(floor.gaps()).isEmpty();
    }

    @Test
    void measures_each_chain_against_its_own_bound() {
        RecordingAck acked = new RecordingAck();
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN);

        floor.settled(entries("orders", 1), acked);
        floor.settled(entries("lines", 1), acked);
        floor.bound(boundAt("orders", 1), acked);
        floor.bound(boundAt("lines", 8), acked);

        // One chain starved while another keeps up is the case worth seeing; a single number over the two
        // would report the average of a healthy chain and a stalled one and read as neither.
        assertThat(floor.gaps()).containsOnly(entry("orders", 0L), entry("lines", 7L));
    }

    @Test
    void reports_how_long_a_chain_has_been_held_short_of_its_bound() {
        RecordingAck acked = new RecordingAck();
        Ticking clock = new Ticking(1_000);
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN, clock);

        floor.settled(entries("orders", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 1), acked);
        clock.advance(30_000);

        // Two entries sit settled above a bound that has stopped climbing, and the durable position has
        // been pinned there for half a minute. The distance says nothing about this - it reads zero for a
        // frontier its bound is holding back, the same as for one keeping up - so how long it has been
        // held is the only reading that says the pin is happening at all.
        assertThat(floor.stalls()).containsExactly(entry("orders", 30_000L));
    }

    @Test
    void reports_nothing_for_a_chain_that_has_caught_up_with_its_bound() {
        RecordingAck acked = new RecordingAck();
        Ticking clock = new Ticking(1_000);
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN, clock);

        floor.settled(entries("orders", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 3), acked);
        clock.advance(86_400_000);

        // The chain acked everything it had and holds nothing its bound is keeping from it. A quiet day
        // that follows is a quiet day, not a pin: nothing is being kept from the durable position, so
        // there is nothing whose age could matter. Measuring from the last advance regardless would put
        // every idle pipeline over any threshold worth setting, which is the alarm meaning nothing.
        assertThat(floor.stalls()).isEmpty();
    }

    @Test
    void measures_from_the_last_advance_rather_than_from_the_beginning() {
        RecordingAck acked = new RecordingAck();
        Ticking clock = new Ticking(1_000);
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN, clock);

        floor.settled(entries("orders", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 1), acked);
        clock.advance(50_000);
        floor.bound(boundAt("orders", 2), acked);
        clock.advance(7_000);

        // The frontier did advance, to 2, and what is being asked is how long it has been stuck since -
        // not how long this chain has had something outstanding, which has been true from the first
        // batch. A measure taken from the beginning would answer 57 seconds here and would go on climbing
        // through every advance, so a chain advancing steadily under load would cross any threshold.
        assertThat(floor.stalls()).containsExactly(entry("orders", 7_000L));
    }

    @Test
    void does_not_restart_the_measure_when_traffic_arrives_that_changes_nothing() {
        RecordingAck acked = new RecordingAck();
        Ticking clock = new Ticking(1_000);
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN, clock);

        floor.settled(entries("orders", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 1), acked);
        clock.advance(40_000);
        floor.settled(entries("orders", 4), acked);
        floor.bound(boundAt("orders", 1), acked);
        clock.advance(20_000);

        // Changes and bounds go on arriving all through a stall - that is what being held back by pending
        // changes looks like from here, not silence - and none of them moved the durable position. Only an
        // advance restarts this, so anything that restarted it on arrival instead would report a minute of
        // pinning as twenty seconds, and would reset again on the next batch, forever.
        assertThat(floor.stalls()).containsExactly(entry("orders", 60_000L));
    }

    @Test
    void measures_a_chain_that_has_never_advanced_from_when_it_was_first_seen() {
        RecordingAck acked = new RecordingAck();
        Ticking clock = new Ticking(1_000);
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN, clock);

        floor.settled(entries("orders", 5, 6), acked);
        clock.advance(20_000);

        // No bound ever arrived, so this chain has acked nothing and there is no advance to measure from.
        // It is also the worst case rather than an edge one - a chain pinned at the position it started
        // from burns the whole retention window, and it is exactly what a pipeline whose parent rows never
        // arrive looks like. Measuring only from an advance would report nothing at all here.
        assertThat(floor.stalls()).containsExactly(entry("orders", 20_000L));
    }

    @Test
    void reports_a_chain_starved_of_positions_as_held_too() {
        RecordingAck acked = new RecordingAck();
        Ticking clock = new Ticking(1_000);
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN, clock);

        floor.settled(entries("orders", 1), acked);
        floor.bound(boundAt("orders", 1), acked);
        floor.bound(boundAt("orders", 40), acked);
        clock.advance(12_000);

        // Nothing is settled above the bound here - the entry was acked and cleared - and the pin is real
        // all the same: the bound climbed over positions the sink was never given, so the durable position
        // sits at 1 while the chain is covered to 40. Reading only what is held above the bound would call
        // this chain caught up, and it is the other of the two stalls, worked on from the opposite end.
        assertThat(floor.stalls()).containsExactly(entry("orders", 12_000L));
    }

    @Test
    void measures_each_chain_from_its_own_last_advance() {
        RecordingAck acked = new RecordingAck();
        Ticking clock = new Ticking(1_000);
        SettledFloor floor = new SettledFloor(AXES, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN, clock);

        floor.settled(entries("orders", 1, 2), acked);
        floor.settled(entries("lines", 1, 2, 3), acked);
        floor.bound(boundAt("orders", 1), acked);
        floor.bound(boundAt("lines", 1), acked);
        clock.advance(9_000);
        // Both chains are still holding something above their bound after this - lines advanced to 2 and
        // still holds 3. A chain left with nothing above its bound would be caught up rather than pinned
        // and would report nothing, which is a different case and is covered on its own.
        floor.bound(boundAt("lines", 2), acked);
        clock.advance(4_000);

        // One chain pinned for thirteen seconds while another advanced four seconds ago. A single measure
        // over the two would be neither, and it is the pinned one that decides whether a retention window
        // is being burned - an average with a healthy chain is how it stays invisible.
        assertThat(floor.stalls()).containsOnly(entry("orders", 13_000L), entry("lines", 4_000L));
    }

    /** The entries a batch of changes on {@code chain} contributes, one per ring sequence. */
    private static List<ChainEntry> entries(String chain, int... seqs) {
        List<ChainEntry> entries = new ArrayList<>();
        for (int seq : seqs) {
            entries.add(new ChainEntry(chain, new ChainPosition(new SourceOrder(1, seq), "w" + seq)));
        }
        return entries;
    }

    /** One change on {@code chain} at ring sequence {@code seq}, as the source stamps it. */
    private static Envelope event(String chain, int seq) {
        return Envelope.insert(1L, chain, Map.of("id", seq), null)
                .withSrcPos("w" + seq)
                .withOrder(new SourceOrder(1, seq));
    }

    /** The bound the engine combined across every input queue for {@code chain}, at ring sequence {@code seq}. */
    private static Watermark boundAt(String chain, int seq) {
        return new Watermark(FrontierOrders.pack(chain, new SourceOrder(1, seq)), AXES.axisOf(chain));
    }

    /**
     * A clock the test moves itself. Wall-clock time is what a pin is measured in - it is racing a log
     * retention, which is measured in the same time - so a durable position's age cannot be asserted
     * against a real clock without the test either sleeping or reading whatever the machine happened to
     * take.
     */
    private static final class Ticking implements LongSupplier {

        private long millis;

        private Ticking(long millis) {
            this.millis = millis;
        }

        private void advance(long by) {
            millis += by;
        }

        @Override
        public long getAsLong() {
            return millis;
        }
    }

    /** Records what was acked, as {@code chain=token}. */
    private static final class RecordingAck implements SinkAck {

        private final List<String> calls = new ArrayList<>();

        @Override
        public void advance(String chain, ChainPosition position) {
            calls.add(chain + "=" + (position.token() == null ? "<no token>" : position.token()));
        }
    }
}
