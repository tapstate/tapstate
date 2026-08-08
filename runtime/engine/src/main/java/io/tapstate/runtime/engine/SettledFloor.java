package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The frontier of a sink fed by an assembly of several chains: per chain, the highest position it has
 * proven durable at or below the bound the engine combined for that chain, and nothing above it.
 *
 * <p>The two halves answer different questions and neither is enough alone. What has settled here is
 * durable, but says nothing about what is still on its way here - the assembly lets a chain's changes out
 * in whatever order their roots completed, so a later position landing first proves nothing about the ones
 * beneath it. The bound says what is on its way, but nothing about what is durable: it is the promise every
 * queue has made, combined by the engine, which is why a queue that has said nothing still holds it down.
 * The frontier is where the two meet.
 *
 * <p>A bound arriving is as much a reason to look as a batch settling. The last batch of a quiet stream
 * settles before the bound that covers it arrives, and waiting for a further batch that may never come
 * would leave the durable position short of what is provably durable for as long as the source stays
 * quiet - the frontier lagging is what burns through a source's retention window.
 *
 * <p>Entries are only dropped once the frontier passes them, so a bound that stalls - one dangling
 * reference upstream is enough - grows what is held at the event rate, inside the sink's own process where
 * the guards over the assembled state cannot see it. Past a chain's capacity line the entries are thinned
 * rather than the job failed: dropping an entry can only lower the floor a bound finds, which is the
 * frontier being more conservative, never less. That is what makes the loss safe here and not upstream,
 * where what would be dropped is the promise itself.
 */
final class SettledFloor implements SinkFrontier {

    /**
     * How many settled positions one chain holds before they are thinned. Sized so a stalled bound has to
     * stay stalled through a great many changes before precision is lost at all, while the memory it can
     * ever hold stays a property of the graph rather than of how long the stall lasts.
     */
    static final int DEFAULT_MAX_ENTRIES_PER_CHAIN = 4096;

    private final ChainAxes axes;
    private final int maxEntriesPerChain;
    // Per chain, ordered by the packed order: what has settled and not yet been acked away. The packing is
    // strictly increasing in the order it carries, so the natural order of these keys is the order itself.
    private final Map<String, NavigableMap<Long, ChainPosition>> settledByChain = new HashMap<>();
    // Per chain: the newest bound the engine combined across every queue, and the last position acked.
    private final Map<String, Long> boundByChain = new HashMap<>();
    private final Map<String, Long> ackedByChain = new HashMap<>();

    /**
     * A frontier over the chains {@code axes} numbers. A null numbering means the graph was built without
     * one: bounds then cannot be attributed to a chain, so none is taken in and the frontier stands still -
     * which is the direction to fail in, since reading this stream as though it were one chain in order
     * would ack positions whose changes are still in flight.
     */
    SettledFloor(ChainAxes axes, int maxEntriesPerChain) {
        if (maxEntriesPerChain < 2) {
            throw new IllegalArgumentException(
                    "a chain must be able to hold its lowest and its highest position, got "
                            + maxEntriesPerChain);
        }
        this.axes = axes;
        this.maxEntriesPerChain = maxEntriesPerChain;
    }

    /**
     * Every position the batch reports, in arrival order - not the highest of them. A bound may fall inside
     * a batch, and a batch reduced to its highest position offers nothing beneath that bound to advance to,
     * so the frontier would sit still until the bound cleared a whole batch.
     *
     * <p>Every chain an event covers is reported, not just the stream it arrived on: a document assembled
     * out of several chains is the whole reason this shape exists, and taking only its own name would ack
     * one chain and silently drop the rest.
     *
     * <p>Taking part is decided on the order alone. A snapshot row has one and carries no token, which is
     * the chain's cdc start being persisted for it rather than a reason to pass it over; a position with
     * no order at all cannot be placed against a bound and reports nothing here.
     */
    @Override
    public List<ChainEntry> positions(List<Envelope> batch) {
        List<ChainEntry> entries = new ArrayList<>();
        for (Envelope event : batch) {
            event.positions().forEach((chain, position) -> {
                if (position.order() != null) {
                    entries.add(new ChainEntry(chain, position));
                }
            });
        }
        return entries;
    }

    @Override
    public void settled(List<ChainEntry> positions, SinkAck ack) {
        for (ChainEntry entry : positions) {
            NavigableMap<Long, ChainPosition> entries = settledByChain.computeIfAbsent(
                    entry.chain(), chain -> new TreeMap<>());
            entries.put(FrontierOrders.pack(entry.chain(), entry.position().order()), entry.position());
            if (entries.size() > maxEntriesPerChain) {
                thin(entries);
            }
            advance(entry.chain(), ack);
        }
    }

    @Override
    public void bound(Watermark bound, SinkAck ack) {
        if (axes == null) {
            return;
        }
        String chain = axes.chainOn(bound.key());
        boundByChain.put(chain, bound.timestamp());
        advance(chain, ack);
    }

    /**
     * How far each chain's bound runs ahead of the position this frontier reached. A chain that has reached
     * none is absent rather than zero: the distance is measured from the position acked, and answering zero
     * where there is none reads as a frontier keeping up with its bound, which is its opposite.
     *
     * <p>What is measured is the packed distance, so within one generation of a chain's ring it counts the
     * positions the frontier could have covered and did not. A bound a whole generation ahead reads as an
     * enormous distance rather than a count of changes - far past any threshold worth setting, which is the
     * direction to be wrong in for something read to raise an alarm.
     */
    @Override
    public Map<String, Long> gaps() {
        Map<String, Long> gaps = new HashMap<>();
        ackedByChain.forEach((chain, reached) -> {
            Long bound = boundByChain.get(chain);
            if (bound != null) {
                gaps.put(chain, bound - reached);
            }
        });
        return gaps;
    }

    /** How many settled entries this chain still holds - the capacity line the thinning keeps. */
    int held(String chain) {
        NavigableMap<Long, ChainPosition> entries = settledByChain.get(chain);
        return entries == null ? 0 : entries.size();
    }

    /**
     * Acks the highest position on {@code chain} that is both settled here and at or below the chain's
     * bound, then forgets it and everything beneath it. Nothing is acked twice and nothing is ever acked
     * beneath what already was: a position settling below the frontier can only mean batches settled out of
     * order, which the single write in flight rules out, and rewriting the durable position downwards would
     * have a restart replay from beneath changes already acked away.
     */
    private void advance(String chain, SinkAck ack) {
        Long bound = boundByChain.get(chain);
        NavigableMap<Long, ChainPosition> entries = settledByChain.get(chain);
        if (bound == null || entries == null) {
            return;
        }
        Map.Entry<Long, ChainPosition> reached = entries.floorEntry(bound);
        if (reached == null) {
            return;
        }
        Long acked = ackedByChain.get(chain);
        if (acked != null && reached.getKey() <= acked) {
            return;
        }
        ackedByChain.put(chain, reached.getKey());
        ack.advance(chain, reached.getValue());
        entries.headMap(reached.getKey(), true).clear();
    }

    /**
     * Halves what a chain holds, keeping its lowest and its highest. The highest is what a bound that
     * catches up advances to, and the lowest is what a bound that has barely moved advances to; dropping
     * either would cost an advance that is provably safe, while dropping what lies between only makes the
     * next advance land lower than it could have.
     */
    private static void thin(NavigableMap<Long, ChainPosition> entries) {
        List<Long> held = new ArrayList<>(entries.keySet());
        for (int i = 1; i < held.size() - 1; i += 2) {
            entries.remove(held.get(i));
        }
    }
}
