package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.SourceOrder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * How far one instance of one level lets the frontier go, chain by chain, and when it is allowed to say
 * so. A bound arrives per edge already combined across the instances feeding that edge; this combines
 * them across the edges and tightens the answer by whatever this instance is still holding.
 *
 * <p><b>Every edge expected to carry a chain must have promised before anything is said about it.</b>
 * The alternative - answering with the lowest of the edges that happen to have spoken - promises a range
 * that may be sitting in the queue of an edge nobody has heard from, which no later message takes back.
 * Which edges are expected is settled while the graph is compiled and never from what has been observed
 * running, because an edge that has not spoken yet and an edge that carries nothing look identical from
 * here. A chain arriving on an edge outside that set means the set is too small, which is the direction
 * that loses data, so it tears down rather than widening itself to fit.
 *
 * <p><b>Holding something only ever lowers the answer.</b> An instance with nothing held passes on what
 * its edges promised, which is what stops an idle instance from pinning the frontier at the last change
 * it happened to see. An instance holding a change answers with the value immediately beneath it: the
 * packing is dense, so that is the exact predecessor rather than an approximation, and it needs no token
 * because a bound in flight is never written down.
 *
 * <p><b>The answer climbs strictly or is not sent.</b> Repeating a bound is fatal - the engine tears the
 * job down on a bound that fails to climb - so an unchanged answer is silence. An answer that has fallen
 * is a different thing entirely: it can only mean an upstream lowered a position it had already
 * reported, and it is reported as the invariant violation it is rather than swallowed by the same
 * silence, which would leave the frontier stopped with nothing said anywhere.
 */
public final class LevelBounds {

    private final Map<String, Set<Integer>> ordinalsByChain;
    private final ChainAxes axes;
    private final Function<String, SourceOrder> lowestHeld;
    private final Map<String, Map<Integer, Long>> promised = new HashMap<>();
    private final Map<String, Long> sent = new HashMap<>();
    private Watermark held;

    /**
     * A level whose edge at each ordinal is expected to carry the named chains, putting its bounds on the
     * axes {@code axes} numbers them by. {@code lowestHeld} answers with the lowest change this instance
     * is still holding on a chain, or null when it holds none - a level that holds nothing at all answers
     * null throughout.
     */
    public LevelBounds(Map<Integer, List<String>> chainsByOrdinal, ChainAxes axes,
            Function<String, SourceOrder> lowestHeld) {
        Map<String, Set<Integer>> byChain = new LinkedHashMap<>();
        chainsByOrdinal.forEach((ordinal, chains) -> chains.forEach(
                chain -> byChain.computeIfAbsent(chain, named -> new LinkedHashSet<>()).add(ordinal)));
        this.ordinalsByChain = byChain;
        this.axes = axes;
        this.lowestHeld = lowestHeld;
    }

    /**
     * Takes in the bound that arrived at {@code ordinal} and offers whatever this level may now promise to
     * {@code emit}, answering whether the level is done with this bound. A refusal means the outbox is
     * full: the engine will bring the same bound back, and the value worked out for it is kept and offered
     * again rather than worked out afresh - a second pass would find it no advance on what was already
     * recorded as sent, and pass on nothing at all.
     */
    public boolean advance(int ordinal, Watermark arrived, Predicate<Watermark> emit) {
        if (held != null) {
            if (!emit.test(held)) {
                return false;
            }
            held = null;
        }
        OptionalLong promise = observe(ordinal, arrived.key(), arrived.timestamp());
        if (promise.isEmpty()) {
            return true;
        }
        Watermark onward = new Watermark(promise.getAsLong(), arrived.key());
        if (emit.test(onward)) {
            return true;
        }
        held = onward;
        return false;
    }

    /**
     * Takes in the bound {@code axis}'s chain has reached on the edge at {@code ordinal}, and answers with
     * what this level may now promise on that same axis, or nothing when it may promise no more than it
     * already has.
     */
    public OptionalLong observe(int ordinal, byte axis, long bound) {
        String chain = axes.chainOn(axis);
        Set<Integer> expected = ordinalsByChain.get(chain);
        if (expected == null || !expected.contains(ordinal)) {
            throw new IllegalStateException("a bound on chain " + chain + " arrived on inbound ordinal "
                    + ordinal + ", which this level is not compiled to carry it over");
        }
        promised.computeIfAbsent(chain, named -> new HashMap<>()).put(ordinal, bound);

        long lowest = Long.MAX_VALUE;
        for (int edge : expected) {
            Long promise = promised.get(chain).get(edge);
            if (promise == null) {
                return OptionalLong.empty();
            }
            lowest = Math.min(lowest, promise);
        }
        SourceOrder holding = lowestHeld.apply(chain);
        if (holding != null) {
            lowest = Math.min(lowest, FrontierOrders.pack(chain, holding) - 1);
        }

        Long already = sent.get(chain);
        if (already != null) {
            if (lowest < already) {
                throw new IllegalStateException("chain " + chain + " was promised up to " + already
                        + " and now works out to " + lowest + "; a bound only falls when an upstream"
                        + " lowered a position it had already reported");
            }
            if (lowest == already) {
                return OptionalLong.empty();
            }
        }
        if (lowest < 0) {
            return OptionalLong.empty();
        }
        sent.put(chain, lowest);
        return OptionalLong.of(lowest);
    }
}
