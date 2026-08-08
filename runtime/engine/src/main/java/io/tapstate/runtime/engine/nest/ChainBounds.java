package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * What one vertex instance is still holding, chain by chain, gathered from the state entries holding it.
 * The one question it answers — the lowest change on a chain that has not left here — is what a level's
 * bound is tightened by. Promise anything at or above it and a restart replays from above a change that
 * was never delivered, so that change is neither sent nor replayable and no count anywhere is wrong.
 *
 * <p><b>A key states what it holds in full, never by increment.</b> Each entry's answer is re-read from
 * the state itself once its drain settles and replaces whatever that key said before, so a release cannot
 * be forgotten: forgetting one pins a chain for good, forgetting a hold loses data, and only one of those
 * two is loud. It also makes re-stating an unchanged answer free, which is what every drain does.
 *
 * <p><b>The answer may fall.</b> It reports what is being held right now, not what has been promised — a
 * key taking in a change lower than anything else held lowers it, which is exactly right. Keeping a
 * promise from being taken back belongs to the level that makes it.
 *
 * <p>Only the order is tracked; the token that came with it is not. A bound in flight is compared and
 * packed, never written down — what gets persisted is what a sink has acknowledged, and that carries its
 * own token.
 *
 * <p>Not serializable and not part of any stored state: after a restart each entry is re-read as its
 * changes are replayed, and a bound is offered only once every edge has spoken again.
 */
final class ChainBounds {

    /** What each state entry currently holds — dropped as soon as it holds nothing. */
    private final Map<Object, Map<String, SourceOrder>> byKey = new HashMap<>();

    /** Held orders per chain, counted rather than set-valued: every row of one snapshot shares an order. */
    private final Map<String, NavigableMap<SourceOrder, Integer>> byChain = new HashMap<>();

    /**
     * Replaces what the state entry at {@code key} is holding with {@code lowestByChain}, the lowest
     * position it still owes on each chain. An empty map means it holds nothing and is forgotten.
     */
    void holding(Object key, Map<String, ChainPosition> lowestByChain) {
        Map<String, SourceOrder> was = byKey.getOrDefault(key, Map.of());
        Map<String, SourceOrder> now = new LinkedHashMap<>();
        lowestByChain.forEach((chain, position) -> now.put(chain, position.order()));
        was.forEach((chain, order) -> {
            if (!order.equals(now.get(chain))) {
                drop(chain, order);
            }
        });
        now.forEach((chain, order) -> {
            if (!order.equals(was.get(chain))) {
                add(chain, order);
            }
        });
        if (now.isEmpty()) {
            byKey.remove(key);
        } else {
            byKey.put(key, now);
        }
    }

    /** The lowest change this instance is holding on {@code chain}, or null while it holds none. */
    SourceOrder lowest(String chain) {
        NavigableMap<SourceOrder, Integer> held = byChain.get(chain);
        return held == null || held.isEmpty() ? null : held.firstKey();
    }

    private void add(String chain, SourceOrder order) {
        byChain.computeIfAbsent(chain, named -> new TreeMap<>()).merge(order, 1, Integer::sum);
    }

    private void drop(String chain, SourceOrder order) {
        NavigableMap<SourceOrder, Integer> held = byChain.get(chain);
        if (held != null) {
            held.computeIfPresent(order, (at, count) -> count == 1 ? null : count - 1);
        }
    }
}
