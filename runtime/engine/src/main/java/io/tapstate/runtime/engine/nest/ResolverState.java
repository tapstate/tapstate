package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What one key of one embed knows: which parent row the rows under this key hang from, and the children
 * that arrived before that was known. It is the state a resolver vertex holds per identity value —
 * declared by the embed's own row, read by the rows of the embed beneath it.
 *
 * <p><b>The higher order wins</b>, as everywhere else: a declaration at or beneath the one already held
 * is refused. <b>A deletion leaves a versioned tombstone rather than removing the entry.</b> The row's
 * children may still be on their way — a change not yet delivered, or one a replay will deliver again —
 * and an entry removed outright would leave them unable to resolve, so they would pile up waiting for a
 * parent that no longer exists and hold the frontier still. A tombstone answers them instead: a child
 * replayed from beneath it is told the parent is absent, and a genuine rebuild above it revives the
 * mapping.
 *
 * <p><b>A deletion empties the waiting bucket at once, and does not wait for a timeout.</b> Those
 * children were waiting for a row that is now known to be gone, so they can never resolve and would
 * otherwise pin the frontier for nothing. They are handed back to the caller to route — where they go
 * is the caller's business, but they must go somewhere: dropping them silently loses data that a
 * document-level assertion can never see, because those rows were never going to appear in a document.
 *
 * <p>The bucket is therefore non-empty only while there is no entry at all — a live mapping resolves on
 * the spot, a tombstone answers absent — which keeps the pending set small and makes "what is waiting
 * here" a question with one answer.
 *
 * <p><b>A held child keeps the position it arrived with.</b> It has been consumed and not emitted, so
 * the durable frontier must stay below it; {@link #lowestHeldByChain()} is what a vertex reports so
 * that can be enforced. Both halves of a position are kept: the order to compare on, the token to
 * persist and hand back to the connector.
 *
 * <p>{@link Serializable} because this state outlives a single run. An order is never null — a null
 * order is an engine invariant violation and crashes bare rather than being reported as a diagnosable
 * error.
 */
public final class ResolverState implements Serializable {

    private Object parentKey;
    private SourceOrder order;
    private boolean deleted;
    private final List<NestElement> waiting = new ArrayList<>();

    /** The parent row these children hang from, or null before it is known and after it is deleted. */
    public Object parentKey() {
        return parentKey;
    }

    /** Whether the row that declared this mapping is known to be deleted. */
    public boolean deleted() {
        return deleted;
    }

    /** The children held here, in the order they arrived. */
    public List<NestElement> waiting() {
        return List.copyOf(waiting);
    }

    /**
     * Declares that rows under this key hang from {@code parentKey}, and releases whatever was waiting
     * for it. Returns the released children — empty when the declaration is refused as already
     * superseded, or when nothing was waiting.
     */
    public List<NestElement> declare(Object parentKey, SourceOrder order) {
        Objects.requireNonNull(order, "order");
        if (!wins(order)) {
            return List.of();
        }
        this.parentKey = parentKey;
        this.order = order;
        this.deleted = false;
        return drain();
    }

    /**
     * Marks the declaring row deleted at {@code order}, keeping a tombstone in place of the mapping.
     * Returns the children that were waiting: they can never resolve now, and the caller must route
     * them rather than drop them.
     */
    public List<NestElement> deleteMapping(SourceOrder order) {
        Objects.requireNonNull(order, "order");
        if (!wins(order)) {
            return List.of();
        }
        this.parentKey = null;
        this.order = order;
        this.deleted = true;
        return drain();
    }

    /**
     * Offers one child event to this key: resolved when the parent row is known, absent when it is known
     * deleted, held otherwise — and a held child is kept until a declaration or a deletion releases it.
     */
    public Resolution resolve(NestElement child) {
        Objects.requireNonNull(child, "child");
        if (deleted) {
            return Resolution.PARENT_ABSENT;
        }
        if (order == null) {
            waiting.add(child);
            return Resolution.HELD;
        }
        return Resolution.RESOLVED;
    }

    /**
     * The lowest position held per chain — the bound below which the durable frontier must stay for the
     * events waiting here. Chains with nothing waiting do not appear.
     */
    public Map<String, ChainPosition> lowestHeldByChain() {
        Map<String, ChainPosition> lowest = new LinkedHashMap<>();
        for (NestElement child : waiting) {
            child.positions().forEach((chain, position) -> lowest.merge(chain, position,
                    (held, candidate) -> candidate.order().compareTo(held.order()) < 0 ? candidate : held));
        }
        return lowest;
    }

    private boolean wins(SourceOrder candidate) {
        return order == null || candidate.compareTo(order) > 0;
    }

    private List<NestElement> drain() {
        if (waiting.isEmpty()) {
            return List.of();
        }
        List<NestElement> released = List.copyOf(waiting);
        waiting.clear();
        return released;
    }
}
