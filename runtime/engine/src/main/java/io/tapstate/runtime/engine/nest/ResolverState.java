package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
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
 * and a genuine newer declaration revives the mapping. A deletion does not prove that its key
 * will never be declared again: independent input streams can deliver that declaration later.
 *
 * <p>Children wait while the mapping is missing, including under a deleted tombstone. Parent deletion
 * preserves the bucket rather than turning a temporarily missing parent into a permanent orphan.
 * The existing per-key capacity limit bounds this wait; it has no time-based release.
 *
 * <p>A held child retains its original order and source positions. The owner writes the state through
 * before advertising durable coverage. An exact replay does not consume another pending slot.
 *
 * <p>{@link Serializable} because this state outlives a single run. An order is never null — a null
 * order is an engine invariant violation and crashes bare rather than being reported as a diagnosable
 * error.
 */
public final class ResolverState implements Serializable {

    /**
     * Explicit so a compatible payload's Java identity is not derived from incidental method changes.
     * The storage layout controls admission; this is not an old-layout migration mechanism.
     */
    private static final long serialVersionUID = 2L;

    private Object parentKey;
    private SourceOrder order;
    private Map<String, ChainPosition> declaredAt = Map.of();
    private boolean deleted;
    private final List<Waiting> waiting = new ArrayList<>();
    private final List<Release> releases = new ArrayList<>();

    /** The original handoff stays durable until its parent's source position is covered downstream. */
    record Release(SourceOrder order, Map<String, SourceOrder> covered, List<KeyedElement> deliveries,
            int children) implements Serializable {
        Release {
            covered = Map.copyOf(covered);
            deliveries = List.copyOf(deliveries);
        }
    }

    List<Release> releases() { return List.copyOf(releases); }

    void retainRelease(SourceOrder parentOrder, Map<String, SourceOrder> covered,
            List<KeyedElement> deliveries, int children) {
        if (children == 0) return;
        var next = new Release(parentOrder, covered, deliveries, children);
        for (int index = 0; index < releases.size(); index++) {
            var previous = releases.get(index);
            if (!previous.order().equals(parentOrder)) continue;
            var declarations = new LinkedHashMap<RoutedReplay, KeyedElement>();
            var received = new LinkedHashMap<RoutedReplay, KeyedElement>();
            for (var part : List.of(previous, next)) {
                int prefix = part.deliveries().size() - part.children();
                for (int item = 0; item < part.deliveries().size(); item++) {
                    var delivery = part.deliveries().get(item);
                    (item < prefix ? declarations : received).putIfAbsent(RoutedReplay.of(delivery), delivery);
                }
            }
            var combined = new ArrayList<>(declarations.values()); combined.addAll(received.values());
            var barriers = new LinkedHashMap<>(covered);
            previous.covered().forEach((chain, value) -> barriers.merge(chain, value,
                    (first, second) -> first.compareTo(second) >= 0 ? first : second));
            releases.set(index, new Release(parentOrder,
                    covered.isEmpty() || previous.covered().isEmpty() ? Map.of() : barriers,
                    combined, received.size()));
            return;
        }
        releases.add(next);
    }

    private record RoutedReplay(Object key, ReplayKey element) {
        static RoutedReplay of(KeyedElement value) { return new RoutedReplay(value.key(), ReplayKey.of(value.element())); }
    }

    SourceOrder mappingOrder() { return order; }
    Map<String, ChainPosition> declaredAt() { return declaredAt; }

    void forgetRelease(Release release) { releases.remove(release); }

    boolean repeats(Object parent, SourceOrder candidate, boolean deletion) {
        return candidate.equals(order) && deleted == deletion
                && (deletion || Objects.equals(parentKey, parent));
    }
    // Derived only in memory; the serialized wait list remains the sole durable representation.
    private transient Set<ReplayKey> waitingValues;

    // Source order and address identify the original change; payload arrays need not share identity
    // with the deserialized copy. Keep the first full payload rather than normalizing its row values.
    private record ReplayKey(ElementRef ref, SourceOrder order, Map<String, ChainPosition> positions,
            ElementRef movedFrom, boolean deletion, boolean departed) {
        static ReplayKey of(NestElement child) {
            return new ReplayKey(child.ref(), child.order(), child.positions(), child.movedFrom(),
                    child.deletion(), child.departed());
        }
    }

    /**
     * One change held here, and when it was first held. The time is a property of the holding rather than
     * of the change — the same change replayed after a restart starts waiting again — which is why it is
     * kept beside the change rather than inside it.
     */
    private record Waiting(NestElement child, long arrivedAt) implements Serializable {
    }

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
        return waiting.stream().map(Waiting::child).toList();
    }

    /** Whether anything is waiting here at all - asked far more often than what, and without building it. */
    public boolean holdsChildren() {
        return !waiting.isEmpty() || !releases.isEmpty();
    }

    /** How many changes are held here for a parent that has not arrived. */
    public int pending() {
        return waiting.size() + releases.stream().mapToInt(Release::children).sum();
    }

    /**
     * Whether this entry now says nothing: no mapping, no tombstone, nothing waiting. Such an entry is
     * indistinguishable from one that was never written, so keeping it costs a key and answers nothing.
     */
    public boolean vacant() {
        return order == null && !deleted && waiting.isEmpty() && releases.isEmpty();
    }

    /**
     * Declares that rows under this key hang from {@code parentKey}, and releases whatever was waiting
     * for it. Returns the released children — empty when the declaration is refused as already
     * superseded, or when nothing was waiting.
     */
    public List<NestElement> declare(Object parentKey, SourceOrder order) {
        return declare(parentKey, order, Map.of());
    }

    List<NestElement> declare(Object parentKey, SourceOrder order, Map<String, ChainPosition> positions) {
        Objects.requireNonNull(order, "order");
        var fixed = Map.copyOf(positions);
        if (!wins(order)) return List.of();
        this.parentKey = parentKey;
        this.order = order;
        this.declaredAt = fixed;
        this.deleted = false;
        return drain();
    }

    /**
     * Stops this entry answering for a parent, without saying the parent is gone. It is what a value being
     * vacated leaves behind: the row that answered to it now answers to another, so nothing carries this
     * value any more - but a row may yet declare it, and children naming it are waiting for exactly that.
     *
     * <p>The value is unclaimed right now. Left answering instead, the mapping the row abandoned goes on placing children under a
     * parent that is not there - in a document they do not belong to, with nothing counting it.
     */
    public void stopAnswering(SourceOrder order) {
        Objects.requireNonNull(order, "order");
        if (!wins(order)) {
            return;
        }
        this.parentKey = null;
        this.order = order;
    }

    /** Marks the mapping deleted without discarding children that a later declaration can resolve. */
    public void deleteMapping(SourceOrder order) {
        Objects.requireNonNull(order, "order");
        if (!wins(order)) return;
        this.parentKey = null;
        this.order = order;
        this.deleted = true;
    }

    /** Resolves through a live mapping; otherwise retains the exact original child until a parent appears. */
    public Resolution resolve(NestElement child, long arrivedAt) {
        Objects.requireNonNull(child, "child");
        if (deleted || order == null || parentKey == null) {
            if (waitingValues == null) {
                waitingValues = new HashSet<>();
                for (Waiting held : waiting) waitingValues.add(ReplayKey.of(held.child()));
            }
            if (waitingValues.add(ReplayKey.of(child))) waiting.add(new Waiting(child, arrivedAt));
            return Resolution.HELD;
        }
        return Resolution.RESOLVED;
    }

    /**
     * The lowest original position held per chain. Durably held waiting does not pin the frontier;
     * these positions remain part of the original change throughout recovery and handoff.
     */
    public Map<String, ChainPosition> lowestHeldByChain() {
        Map<String, ChainPosition> lowest = new LinkedHashMap<>();
        for (Waiting held : waiting) {
            held.child().positions().forEach((chain, position) -> lowest.merge(chain, position,
                    (kept, candidate) -> candidate.order().compareTo(kept.order()) < 0 ? candidate : kept));
        }
        return lowest;
    }

    /**
     * Stops waiting for everything held here, leaving the mapping itself alone. It is what a key being
     * vacated gives up: those children asked a question of a value the row no longer answers to, and the
     * row now answers to another one, so the answer they are waiting for will never come here. The mapping
     * is not given up with them - the row declares it again where it now belongs.
     *
     * <p><b>Separate from reading them on purpose.</b> The rows have to be somewhere durable before this
     * entry stops being the only place they are: a drain stores what it touched however it ended, so an
     * entry emptied for a hand-over that then failed to land is stored empty, and the replay that would
     * rebuild it is rejected as a change already seen. Read with {@link #waiting()}, publish, then call
     * this.
     */
    public void forgetWaiting() {
        drain();
    }

    /**
     * Whether a row at {@code candidate} is newer than whatever this entry has already applied - asked
     * before anything is sent on, so that what leaves this level agrees with what it kept. A row it would
     * reject and that went out anyway puts an element back into a document a later change took it out of,
     * which no count anywhere is about.
     */
    public boolean accepts(SourceOrder candidate) {
        Objects.requireNonNull(candidate, "candidate");
        return wins(candidate);
    }

    private boolean wins(SourceOrder candidate) {
        return order == null || candidate.compareTo(order) > 0;
    }

    private List<NestElement> drain() {
        if (waiting.isEmpty()) {
            return List.of();
        }
        List<NestElement> released = waiting();
        waiting.clear();
        waitingValues = null;
        return released;
    }
}
