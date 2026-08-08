package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One nested document under assembly: the root row, the tree of elements attached beneath it, and the
 * tombstones that keep a replay from undoing a deletion. It is the state a stateful nest node holds per
 * root key, mutated in place as events arrive and rendered into the document that goes downstream.
 *
 * <p><b>Every mutation carries an order and the higher order wins.</b> A mutation whose order is not
 * strictly greater than what the element already holds is refused and reported as no change, so a
 * replay, a re-delivered snapshot row and an out-of-order arrival all converge on the same document. A
 * tie keeps what is already there: two events that compare equal are never two versions of one row.
 *
 * <p><b>A delete leaves a versioned tombstone; it never drops the element.</b> The element disappears
 * from the document but its order — and the subtree hanging beneath it — stay behind, so an insert
 * replayed from beneath it stays deleted while a genuine rebuild above it brings the element back with
 * everything that had been attached to it. Dropping the entry instead would let any replay resurrect
 * deleted data, and would lose a subtree that no later event will resend.
 *
 * <p><b>Deleting the root keeps its elements attached</b>, for the same reason and by the same means: a
 * root tombstone is this assembly with the root marked absent, not a small object that replaces it.
 * Reclaiming the whole key is a memory optimisation performed elsewhere, under its own conditions, and
 * is never what makes the deletion correct.
 *
 * <p><b>An element hangs under the row its join key points at, not under a level.</b> Elements are
 * placed by the path of field names from the root, so two embeds side by side stay apart even when
 * their rows carry the same key value — a policy 77 and an order 77 are different parents. A child
 * whose parent row has not arrived is held until it does and then attached, which is what lets a deep
 * row travel with nothing but its own parent's key. Held children are state like any other: they
 * survive being stored and restored, and a delete held that way still wins over an insert replayed
 * from beneath it.
 *
 * <p><b>Every change taken in is held until a document carrying it goes out</b>, and
 * {@link #lowestHeldByChain()} is what a vertex reports so the durable frontier can be kept below it.
 * Let the frontier past a change that is still here and a restart neither replays it nor finds it,
 * leaving the document an element short with nothing anywhere to signal it. Two quite different things
 * are held that way: a child whose ancestor has not arrived, put where no sink can see it; and anything
 * absorbed into a document that did not go out — while the root is absent nothing is rendered at all, so
 * an element can be attached, in its right place, and still have been shown to nobody. {@link
 * #documentSent()} is what releases them, and only a whole document does: what goes out for a deleted
 * root is its key, which carries no element with it.
 *
 * <p><b>A document is rendered only while the root is present.</b> Until the root arrives, and again
 * once it is deleted, there is no document at all, whatever triggered the render — a child arriving, an
 * emit window closing, a move finishing. A rootless skeleton would become a permanent ghost document
 * downstream. The deletion of the root is not an assembled document and is not governed by this:
 * emitting it is the caller's business.
 *
 * <p>An array embed with no live element renders an empty array; an object embed with none omits its
 * field rather than rendering null, so two correct implementations cannot differ in the shape they
 * produce for the same input. An object embed that ends up holding several rows — a one-to-one the
 * source contradicted — shows the one with the highest order.
 *
 * <p>An element's identity is taken when it first appears. A row whose identity value changes later is
 * a structural key change, which this state does not track: its existing children stay where they are.
 *
 * <p>Field maps handed in are copied and the rendered document is built fresh each time, so neither
 * side can mutate the other's data. {@link Serializable} because this state outlives a single run.
 *
 * <p>An order is never null. A null order is an engine invariant violation and crashes bare rather
 * than being reported as a diagnosable error: comparing it would silently reorder data instead.
 */
public final class RootAssembly implements Serializable {

    private Map<String, Object> rootFields;
    private SourceOrder rootOrder;
    private boolean rootPresent;

    /** The embeds directly under the root: field name, then element key. */
    private final Map<String, Map<List<Object>, ElementNode>> children = new LinkedHashMap<>();

    /** Which element each identity value names, per embed — how a child finds the row it hangs under. */
    private final Map<List<String>, Map<Object, ElementNode>> byIdentity = new LinkedHashMap<>();

    /** Children whose parent row has not arrived yet, by the parent they are waiting for. */
    private final Map<WaitingOn, List<Pending>> waiting = new LinkedHashMap<>();

    /**
     * What has been taken in since the last thing went out, kept apart by what carries it out again. A
     * rendered document carries the root row and every element, so both are released by one; the key row
     * of a deleted root carries the root's own change alone, and releasing the elements with it would say
     * they had been shown when a key row holds none of them.
     */
    private final Absorbed fromRoot = new Absorbed();
    private final Absorbed fromElements = new Absorbed();

    /** Whether the root row is currently in the document — false before it arrives and after it is deleted. */
    public boolean rootPresent() {
        return rootPresent;
    }

    /**
     * Applies the root row of an insert, update or snapshot read. Returns whether the assembly changed:
     * an order at or beneath the root's own is refused.
     */
    public boolean applyRoot(Map<String, Object> fields, SourceOrder order) {
        return applyRoot(fields, order, Map.of());
    }

    /**
     * The same, taking note of where the change came from so the frontier stays below it until a document
     * carrying it goes out. A change refused as too old takes no note: whoever sent the one that beat it
     * has already shown this spot, and holding for a change that will never be rendered pins the chain.
     */
    public boolean applyRoot(Map<String, Object> fields, SourceOrder order,
            Map<String, ChainPosition> positions) {
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(positions, "positions");
        if (!wins(order, rootOrder)) {
            return false;
        }
        rootFields = copyOf(fields);
        rootOrder = order;
        rootPresent = true;
        fromRoot.add(positions);
        return true;
    }

    /**
     * Marks the root deleted at {@code order}, keeping every element attached for a root that returns.
     * Returns whether the assembly changed.
     */
    public boolean deleteRoot(SourceOrder order) {
        return deleteRoot(order, Map.of());
    }

    /** The same, taking note of where the deletion came from — see {@link #applyRoot}. */
    public boolean deleteRoot(SourceOrder order, Map<String, ChainPosition> positions) {
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(positions, "positions");
        if (!wins(order, rootOrder)) {
            return false;
        }
        rootFields = null;
        rootOrder = order;
        rootPresent = false;
        fromRoot.add(positions);
        return true;
    }

    /**
     * Applies one element's row — an update of an element already there keeps its place in the array and
     * the children beneath it. Returns whether the assembly changed; a child held for a parent that has
     * not arrived has changed it. {@code positions} is where the change came from on the chains that
     * carried it, kept only while it is held, so the frontier can be told to stay below it.
     */
    public boolean applyElement(ElementRef ref, Map<String, Object> fields, SourceOrder order,
            Map<String, ChainPosition> positions) {
        Objects.requireNonNull(fields, "fields");
        return mutate(ref, copyOf(fields), order, positions);
    }

    /**
     * Deletes one element, leaving a tombstone at {@code order} and its subtree in place. Returns
     * whether the assembly changed.
     */
    public boolean deleteElement(ElementRef ref, SourceOrder order, Map<String, ChainPosition> positions) {
        return mutate(ref, null, order, positions);
    }

    /**
     * Moves one element from the parent {@code from} names to the parent {@code to} names, within this
     * root. **The whole node travels, children and all** — moving only the row would strand the subtree
     * beneath it, and nothing will ever resend those descendants. Returns whether the assembly changed.
     *
     * <p>When the new parent has not arrived yet the element is held rather than left where it was:
     * the source has already said it belongs elsewhere, so showing the old placement states a
     * relationship that is no longer true, and if the new parent never arrives that stays wrong for good
     * with nothing to signal it. Held, it sits in the pending bucket where an unresolvable parent is
     * already accounted for. An element that was never at {@code from} is simply placed at {@code to}.
     */
    public boolean reparentElement(ElementRef from, ElementRef to, Map<String, Object> fields, SourceOrder order,
            Map<String, ChainPosition> positions) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(order, "order");
        if (!from.pathId().equals(to.pathId()) || !from.elementKey().equals(to.elementKey())) {
            throw new IllegalArgumentException("a move names one element: " + from + " -> " + to);
        }
        Map<String, Map<List<Object>, ElementNode>> source = containerFor(from);
        Map<List<Object>, ElementNode> slot = source == null ? null : source.get(from.field());
        ElementNode moved = slot == null ? null : slot.get(from.elementKey());
        if (slot == null || moved == null || moved.deleted()) {
            return mutate(to, copyOf(fields), order, positions);
        }
        if (!wins(order, moved.order())) {
            return false;
        }
        slot.remove(from.elementKey());
        moved.set(copyOf(fields), order);
        absorbed(positions);
        Map<String, Map<List<Object>, ElementNode>> target = containerFor(to);
        if (target == null) {
            waiting.computeIfAbsent(new WaitingOn(to.parentPathId(), to.parentIdentity()), on -> new ArrayList<>())
                    .add(Pending.of(to, moved, order, positions));
            return true;
        }
        target.computeIfAbsent(to.field(), field -> new LinkedHashMap<>()).put(to.elementKey(), moved);
        return true;
    }

    /**
     * The lowest position held per chain — the bound the durable frontier must stay below for everything
     * this assembly has taken in and not sent on: the elements waiting for an ancestor that has not
     * arrived, and whatever has been absorbed since the last document went out. Chains holding nothing do
     * not appear, and a chain's two halves are reported as they arrived: the order to compare on, the
     * token to persist.
     */
    public Map<String, ChainPosition> lowestHeldByChain() {
        Map<String, ChainPosition> lowest = new LinkedHashMap<>(fromRoot.lowest);
        lowest(lowest, fromElements.lowest);
        for (List<Pending> held : waiting.values()) {
            for (Pending pending : held) {
                lowest(lowest, pending.positions());
            }
        }
        return lowest;
    }

    /**
     * What a document going out now covers, chain by chain: the highest position it has taken in and not
     * yet sent on. The highest and not the lowest, because the document carries every one of them — report
     * the lowest and the ones above it are reported by nobody and trailed for good.
     *
     * <p>Nothing here weighs what is still held beneath: a position covered is a candidate, and how far a
     * frontier may really go is decided by the bound combined across every instance, which is exactly what
     * everything still held keeps down. An element that has not found its ancestor is in no document and
     * so is covered by nothing.
     */
    public Map<String, ChainPosition> covered() {
        Map<String, ChainPosition> highest = new LinkedHashMap<>(fromRoot.highest);
        highest(highest, fromElements.highest);
        return highest;
    }

    /**
     * What the key row of a deleted root covers: the root's own change and nothing else. A key row carries
     * no element, so whatever was absorbed alongside the deletion has still been shown to nobody.
     */
    public Map<String, ChainPosition> coveredByADeletion() {
        return new LinkedHashMap<>(fromRoot.highest);
    }

    /**
     * Records that a document carrying everything absorbed so far has gone downstream, releasing it. What
     * is still waiting for an ancestor was in no document and keeps holding the frontier back.
     */
    public void documentSent() {
        fromRoot.clear();
        fromElements.clear();
    }

    /**
     * Records that the key row of a deleted root has gone downstream. It releases the root's own change
     * alone — without this a root deleted and never seen again would pin its chain for as long as the job
     * runs, and with any more than this the elements it left behind would be reported as shown.
     */
    public void deletionSent() {
        fromRoot.clear();
    }

    /**
     * The document as it now stands, or empty while the root is absent. {@code slots} is the declared
     * shape of the embeds under the root, each carrying its own: it decides which field every embed
     * occupies and whether an absent one renders as an empty array or not at all.
     */
    public Optional<Map<String, Object>> render(List<EmbedSlot> slots) {
        Objects.requireNonNull(slots, "slots");
        if (!rootPresent) {
            return Optional.empty();
        }
        Map<String, Object> document = new LinkedHashMap<>(rootFields);
        renderInto(document, children, slots);
        return Optional.of(document);
    }

    private boolean mutate(ElementRef ref, Map<String, Object> fields, SourceOrder order,
            Map<String, ChainPosition> positions) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(positions, "positions");
        Map<String, Map<List<Object>, ElementNode>> container = containerFor(ref);
        if (container == null) {
            // Not recorded as absorbed: what waits keeps its own position and is reported from there, and
            // it goes on being reported after a document has released everything absorbed.
            waiting.computeIfAbsent(new WaitingOn(ref.parentPathId(), ref.parentIdentity()), on -> new ArrayList<>())
                    .add(Pending.of(ref, fields, order, positions));
            return true;
        }
        Map<List<Object>, ElementNode> slot = container.computeIfAbsent(ref.field(), field -> new LinkedHashMap<>());
        ElementNode held = slot.get(ref.elementKey());
        if (held != null) {
            if (!wins(order, held.order())) {
                return false;
            }
            held.set(fields, order);
            absorbed(positions);
            return true;
        }
        ElementNode element = new ElementNode(fields, order);
        slot.put(ref.elementKey(), element);
        absorbed(positions);
        if (ref.identity() != null) {
            byIdentity.computeIfAbsent(ref.pathId(), path -> new LinkedHashMap<>()).put(ref.identity(), element);
            release(ref.pathId(), ref.identity());
        }
        return true;
    }

    /**
     * How many elements this document holds, at every depth. It is the quantity a per-document limit is
     * about: a document is assembled whole, so what it holds is what has to be in memory at once, and an
     * element four levels down occupies that as much as one hanging off the root.
     *
     * <p>Deleted elements are not held and are not counted. What is left where one was is a record that it
     * was deleted, kept only until a replay can no longer bring it back; counting those would tie the width
     * of a document to how far behind the replay window is, which is neither a property of the data nor
     * something whoever set a limit can see.
     *
     * <p>Nor is what is waiting on a parent that has not arrived: it is not in the document, it is not
     * rendered, and it is already accounted for where things wait.
     */
    public long elements() {
        return countIn(children);
    }

    private static long countIn(Map<String, Map<List<Object>, ElementNode>> nodes) {
        long held = 0;
        for (Map<List<Object>, ElementNode> slot : nodes.values()) {
            for (ElementNode element : slot.values()) {
                if (!element.deleted()) {
                    held++;
                }
                held += countIn(element.children());
            }
        }
        return held;
    }

    /** Applies whatever was waiting for the element just added, which may in turn release its own children. */
    private void release(List<String> pathId, Object identity) {
        List<Pending> held = waiting.remove(new WaitingOn(pathId, identity));
        if (held == null) {
            return;
        }
        for (Pending pending : held) {
            if (pending.node() == null) {
                mutate(pending.ref(), pending.fields(), pending.order(), pending.positions());
            } else {
                ElementRef ref = pending.ref();
                // Released only from the parent that just arrived, so its embed is there by construction;
                // if it were not, attaching would lose the node and its subtree with no error anywhere.
                Map<String, Map<List<Object>, ElementNode>> parent = Objects.requireNonNull(
                        containerFor(ref), "a held child is released only once its parent is present");
                parent.computeIfAbsent(ref.field(), field -> new LinkedHashMap<>())
                        .put(ref.elementKey(), pending.node());
                // Off the waiting bucket and into the tree, which is not the same as out of here: until a
                // document carries it away it is as unsent as it was while it waited.
                absorbed(pending.positions());
            }
        }
    }

    /** Takes note of where a change taken in came from, so the frontier is kept below it until it leaves. */
    private void absorbed(Map<String, ChainPosition> positions) {
        fromElements.add(positions);
    }

    private static void lowest(Map<String, ChainPosition> into, Map<String, ChainPosition> positions) {
        positions.forEach((chain, position) -> into.merge(chain, position,
                (kept, candidate) -> candidate.order().compareTo(kept.order()) < 0 ? candidate : kept));
    }

    private static void highest(Map<String, ChainPosition> into, Map<String, ChainPosition> positions) {
        positions.forEach((chain, position) -> into.merge(chain, position,
                (kept, candidate) -> candidate.order().compareTo(kept.order()) > 0 ? candidate : kept));
    }

    /**
     * What one kind of change has contributed since the last thing carrying it went out, from both ends:
     * the lowest is what the frontier must stay below while it is still here, the highest is what goes out
     * with it. Both are positions that really arrived — a frontier is persisted with the token that came
     * with the order, so a value made up between two of them could never be written down.
     */
    private static final class Absorbed implements Serializable {

        private final Map<String, ChainPosition> lowest = new LinkedHashMap<>();
        private final Map<String, ChainPosition> highest = new LinkedHashMap<>();

        void add(Map<String, ChainPosition> positions) {
            RootAssembly.lowest(lowest, positions);
            RootAssembly.highest(highest, positions);
        }

        void clear() {
            lowest.clear();
            highest.clear();
        }
    }

    /** Where {@code ref}'s embed lives, or null while the parent row it names has not arrived. */
    private Map<String, Map<List<Object>, ElementNode>> containerFor(ElementRef ref) {
        List<String> parentPath = ref.parentPathId();
        if (parentPath.isEmpty()) {
            return children;
        }
        ElementNode parent = byIdentity.getOrDefault(parentPath, Map.of()).get(ref.parentIdentity());
        return parent == null ? null : parent.children();
    }

    private static void renderInto(Map<String, Object> document,
            Map<String, Map<List<Object>, ElementNode>> held, List<EmbedSlot> slots) {
        for (EmbedSlot slot : slots) {
            Map<List<Object>, ElementNode> elements = held.get(slot.path());
            switch (slot.as()) {
                case ARRAY -> document.put(slot.path(), liveOf(elements, slot));
                case OBJECT -> latestOf(elements)
                        .ifPresent(element -> document.put(slot.path(), renderOne(element, slot)));
            }
        }
    }

    private static Map<String, Object> renderOne(ElementNode element, EmbedSlot slot) {
        Map<String, Object> rendered = new LinkedHashMap<>(element.fields());
        renderInto(rendered, element.children(), slot.children());
        return rendered;
    }

    private static List<Map<String, Object>> liveOf(Map<List<Object>, ElementNode> elements, EmbedSlot slot) {
        List<Map<String, Object>> live = new ArrayList<>();
        if (elements != null) {
            for (ElementNode element : elements.values()) {
                if (!element.deleted()) {
                    live.add(renderOne(element, slot));
                }
            }
        }
        return live;
    }

    private static Optional<ElementNode> latestOf(Map<List<Object>, ElementNode> elements) {
        ElementNode latest = null;
        if (elements != null) {
            for (ElementNode element : elements.values()) {
                if (!element.deleted() && (latest == null || element.order().compareTo(latest.order()) > 0)) {
                    latest = element;
                }
            }
        }
        return Optional.ofNullable(latest);
    }

    private static boolean wins(SourceOrder candidate, SourceOrder held) {
        return held == null || candidate.compareTo(held) > 0;
    }

    private static Map<String, Object> copyOf(Map<String, Object> fields) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    /** One element as last applied: its row, or null once deleted, the order that put it there, and its own embeds. */
    private static final class ElementNode implements Serializable {

        private Map<String, Object> fields;
        private SourceOrder order;
        private final Map<String, Map<List<Object>, ElementNode>> children = new LinkedHashMap<>();

        private ElementNode(Map<String, Object> fields, SourceOrder order) {
            this.fields = fields;
            this.order = order;
        }

        private void set(Map<String, Object> newFields, SourceOrder newOrder) {
            fields = newFields;
            order = newOrder;
        }

        private Map<String, Object> fields() {
            return fields;
        }

        private SourceOrder order() {
            return order;
        }

        private Map<String, Map<List<Object>, ElementNode>> children() {
            return children;
        }

        private boolean deleted() {
            return fields == null;
        }
    }

    /** The parent an element is waiting for: the embed the parent belongs to, and the value it answers to. */
    private record WaitingOn(List<String> pathId, Object identity) implements Serializable { }

    /**
     * Held until the row it hangs under arrives: either an element event to apply ({@code node} null,
     * a null {@code fields} meaning a deletion), or a whole node being moved, which is attached as it
     * stands so its subtree travels with it.
     */
    private record Pending(ElementRef ref, Map<String, Object> fields, SourceOrder order, ElementNode node,
            Map<String, ChainPosition> positions) implements Serializable {

        private static Pending of(ElementRef ref, Map<String, Object> fields, SourceOrder order,
                Map<String, ChainPosition> positions) {
            return new Pending(ref, fields, order, null, positions);
        }

        private static Pending of(ElementRef ref, ElementNode node, SourceOrder order,
                Map<String, ChainPosition> positions) {
            return new Pending(ref, null, order, node, positions);
        }
    }
}
