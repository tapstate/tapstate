package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.ReplayFloor;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

    /**
     * Said rather than derived, so this state's identity does not move when its fields do. Derived, it
     * changes with every field added or dropped, and every document already written becomes unreadable on
     * the first key that has to come back from the cold layer - an upgrade that looked clean until the
     * first miss. Held still, bytes written before a field existed still load, and that field reads back
     * at its zero. Bump this deliberately when that is the wrong answer and the old bytes should be
     * refused outright instead.
     *
     * <p>Bumped to 2 when what a document holds stopped being wrapped in a type of its own: bytes written
     * before that name a class this build does not have, so they cannot be read at all. Refusing them on
     * the identity says that; letting them through says it as a missing class, from inside a read of a
     * key that had gone cold, which is the same outcome reported as something else entirely.
     */
    private static final long serialVersionUID = 2L;

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
     * What the root row has contributed since the last thing carrying it went out. Kept apart from the
     * elements by what carries it out again: a rendered document carries the root row and every element, so
     * both are released by one; the key row of a deleted root carries the root's own change alone, and
     * releasing the elements with it would say they had been shown when a key row holds none of them.
     */
    private final Absorbed fromRoot = new Absorbed();

    /**
     * Every element change taken in since the last document went out, one entry each rather than merged.
     * They are merged for the bound they report and would take far less room that way, but each is also
     * what a document going out covers on its chain, and a merged low-water mark cannot say which
     * positions those were.
     */
    private final List<NestElement> heldElements = new ArrayList<>();

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
        return mutate(new NestElement(ref, fields, order, positions));
    }

    /**
     * Deletes one element, leaving a tombstone at {@code order} and its subtree in place. Returns
     * whether the assembly changed.
     */
    public boolean deleteElement(ElementRef ref, SourceOrder order, Map<String, ChainPosition> positions) {
        return mutate(new NestElement(ref, null, order, positions));
    }

    /**
     * Applies one element's change — the row it carries, or its deletion. Returns whether the assembly
     * changed.
     */
    public boolean take(NestElement change) {
        Objects.requireNonNull(change, "change");
        return mutate(change);
    }

    /**
     * Whether this document is holding anything the durable frontier has to stay below — asked far more
     * often than what, and answered without building it.
     */
    public boolean holdsAnything() {
        return !heldElements.isEmpty() || !waiting.isEmpty() || !fromRoot.lowest.isEmpty();
    }

    /**
     * How many changes this document has taken in and not passed on, wherever each of them is waiting: the
     * ones absorbed into the tree while the root is absent, and the ones parked for an ancestor that has not
     * arrived. Both are a change consumed and shown to nobody, which is the quantity a pending limit is
     * about; what ends each wait differs, what it costs to wait does not.
     *
     * <p>Counted by change rather than by element, unlike {@link #elements()}. One element written a
     * thousand times under an absent root is one element and a thousand changes held against it, and this is
     * the count that says so.
     */
    public long pending() {
        long parked = 0L;
        for (List<Pending> bucket : waiting.values()) {
            parked += bucket.size();
        }
        return heldElements.size() + parked;
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
        return move(new NestElement(to, fields, order, positions, from));
    }

    /**
     * Carries one element from where it was to where its row now says it is. Both halves of an address can
     * change and either may change alone: the parent it hangs under, the key the document shows it by, or
     * both at once when a tree keys an embed by the same column its children point at.
     *
     * <p>The node is carried rather than rebuilt, which is the whole of "the subtree travels": what hangs
     * beneath is held by the node itself, and what points at it points at the node rather than at where it
     * sat. So nothing beneath has to be visited, and nothing beneath can be left behind.
     *
     * <p>An element that is not where the change says it was is simply placed where it now belongs. That
     * covers a replay of a move already applied and a source whose earlier row never reached this document,
     * and both want the same answer: the element ends up where the row says, once.
     */
    private boolean move(NestElement change) {
        ElementRef from = change.movedFrom();
        ElementRef to = change.ref();
        Map<String, Map<List<Object>, ElementNode>> source = containerFor(from);
        Map<List<Object>, ElementNode> slot = source == null ? null : source.get(from.field());
        ElementNode moved = slot == null ? null : slot.get(from.elementKey());
        if (slot == null || moved == null || moved.deleted()) {
            return place(change);
        }
        if (!wins(change.order(), moved.order())) {
            return false;
        }
        slot.remove(from.elementKey());
        // The name children reach a parent by is that parent's identity, so one given up may not go on
        // answering: a child naming it would be placed under a node that is no longer what the name says,
        // and the document would be wrong with every count in place. Removed only where it still points at
        // this node, since another may have taken the name over in the meantime.
        if (from.identity() != null && !from.identity().equals(to.identity())) {
            Map<Object, ElementNode> named = byIdentity.get(from.pathId());
            if (named != null) {
                named.remove(from.identity(), moved);
            }
        }
        moved.set(change.fields(), change.order(), change.positions());
        absorbed(change);
        Map<String, Map<List<Object>, ElementNode>> target = containerFor(to);
        if (target == null) {
            waiting.computeIfAbsent(new WaitingOn(to.parentPathId(), to.parentIdentity()), on -> new ArrayList<>())
                    .add(new Pending(change, moved));
            return true;
        }
        target.computeIfAbsent(to.field(), field -> new LinkedHashMap<>()).put(to.elementKey(), moved);
        if (to.identity() != null) {
            byIdentity.computeIfAbsent(to.pathId(), path -> new LinkedHashMap<>()).put(to.identity(), moved);
        }
        return true;
    }

    /**
     * Takes the element at {@code from} out of this document together with everything beneath it, and
     * hands back what hung there so another document can be given it.
     *
     * <p>What hangs beneath cannot travel as changes of its own. Those rows arrived long ago and nothing
     * will ever send them again — the source edited one row, the one that moved — so unless they are read
     * out of the document that has them they are simply lost, quietly, with nothing anywhere reporting it.
     *
     * <p><b>The element itself is not handed over.</b> It is the row the source did edit, so it arrives at
     * its new document as an ordinary change. Included here as well, two paths could place it and the two
     * would have to agree about which order won.
     *
     * <p>Handed over shallowest first, so whoever applies them places a parent before its children and
     * nothing is parked as waiting on the way in. Records of deletions travel like anything else: left
     * behind, a replay from beneath the frontier would put the row back in the document that gained the
     * element, where nothing would ever delete it again.
     *
     * <p>Positions are not carried. What each of those rows covered was accounted for by the document that
     * held them, and what keeps the frontier below a hand-over in flight is the hand-over itself being
     * counted as still held — not a second copy of every position in the subtree.
     */
    /**
     * Whether the element at {@code from} is here and still holds something beneath it. Asked before a move
     * is settled, because an element with nothing beneath it needs no hand-over at all and one with a
     * subtree cannot be taken out without one.
     */
    public boolean holdsSubtreeAt(ElementRef from) {
        Objects.requireNonNull(from, "from");
        Map<String, Map<List<Object>, ElementNode>> source = containerFor(from);
        Map<List<Object>, ElementNode> slot = source == null ? null : source.get(from.field());
        ElementNode held = slot == null ? null : slot.get(from.elementKey());
        return held != null && !held.children().isEmpty();
    }

    /**
     * The rows beneath {@code from}, without taking them out of this document.
     *
     * <p>Separate from taking them out because the two may not happen in the other order: a drain stores
     * every document it touched however it ended, so a document that gave up a subtree which then failed to
     * be published is stored without it, while the rows reached nowhere else. A replay does not bring them
     * back - it resumes above the change that moved them, so nothing looks for them again.
     */
    public List<NestElement> subtreeAt(ElementRef from) {
        Objects.requireNonNull(from, "from");
        ElementNode leaving = nodeAt(from);
        if (leaving == null) {
            return List.of();
        }
        List<NestElement> beneath = new ArrayList<>();
        collectBeneath(leaving, from.pathId(), identityOf(from.pathId(), leaving), beneath);
        return beneath;
    }

    public List<NestElement> detachSubtree(ElementRef from) {
        Objects.requireNonNull(from, "from");
        Map<List<Object>, ElementNode> slot = slotFor(from);
        ElementNode leaving = slot == null ? null : slot.get(from.elementKey());
        if (leaving == null) {
            return List.of();
        }
        List<NestElement> handedOver = subtreeAt(from);
        slot.remove(from.elementKey());
        forgetNames(leaving, from.pathId());
        return handedOver;
    }

    /** The elements one embed of one parent holds, or null where nothing addresses that place yet. */
    private Map<List<Object>, ElementNode> slotFor(ElementRef from) {
        Map<String, Map<List<Object>, ElementNode>> source = containerFor(from);
        return source == null ? null : source.get(from.field());
    }

    private ElementNode nodeAt(ElementRef from) {
        Map<List<Object>, ElementNode> slot = slotFor(from);
        return slot == null ? null : slot.get(from.elementKey());
    }

    /**
     * Reads out everything this document holds and leaves it empty, handing back what hung here so the key
     * this row now carries can be given all of it.
     *
     * <p>The whole of it rather than a subtree, because the row the source edited is the <b>root</b>. The key
     * this document was filed under names a row the source no longer has, so nothing about this key will ever
     * be sent again — not the root row, and not one of the elements. An element directly under the root is in
     * exactly the same position as one four levels down, which is the one way this differs from
     * {@link #detachSubtree}: there the element that moved arrives on its own as an ordinary change and so is
     * left out, here there is no such row for any of them.
     *
     * <p>What was waiting for a parent that never arrived travels too. It is in this document's state and in
     * nothing else; left behind under a key that is about to be deleted it goes with the key, and nothing
     * anywhere reports that it went. Carried over, it simply goes on waiting under the new key.
     *
     * <p><b>Nothing is left held.</b> The positions of what has been taken in and not sent are dropped here
     * along with the rows they belonged to: they are somebody else's to account for now, and what keeps the
     * frontier below a move in flight is the move itself being counted as still held. Kept, they would leave
     * the deleted key un-droppable for the life of the job with every count reading healthy.
     */
    public List<NestElement> detachEverything() {
        List<NestElement> handedOver = new ArrayList<>();
        collectFrom(children, List.of(), null, handedOver);
        for (List<Pending> bucket : waiting.values()) {
            for (Pending pending : bucket) {
                NestElement held = pending.held();
                handedOver.add(new NestElement(held.ref(), held.fields(), held.order(), Map.of()));
                if (pending.node() != null) {
                    // A node moved to a parent that had not arrived is attached as it stands, so its own
                    // subtree is hanging off it and is here rather than in the tree above.
                    collectFrom(pending.node().children(), held.ref().pathId(), held.ref().identity(),
                            handedOver);
                }
            }
        }
        children.clear();
        byIdentity.clear();
        waiting.clear();
        heldElements.clear();
        return handedOver;
    }

    /**
     * Everything under one element, level by level so a parent is always listed before its children. Each
     * is rebuilt as the change that would put it back: where it hangs, the row it holds, and the order that
     * decides whether it still wins where it lands.
     */
    private void collectBeneath(ElementNode holder, List<String> holderPathId, Object holderIdentity,
            List<NestElement> into) {
        collectFrom(holder.children(), holderPathId, holderIdentity, into);
    }

    /**
     * The same, over a map of embeds rather than over the element holding them, so the root's own children
     * are read out by the one path that reads out anyone else's. The root differs only in having no element
     * above it: its path is empty and there is no identity for its children to name.
     */
    private void collectFrom(Map<String, Map<List<Object>, ElementNode>> embeds, List<String> holderPathId,
            Object holderIdentity, List<NestElement> into) {
        List<ElementNode> nodes = new ArrayList<>();
        List<List<String>> paths = new ArrayList<>();
        List<Object> identities = new ArrayList<>();
        for (Map.Entry<String, Map<List<Object>, ElementNode>> embed : embeds.entrySet()) {
            List<String> pathId = deeper(holderPathId, embed.getKey());
            for (Map.Entry<List<Object>, ElementNode> held : embed.getValue().entrySet()) {
                ElementNode node = held.getValue();
                Object identity = identityOf(pathId, node);
                into.add(new NestElement(new ElementRef(pathId, holderIdentity, held.getKey(), identity),
                        node.fields(), node.order(), Map.of()));
                nodes.add(node);
                paths.add(pathId);
                identities.add(identity);
            }
        }
        for (int i = 0; i < nodes.size(); i++) {
            collectBeneath(nodes.get(i), paths.get(i), identities.get(i), into);
        }
    }

    /** Which identity value names this node in its embed, or null where the embed has no children. */
    private Object identityOf(List<String> pathId, ElementNode node) {
        Map<Object, ElementNode> named = byIdentity.get(pathId);
        if (named == null) {
            return null;
        }
        for (Map.Entry<Object, ElementNode> entry : named.entrySet()) {
            if (entry.getValue() == node) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Drops the names of a node and everything beneath it. Left behind, a name would go on answering for a
     * row this document no longer holds, and the next child to arrive would be filed under it: rendered by
     * nothing, waiting for nothing, reported by nobody.
     */
    private void forgetNames(ElementNode node, List<String> pathId) {
        Map<Object, ElementNode> named = byIdentity.get(pathId);
        if (named != null) {
            named.values().remove(node);
        }
        node.children().forEach((field, held) -> {
            List<String> deeper = deeper(pathId, field);
            held.values().forEach(child -> forgetNames(child, deeper));
        });
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
        for (NestElement held : heldElements) {
            lowest(lowest, held.positions());
        }
        for (List<Pending> bucket : waiting.values()) {
            for (Pending pending : bucket) {
                lowest(lowest, pending.held().positions());
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
        for (NestElement held : heldElements) {
            highest(highest, held.positions());
        }
        return highest;
    }

    /**
     * The lowest position per chain of everything a document going out now would carry. The mirror of
     * {@link #covered()}: that says what a send releases, this says what not sending yet has to keep the
     * frontier below - a document that has changed and is waiting to go out is a change that survives a
     * restart and that nothing will ever send again, since no further event is due for that root.
     *
     * <p><b>What is waiting for an ancestor is deliberately absent.</b> It travels to the store inside this
     * state and comes back out when its ancestor arrives, so something does finish it and the frontier may
     * pass it. Counting it here would pin the frontier on a foreign key pointing at a row that never
     * arrives, for as long as the job runs.
     */
    public Map<String, ChainPosition> lowestUnsentByChain() {
        Map<String, ChainPosition> lowest = new LinkedHashMap<>(fromRoot.lowest);
        for (NestElement held : heldElements) {
            lowest(lowest, held.positions());
        }
        return lowest;
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
        heldElements.clear();
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
        return render(slots, Map.of());
    }

    /**
     * The document as it now stands, with the rows it points at filled in from {@code resolved} - what was
     * fetched for the identities {@link #referencesNeeded} asked for, by namespace.
     *
     * <p>A reference with nothing fetched for it renders no field at all, exactly as an object embed with
     * no element does. The two cases it covers want the same thing: a row that was deleted is gone from the
     * document rather than frozen at its last value, and a row that has not arrived yet is not shown as
     * absent data. Telling those apart is not this method's to do - it renders what it was handed.
     */
    public Optional<Map<String, Object>> render(List<EmbedSlot> slots,
            Map<String, Map<Object, Map<String, Object>>> resolved) {
        Objects.requireNonNull(slots, "slots");
        Objects.requireNonNull(resolved, "resolved");
        if (!rootPresent) {
            return Optional.empty();
        }
        Map<String, Object> document = new LinkedHashMap<>(rootFields);
        renderInto(document, children, slots, resolved);
        return Optional.of(document);
    }

    /**
     * Which rows this document would have to be handed to render, by the namespace each is kept in. It is
     * asked before rendering rather than during it, so that a document naming two hundred rows is one reach
     * for two hundred keys instead of two hundred reaches - and so that the depth they sit at costs
     * nothing, every level's references landing in the same request as the root's.
     *
     * <p>A reference whose columns are null on the row carrying it asks for nothing. There is no row that
     * answers to a key of nulls, and asking would spend a lookup to be told so.
     */
    public Map<String, Set<List<Object>>> referencesNeeded(List<EmbedSlot> slots) {
        Objects.requireNonNull(slots, "slots");
        Map<String, Set<List<Object>>> needed = new LinkedHashMap<>();
        if (rootPresent) {
            collectReferences(rootFields, children, slots, needed);
        }
        return needed;
    }

    private static void collectReferences(Map<String, Object> fields,
            Map<String, Map<List<Object>, ElementNode>> held, List<EmbedSlot> slots,
            Map<String, Set<List<Object>>> needed) {
        for (EmbedSlot slot : slots) {
            if (slot.isReference()) {
                List<Object> key = NestKeys.valuesOf(fields, slot.referenceFields());
                if (!key.contains(null)) {
                    needed.computeIfAbsent(slot.lookupMap(), namespace -> new LinkedHashSet<>()).add(key);
                }
                continue;
            }
            Map<List<Object>, ElementNode> elements = held.get(slot.path());
            if (elements == null) {
                continue;
            }
            for (ElementNode element : elements.values()) {
                if (!element.deleted()) {
                    collectReferences(element.fields(), element.children(), slot.children(), needed);
                }
            }
        }
    }

    private boolean mutate(NestElement change) {
        if (change.departure()) {
            return depart(change);
        }
        return change.moves() ? move(change) : place(change);
    }

    /**
     * Takes out an element whose row now hangs from a parent this document does not hold. Nothing is left
     * where it was: the element is not deleted, it is somewhere else, and a record of a deletion here would
     * answer a question about this document that the source answers differently.
     *
     * <p><b>An element still holding a subtree is left exactly where it is.</b> Carrying it across means
     * handing the whole subtree to the document that gains it, and until there is somewhere to hand it
     * through, taking the element out here would drop rows that nothing will ever resend - invisibly, since
     * they were never going to be reported by anything. A stale copy is a document that disagrees with its
     * source and can be seen to; rows gone are not. So the weaker failure is chosen deliberately, and the
     * choice lives here rather than at the level that noticed the move, which cannot know what hangs
     * beneath.
     */
    private boolean depart(NestElement change) {
        ElementRef from = change.movedFrom();
        Map<String, Map<List<Object>, ElementNode>> source = containerFor(from);
        Map<List<Object>, ElementNode> slot = source == null ? null : source.get(from.field());
        ElementNode leaving = slot == null ? null : slot.get(from.elementKey());
        if (leaving == null || leaving.deleted() || !wins(change.order(), leaving.order())) {
            return false;
        }
        if (!leaving.children().isEmpty()) {
            return false;
        }
        slot.remove(from.elementKey());
        Map<Object, ElementNode> named = byIdentity.get(from.pathId());
        if (named != null) {
            named.values().remove(leaving);
        }
        absorbed(change);
        return true;
    }

    private boolean place(NestElement change) {
        ElementRef ref = change.ref();
        Map<String, Object> fields = change.fields();
        SourceOrder order = change.order();
        Map<String, Map<List<Object>, ElementNode>> container = containerFor(ref);
        if (container == null) {
            // Not recorded among what the document absorbed: what waits keeps its own position and is
            // reported from there, and it goes on being reported after a document has released the rest.
            waiting.computeIfAbsent(new WaitingOn(ref.parentPathId(), ref.parentIdentity()), on -> new ArrayList<>())
                    .add(new Pending(change, null));
            return true;
        }
        Map<List<Object>, ElementNode> slot = container.computeIfAbsent(ref.field(), field -> new LinkedHashMap<>());
        ElementNode held = slot.get(ref.elementKey());
        if (held != null) {
            if (!wins(order, held.order())) {
                return false;
            }
            held.set(fields, order, change.positions());
            absorbed(change);
            return true;
        }
        ElementNode element = new ElementNode(fields, order, change.positions());
        slot.put(ref.elementKey(), element);
        absorbed(change);
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
     * rendered, and it is counted by {@link #pending()} instead, which is what the limit on waiting reads.
     */
    public long elements() {
        return countIn(children);
    }

    /**
     * How many deleted elements this document is keeping the record of, at every depth. It is the quantity a
     * limit on records of deletion is about, and neither of the other two counts reaches it: what is deleted
     * renders as nothing, so {@link #elements()} does not count it, and it waits for nothing, so {@link
     * #pending()} does not either. A document can hold a million of these while both of those read small.
     *
     * <p>What makes it worth counting at all is that it does not follow the shape of the data. A record of
     * deletion is kept until a replay can no longer undo it, so how many there are follows how far behind the
     * durable frontier is — and a frontier that has stopped moving is a document that grows without bound.
     */
    public long tombstones() {
        return deletionsIn(children);
    }

    /**
     * Drops the records of deletion that no replay can reach any more, and says how many went. A record is
     * kept until every position the deletion covered sits strictly below where its chain would resume: at the
     * floor is delivered again, and below it never is.
     *
     * <p>Every way of not knowing keeps the record. A chain whose floor cannot be read, and a deletion that
     * arrived carrying no position at all, both leave nothing to weigh — and being wrong in this direction
     * costs one entry until the next sweep, while being wrong in the other is a row the source deleted coming
     * back at the next restart with nothing anywhere reporting it.
     *
     * <p><b>A record still holding a subtree is kept whatever the floor says</b>, and the sweep works upwards
     * so that a subtree of deletions collapses from the bottom. What hangs beneath a deleted element is kept
     * on purpose — nothing will ever resend those rows — so dropping the element they hang from would take
     * them with it. This is also the whole of "nothing is waiting on it": a child arriving under a deleted
     * parent is filed beneath it rather than parked anywhere else, so anything that could be waiting is
     * already part of the subtree this refuses to drop.
     */
    public long forgetDeletionsBelow(ReplayFloor floor) {
        Objects.requireNonNull(floor, "floor");
        long forgotten = forgetIn(children, List.of(), floor);
        byIdentity.values().removeIf(Map::isEmpty);
        return forgotten;
    }

    /**
     * Sweeps one embed's elements and everything beneath them, dropping what may go. Children are swept
     * before the element holding them is weighed, which is what lets a whole subtree of deletions go in one
     * pass rather than one level per sweep.
     */
    private long forgetIn(Map<String, Map<List<Object>, ElementNode>> nodes, List<String> path,
            ReplayFloor floor) {
        long forgotten = 0;
        for (Map.Entry<String, Map<List<Object>, ElementNode>> embed : nodes.entrySet()) {
            List<String> pathId = deeper(path, embed.getKey());
            Map<Object, ElementNode> named = byIdentity.get(pathId);
            Iterator<ElementNode> elements = embed.getValue().values().iterator();
            while (elements.hasNext()) {
                ElementNode element = elements.next();
                forgotten += forgetIn(element.children(), pathId, floor);
                if (forgettable(element, floor)) {
                    elements.remove();
                    // Out of everything that names it, not just out of the embed it was rendered from. Left
                    // behind, the name would go on answering, and the next child to arrive would be filed
                    // under a row that is no longer part of this document: rendered by nothing, waiting for
                    // nothing, reported by nobody.
                    if (named != null) {
                        named.values().remove(element);
                    }
                    forgotten++;
                }
            }
        }
        nodes.values().removeIf(Map::isEmpty);
        return forgotten;
    }

    /** Whether this element is a record of a deletion that nothing can undo any longer — see above. */
    private static boolean forgettable(ElementNode element, ReplayFloor floor) {
        if (!element.deleted() || !element.children().isEmpty()) {
            return false;
        }
        Map<String, SourceOrder> covered = element.deletedAt();
        if (covered.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, SourceOrder> position : covered.entrySet()) {
            Optional<SourceOrder> resumesAt = floor.of(position.getKey());
            if (resumesAt.isEmpty() || position.getValue().compareTo(resumesAt.get()) >= 0) {
                return false;
            }
        }
        return true;
    }

    private static List<String> deeper(List<String> path, String field) {
        List<String> below = new ArrayList<>(path);
        below.add(field);
        return List.copyOf(below);
    }

    private static long deletionsIn(Map<String, Map<List<Object>, ElementNode>> nodes) {
        long kept = 0;
        for (Map<List<Object>, ElementNode> slot : nodes.values()) {
            for (ElementNode element : slot.values()) {
                if (element.deleted()) {
                    kept++;
                }
                kept += deletionsIn(element.children());
            }
        }
        return kept;
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
            NestElement change = pending.held();
            if (pending.node() == null) {
                mutate(change);
            } else {
                ElementRef ref = change.ref();
                // Released only from the parent that just arrived, so its embed is there by construction;
                // if it were not, attaching would lose the node and its subtree with no error anywhere.
                Map<String, Map<List<Object>, ElementNode>> parent = Objects.requireNonNull(
                        containerFor(ref), "a held child is released only once its parent is present");
                parent.computeIfAbsent(ref.field(), field -> new LinkedHashMap<>())
                        .put(ref.elementKey(), pending.node());
                // Off the waiting bucket and into the tree, which is not the same as out of here: until a
                // document carries it away it is as unsent as it was while it waited.
                absorbed(change);
                // And it takes up its name here like any element placed directly, letting go of whatever
                // was waiting on it. A move may change the name children reach it by, and one arriving this
                // way without taking it up leaves its descendants waiting for the rest of the run - in this
                // document's state, in no rendered document, and reported by nothing.
                if (ref.identity() != null) {
                    byIdentity.computeIfAbsent(ref.pathId(), path -> new LinkedHashMap<>())
                            .put(ref.identity(), pending.node());
                    release(ref.pathId(), ref.identity());
                }
            }
        }
    }

    /** Takes note of a change taken in, so the frontier is kept below it until a document carries it out. */
    private void absorbed(NestElement change) {
        heldElements.add(change);
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
     * What the root row has contributed since the last thing carrying it went out, from both ends: the
     * lowest is what the frontier must stay below while it is still here, the highest is what goes out with
     * it. Both are positions that really arrived — a frontier is persisted with the token that came with the
     * order, so a value made up between two of them could never be written down.
     *
     * <p>Merged rather than kept one by one, unlike the elements: nothing ever stops holding for a root row
     * on its own, because the root is the one thing a document cannot be assembled without.
     */
    private static final class Absorbed implements Serializable {

        /** Said rather than derived, for the reason the assembly holding this gives. */
        private static final long serialVersionUID = 1L;

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
            Map<String, Map<List<Object>, ElementNode>> held, List<EmbedSlot> slots,
            Map<String, Map<Object, Map<String, Object>>> resolved) {
        for (EmbedSlot slot : slots) {
            if (slot.isReference()) {
                // Read off the row this level already carries. The columns holding the reference are ones
                // the row has anyway, so pointing at something costs the document no bytes of its own.
                List<Object> key = NestKeys.valuesOf(document, slot.referenceFields());
                Map<String, Object> row = resolved.getOrDefault(slot.lookupMap(), Map.of()).get(key);
                if (row != null) {
                    document.put(slot.path(), new LinkedHashMap<>(row));
                }
                continue;
            }
            Map<List<Object>, ElementNode> elements = held.get(slot.path());
            switch (slot.as()) {
                case ARRAY -> document.put(slot.path(), liveOf(elements, slot, resolved));
                case OBJECT -> latestOf(elements)
                        .ifPresent(element -> document.put(slot.path(), renderOne(element, slot, resolved)));
            }
        }
    }

    private static Map<String, Object> renderOne(ElementNode element, EmbedSlot slot,
            Map<String, Map<Object, Map<String, Object>>> resolved) {
        Map<String, Object> rendered = new LinkedHashMap<>(element.fields());
        renderInto(rendered, element.children(), slot.children(), resolved);
        return rendered;
    }

    private static List<Map<String, Object>> liveOf(Map<List<Object>, ElementNode> elements, EmbedSlot slot,
            Map<String, Map<Object, Map<String, Object>>> resolved) {
        List<Map<String, Object>> live = new ArrayList<>();
        if (elements != null) {
            for (ElementNode element : elements.values()) {
                if (!element.deleted()) {
                    live.add(renderOne(element, slot, resolved));
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

        /** Said rather than derived, for the reason the assembly holding this gives. */
        private static final long serialVersionUID = 1L;

        private Map<String, Object> fields;
        private SourceOrder order;

        /**
         * Where the deletion that emptied this element came from, empty while it holds a row. It is what the
         * record of a deletion is weighed against before being dropped, and it is kept here rather than
         * worked out later because by then the change it came on is long gone.
         *
         * <p>The order alone and not the token that travelled with it: a token is what a frontier is written
         * down with, and nothing writes a frontier down from a record of a deletion. Keeping it would be a
         * second copy of every chain token in the document, held for as long as the deletions are.
         */
        private Map<String, SourceOrder> deletedAt = Map.of();

        private final Map<String, Map<List<Object>, ElementNode>> children = new LinkedHashMap<>();

        private ElementNode(Map<String, Object> fields, SourceOrder order,
                Map<String, ChainPosition> positions) {
            set(fields, order, positions);
        }

        private void set(Map<String, Object> newFields, SourceOrder newOrder,
                Map<String, ChainPosition> positions) {
            fields = newFields;
            order = newOrder;
            // An element brought back is no longer a record of a deletion, so what the deletion covered goes
            // with it: weighed against a floor later it would drop a row that is live.
            deletedAt = newFields == null ? ordersOf(positions) : Map.of();
        }

        private Map<String, SourceOrder> deletedAt() {
            return deletedAt;
        }

        private static Map<String, SourceOrder> ordersOf(Map<String, ChainPosition> positions) {
            Map<String, SourceOrder> orders = new LinkedHashMap<>();
            positions.forEach((chain, position) -> orders.put(chain, position.order()));
            return orders;
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
     * Held until the row it hangs under arrives: either an element change to apply ({@code node} null),
     * or a whole node being moved, which is attached as it stands so its subtree travels with it.
     */
    private record Pending(NestElement held, ElementNode node) implements Serializable {
    }
}
