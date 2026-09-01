package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.ReplayFloor;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * One resolver vertex: it answers, for one embed, which row of the level above each of its keys hangs
 * from, and passes every change it sees one step nearer the document.
 *
 * <p>Two kinds of thing arrive on the same key and are meant to. A row of the embed itself declares
 * "my key hangs from that parent" and is also an element of the document in its own right, so it both
 * writes the mapping and travels on. A row from beneath asks the same key that question: answered, it
 * travels on; unanswered, it waits here until the row that answers it arrives; and answered with "that
 * parent is gone" it can never reach a document and goes to the dead-letter rather than being dropped
 * or held forever.
 *
 * <p>Which of the two an item is, is decided by the ordinal it came in on and nothing else - the edges
 * were laid out while compiling, so no inspection of the item is needed or allowed.
 *
 * <p>The vertex is not cooperative: it reads and writes a store that may go to disk, and a cooperative
 * processor may not block. State is written back per drain rather than per event - the assembly is held
 * locally while the batch is worked through and stored once per key at the end, so a key touched many
 * times in one drain costs one write. That is safe against eviction because nothing is written until the
 * batch is done: an entry evicted mid-drain is still the clean one already on disk, and the events that
 * would have changed it have not been acknowledged, so a crash replays them.
 */
public final class ResolverProcessor extends AbstractProcessor {

    /**
     * The shortest gap between two passes over the tombstones that may stop being kept.
     *
     * <p>A vertex with nothing arriving is asked to make progress over and over, and a pass reads back
     * every entry still keeping one and then asks where each chain its deletion covered would resume -
     * a crossing to the durable plane, once per chain, uncached. A tombstone is kept precisely until the
     * frontier passes it, so without an interval those reads repeat as fast as the idle loop turns for
     * as long as the frontier lags. What is being bounded here is measured in hours.
     */
    private static final long SWEEP_INTERVAL_MILLIS = 1_000L;

    /** When the tombstones were last weighed for dropping, or null before the first pass. */
    private Long weighedAt;

    private final NestVertex vertex;
    private final NestStore<ResolverState> store;
    private final NestDeadLetter deadLetter;
    private final Deque<Object> outgoing = new ArrayDeque<>();
    private final LevelBounds bounds;
    private final ReplayFloor floor;

    /**
     * Keys whose mapping is a tombstone and whose record is still kept, against what that deletion
     * covered. A tombstone occupies a key just as a live mapping does, so it is counted by whatever caps
     * this vertex, which is why dropping it once it is safe to is worth doing at all.
     */
    private final Map<Object, Map<String, ChainPosition>> deleted = new LinkedHashMap<>();

    /**
     * What time it is, which is stamped on a change as it starts waiting and read again when it is handed
     * over unassemblable. Nothing is decided by it — the wait ends on the parent arriving or on the parent
     * being known gone — it only says how long the wait was, which is what tells a dangling reference from
     * a deletion that just happened.
     */
    private final NestClock clock;

    /** How many changes one key here may hold for a parent that has not arrived. */
    private final long pendingLimit;

    /**
     * Where children waiting under an identity sit while that identity is being vacated, or null where this
     * run has nowhere to hand them through. A row whose identity column changes leaves behind everything
     * that was waiting on the old value: the mapping rebuilds itself where the row now belongs, but those
     * children asked a question of a value nothing answers to any more, and left where they are they wait
     * for an answer that can never come.
     */
    private final NestStore<ParkedSubtree> parking;

    /**
     * Where what killed this vertex is written down before Jet is told, so a nest's own code reaches the
     * read faces rather than the generic engine failure. Resolved on the member in {@code init}; a vertex
     * driven directly by a test never gets one and records nothing.
     */
    private NestFailureRecording failures = NestFailureRecording.of(null);

    /**
     * Identities that took over from another and had nothing waiting for them yet. Which of the two copies
     * of a row runs first is not something either can decide, so the one that takes an identity over may
     * look before the one vacating it has left anything - and it is the only thing that would ever look.
     *
     * <p>Recorded only where the identity really did change, which the row itself says. Every other row
     * would be waiting for something nobody is ever going to leave.
     */
    private final Map<List<Object>, Awaited> tookOver = new LinkedHashMap<>();

    /**
     * An identity taken over: the source's time for the change that took it, and when this instance started
     * looking. The second is what bounds the first - the half that would hand something over may never be
     * worked at all, and only time tells that apart from one that has not been worked yet.
     */
    private record Awaited(long ts, long since) {
    }

    /** How long a half of a move is waited for before it is taken to be one that is never coming. */
    private final long migrationProtection;

    /**
     * What this instance has left in the parking area and not yet seen taken in, against the positions of the
     * change that left it there. It is the whole of what this level keeps the frontier below.
     *
     * <p>Kept per address rather than as one lowest value, because the release condition is per address: the
     * entry is gone from the parking area or it is not. The lowest of what remains is worked out when it is
     * asked for, which is once per bound rather than once per move.
     *
     * <p>Recording it and letting go of it happen in two different instances whenever the two identities land
     * on different partitions, which is the normal case rather than the exception - that they do is the whole
     * reason the parking area exists. So the record here is never removed by the collecting side reaching
     * into it: what says the hand-over landed is the parking area itself, which both can see.
     */
    private final Map<ParkedSubtree.At, Vacated> vacated = new LinkedHashMap<>();

    /**
     * A subtree this instance left for another identity: the positions the frontier has to stay below while
     * it sits there, and when it was left. The second bounds the first, for the same reason the arriving
     * side is bounded - an identity that never takes it in would pin the chain for the life of the job.
     */
    private record Vacated(Map<String, ChainPosition> since, long parkedAt) {
    }

    /** A resolver in a job that propagates no frontier: it promises nothing and passes nothing on. */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter) {
        this(vertex, store, deadLetter, null, null, ReplayFloor.NONE);
    }

    /** A resolver held to what {@code settings} allows one of its keys to hold. */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            NestSettings settings) {
        this(vertex, store, deadLetter, null, null, ReplayFloor.NONE, NestClock.SYSTEM, settings);
    }

    /** A resolver that forgets a tombstone once {@code floor} says its deletion cannot come back. */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            ReplayFloor floor) {
        this(vertex, store, deadLetter, null, null, floor);
    }

    /** A resolver timing its waits by {@code clock} rather than by the system's. */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            NestClock clock) {
        this(vertex, store, deadLetter, null, null, ReplayFloor.NONE, clock);
    }

    /**
     * A resolver that also passes a frontier on. {@code chainsByOrdinal} says which chains each of its
     * edges is compiled to carry, and all of them must have promised before it says anything about one.
     * What it is holding does not enter into it - see where the bound is built.
     */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal) {
        this(vertex, store, deadLetter, axes, chainsByOrdinal, ReplayFloor.NONE);
    }

    /** The whole of it: a frontier passed on, and tombstones forgotten once it is safe to. */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal, ReplayFloor floor) {
        this(vertex, store, deadLetter, axes, chainsByOrdinal, floor, NestClock.SYSTEM);
    }

    /** All of the above, timing its waits by {@code clock}. */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal, ReplayFloor floor,
            NestClock clock) {
        this(vertex, store, deadLetter, axes, chainsByOrdinal, floor, clock, NestSettings.defaults());
    }

    /** The whole of it, held to what {@code settings} allows one of its keys to hold at once. */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal, ReplayFloor floor,
            NestClock clock, NestSettings settings) {
        this(vertex, store, deadLetter, axes, chainsByOrdinal, floor, clock, settings, null);
    }

    /**
     * All of the above, able to carry what was waiting under an identity a row is vacating. Null where a run
     * has nowhere to carry it through, which leaves those children where they are rather than moving them
     * somewhere they cannot be found.
     */
    public ResolverProcessor(NestVertex vertex, NestStore<ResolverState> store, NestDeadLetter deadLetter,
            ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal, ReplayFloor floor,
            NestClock clock, NestSettings settings, NestStore<ParkedSubtree> parking) {
        this.parking = parking;
        this.pendingLimit =
                Objects.requireNonNull(settings, "settings").pendingAllowedIn(vertex.mapName());
        this.migrationProtection = settings.migrationProtectionIn(vertex.mapName());
        this.vertex = Objects.requireNonNull(vertex, "vertex");
        this.store = Objects.requireNonNull(store, "store");
        this.deadLetter = Objects.requireNonNull(deadLetter, "deadLetter");
        // Waiting for a parent lowers nothing, and that stays true: such a change is written through to the
        // store as the drain settles, so the frontier passing it costs nothing to recover - the parent's own
        // arrival is what brings it out again. What one identity gives up to another is the exception, and
        // not because those rows are anywhere less durable. It is what would go looking for them that is
        // not: the identity taking over is the only thing that ever looks, and it knows it is owed something
        // only in memory. Past the change that parked them, a restart replays nothing that looks again.
        this.bounds = axes == null ? null : new LevelBounds(chainsByOrdinal, axes, this::lowestVacatedOn);
        this.floor = Objects.requireNonNull(floor, "floor");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (vertex.isAssembler()) {
            throw new IllegalArgumentException("the assembler is not a resolver: " + vertex.name());
        }
    }

    /**
     * Run when there is nothing arriving: the moment to drop the tombstones whose deletion can no longer
     * be delivered again, to look again for what an identity taken over is owed, and to let go of the holds
     * on what has since been taken in. Off the path a change takes, for the same reason the assembler's
     * sweep is - reading the durable plane costs more than the work a change does, and reclaiming late
     * costs nothing.
     */
    /** Resolves where this vertex writes down what killed it; see {@link NestFailureRecording}. */
    @Override
    protected void init(Processor.Context context) {
        this.failures = NestFailureRecording.of(context);
    }

    @Override
    public boolean tryProcess() {
        return failures.recording(this::tryProcessRecording);
    }

    private boolean tryProcessRecording() {
        // Anything already worked out goes first, and what this turn works out goes after it. A second look
        // that finds something has something to send, and a path that never empties what it queued would
        // leave it there until an event happened to arrive - which for an identity nobody writes to again is
        // never.
        if (!flush()) {
            return false;
        }
        collectWhatWasTakenOver();
        weighTombstones();
        if (!flush()) {
            return false;
        }
        // Only once what the second look sent has actually left: a bound offered ahead of the changes queued
        // behind it would say they had gone, and they are right here.
        return sayWhatIsNoLongerHeld();
    }

    /**
     * Drops the tombstones whose deletion a restart could no longer deliver again. Held to the sweep
     * interval, for the reason the interval exists - see where it is declared.
     */
    private void weighTombstones() {
        if (deleted.isEmpty()) {
            return;
        }
        long now = clock.millis();
        if (weighedAt != null && now - weighedAt < SWEEP_INTERVAL_MILLIS) {
            return;
        }
        weighedAt = now;
        Iterator<Map.Entry<Object, Map<String, ChainPosition>>> candidates = deleted.entrySet().iterator();
        while (candidates.hasNext()) {
            Map.Entry<Object, Map<String, ChainPosition>> candidate = candidates.next();
            ResolverState state = store.load(candidate.getKey());
            if (state == null || !state.deleted()) {
                candidates.remove();
            } else if (forgettable(state, candidate.getValue())) {
                store.remove(candidate.getKey());
                candidates.remove();
            }
        }
    }

    /**
     * Whether a tombstone can be dropped: nothing is waiting on the key, and every position its deletion
     * covered sits below where a restart would resume. A chain whose floor is not known holds it back -
     * dropping it early lets a child that arrives afterwards find no answer where there was one, and wait
     * for a parent that has been deleted rather than being told so.
     *
     * <p>Nothing waits on a deleted key today: a deletion drains what was waiting, and a child arriving
     * afterwards is told the parent is absent rather than held. The check is still made, because that is
     * what the rule says and because a later bucket landing in the same place inherits it for free.
     */
    private boolean forgettable(ResolverState state, Map<String, ChainPosition> covered) {
        if (!state.lowestHeldByChain().isEmpty()) {
            return false;
        }
        for (Map.Entry<String, ChainPosition> position : covered.entrySet()) {
            Optional<SourceOrder> resumesAt = floor.of(position.getKey());
            if (resumesAt.isEmpty() || position.getValue().order().compareTo(resumesAt.get()) >= 0) {
                return false;
            }
        }
        return !covered.isEmpty();
    }

    /**
     * Stops the job once one key is holding more than it may for a parent that has not arrived. Checked per
     * key rather than across the level: how much one key has waiting says nothing about the others, and a
     * limit spent by whichever key filled up first would fail the rest for its queue.
     *
     * <p>Failed rather than released. Nothing here says the parent is absent - only that this much arrived
     * before it did - and letting go on that would drop rows that were going to reach a document, which is
     * the whole reason a wait is ended by evidence about the parent's own stream instead.
     */
    private void refuseToLetOneKeyHoldMoreThanItMay(Object key, long pending) {
        // Reported before it is weighed, so that the count that stopped the run is the one on record rather
        // than the last one that was allowed.
        store.holding(pending);
        NestLimits.refuse(vertex, key, pending, pendingLimit);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        failures.recording(() -> {
            processRecording(ordinal, inbox);
            return null;
        });
    }

    private void processRecording(int ordinal, Inbox inbox) {
        if (!flush()) {
            return;
        }
        NestInbound edge = vertex.inbound().get(ordinal);
        Map<Object, ResolverState> touched = new LinkedHashMap<>();
        try {
            for (Object item; (item = inbox.peek()) != null; ) {
                handle(edge, item, touched);
                inbox.remove();
                if (!flush()) {
                    return;
                }
                if (touched.size() >= DrainFolding.MAX_KEYS_HELD) {
                    settle(touched);
                    touched.clear();
                }
            }
        } finally {
            settle(touched);
            collectWhatWasTakenOver();
        }
    }

    /**
     * Stores every entry this drain touched, and does so before this level says anything about how far the
     * frontier may go.
     *
     * <p><b>That order is the whole basis for letting the frontier past a change still held here.</b> The
     * bound is worked out and sent from the watermark callback, which the engine calls only once a drain
     * has returned - and a drain returns through here. So by the time a bound covering a change is sent,
     * the write holding that change has already come back from the store. Reverse the two and the promise
     * is made about a change that is in neither place, which no test that is not a crash would notice.
     */
    private void settle(Map<Object, ResolverState> touched) {
        touched.forEach((key, state) -> {
            store.save(key, state);
            refuseToLetOneKeyHoldMoreThanItMay(key, state.pending());
        });
    }

    /**
     * Works out what this level may promise on the chain the bound travels on, and passes that on rather
     * than the bound itself. Whatever is waiting here for a parent keeps the answer below it.
     *
     * <p>Anything already worked out goes first: a bound emitted ahead of the changes queued behind it
     * would claim they had left, and they are still right here. That holds for what the second look released
     * on this very turn as much as for what was queued before it - those changes came out of the parking area
     * rather than off an edge, so nothing about the turn looks like it is carrying anything, and the bound
     * being worked out here is on a stream they are sitting on.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        if (bounds == null) {
            return true;
        }
        if (!flush()) {
            return false;
        }
        collectWhatWasTakenOver();
        if (!flush()) {
            return false;
        }
        return bounds.advance(ordinal, watermark, this::tryEmit);
    }

    /**
     * Refuses to pass on a bound that arrived with no edge attached to it. Such a bound has already been
     * combined across every edge feeding this vertex, so sending it on would say "everything at or below
     * this has left here" — a claim about this vertex rather than about its upstream, and one it has not
     * made: the children waiting here for a parent sit below that value and have gone nowhere. The engine
     * forwards it by default, silently, which is why saying otherwise is explicit. What this vertex does
     * promise is worked out from the bounds arriving per edge and sent on its own.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return true;
    }

    private void handle(NestInbound edge, Object item, Map<Object, ResolverState> touched) {
        // Asked of the item rather than of the edge: word of an edit reaches a level both on the edge from
        // the vertex filing the rows that level points at, and on an ordinary cascade from a level below
        // that points at something of its own. The ordinal tells the two levels apart, not the two kinds.
        if (item instanceof NestTouch word) {
            passOn(word, touched);
            return;
        }
        if (edge.isCascade()) {
            KeyedElement arrived = (KeyedElement) item;
            route(arrived.key(), arrived.element(), arrived.ts(), touched);
            return;
        }
        Envelope event = (Envelope) item;
        NestKeys.requireBeforeImageWhereKeysAreTracked(edge, event);
        Map<String, Object> row = NestKeys.rowOf(event);
        if (edge.pathId().equals(vertex.pathId())) {
            if (edge.carriesDepartures()) {
                vacate(edge, event, row, touched);
                return;
            }
            own(edge, event, row, touched);
        } else {
            List<Object> parent = NestKeys.valuesOf(row, edge.keyFields());
            Map<String, Object> was = NestKeys.replacedRow(edge, event);
            List<Object> parentBefore = was == null ? null : NestKeys.valuesOf(was, edge.keyFields());
            Object parentWas = was == null ? null : parentIdentity(edge, parentBefore);
            NestElement arriving = departing(
                    element(edge, event, row, parentIdentity(edge, parent), null, was, parentWas),
                    parentBefore, parent);
            if (edge.carriesDepartures()) {
                // This copy of the row was keyed by what it is leaving, so the entry it resolves against is
                // this instance's own. A row that is leaving nowhere arrives here keyed the same as its twin
                // and is dropped: it has already been dealt with as an arrival.
                if (parentBefore != null && !parentBefore.equals(parent)) {
                    route(parentBefore, departureOf(arriving), event.ts(), touched);
                }
                return;
            }
            route(parent, arriving, event.ts(), touched);
        }
    }

    /**
     * Says on the key the element used to hang from that it has gone, whenever a row's join key now names
     * a different one. It travels the old key rather than the new one on purpose: only that key leads to
     * the document holding the element today, and whether that is the same document the element is going to
     * is not something this level can answer - the two keys resolve independently and may lead anywhere.
     *
     * <p>Nothing is sent where the key did not move. The element is where it always was, and a departure
     * from an address it never left would take it out of the only document it is in.
     */
    private void sendDeparture(List<Object> parentBefore, List<Object> parent, NestElement arriving, long ts) {
        if (parentBefore == null || parentBefore.equals(parent)) {
            return;
        }
        emit(new KeyedElement(parentBefore, departureOf(arriving), ts));
    }

    /** The half of a move that stays behind: the same change with no row, so it places nothing. */
    private static NestElement departureOf(NestElement arriving) {
        return new NestElement(arriving.ref(), null, arriving.order(), arriving.positions(),
                arriving.movedFrom(), true);
    }

    /**
     * Marks a change as one whose old key is being told the element has gone. Only this level can say it -
     * whether the element changed documents is decided by the key it joins on, and this is where both values
     * of that key are read - and the level that receives it needs it, or it would wait for a hand-over after
     * every rename of an element that never left the document it was in.
     */
    private static NestElement departing(NestElement arriving, List<Object> parentBefore,
            List<Object> parent) {
        if (parentBefore == null || parentBefore.equals(parent)) {
            return arriving;
        }
        return new NestElement(arriving.ref(), arriving.fields(), arriving.order(), arriving.positions(),
                arriving.movedFrom(), true);
    }

    /**
     * A row of this vertex's own embed: it names the parent its key hangs from, which releases whatever
     * was waiting on that key, and it is an element of the document itself, so it travels on either way.
     * A deletion leaves a tombstone rather than removing the mapping, because rows beneath it may still
     * be on their way and would otherwise wait for a parent that no longer exists.
     */
    private void own(NestInbound edge, Envelope event, Map<String, Object> row,
            Map<Object, ResolverState> touched) {
        SourceOrder order = NestKeys.orderOf(event);
        List<Object> key = NestKeys.valuesOf(row, vertex.partitionKey());
        List<Object> parent = NestKeys.valuesOf(row, vertex.parentKeyFields());
        ResolverState state = stateFor(key, touched);
        // Asked before anything goes out, because what this level sends on has to agree with what it kept.
        // The entry rejects a row it has already moved past - a replay resuming below a reparent is the
        // ordinary way to meet one - and sending it on regardless puts the element back into the document
        // that reparent took it out of, with the entry here still right and nothing counting the document.
        if (!state.accepts(order)) {
            return;
        }
        Map<String, Object> was = NestKeys.replacedRow(edge, event);
        List<Object> parentBefore = was == null ? null : NestKeys.valuesOf(was, vertex.parentKeyFields());
        Object parentWas = was == null ? null : parentIdentity(edge, parentBefore);
        NestElement arriving = departing(
                element(edge, event, row, parentIdentity(edge, parent), key, was, parentWas),
                parentBefore, parent);
        // The half that stays behind goes first, so whatever it hands over is already there when the half
        // that arrives looks for it. Only an ordering, not a guarantee: the two are routed by different keys
        // and may be worked by different members, so what makes the hand-over land is the arriving side
        // looking again rather than the two being sent in this order.
        sendDeparture(parentBefore, parent, arriving, event.ts());
        emit(new KeyedElement(parent, arriving, event.ts()));
        if (NestKeys.isDeletion(event)) {
            deleted.put(key, event.positions());
            // Written out before the entry gives them up: the drain stores what it touched however it
            // ended, so an entry emptied for children that were never written is stored with them gone
            // from the one place they were, and the replay that would rebuild it is rejected as a change
            // already seen. A failure here costs a retry that writes some of them twice instead.
            long releasedAt = clock.millis();
            for (ReleasedChild child : state.wouldRelease(order, releasedAt)) {
                deadLetter.unassemblable(vertex, child);
            }
            state.deleteMapping(order, releasedAt);
        } else {
            deleted.remove(key);
            for (NestElement child : state.declare(parent, order)) {
                emit(new KeyedElement(parent, child, event.ts()));
            }
            if (!collectVacated(key, state, event.ts()) && tookOverFrom(edge, event, row)) {
                tookOver.put(key, new Awaited(event.ts(), clock.millis()));
            }
        }
    }

    /**
     * Lets go of what was waiting under the identity a row is vacating, and leaves it where the identity the
     * row now has can be given it.
     *
     * <p>This copy of the row was routed by the value it is leaving, so the entry it reads is this
     * instance's own - which is the whole reason the second edge exists. The mapping itself is not carried:
     * the row declares it again wherever it now belongs. What cannot rebuild itself is the children that
     * arrived before their parent and are waiting under the old value, because nothing answers to that
     * value any more; left there they wait for an answer that can never come and hold the frontier below
     * them for as long as the job runs.
     */
    private void vacate(NestInbound edge, Envelope event, Map<String, Object> row,
            Map<Object, ResolverState> touched) {
        Map<String, Object> was = NestKeys.replacedRow(edge, event);
        if (was == null) {
            return;
        }
        List<Object> leaving = NestKeys.valuesOf(was, vertex.partitionKey());
        List<Object> joining = NestKeys.valuesOf(row, vertex.partitionKey());
        if (leaving.equals(joining)) {
            return;
        }
        ResolverState leftBehind = stateFor(leaving, touched);
        // The value this row answered to is unclaimed now, and the entry it declared may not go on
        // answering for it: a child naming it afterwards would be placed under a parent no row carries,
        // which is a document it does not belong to rather than an error anything counts. Said whether or
        // not there is anywhere to hand children through, because it is about the mapping and not them.
        leftBehind.stopAnswering(NestKeys.orderOf(event));
        if (parking == null) {
            return;
        }
        List<NestElement> waiting = leftBehind.waiting();
        if (waiting.isEmpty()) {
            return;
        }
        ParkedSubtree.At at = new ParkedSubtree.At(vertex.pathId(), joining);
        ParkedSubtree held = parking.load(at);
        ParkedSubtree now = new ParkedSubtree(waiting);
        parking.save(at, held == null ? now : held.and(now));
        // Emptied only now that the rows are somewhere both instances can reach. Emptying first and then
        // failing to publish stores an entry that has given up rows nothing else ever received, and the
        // replay that would rebuild them is rejected as a change already seen. A failure here instead costs
        // a retry that parks the same rows twice, which their identities make harmless.
        leftBehind.forgetWaiting();
        // The frontier stays below this change until those children have been taken in. The first change to
        // park under an address is the one kept: a second move onto the same address is further along the
        // same stream, and it is the earlier of the two that has to be replayable.
        vacated.putIfAbsent(at, new Vacated(event.positions(), clock.millis()));
    }

    /**
     * The lowest position on {@code chain} that something parked by this instance is holding back, or null
     * when nothing is.
     */
    private SourceOrder lowestVacatedOn(String chain) {
        SourceOrder lowest = null;
        for (Vacated outstanding : vacated.values()) {
            ChainPosition held = outstanding.since().get(chain);
            if (held != null && (lowest == null || held.order().compareTo(lowest) < 0)) {
                lowest = held.order();
            }
        }
        return lowest;
    }

    /**
     * Says what this level may promise now that it is holding less than it was, and answers whether it is
     * done saying it.
     *
     * <p>It has to be said here rather than left to the next bound that arrives. What a level may promise
     * depends on what its edges promised <em>and</em> on what it is holding, and only the first of those turns
     * up as a message: an upstream that has said its last word sends nothing more to prompt a recount, so a
     * level that answered only on a bound arriving would leave the chain pinned where the hold had it for as
     * long as the job runs, with every count reading healthy.
     */
    private boolean sayWhatIsNoLongerHeld() {
        // Both, and never short-circuited: giving up on one subtree and having another taken in are
        // independent ways of holding less, and a turn where both happen has to say so once for both.
        boolean collected = forgetWhatHasBeenTakenIn();
        boolean gaveUp = giveUpOnSubtreesNobodyCollected();
        if (!(collected || gaveUp) || bounds == null) {
            return true;
        }
        return bounds.release(this::tryEmit);
    }

    /**
     * Hands on the subtrees left for an identity that never took them in, and lets go of what they were
     * holding back. Whether the other half is coming is not something this instance can be told - the two
     * are routed by different keys and may be worked by different members - so time is the only thing that
     * separates a hand-over not collected <em>yet</em> from one nobody is ever going to collect.
     *
     * <p><b>Handed on rather than dropped.</b> These rows were read out of the entry they were waiting in
     * and nothing will send them again, so dropping them loses data no assertion about a document could
     * see. The hold goes in the same breath, deliberately: an entry given up on is no longer something a
     * replay would finish, so keeping the frontier beneath it would be waiting for what has already been
     * decided against - which is how a bound becomes unreachable for the life of the job.
     */
    private boolean giveUpOnSubtreesNobodyCollected() {
        if (parking == null || vacated.isEmpty()) {
            return false;
        }
        long now = clock.millis();
        boolean letGo = false;
        Iterator<Map.Entry<ParkedSubtree.At, Vacated>> outstanding = vacated.entrySet().iterator();
        while (outstanding.hasNext()) {
            Map.Entry<ParkedSubtree.At, Vacated> entry = outstanding.next();
            if (now - entry.getValue().parkedAt() < migrationProtection) {
                continue;
            }
            ParkedSubtree waiting = parking.load(entry.getKey());
            if (waiting != null) {
                Duration heldFor = Duration.ofMillis(now - entry.getValue().parkedAt());
                for (NestElement change : waiting.changes()) {
                    deadLetter.unassemblable(vertex, new ReleasedChild(change, heldFor));
                }
                parking.remove(entry.getKey());
            }
            outstanding.remove();
            letGo = true;
        }
        return letGo;
    }

    /**
     * Lets go of the holds on whatever is no longer in the parking area, and answers whether any were let go.
     *
     * <p><b>The parking area is what is asked, not this instance's own bookkeeping.</b> Whoever takes an entry
     * in is on the partition of the identity that took over, which is a different one from the identity that
     * gave it up - so the instance holding the frontier down is, in the normal case, not the instance that
     * collects. Nothing it keeps locally would ever hear about the landing. What both can see is the entry
     * itself, and its absence is exactly the condition the hold was waiting on.
     */
    private boolean forgetWhatHasBeenTakenIn() {
        if (parking == null || vacated.isEmpty()) {
            return false;
        }
        boolean letGo = false;
        Iterator<Map.Entry<ParkedSubtree.At, Vacated>> outstanding = vacated.entrySet().iterator();
        while (outstanding.hasNext()) {
            if (parking.load(outstanding.next().getKey()) == null) {
                outstanding.remove();
                letGo = true;
            }
        }
        return letGo;
    }

    /**
     * Takes in whatever was waiting under an identity this key has just taken over, offering each child the
     * mapping that now exists. Offered rather than released outright: the row that declares this key may not
     * have arrived, in which case they go on waiting - here, where something will answer them.
     */
    private boolean collectVacated(List<Object> key, ResolverState state, long ts) {
        if (parking == null) {
            return false;
        }
        ParkedSubtree.At at = new ParkedSubtree.At(vertex.pathId(), key);
        ParkedSubtree waiting = parking.load(at);
        if (waiting == null) {
            return false;
        }
        for (NestElement child : waiting.changes()) {
            switch (state.resolve(child, clock.millis())) {
                case RESOLVED -> emit(new KeyedElement(state.parentKey(), child, ts));
                case HELD -> { }
                case PARENT_ABSENT ->
                        deadLetter.unassemblable(vertex, new ReleasedChild(child, Duration.ZERO));
            }
        }
        parking.remove(at);
        tookOver.remove(key);
        return true;
    }

    /** Whether this row says the value its children point at has changed. */
    private boolean tookOverFrom(NestInbound edge, Envelope event, Map<String, Object> row) {
        Map<String, Object> was = NestKeys.replacedRow(edge, event);
        return was != null && !NestKeys.valuesOf(was, vertex.partitionKey())
                .equals(NestKeys.valuesOf(row, vertex.partitionKey()));
    }

    /**
     * Looks again for what an identity this key took over was owed. Run wherever this vertex already does
     * work that nothing asked for, and for the same reason the assembler's own second look is: a hand-over
     * being left somewhere produces no event of its own, so it has to travel on somebody else's.
     */
    private void collectWhatWasTakenOver() {
        if (parking == null || tookOver.isEmpty()) {
            return;
        }
        long now = clock.millis();
        for (Map.Entry<List<Object>, Awaited> waited : List.copyOf(tookOver.entrySet())) {
            // Nothing has been handed over for this identity, and waiting is right until it stops being.
            // Left recorded it is read from the store on every turn for the life of the job, and in the
            // ordinary case - a tracked key change on a row with no children waiting under the old value -
            // there was never going to be anything to collect.
            if (now - waited.getValue().since() >= migrationProtection) {
                tookOver.remove(waited.getKey());
                continue;
            }
            ResolverState state = store.load(waited.getKey());
            if (state == null) {
                continue;
            }
            // The time of the change that took the identity over, kept since it was recorded. What lands
            // here was parked before this level ever saw it, so its own time is no longer anywhere - and
            // this is the change that puts it in a document, which is the time a document is stamped by.
            if (collectVacated(waited.getKey(), state, waited.getValue().ts())) {
                store.save(waited.getKey(), state);
            }
        }
    }

    /**
     * Sends word that a row this level points at was edited on to the level above, addressed by the parent
     * this one hangs from - the same climb this level's own rows make, which is what lets a level nested
     * anywhere point at a row without a path of its own.
     *
     * <p><b>Word for a row whose parent is not known yet is dropped, and that loses nothing.</b> Nothing of
     * this level is in a document until its parent turns up, and whatever puts it there draws the document
     * then - reading the edited row as it now stands, because it was filed before this word was ever sent.
     * Holding the word instead would mean queueing a wake-up for a document that does not exist, on a key
     * that may never resolve.
     */
    private void passOn(NestTouch word, Map<Object, ResolverState> touched) {
        ResolverState state = stateFor(word.key(), touched);
        if (state.parentKey() != null) {
            emit(word.routedBy(state.parentKey()));
        }
    }

    /** One change from beneath, offered to the key its join field names. */
    private void route(Object key, NestElement element, long ts, Map<Object, ResolverState> touched) {
        ResolverState state = stateFor(key, touched);
        switch (state.resolve(element, clock.millis())) {
            case RESOLVED -> emit(new KeyedElement(state.parentKey(), element, ts));
            // Held: it is in the state now, and the drain writes that through before this level promises
            // anything, so there is nothing further to do here and nothing to record about it.
            case HELD -> { }
            // Zero: this change arrived after the parent was already gone, so it waited no time at all.
            case PARENT_ABSENT -> deadLetter.unassemblable(vertex, new ReleasedChild(element, Duration.ZERO));
        }
    }

    /**
     * Where an element sits in the document, read off its own row. At depth one the parent is the root
     * itself and there is no element above to name.
     */
    private static Object parentIdentity(NestInbound edge, Object joinKey) {
        return edge.pathId().size() == 1 ? null : joinKey;
    }

    /**
     * One change as an element of the document, carrying where the element used to sit whenever the row it
     * replaces says somewhere else.
     *
     * <p>Where it used to sit is built from the earlier row's own address and never from its identity: the
     * identity is what rows beneath point at, not where this element is shown, and an entry filed under a
     * value that has changed is moved by the level that holds it rather than by the document.
     */
    private static NestElement element(NestInbound edge, Envelope event, Map<String, Object> row,
            Object parentIdentity, Object identity, Map<String, Object> was, Object parentIdentityWas) {
        ElementRef ref = new ElementRef(edge.pathId(), parentIdentity,
                NestKeys.valuesOf(row, edge.elementKey()), identity);
        ElementRef from = was == null ? null : new ElementRef(edge.pathId(), parentIdentityWas,
                NestKeys.valuesOf(was, edge.elementKey()), identity);
        return new NestElement(ref, NestKeys.isDeletion(event) ? null : row,
                NestKeys.orderOf(event), event.positions(), from);
    }

    private ResolverState stateFor(Object key, Map<Object, ResolverState> touched) {
        return touched.computeIfAbsent(key, k -> {
            ResolverState kept = store.load(k);
            if (kept == null) {
                return new ResolverState();
            }
            return kept;
        });
    }

    private void emit(Object item) {
        outgoing.add(item);
    }

    /** Empties what is waiting to go out, reporting whether it all got out. */
    private boolean flush() {
        while (!outgoing.isEmpty()) {
            if (!tryEmit(outgoing.peek())) {
                return false;
            }
            outgoing.poll();
        }
        return true;
    }
}
