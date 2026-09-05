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
import io.tapstate.runtime.engine.SettledPositions;

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
 * The vertex that holds whole documents: root rows arrive and become the document, elements arrive
 * already knowing where in it they belong, and what goes out is the document as it now stands.
 *
 * <p>Output is the state a document has reached, not the change that got it there - every emission is a
 * whole document, which is what makes it safe for a sink to apply out of order and for a restart to
 * repeat. One emission per document per drain rather than per event: a document touched by twenty
 * elements in one batch is rendered once, which is where most of the write amplification of assembling
 * documents goes.
 *
 * <p>While the root is absent there is no document to emit, whatever arrived - an element on its own
 * would render a skeleton with no root fields, and downstream that skeleton is a ghost document that
 * nothing later removes. A root that is deleted is the one thing that still goes out, because the sink
 * has a document to remove; it carries the key and nothing else, and is not an assembled document.
 */
public final class AssemblerProcessor extends AbstractProcessor {

    /**
     * The shortest gap between two sweeps for changes that may stop being held.
     *
     * <p>A vertex with nothing arriving is asked to make progress over and over, and a sweep reads the
     * layer behind the map — what each document is still holding, and whether the stream its root would
     * come on has finished loading. Sweeping every turn would put those reads on the idle path, which is
     * the one place they must never be. What is being bounded here is measured in hours.
     */
    private static final long SWEEP_INTERVAL_MILLIS = 1_000L;

    private final NestVertex vertex;
    private final List<EmbedSlot> slots;

    /**
     * Where the rows this tree's levels point at are read from, by the namespace the slot naming them
     * carries. Read only, and read by key only: this vertex never writes an entry of any of them, which is
     * what lets it reach outside its own partition for one at all.
     */
    private final Map<String, NestStore<Map<String, Object>>> referenced;
    private final NestStore<RootAssembly> store;
    private final String outputStream;
    private final Deque<Object> outgoing = new ArrayDeque<>();
    private final LevelBounds bounds;
    private final ReplayFloor floor;

    /**
     * How many elements any one document may hold. Read once: it is chosen where the job is built, not
     * here.
     */
    private final long elementLimit;

    /** How many changes one document here may hold for a root, or an ancestor, that has not arrived. */
    private final long pendingLimit;

    /** How many records of deleted elements one document here may keep once what may go has gone. */
    private final long tombstoneLimit;

    /** How many changes one entry of the parking area carries. Read once, where everything else is chosen. */
    private final long migrationBatch;

    /** How many changes one hand-over may leave in the parking area before the job is failed. */
    private final long parkingLimit;

    /** How long a hand-over may sit uncollected before it is given up on. */
    private final long migrationProtection;

    /**
     * Where rows that can never reach a document go. Reached only from the parking area here: everything else
     * this vertex holds is in a document or on its way into one.
     */
    private final NestDeadLetter deadLetter;

    /**
     * Keys keeping the record of a deletion, which is where the sweep that drops them has to look. Kept for
     * the same reason as {@link #holding}: the store cannot be asked which of its keys hold anything.
     *
     * <p>A restart starts it empty, and unlike {@link #holding} the keys do not all come back on their own —
     * a record whose deletion the frontier has already passed is not replayed, so nothing re-files it until
     * that key is touched again. What is left behind that way is bounded and idle: a key nothing touches
     * again is a key that has stopped growing, and one that is touched is filed here again on the spot.
     */
    private final Set<Object> keeping = new LinkedHashSet<>();

    /**
     * Keys held back because a row they point at has not been read yet. Kept so that the word saying such
     * a row is already filed can be dropped without reading anything: it is sent on every row of a
     * pointing stream, and on the ordinary path none of them is waiting.
     *
     * <p><b>Wrong in only one direction, and it is the harmless one.</b> A key left here after it stopped
     * waiting costs one drain that had nothing to do; a key missing from here while it waits is the lost
     * word this exists to deliver. So it is added wherever a wait is seen and removed only where the
     * document demonstrably went out or went away - never on a path that returned early for some other
     * reason, where what it is waiting for was not re-decided.
     *
     * <p>A restart starts it empty, and needs nothing else: a document waiting on a row it points at has
     * its own row held short of the durable frontier, so that row is replayed and the document is drawn
     * again from it, reading the row it wanted rather than being told about it.
     */
    private final Set<Object> waiting = new LinkedHashSet<>();

    /** When the records of deletion were last swept, or null before the first sweep. */
    private Long forgottenAt;

    /**
     * When the deleted roots were last weighed for dropping, or null before the first pass. Its own
     * reading rather than the one beside it, so neither sweep can starve the other by running first and
     * moving a clock they share.
     */
    private Long weighedAt;

    /**
     * What the sweeps here measure their own interval against. It is only ever read to decide whether
     * enough time has passed to look again — nothing this vertex holds is ever given up because a clock
     * reached some time.
     */
    private final NestClock clock;

    /**
     * Roots whose deletion has gone downstream and whose record is still kept, against what that deletion
     * covered. They are remembered as they happen rather than looked for later, because a store is not
     * something this can walk: asking one for its keys is what read-through exists to avoid.
     */
    private final Map<Object, Map<String, ChainPosition>> deleted = new LinkedHashMap<>();

    /** How often a document may go out, and whether versions of it may be merged into one send. */
    private final NestSendPolicy sending;

    /**
     * Where subtrees sit while they move between documents, or null where this run has nowhere to hand one
     * through. Not the same store as the documents: what is here belongs to no key of this vertex until the
     * document gaining it takes it, and the sweeps here walk the keys of documents.
     */
    private final NestStore<ParkedSubtree> parking;

    /**
     * Where what killed this vertex is written down before Jet is told, so a nest's own code reaches the
     * read faces rather than the generic engine failure. Resolved on the member in {@code init}; a vertex
     * driven directly by a test never gets one and records nothing, which is what the default stands for.
     */
    private NestFailureRecording failures = NestFailureRecording.of(null);

    /**
     * The documents with a window open over them: when the window opened, and what a send folded into it is
     * keeping the frontier below. Only ever as many entries as there are roots changed within one window of
     * each other, and the sweep that ends them is what keeps it that way.
     *
     * <p>What is kept per entry is the position and not the state. Holding the assembly itself would spare a
     * read at the end of the window and put every recently changed document in the heap, outside the budget
     * that decides what stays in memory - a second, invisible copy of exactly what that budget is set to
     * bound.
     */
    private final Map<Object, Window> windows = new LinkedHashMap<>();

    // The highest position per chain that a lookup has said owes nothing, waiting until this level holds
    // nothing lower on that chain. In memory, like the windows beside it: a restart that has lost it has
    // lost only a chance to advance a frontier, which is the direction to lose in.
    private final Map<String, ChainPosition> settledAhead = new LinkedHashMap<>();

    /**
     * The moves whose subtree is parked and not yet collected, against what the change that started each was
     * covering. It is what keeps the durable frontier below a hand-over in flight: those rows are in no
     * document at all while they sit there, so a frontier allowed past would leave a restart resuming above
     * the move, with nothing to replay it and nothing to collect what was parked.
     *
     * <p>In memory, like the windows beside it, and for the same reason: a restart that has lost this has
     * also not acknowledged the change that made the entry, so the move is replayed and recorded again.
     */
    private final Map<ParkedSubtree.At, Held> handedOver = new LinkedHashMap<>();

    /**
     * One hand-over in flight: what the change that started it was covering, when it was parked, how many
     * pieces it was written in and how many changes it left there.
     *
     * <p>Here rather than in the entry itself, so that nothing about a hand-over's shape is written down and
     * read back by a later build. A restart loses this - and has by definition not acknowledged the change
     * that made the entry, since the frontier is held below it - so the move is replayed and recorded again.
     */
    private record Held(Map<String, ChainPosition> since, long parkedAt, int pieces, long changes) {
    }

    /**
     * Documents owed a subtree that had not been parked when the element arrived, by the key of the document
     * owed it. Bounded by the moves actually in flight, because only a change whose old key was told it had
     * gone is ever recorded here.
     */
    private final Map<ParkedSubtree.At, Owed> owed = new LinkedHashMap<>();

    /**
     * A document owed a hand-over: which key it is, and the source's time for the change that made it owed.
     * The time travels because the document goes out again once the rows land, and a send with no time on it
     * would place a change downstream at the epoch rather than where the source put it.
     *
     * <p>{@code since} is the position of that change, and it is what keeps the frontier beneath it. Being
     * owed is held here and nowhere else: the rows are still in the document the other half has yet to
     * empty, so nothing durable is missing - what is missing is anything that would look again. Let the
     * frontier past and a restart resumes above the change, so the other half is never replayed and this
     * one never asks, leaving the document under neither key with every count reading healthy.
     */
    private record Owed(Object key, long ts, long awaitedSince, Map<String, ChainPosition> since) {
    }

    /** An assembler in a job that propagates no frontier: it promises nothing and passes nothing on. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream) {
        this(vertex, slots, store, outputStream, null, null, ReplayFloor.NONE);
    }

    /** An assembler that forgets deleted roots once {@code floor} says their deletion cannot come back. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ReplayFloor floor) {
        this(vertex, slots, store, outputStream, null, null, floor);
    }

    /**
     * An assembler that also passes a frontier on. {@code chainsByOrdinal} says which chains each of its
     * edges is compiled to carry - all of them must have promised before it says anything about one - and
     * everything its documents are still holding tightens the answer further.
     */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal) {
        this(vertex, slots, store, outputStream, axes, chainsByOrdinal, ReplayFloor.NONE);
    }

    /** An assembler held to what {@code settings} allows this nest to hold, and to nothing else. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, NestSettings settings) {
        this(vertex, slots, store, outputStream, null, null, ReplayFloor.NONE, settings);
    }

    /** The whole of it: a frontier passed on, and deleted roots forgotten once it is safe to. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal,
            ReplayFloor floor) {
        this(vertex, slots, store, outputStream, axes, chainsByOrdinal, floor, NestSettings.defaults());
    }

    /** The whole of it, held to what this nest is allowed to hold. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal,
            ReplayFloor floor, NestSettings settings) {
        this(vertex, slots, store, outputStream, axes, chainsByOrdinal, floor, settings, NestClock.SYSTEM);
    }

    /** All of it, sweeping on {@code clock} rather than on the system's. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal,
            ReplayFloor floor, NestSettings settings, NestClock clock) {
        this(vertex, slots, store, outputStream, axes, chainsByOrdinal, floor, settings, clock,
                NestSendPolicy.within(0L));
    }

    /** All of it, sending as {@code sending} allows rather than as each drain settles. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal,
            ReplayFloor floor, NestSettings settings, NestClock clock, NestSendPolicy sending) {
        this(vertex, slots, store, outputStream, axes, chainsByOrdinal, floor, settings, clock, sending, null);
    }

    /**
     * All of the above, able to hand a subtree over to another document through {@code parking}.
     *
     * <p>Null where a run has nowhere to hand one through, which is not a degraded store but a different
     * answer: an element that still holds a subtree then stays in the document it is in rather than being
     * taken apart. A stale copy is a document that disagrees with its source and can be seen to; rows read
     * out of one document and handed nowhere are gone, with nothing anywhere reporting it.
     */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal,
            ReplayFloor floor, NestSettings settings, NestClock clock, NestSendPolicy sending,
            NestStore<ParkedSubtree> parking) {
        // A run that hands nothing over needs nowhere to put what it gives up on, and one that does is built
        // with the channel below. Left as something that fails loudly rather than as a channel that discards:
        // reaching it would mean rows being dropped where the code says they are handed on.
        this(vertex, slots, store, outputStream, axes, chainsByOrdinal, floor, settings, clock, sending,
                parking, (from, released) -> {
                    throw new IllegalStateException(
                            "a hand-over was given up on with nowhere to put it: " + from.name());
                });
    }

    /** The whole of it, with somewhere to put a hand-over nobody ever collected. */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal,
            ReplayFloor floor, NestSettings settings, NestClock clock, NestSendPolicy sending,
            NestStore<ParkedSubtree> parking, NestDeadLetter deadLetter) {
        this(vertex, slots, store, outputStream, axes, chainsByOrdinal, floor, settings, clock, sending,
                parking, deadLetter, Map.of());
    }

    /**
     * All of the above, with the rows this tree's levels point at reachable through {@code referenced} -
     * one store per namespace a slot names. Empty for a tree that points at nothing, which is every tree
     * written before there was a second direction.
     */
    public AssemblerProcessor(NestVertex vertex, List<EmbedSlot> slots, NestStore<RootAssembly> store,
            String outputStream, ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal,
            ReplayFloor floor, NestSettings settings, NestClock clock, NestSendPolicy sending,
            NestStore<ParkedSubtree> parking, NestDeadLetter deadLetter,
            Map<String, NestStore<Map<String, Object>>> referenced) {
        this.referenced = Map.copyOf(referenced);
        this.parking = parking;
        this.deadLetter = Objects.requireNonNull(deadLetter, "deadLetter");
        this.vertex = Objects.requireNonNull(vertex, "vertex");
        this.slots = List.copyOf(slots);
        this.store = Objects.requireNonNull(store, "store");
        this.outputStream = Objects.requireNonNull(outputStream, "outputStream");
        this.sending = Objects.requireNonNull(sending, "sending");
        // What lowers the bound is a document changed and waiting for its window: it survives a restart in
        // the state, and nothing will ever send it, because no further event is due for that root. An
        // orphan waiting for its ancestor is the other case and is not counted - it is in the document's
        // own state, written through as the drain settles, and comes out when the ancestor arrives.
        this.bounds = axes == null ? null : new LevelBounds(chainsByOrdinal, axes, this::lowestUnsentOn);
        this.floor = Objects.requireNonNull(floor, "floor");
        this.elementLimit =
                Objects.requireNonNull(settings, "settings").elementsAllowedIn(vertex.mapName());
        this.pendingLimit = settings.pendingAllowedIn(vertex.mapName());
        this.tombstoneLimit = settings.tombstonesAllowedIn(vertex.mapName());
        this.migrationBatch = settings.migrationBatchIn(vertex.mapName());
        this.parkingLimit = settings.parkingAllowedIn(vertex.mapName());
        this.migrationProtection = settings.migrationProtectionIn(vertex.mapName());
        this.clock = Objects.requireNonNull(clock, "clock");
        if (!vertex.isAssembler()) {
            throw new IllegalArgumentException("a resolver does not assemble documents: " + vertex.name());
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Run when there is nothing arriving: the moment to forget the deleted roots that can no longer come
     * back. It sits here rather than on the path a change takes because reading the durable plane costs
     * more than assembling a document does, and because reclaiming memory later than possible costs
     * nothing - what a busy operator postpones, an idle one does.
     */
    @Override
    public boolean tryProcess() {
        return failures.recording(this::tryProcessRecording);
    }

    private boolean tryProcessRecording() {
        if (!flush()) {
            return false;
        }
        collectWhatIsOwed();
        // After the look for what may still land and before anything is said about the frontier: an entry
        // given up on stops holding it, and saying so on the same turn is what keeps a move that is never
        // coming from costing a chain more than its protection.
        giveUpOnHandOversNobodyCollected();
        if (!sendWhatWindowsHaveRunOutOn()) {
            return false;
        }
        if (!passOnWhatOwesNothing()) {
            return false;
        }
        weighDeletedRoots();
        forgetDeletionsReplayCannotReach();
        // Only after the documents above have gone out, for the usual reason: a bound offered ahead of what is
        // queued behind it would say it had left.
        return !forgetHandOversThatHaveLanded() || bounds == null || bounds.release(this::tryEmit);
    }

    /**
     * Lets go of the holds on hand-overs that are no longer in the parking area, and answers whether any were
     * let go of.
     *
     * <p><b>The parking area is what is asked, not what this instance did.</b> A hand-over is between two
     * documents whose keys are on different partitions, so the instance that let a subtree go and the instance
     * that takes it in are, in the normal case, different instances - and taking it in is the only thing that
     * ever removes the record locally. Measured: with the two on separate instances the hold was never let go
     * of at all, so the chain stayed pinned at the change that started the move for the life of the job, every
     * count reading healthy. Absence from the parking area is a condition both of them can see.
     */
    private boolean forgetHandOversThatHaveLanded() {
        if (parking == null || handedOver.isEmpty()) {
            return false;
        }
        boolean letGo = false;
        Iterator<Map.Entry<ParkedSubtree.At, Held>> outstanding = handedOver.entrySet().iterator();
        while (outstanding.hasNext()) {
            if (parking.load(outstanding.next().getKey()) == null) {
                outstanding.remove();
                letGo = true;
            }
        }
        return letGo;
    }

    /**
     * Gives up on the hand-overs nobody has collected within their protection, handing what was parked to the
     * dead-letter channel and letting go of the hold.
     *
     * <p>Without it the unhealthy case has no end. What finishes a hand-over is the other half of the move
     * being worked, and a half that is never coming leaves rows parked for the life of the job and - the part
     * that costs more - leaves the durable frontier pinned at the change that moved them, so a source's read
     * position never advances past it while every count reads healthy.
     *
     * <p><b>Handed over rather than dropped.</b> These rows were read out of a document and will not be sent
     * again by anything, so dropping them loses data that no assertion about a document could ever see. And
     * the hold is released in the same breath, deliberately: an entry given up on is no longer something a
     * replay would finish, so keeping the frontier beneath it would be waiting for what has already been
     * decided against.
     */
    private void giveUpOnHandOversNobodyCollected() {
        if (parking == null || handedOver.isEmpty()) {
            return;
        }
        long now = clock.millis();
        Iterator<Map.Entry<ParkedSubtree.At, Held>> outstanding = handedOver.entrySet().iterator();
        while (outstanding.hasNext()) {
            Map.Entry<ParkedSubtree.At, Held> entry = outstanding.next();
            Held held = entry.getValue();
            if (now - held.parkedAt() < migrationProtection) {
                continue;
            }
            ParkedSubtree waiting = parking.load(entry.getKey());
            if (waiting != null) {
                Duration heldFor = Duration.ofMillis(now - held.parkedAt());
                waiting.changes().forEach(change ->
                        deadLetter.unassemblable(vertex, new ReleasedChild(change, heldFor)));
                for (int piece = 1; piece <= waiting.batches(); piece++) {
                    ParkedSubtree more = parking.load(entry.getKey().piece(piece));
                    if (more != null) {
                        more.changes().forEach(change ->
                                deadLetter.unassemblable(vertex, new ReleasedChild(change, heldFor)));
                    }
                }
                letGoOf(entry.getKey(), waiting.batches());
            }
            owed.remove(entry.getKey());
            outstanding.remove();
        }
    }

    /**
     * Drops what is left of the roots whose deletion has gone downstream and can no longer come back.
     *
     * <p>Held to the sweep interval, which is what the interval is for. Each pass reads back every
     * candidate document and then asks where each chain the deletion covered would resume - and that
     * second question crosses to the durable plane once per chain, uncached. A candidate that is not
     * droppable yet stays a candidate, so without the interval those reads repeat as fast as the idle
     * loop turns, for as long as the frontier has not passed the deletion.
     */
    private void weighDeletedRoots() {
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
            RootAssembly assembly = store.load(candidate.getKey());
            if (assembly == null) {
                candidates.remove();
            } else if (assembly.rootPresent()) {
                candidates.remove();
            } else if (forgettable(assembly, candidate.getValue())) {
                store.remove(candidate.getKey());
                candidates.remove();
            }
        }
    }

    /**
     * Drops the records of deletion whose deletions a restart could no longer replay. Here rather than where
     * a change is applied because it reads the durable plane, and because dropping one later than possible
     * costs an entry while dropping it earlier costs a deleted row coming back.
     *
     * <p>Held to the same interval as the sweep above, for the reason that one is: a vertex with nothing
     * arriving is asked to make progress over and over, and each pass reads back every document keeping a
     * record. A document whose records are not droppable yet stays a candidate, so without an interval those
     * reads would repeat as fast as the idle loop turns.
     */
    private void forgetDeletionsReplayCannotReach() {
        if (keeping.isEmpty()) {
            return;
        }
        long now = clock.millis();
        if (forgottenAt != null && now - forgottenAt < SWEEP_INTERVAL_MILLIS) {
            return;
        }
        forgottenAt = now;
        Iterator<Object> keys = keeping.iterator();
        while (keys.hasNext()) {
            Object key = keys.next();
            RootAssembly assembly = store.load(key);
            if (assembly == null) {
                keys.remove();
                continue;
            }
            if (assembly.forgetDeletionsBelow(floor) > 0) {
                store.save(key, assembly);
            }
            if (assembly.tombstones() == 0) {
                keys.remove();
            }
        }
    }

    /**
     * Whether what is left of a deleted root can be dropped: the assembly is holding nothing it has not
     * shown anyone, and every position its deletion covered sits below where a restart would resume.
     *
     * <p>A chain whose floor is not known holds the whole thing back. That is the safe way round: dropping
     * the record early lets a replayed insert build the root again with nothing left to say it was deleted,
     * whereas dropping it late costs one entry until the next sweep.
     */
    private boolean forgettable(RootAssembly assembly, Map<String, ChainPosition> covered) {
        if (!assembly.lowestHeldByChain().isEmpty()) {
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

    /** Resolves where this vertex writes down what killed it; see {@link NestFailureRecording}. */
    @Override
    protected void init(Processor.Context context) {
        this.failures = NestFailureRecording.of(context);
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
        Map<Object, Touched> touched = new LinkedHashMap<>();
        try {
            for (Object item; (item = inbox.peek()) != null; ) {
                handle(edge, item, touched);
                inbox.remove();
                // Under append every change is a record of its own, so a drain may not be merged into one
                // document any more than a window may: settling here rather than at the end is what makes
                // the count of records that go out the count of changes that arrived.
                if (!sending.foldingAllowed()) {
                    settle(touched);
                    touched.clear();
                    if (!flush()) {
                        return;
                    }
                    continue;
                }
                // A wide drain writes back what it holds rather than holding a document per key to the
                // end. A root touched again afterwards is read back and goes out a second time in the
                // same batch, which costs a write and is otherwise invisible: what goes out is the whole
                // document, so a sink upserting it twice lands where it would have landed once.
                if (touched.size() >= DrainFolding.MAX_KEYS_HELD) {
                    settle(touched);
                    touched.clear();
                    if (!flush()) {
                        return;
                    }
                }
            }
        } finally {
            settle(touched);
        }
        // Here as well as on the idle path, because a vertex fed steadily on one key never reaches the idle
        // path at all - and the document whose window ran out while that was going on is the one nothing
        // else is coming for.
        collectWhatIsOwed();
        sendWhatWindowsHaveRunOutOn();
    }

    /**
     * Works out what this level may promise on the chain the bound travels on, and passes that on rather
     * than the bound itself. Everything its documents have taken in and not sent keeps the answer below it.
     *
     * <p>Anything already worked out goes first: a bound emitted ahead of the documents queued behind it
     * would claim they had left, and they are still right here.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        // Before the bound, and on this path as well as the other two, because a level whose rows have
        // stopped while its bounds have not is neither draining nor idle - the ordinary shape of a source
        // that has caught up. A window ending only on those two would hold its last version of every
        // document for as long as the bounds kept arriving.
        collectWhatIsOwed();
        if (!sendWhatWindowsHaveRunOutOn()) {
            return false;
        }
        if (!passOnWhatOwesNothing()) {
            return false;
        }
        if (bounds == null) {
            return true;
        }
        return bounds.advance(ordinal, watermark, this::tryEmit);
    }

    /**
     * Refuses to pass on a bound that arrived with no edge attached to it. Such a bound has already been
     * combined across every edge feeding this vertex, so sending it on would say "everything at or below
     * this has left here" — while the orphans held for a root that has not arrived, and the documents
     * touched but not yet rendered, sit below that value and have gone nowhere. The engine forwards it by
     * default, silently, which is why saying otherwise is explicit. What this vertex does promise is
     * worked out from the bounds arriving per edge and sent on its own.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return true;
    }

    private void handle(NestInbound edge, Object item, Map<Object, Touched> touched) {
        // Asked of the item rather than of the edge, because two edges bring it. One is the edge from the
        // vertex filing the rows this document itself points at; the other is an ordinary cascade, carrying
        // word that a row some level beneath points at was edited, climbing with that level's own changes.
        // The ordinal says which level it came from and says nothing about which of the two it is.
        if (item instanceof SettledPositions settled) {
            SettledPositions.fold(settledAhead, settled.positions());
            return;
        }
        if (item instanceof NestTouch word) {
            // A word that only answers a wait is dropped where there is none, before anything is read: it
            // is sent on every row of a pointing stream whose row is already filed, and drawing those
            // documents again would double what a stream costs to say nothing new.
            if (word.onlyIfWaiting() && !waiting.contains(word.key())) {
                return;
            }
            // Nothing here changes - what the document should now show is read out of that row's own
            // namespace when it is drawn - so all this does is put the document in the drain, which is
            // what gets it drawn and sent again.
            Touched document = touched(word.key(), touched);
            document.ts = Math.max(document.ts, word.ts());
            document.assembly.absorb(word.positions());
            return;
        }
        if (edge.isCascade()) {
            KeyedElement arrived = (KeyedElement) item;
            Touched document = touched(arrived.key(), touched);
            document.ts = arrived.ts();
            settle(arrived.key(), document, arrived.element());
            return;
        }
        Envelope event = (Envelope) item;
        NestKeys.requireBeforeImageWhereKeysAreTracked(edge, event);
        Map<String, Object> row = NestKeys.rowOf(event);
        SourceOrder order = NestKeys.orderOf(event);
        if (edge.pathId().isEmpty()) {
            handleRoot(edge, event, row, order, touched);
            return;
        }
        ElementRef ref = new ElementRef(edge.pathId(), null,
                NestKeys.valuesOf(row, edge.elementKey()), null);
        Map<String, Object> was = NestKeys.replacedRow(edge, event);
        ElementRef from = was == null ? null
                : new ElementRef(edge.pathId(), null, NestKeys.valuesOf(was, edge.elementKey()), null);
        List<Object> joining = NestKeys.valuesOf(row, edge.keyFields());
        List<Object> leaving = was == null ? null : NestKeys.valuesOf(was, edge.keyFields());
        boolean departed = leaving != null && !leaving.equals(joining);
        // Which document this copy is about is the key it was routed on, not the key its row now names. The
        // departure copy was sent here by what the row is leaving, so that is the document this instance
        // holds and the only one it may touch.
        List<Object> key = edge.carriesDepartures() ? leaving : joining;
        if (edge.carriesDepartures() && !departed) {
            // Leaving nowhere: this copy landed beside its twin, which has the row in hand already.
            return;
        }
        Touched document = touched(key, touched);
        document.ts = event.ts();
        settle(key, document, edge.carriesDepartures()
                ? new NestElement(ref, null, order, event.positions(), from, true)
                : new NestElement(ref, NestKeys.isDeletion(event) ? null : row, order,
                        event.positions(), from, false));
        // A leaf hanging straight off the root belongs to whichever document its join key names, so a row
        // re-pointed at another root has to be taken out of the one it was in. Both are held here, so the
        // pair needs no routing: this vertex is where the two keys would have met anyway.
        //
        // The arriving half claims no hand-over, whatever the join key did. Everything that reaches this
        // vertex as a row of its own is a leaf of the tree - anything with children below it arrives having
        // cascaded, already routed, and is settled above - and a leaf has no subtree for the half it left
        // behind to park. The half that stays behind agrees: it parks only what the document actually holds
        // beneath the element, which for a leaf is nothing.
        //
        // Claiming one anyway is the exact failure the flag exists to prevent, arrived at from the other
        // side: the document is owed something nobody will ever send, so it is stored and never emitted,
        // the element is in neither document downstream, and the change that started the move goes on
        // holding the frontier down - with the run reporting RUNNING and no error.
    }

    /**
     * Applies one root row: the document it names becomes what the row says, or is removed where the row is
     * a deletion - and where the row has changed the very key it is filed under, the document changes
     * identity and the whole tree goes with it.
     *
     * <p>That last case is the one thing the root has that an element does not: the key a document is filed
     * under is read off the root row, so editing it ends one document and starts another. Both halves arrive
     * here, the ordinary edge keyed by where the row now is and its twin keyed by where it was, so the
     * instance holding each side does its own half and neither reaches across.
     */
    private void handleRoot(NestInbound edge, Envelope event, Map<String, Object> row, SourceOrder order,
            Map<Object, Touched> touched) {
        List<Object> key = NestKeys.valuesOf(row, vertex.partitionKey());
        Map<String, Object> was = NestKeys.replacedRow(edge, event);
        List<Object> leaving = was == null ? null : NestKeys.valuesOf(was, vertex.partitionKey());
        boolean movedKey = leaving != null && !leaving.equals(key);
        if (edge.carriesDepartures()) {
            // Every root row travels the twin edge, not only the ones that moved - an edge cannot filter.
            // One that is leaving nowhere landed beside its twin, which has the row in hand already.
            if (movedKey) {
                letGoOfTheWholeDocument(leaving, key, order, event, touched);
            }
            return;
        }
        Touched document = touched(key, touched);
        document.ts = event.ts();
        if (NestKeys.isDeletion(event)) {
            document.assembly.deleteRoot(order, event.positions());
            document.rootDeleted = true;
            return;
        }
        document.assembly.applyRoot(row, order, event.positions());
        if (movedKey && parking != null) {
            // The half that has never seen this document before. What it was is somewhere both halves can
            // reach, addressed by the key that arrived - which is the only thing this side knows.
            ParkedSubtree.At at = ParkedSubtree.At.ofRoot(key);
            if (!collect(key, document.assembly, at)) {
                owed.put(at, new Owed(key, document.ts, clock.millis(), event.positions()));
            }
        }
    }

    /**
     * Empties the document the root row is leaving into the parking area and removes it, so the key the row
     * now carries can be given all of it.
     *
     * <p>The removal is not conditional on this instance having seen the root. State here can be cold or
     * rebuilt from a floor, so "no document under that key" says nothing about what a sink is holding - and
     * the source has just said that key no longer exists. Staying silent would leave a document downstream
     * that nothing ever removes again.
     *
     * <p>With nowhere to hand the tree through, nothing moves at all: the document stays whole under the key
     * it is on rather than being emptied into nothing. A stale copy disagrees with its source and can be
     * seen to; rows read out and handed nowhere are gone, with nothing anywhere reporting it.
     */
    private void letGoOfTheWholeDocument(List<Object> leaving, List<Object> arriving, SourceOrder order,
            Envelope event, Map<Object, Touched> touched) {
        if (parking == null) {
            return;
        }
        Touched document = touched(leaving, touched);
        document.ts = event.ts();
        hand(ParkedSubtree.At.ofRoot(arriving), document.assembly.detachEverything(), event.positions());
        document.assembly.deleteRoot(order, event.positions());
        document.rootDeleted = true;
    }

    /**
     * Applies one change to one document, handing a subtree over or collecting one where the change is half
     * of a move.
     *
     * <p>The half that stays behind reads the subtree out and parks it: those rows arrived long ago and
     * nothing will send them again, so leaving them to be dropped with the element loses them silently. The
     * half that arrives looks for one that was parked. Neither half knows whether the other is in the same
     * document, the same drain, or even the same member - they are routed by two different keys - so each
     * does its own side and the parking area is where they meet.
     *
     * <p>Order between the two does not matter for what is parked: an arrival that finds nothing simply
     * finds nothing, and the subtree it is owed is collected when it is looked for again.
     */
    private void settle(Object key, Touched document, NestElement change) {
        RootAssembly assembly = document.assembly;
        if (parking != null && change.departure() && assembly.holdsSubtreeAt(change.movedFrom())) {
            // Published before the document gives it up. Detaching first and then failing to park stores a
            // document without rows that reached nowhere else, and the replay that would move them again
            // resumes above the change that moved them - so nothing looks for them ever again. A failure
            // here costs a retry that parks the same rows twice, which their identities make harmless.
            hand(ParkedSubtree.At.of(change), assembly.subtreeAt(change.movedFrom()), change.positions());
            assembly.detachSubtree(change.movedFrom());
            return;
        }
        assembly.take(change);
        if (parking == null || !change.departed() || change.deletion()) {
            return;
        }
        // Only where something really was sent to the key it came from. Every other change knowing its old
        // address is an element renamed inside the document it never left, and waiting for a hand-over after
        // each of those is waiting for something that is never coming, once per change, for as long as the
        // pipeline runs.
        ParkedSubtree.At at = ParkedSubtree.At.of(change);
        if (!collect(key, assembly, at)) {
            owed.put(at, new Owed(key, document.ts, clock.millis(), change.positions()));
        }
    }

    /**
     * Leaves a subtree where the document gaining the element can be given it, and keeps the frontier below
     * the change that started the move until it has landed.
     *
     * <p>The bound is the whole reason a hand-over is not free. Those rows are in no document while they sit
     * here: the one that had them has let them go and the one that will have them has not taken them. Let
     * the frontier past the change that moved them and a restart resumes above it, so nothing replays the
     * move, nothing collects what is parked, and the subtree is gone from both documents with the pipeline
     * running and no error anywhere.
     */
    private void hand(ParkedSubtree.At at, List<NestElement> subtree, Map<String, ChainPosition> since) {
        if (subtree.isEmpty()) {
            return;
        }
        ParkedSubtree held = parking.load(at);
        Held outstanding = handedOver.get(at);
        long parked = (outstanding == null ? 0L : outstanding.changes()) + subtree.size();
        if (parked > parkingLimit) {
            throw new TapstateException(NestError.MIGRATION_PARKING_LIMIT_EXCEEDED,
                    Map.of("address", NestStateKeys.nameOf(at), "changes", parked, "limit", parkingLimit),
                    null);
        }
        int size = (int) Math.min(Integer.MAX_VALUE, migrationBatch);
        List<NestElement> first = held == null ? null : held.changes();
        int cursor = 0;
        if (first == null) {
            first = List.copyOf(subtree.subList(0, Math.min(size, subtree.size())));
            cursor = first.size();
        }
        int pieces = held == null ? 0 : held.batches();
        while (cursor < subtree.size()) {
            int take = Math.min(size, subtree.size() - cursor);
            parking.save(at.piece(++pieces), new ParkedSubtree(subtree.subList(cursor, cursor + take)));
            cursor += take;
        }
        // The entry naming the rest is written last, and it is the only one anyone looks for. A failure part
        // way through therefore leaves pieces nobody reads rather than an address promising pieces that are
        // not there - and the change that started the move is replayed, because the frontier is held below
        // it until it lands, so they are written again.
        parking.save(at, new ParkedSubtree(first, pieces));
        // Kept from the first hand-over onto this address rather than reset by a later one: what the frontier
        // must stay below is the earliest change still in flight, and how long this has been outstanding is
        // measured from when it started rather than from the last thing added to it.
        handedOver.put(at, outstanding == null
                ? new Held(since, clock.millis(), pieces, parked)
                : new Held(outstanding.since(), outstanding.parkedAt(), pieces, parked));
    }

    /**
     * Takes whatever was parked for this element into the document that now holds it, and lets go of the
     * entry. Applied in the order it was handed over, so a parent is placed before its children.
     *
     * <p><b>The document is stored before the parking area is let go of.</b> Both are writes to a durable
     * plane, and between them the rows exist in exactly one place; done the other way round, that one place
     * is the one being emptied, and a failure in the gap loses them with the state consistent and nothing
     * anywhere to say a hand-over was ever owed.
     */
    private boolean collect(Object key, RootAssembly assembly, ParkedSubtree.At at) {
        ParkedSubtree waiting = parking.load(at);
        if (waiting == null) {
            return false;
        }
        takeIn(assembly, at, waiting);
        store.save(key, assembly);
        letGoOf(at, waiting.batches());
        handedOver.remove(at);
        owed.remove(at);
        return true;
    }

    /** Applies a hand-over, whichever pieces it was written in, in the order they were written. */
    private void takeIn(RootAssembly assembly, ParkedSubtree.At at, ParkedSubtree waiting) {
        waiting.changes().forEach(assembly::take);
        for (int piece = 1; piece <= waiting.batches(); piece++) {
            ParkedSubtree more = parking.load(at.piece(piece));
            if (more != null) {
                more.changes().forEach(assembly::take);
            }
        }
    }

    /**
     * Drops a hand-over, its own entry last. Dropped first, a failure in the gap would strand every piece
     * behind it: the entry naming them is the only thing that says they exist.
     */
    private void letGoOf(ParkedSubtree.At at, int pieces) {
        for (int piece = pieces; piece >= 1; piece--) {
            parking.remove(at.piece(piece));
        }
        parking.remove(at);
    }

    /**
     * Looks again for the hand-overs this vertex is owed and had not been given when the element arrived.
     *
     * <p>The two halves of a move are routed by different keys and may be worked by different members, so
     * which of them runs first is not something either can decide. The arriving half looking once is enough
     * only when the other has already been through; otherwise the rows sit parked and the document that
     * should have them never asks again.
     *
     * <p>Run wherever the windows are swept, and for the same reason: nothing about a hand-over landing
     * produces an event of its own, so it has to travel on somebody else's. A vertex that is never idle
     * still drains, and one fed nothing but bounds still gets those.
     */
    private void collectWhatIsOwed() {
        if (parking == null || owed.isEmpty()) {
            return;
        }
        Map<Object, Touched> landed = new LinkedHashMap<>();
        Map<ParkedSubtree.At, Integer> taken = new LinkedHashMap<>();
        long now = clock.millis();
        Iterator<Map.Entry<ParkedSubtree.At, Owed>> pending = owed.entrySet().iterator();
        while (pending.hasNext()) {
            Map.Entry<ParkedSubtree.At, Owed> entry = pending.next();
            ParkedSubtree waiting = parking.load(entry.getKey());
            Object key = entry.getValue().key();
            if (waiting == null) {
                // Nothing has been parked for this document yet. Waiting is right until it stops being: the
                // half that would complete it may never be worked at all, and a document held for something
                // that is not coming is one nothing ever sends - the same silence holding it back was for.
                if (now - entry.getValue().awaitedSince() >= migrationProtection) {
                    RootAssembly asItStands = store.load(key);
                    if (asItStands != null) {
                        Touched document = landed.computeIfAbsent(key, ignored -> new Touched(asItStands));
                        document.ts = Math.max(document.ts, entry.getValue().ts());
                    }
                    pending.remove();
                }
                continue;
            }
            // One document may be owed several hand-overs at once, and a store answers a second read with
            // its own copy - so they are taken into one state here rather than each into a fresh read, where
            // the last write would drop what the others had applied.
            Touched document = landed.get(key);
            if (document == null) {
                RootAssembly assembly = store.load(key);
                if (assembly == null) {
                    continue;
                }
                document = new Touched(assembly);
                landed.put(key, document);
            }
            document.ts = Math.max(document.ts, entry.getValue().ts());
            takeIn(document.assembly, entry.getKey(), waiting);
            taken.put(entry.getKey(), waiting.batches());
            handedOver.remove(entry.getKey());
            pending.remove();
        }
        // Sent, not merely stored. A document that gained rows this way changed after it had already gone
        // out, and nothing else is due for that key - so without a send of its own the sink keeps the
        // version without them for good, with the state correct, every reading healthy and no error counted.
        // The hold on the frontier is let go of in the same breath, so a restart resumes above the change
        // and nothing replays it either.
        if (!landed.isEmpty()) {
            settle(landed);
        }
        // Only once the documents holding those rows have been stored, for the reason collect gives: between
        // the two writes the rows exist in one place, and it must not be the one being emptied.
        taken.forEach(this::letGoOf);
    }

    /**
     * Fetches the rows every document in this drain points at, one reach per namespace for all of them at
     * once. Gathered across the whole drain rather than per document: the documents of one drain overlap in
     * what they point at far more often than not - that is what a reference is - so a shared batch asks for
     * each row once where a batch per document would ask for a popular one as many times as it appeared.
     *
     * <p>A tree that points at nothing does none of this and reaches for nothing, which is what keeps the
     * cost of the direction that already existed exactly where it was.
     */
    private Map<String, Map<Object, Map<String, Object>>> resolveReferences(Map<Object, Touched> touched) {
        if (referenced.isEmpty()) {
            return Map.of();
        }
        Map<String, Set<List<Object>>> needed = new LinkedHashMap<>();
        for (Touched document : touched.values()) {
            document.assembly.referencesNeeded(slots).forEach((namespace, keys) ->
                    needed.computeIfAbsent(namespace, name -> new LinkedHashSet<>()).addAll(keys));
        }
        Map<String, Map<Object, Map<String, Object>>> resolved = new LinkedHashMap<>();
        needed.forEach((namespace, keys) ->
                resolved.put(namespace, storeOf(namespace).loadAll(new LinkedHashSet<>(keys))));
        return resolved;
    }

    /** The rows one document points at, for the places a single document is rendered on its own. */
    private Map<String, Map<Object, Map<String, Object>>> referencesFor(RootAssembly assembly) {
        if (referenced.isEmpty()) {
            return Map.of();
        }
        Map<String, Map<Object, Map<String, Object>>> resolved = new LinkedHashMap<>();
        assembly.referencesNeeded(slots).forEach((namespace, keys) ->
                resolved.put(namespace, storeOf(namespace).loadAll(new LinkedHashSet<>(keys))));
        return resolved;
    }

    /**
     * Where the rows of one namespace are read from. Named rather than dereferenced at each call site,
     * because the two callers had two different failures for the same wiring mistake - one said which
     * namespace had no store and the other dereferenced null, on a path a window reaches and a drain
     * does not.
     */
    private NestStore<Map<String, Object>> storeOf(String namespace) {
        NestStore<Map<String, Object>> store = referenced.get(namespace);
        if (store == null) {
            throw new IllegalStateException(
                    "a slot points at " + namespace + ", which this vertex was given no store for");
        }
        return store;
    }

    /**
     * Stores every document this drain touched and emits each one once, in the state it now stands in.
     *
     * <p>A document goes out as a whole row and is applied by upserting it on its key, which is what makes
     * a resend harmless and lets any idempotent sink take it unchanged. It is deliberately not sent as a
     * change: there is no before image to offer - the elements that moved came from other rows entirely -
     * and a sink handed a change with no before image matches nothing, so it writes nothing and reports
     * nothing wrong.
     *
     * <p>A document going out is also what releases the changes it carried, and it goes out saying which
     * chains it drew on and how far - the only thing that ever leaves here, so a sink that is told nothing
     * can never ack a chain that ran through a nest. A deleted root's key row says the same of the deletion
     * alone: it carries no element, so an element absorbed alongside that deletion has still been shown to
     * nobody and goes on holding the frontier back. The state is stored after that, so what is written down
     * is what is still owed rather than what has just been paid.
     */
    private void settle(Map<Object, Touched> touched) {
        Map<String, Map<Object, Map<String, Object>>> resolved = resolveReferences(touched);
        touched.forEach((key, document) -> {
            document.assembly.render(slots, resolved).ifPresentOrElse(
                    rendered -> {
                        deleted.remove(key);
                        if (isOwedAHandOver(key)) {
                            return;
                        }
                        if (document.assembly.waitsForARowItPointsAt(slots, resolved)) {
                            waiting.add(key);
                            return;
                        }
                        waiting.remove(key);
                        if (mayGoOutNow(key)) {
                            outgoing.add(Envelope.insert(document.ts, outputStream, rendered, null)
                                    .withPositions(document.assembly.covered())
                                    // Said out loud, because a field that stopped being rendered and a
                                    // field this tree never had are the same document downstream - and a
                                    // target applies one by setting what is in it, so what is gone from it
                                    // stays there at its last value unless the emission names it.
                                    .withRemoved(RootAssembly.embedsNotRendered(slots, rendered)));
                            document.assembly.documentSent();
                        } else {
                            windows.get(key).holds(document.ts, document.assembly.lowestUnsentByChain());
                        }
                    },
                    () -> {
                        if (document.rootDeleted) {
                            Map<String, ChainPosition> covered = document.assembly.coveredByADeletion();
                            outgoing.add(Envelope.delete(document.ts, outputStream, keyRow(key), null)
                                    .withPositions(covered));
                            document.assembly.deletionSent();
                            deleted.put(key, covered);
                            // The window goes with it. What it was holding back names a key that is gone,
                            // and the row bringing that key back is a change nothing should delay.
                            windows.remove(key);
                            waiting.remove(key);
                        }
                    });
            store.save(key, document.assembly);
            refuseToLetOneDocumentGrowPastItsWidth(key, document.assembly);
            long pending = document.assembly.pending();
            // Reported before it is weighed, so that the count that stopped the run is the one on record
            // rather than the last one that was allowed.
            store.holding(pending);
            NestLimits.refuse(vertex, key, pending, pendingLimit);
            if (refuseToKeepMoreDeletionsThanAllowed(key, document.assembly) > 0) {
                keeping.add(key);
            } else {
                keeping.remove(key);
            }
        });
    }

    /**
     * Whether this document is still waiting for rows that belong in it, in which case it is not shown at
     * all until they arrive.
     *
     * <p>A key gaining a tree routinely has its root row in hand before anything has been parked for it, and
     * rendered then, what goes downstream is a document with its whole tree missing. The version after it is
     * correct, so nothing stays wrong; what is wrong is that anything reading in between sees a document the
     * source never had, and a sink that fans out has already passed it on. Held back, a reader sees the state
     * before the move for a moment longer and then the whole thing.
     *
     * <p>Deliberately not the window mechanism beside it. A window ends on a clock and would send the half
     * built document when it ran out, which is the one outcome this exists to prevent; what ends this is the
     * rows arriving, or the wait being given up on where they never do.
     *
     * <p>Asked by walking what is owed rather than by keeping a second index of it: the map is bounded by the
     * moves actually in flight, and a mirror of it is one more thing that can disagree with the truth.
     */
    private boolean isOwedAHandOver(Object key) {
        if (owed.isEmpty()) {
            return false;
        }
        for (Owed awaited : owed.values()) {
            if (awaited.key().equals(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a document that has just changed may go out now, opening or re-opening its window if it may.
     *
     * <p>Leading edge: a document nobody has been writing to goes out on the spot and opens the window
     * behind it, so a root changing less often than the window pays nothing for it at all. Trailing edge
     * would delay every change by the window to merge the ones that mostly are not there.
     */
    private boolean mayGoOutNow(Object key) {
        if (sending.windowMillis() <= 0) {
            return true;
        }
        long now = clock.millis();
        Window window = windows.get(key);
        if (window == null) {
            windows.put(key, new Window(now));
            return true;
        }
        if (now - window.openedAt < sending.windowMillis()) {
            return false;
        }
        window.reopen(now);
        return true;
    }

    /**
     * Sends every document whose window has run out with something folded into it, and ends the windows
     * that ran out with nothing. Without this a folded change waits for the next change to that same root,
     * which for the root that just went quiet is never: the sink keeps the version before it, the job stays
     * RUNNING, and nothing counts an error.
     *
     * <p>Reading the state back is the cost of not keeping it here, and it is bounded by what it is for:
     * once per window per document that actually changed, never per idle turn. A window that ran out with
     * nothing folded into it is dropped without reading anything.
     */
    private boolean sendWhatWindowsHaveRunOutOn() {
        return sendFolded(true);
    }

    /**
     * Sends what every window is still holding, run out or not. Nothing else will run: the inputs are
     * done, so no drain and no idle turn is coming, and a version folded into a window that never ends is
     * one this level took in and nobody will ever see. A finite run - a backfill, a test, a pipeline
     * stopped on purpose - would otherwise end with its last version of every recently changed document
     * missing, and the sink holding the version before it with nothing reported.
     */
    @Override
    public boolean complete() {
        return sendFolded(false);
    }

    private boolean sendFolded(boolean onlyWhatHasRunOut) {
        if (windows.isEmpty()) {
            return flush();
        }
        long now = clock.millis();
        boolean letGo = false;
        Iterator<Map.Entry<Object, Window>> open = windows.entrySet().iterator();
        while (open.hasNext()) {
            Map.Entry<Object, Window> entry = open.next();
            Window window = entry.getValue();
            if (onlyWhatHasRunOut && now - window.openedAt < sending.windowMillis()) {
                continue;
            }
            if (window.unsent == null) {
                open.remove();
                continue;
            }
            // The same hold a fresh render is subject to, applied to a version folded into a window that
            // was already open before the move began. Without it the window is the way round the hold: it
            // renders and sends the document while the subtree it is owed is still parked, which is the
            // half-built version the hold exists to keep from ever being seen. The window stays open, so
            // the version goes out on the next pass after the rows land.
            if (isOwedAHandOver(entry.getKey())) {
                continue;
            }
            // Asked before anything is read, because what ends this wait is an arrival and never this pass:
            // the row turning up wakes the document through the drain, which is where the wait is lifted. A
            // window that has run out is looked at again on every idle turn, so re-reading the state and the
            // rows it names each time is one state reach per waiting document per turn, for as long as the
            // wait lasts - and the reach for a row that is not there is a miss the layer behind the map
            // answers every single time.
            if (waiting.contains(entry.getKey())) {
                continue;
            }
            RootAssembly assembly = store.load(entry.getKey());
            Map<String, Map<Object, Map<String, Object>>> references = assembly == null
                    ? Map.of()
                    : referencesFor(assembly);
            Optional<Map<String, Object>> rendered = assembly == null
                    ? Optional.empty()
                    : assembly.render(slots, references);
            if (rendered.isEmpty()) {
                open.remove();
                continue;
            }
            // The same as the hold above, and for the same reason: a window running out is a clock, and a
            // clock is exactly what must not release a document that is still missing a row it names.
            if (assembly.waitsForARowItPointsAt(slots, references)) {
                // Recorded as well as skipped, so the next turn takes the cheap exit above rather than
                // reading the same absence again. A restart arrives here with nothing recorded, which is
                // what this covers: the drain that first saw the wait may be on the other side of it.
                waiting.add(entry.getKey());
                continue;
            }
            waiting.remove(entry.getKey());
            outgoing.add(Envelope.insert(window.ts, outputStream, rendered.get(), null)
                    .withPositions(assembly.covered())
                    // As on the drain's own path. A document released by the window is the same document
                    // and needs the same saying-so - and this is the path a deployment with a window open
                    // sends most of them on, so leaving it out would fix nothing where it matters.
                    .withRemoved(RootAssembly.embedsNotRendered(slots, rendered.get())));
            assembly.documentSent();
            store.save(entry.getKey(), assembly);
            window.reopen(now);
            letGo = true;
        }
        if (!flush()) {
            return false;
        }
        // Only after they have actually left: a bound offered ahead of the documents queued behind it
        // would say they had gone. And it has to be offered at all, because what this level may promise
        // depends on what it is holding as well as on what its edges said - an upstream that has finished
        // speaking sends nothing more to prompt the recount, and the chain would stay pinned at whatever
        // the fold held it to.
        return !letGo || bounds == null || bounds.release(this::tryEmit);
    }

    /**
     * Queues on, for each chain, the position a lookup said owes nothing, once this level holds nothing
     * lower on that chain.
     *
     * <p><b>Held rather than passed straight on, and the hold is the whole of what makes it safe.</b> What
     * it says is true where it was said: those rows are durable and no record about them is coming. What it
     * cannot see is a document sitting here in its window holding a <em>lower</em> position on the same
     * chain - a document that is not durable anywhere, because a word about a row it points at changes
     * nothing in the state, only what has to be drawn again. Let past that document, this would have a sink
     * ack above a change that is then neither delivered nor replayable: the document stays at its previous
     * version for ever, the job running and every count healthy.
     *
     * <p>So the condition is the one the bound already uses, asked of the same reading: whatever a window
     * or an uncollected hand-over keeps this level's promise below, keeps this below it too. Queued after
     * the documents whose windows just ran out, for the same reason a bound is - one sent ahead of what is
     * queued behind it would say those documents had gone.
     */
    private boolean passOnWhatOwesNothing() {
        if (settledAhead.isEmpty()) {
            return true;
        }
        Map<String, ChainPosition> free = new LinkedHashMap<>();
        settledAhead.entrySet().removeIf(entry -> {
            SourceOrder unsent = lowestUnsentOn(entry.getKey());
            if (unsent != null && unsent.compareTo(entry.getValue().order()) <= 0) {
                return false;
            }
            free.put(entry.getKey(), entry.getValue());
            return true;
        });
        if (free.isEmpty()) {
            return true;
        }
        outgoing.add(new SettledPositions(free));
        // Flushed here rather than left to whatever runs next: every caller has already flushed by the
        // time this is reached, so a word only queued would wait for the next turn that happens to flush
        // - and on the stream this is for, that turn is one no further row is coming to cause.
        return flush();
    }

    /**
     * The lowest position on {@code chain} that a window is holding back, or null when none is. This is the
     * whole of what this level keeps the frontier below: everything else it holds is written through with
     * the state and comes back out on its own.
     */
    private SourceOrder lowestUnsentOn(String chain) {
        SourceOrder lowest = null;
        for (Window window : windows.values()) {
            if (window.unsent == null) {
                continue;
            }
            ChainPosition held = window.unsent.get(chain);
            if (held != null && (lowest == null || held.order().compareTo(lowest) < 0)) {
                lowest = held.order();
            }
        }
        for (Held outstanding : handedOver.values()) {
            ChainPosition held = outstanding.since().get(chain);
            if (held != null && (lowest == null || held.order().compareTo(lowest) < 0)) {
                lowest = held.order();
            }
        }
        // The half that arrives first holds nothing durable, which is exactly why it has to hold the
        // frontier: what it knows - that this key is owed a document - is in memory alone, and a restart
        // above the change that made it owed leaves nobody to ask again.
        for (Owed outstanding : owed.values()) {
            ChainPosition held = outstanding.since().get(chain);
            if (held != null && (lowest == null || held.order().compareTo(lowest) < 0)) {
                lowest = held.order();
            }
        }
        return lowest;
    }

    /**
     * Stops the job once one document has absorbed more elements than it is allowed to. Checked per
     * document rather than across the nest: how wide one has grown says nothing about the others, and a
     * limit spent by whichever document happened to be assembled first would fail the rest for its width.
     *
     * <p>This bounds memory where a count of entries cannot. A document is rendered whole, so however much
     * it holds is what has to be there at once, and no eviction reaches inside one.
     */
    private void refuseToLetOneDocumentGrowPastItsWidth(Object key, RootAssembly assembly) {
        long elements = assembly.elements();
        if (elements > elementLimit) {
            throw new TapstateException(NestError.ROOT_FANOUT_LIMIT_EXCEEDED,
                    Map.of("rootKey", String.valueOf(keyRow(key)), "elements", elements,
                            "limit", elementLimit), null);
        }
    }

    /**
     * Stops the job once one document keeps more records of deleted elements than it is allowed to, and
     * answers with how many it is left keeping.
     *
     * <p><b>What may be dropped is dropped before the count is weighed</b>, which is what makes this limit
     * mean something an operator can act on. The sweep that drops them runs when nothing is arriving, so a
     * vertex that never goes idle would otherwise fail for being busy — and a limit that fires on load says
     * nothing about the frontier, which is the one thing reaching it is supposed to report.
     *
     * <p>That crossing to the durable plane is paid only by a document already at its limit, never by every
     * document on every drain: below the limit this reads a count and returns.
     */
    private long refuseToKeepMoreDeletionsThanAllowed(Object key, RootAssembly assembly) {
        long kept = assembly.tombstones();
        if (kept <= tombstoneLimit) {
            return kept;
        }
        if (assembly.forgetDeletionsBelow(floor) > 0) {
            store.save(key, assembly);
            kept = assembly.tombstones();
        }
        if (kept > tombstoneLimit) {
            throw new TapstateException(NestError.TOMBSTONE_LIMIT_EXCEEDED,
                    Map.of("namespace", vertex.mapName(), "key", String.valueOf(key),
                            "tombstones", kept, "limit", tombstoneLimit), null);
        }
        return kept;
    }

    /** The key of a document that is gone, as the row a sink needs to find and remove it. */
    private Map<String, Object> keyRow(Object key) {
        List<?> values = (List<?>) key;
        Map<String, Object> row = new LinkedHashMap<>();
        List<String> fields = vertex.partitionKey();
        for (int i = 0; i < fields.size(); i++) {
            row.put(fields.get(i), values.get(i));
        }
        return row;
    }

    private Touched touched(Object key, Map<Object, Touched> touched) {
        return touched.computeIfAbsent(key, k -> {
            RootAssembly held = store.load(k);
            return new Touched(held == null ? new RootAssembly() : held);
        });
    }

    private boolean flush() {
        while (!outgoing.isEmpty()) {
            if (!tryEmit(outgoing.peek())) {
                return false;
            }
            outgoing.poll();
        }
        return true;
    }

    /** One document being worked on during a drain, before it is stored and emitted. */
    private static final class Touched {

        private final RootAssembly assembly;
        private boolean rootDeleted;
        private long ts;

        private Touched(RootAssembly assembly) {
            this.assembly = assembly;
        }
    }

    /** The window open over one document: when it opened, and what it is holding back if anything. */
    private static final class Window {

        private long openedAt;

        /**
         * The lowest position per chain of what a send folded into this window is keeping from the
         * frontier, or null when nothing has been folded in. Null rather than an empty map: an empty map
         * would read as "holding nothing on any chain", which is true of both, and the two are told apart
         * everywhere else by which one ends the window.
         */
        private Map<String, ChainPosition> unsent;

        /** The event time of the last change folded in, which is what the document goes out carrying. */
        private long ts;

        private Window(long openedAt) {
            this.openedAt = openedAt;
        }

        private void holds(long ts, Map<String, ChainPosition> lowestUnsent) {
            this.ts = ts;
            this.unsent = lowestUnsent;
        }

        private void reopen(long now) {
            this.openedAt = now;
            this.unsent = null;
        }
    }
}
