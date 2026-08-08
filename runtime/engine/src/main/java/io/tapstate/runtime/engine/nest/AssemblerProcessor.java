package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.ReplayFloor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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

    private final NestVertex vertex;
    private final List<EmbedSlot> slots;
    private final NestStore<RootAssembly> store;
    private final String outputStream;
    private final Deque<Object> outgoing = new ArrayDeque<>();
    private final ChainBounds held = new ChainBounds();
    private final LevelBounds bounds;
    private final ReplayFloor floor;

    /**
     * How many elements any one document may hold. Read once: it is chosen where the job is built, not
     * here.
     */
    private final long elementLimit;

    /**
     * Roots whose deletion has gone downstream and whose record is still kept, against what that deletion
     * covered. They are remembered as they happen rather than looked for later, because a store is not
     * something this can walk: asking one for its keys is what read-through exists to avoid.
     */
    private final Map<Object, Map<String, ChainPosition>> deleted = new LinkedHashMap<>();

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
        this.vertex = Objects.requireNonNull(vertex, "vertex");
        this.slots = List.copyOf(slots);
        this.store = Objects.requireNonNull(store, "store");
        this.outputStream = Objects.requireNonNull(outputStream, "outputStream");
        this.bounds = axes == null ? null : new LevelBounds(chainsByOrdinal, axes, held::lowest);
        this.floor = Objects.requireNonNull(floor, "floor");
        this.elementLimit =
                Objects.requireNonNull(settings, "settings").elementsAllowedIn(vertex.mapName());
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
        return true;
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

    @Override
    public void process(int ordinal, Inbox inbox) {
        if (!flush()) {
            return;
        }
        NestInbound edge = vertex.inbound().get(ordinal);
        Map<Object, Touched> touched = new LinkedHashMap<>();
        try {
            for (Object item; (item = inbox.peek()) != null; ) {
                handle(edge, item, touched);
                inbox.remove();
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
        flush();
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
        if (bounds == null) {
            return true;
        }
        if (!flush()) {
            return false;
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
        if (edge.isCascade()) {
            KeyedElement arrived = (KeyedElement) item;
            apply(touched(arrived.key(), touched), arrived.element());
            return;
        }
        Envelope event = (Envelope) item;
        Map<String, Object> row = NestKeys.rowOf(event);
        SourceOrder order = NestKeys.orderOf(event);
        if (edge.pathId().isEmpty()) {
            List<Object> key = NestKeys.valuesOf(row, vertex.partitionKey());
            Touched document = touched(key, touched);
            document.ts = event.ts();
            if (NestKeys.isDeletion(event)) {
                document.assembly.deleteRoot(order, event.positions());
                document.rootDeleted = true;
            } else {
                document.assembly.applyRoot(row, order, event.positions());
            }
            return;
        }
        List<Object> key = NestKeys.valuesOf(row, edge.keyFields());
        Touched document = touched(key, touched);
        document.ts = event.ts();
        ElementRef ref = new ElementRef(edge.pathId(), null,
                NestKeys.valuesOf(row, edge.elementKey()), null);
        apply(document, new NestElement(ref, NestKeys.isDeletion(event) ? null : row, order,
                event.positions()));
    }

    private static void apply(Touched document, NestElement element) {
        if (element.deletion()) {
            document.assembly.deleteElement(element.ref(), element.order(), element.positions());
        } else {
            document.assembly.applyElement(element.ref(), element.fields(), element.order(), element.positions());
        }
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
        touched.forEach((key, document) -> {
            document.assembly.render(slots).ifPresentOrElse(
                    rendered -> {
                        outgoing.add(Envelope.insert(document.ts, outputStream, rendered, null)
                                .withPositions(document.assembly.covered()));
                        document.assembly.documentSent();
                        deleted.remove(key);
                    },
                    () -> {
                        if (document.rootDeleted) {
                            Map<String, ChainPosition> covered = document.assembly.coveredByADeletion();
                            outgoing.add(Envelope.delete(document.ts, outputStream, keyRow(key), null)
                                    .withPositions(covered));
                            document.assembly.deletionSent();
                            deleted.put(key, covered);
                        }
                    });
            store.save(key, document.assembly);
            refuseToLetOneDocumentGrowPastItsWidth(key, document.assembly);
            if (bounds != null) {
                held.holding(key, document.assembly.lowestHeldByChain());
            }
        });
    }

    /**
     * Stops the job once one document has absorbed more elements than it is allowed to. Checked per
     * document rather than across the nest: how wide one has grown says nothing about the others, and a
     * limit spent by whichever document happened to be assembled first would fail the rest for its width.
     *
     * <p>This is the one of the three limits that bounds memory. A document is rendered whole, so however
     * much it holds is what has to be there at once, and no eviction reaches inside one.
     */
    private void refuseToLetOneDocumentGrowPastItsWidth(Object key, RootAssembly assembly) {
        long elements = assembly.elements();
        if (elements > elementLimit) {
            throw new TapstateException(NestError.ROOT_FANOUT_LIMIT_EXCEEDED,
                    Map.of("rootKey", String.valueOf(keyRow(key)), "elements", elements,
                            "limit", elementLimit), null);
        }
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
}
