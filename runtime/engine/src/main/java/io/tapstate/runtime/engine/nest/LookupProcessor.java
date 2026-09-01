package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.LevelBounds;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * Files away the rows one level points at, one entry per row under what identifies it. It assembles
 * nothing and holds nothing per document: a row arriving here is stored as it stands, and a row deleted
 * here is taken out, whatever number of documents happen to name it.
 *
 * <p><b>It is the only writer of its namespace, and that is what makes the read from elsewhere safe.</b>
 * The edge into this vertex is partitioned by the very key the entries are filed under, so one member
 * owns each row and no two instances ever write the same entry. The assemblers that read it never write
 * it. So the reach across partitions this whole shape rests on carries no write race with it - which is
 * the thing the rule it is an exception to was actually protecting.
 *
 * <p><b>It also keeps what the row itself cannot say: which rows point at it.</b> Those arrive on their
 * own edge, delivered a second time from the stream doing the pointing and keyed by the row they name, so
 * they land on the instance already owning everything else about that row. They are spread over a fixed
 * number of buckets rather than gathered into one entry per row, because the number of rows pointing at
 * one row is the only thing here that grows without bound - and an entry holding all of them is right
 * until the day it is too large to store, with nothing before then to tell the two apart.
 *
 * <p><b>One outbound edge, and one kind of thing on it.</b> A row landing here changes no document by
 * arriving - which documents refer to it is not knowable from the row - so what goes out is worked out
 * from what was recorded about who points where: a word to each of them that the row they point at has
 * changed. It carries no fields; the document reads what to show out of the namespace this just wrote.
 */
final class LookupProcessor extends AbstractProcessor {

    /** The edge carrying the rows this namespace holds. */
    static final int ROWS = 0;

    /** The edge carrying the rows pointing at them, keyed by what they point at rather than by themselves. */
    static final int REGISTRATIONS = 1;

    /**
     * The twin of that edge, carrying the same rows keyed by what they pointed at <em>before</em>, so a row
     * that now names something else can be taken out of where it was. Drawn only where the pointing stream
     * carries the row it replaces, since that is the only thing that says where it was.
     */
    static final int DEPARTED_REGISTRATIONS = 2;

    private final NestLookup lookup;
    private final NestStore<Map<String, Object>> store;
    private final NestStore<Set<Object>> references;
    private final LevelBounds bounds;
    private final Queue<Object> outgoing = new ArrayDeque<>();

    LookupProcessor(NestLookup lookup, NestStore<Map<String, Object>> store,
            NestStore<Set<Object>> references, LevelBounds bounds) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.store = Objects.requireNonNull(store, "store");
        this.references = Objects.requireNonNull(references, "references");
        this.bounds = bounds;
    }

    /**
     * Like every other vertex that reaches the state layer, and for the same reason: filing a row is a
     * call into the map, and a call that waits made on a cooperative thread stops every other vertex
     * sharing that thread rather than only this one. Cooperative is what a processor is unless it says
     * otherwise, so saying nothing is the whole of the mistake - and it looks like nothing until some
     * unrelated pipeline sharing the thread goes quiet.
     */
    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Drains by hand rather than through {@code tryProcess}, because a row here does work before anything
     * goes out: it is filed, and then everything pointing at it is woken. Refusing the row when the outbox
     * is full would bring the same row back and do that work twice - filing it again is harmless, waking
     * everything again is a second round of documents. So the row leaves the inbox once its work is done
     * and what it produced waits in the queue instead.
     */
    @Override
    public void process(int ordinal, Inbox inbox) {
        if (!flush()) {
            return;
        }
        for (Object item; (item = inbox.peek()) != null; ) {
            handle(ordinal, (Envelope) item);
            inbox.remove();
            if (!flush()) {
                return;
            }
        }
    }

    private void handle(int ordinal, Envelope event) {
        Map<String, Object> row = NestKeys.rowOf(event);
        if (ordinal == REGISTRATIONS) {
            register(row, NestKeys.isDeletion(event));
            return;
        }
        if (ordinal == DEPARTED_REGISTRATIONS) {
            unregister(event, row);
            return;
        }
        List<Object> key = NestKeys.valuesOf(row, lookup.partitionKey());
        // Left standing as an empty row rather than taken out. A document pointing at a row that is not
        // here has to know which kind of not-here it is: one that has not arrived is worth waiting for and
        // one that has been deleted never will be, and an absent entry cannot say which.
        store.save(key, NestKeys.isDeletion(event) ? NestLookup.gone() : row);
        wake(key, event);
    }

    /**
     * Tells every row recorded as pointing at {@code key} that it has changed, addressed by that row's own
     * identity so the word climbs to its document the way the row itself would.
     *
     * <p>Every bucket is asked for in one request rather than one at a time. Which of them hold anything is
     * not knowable without asking - an empty bucket is never written - so the choice is between one round
     * trip and as many as there are buckets, and the second turns a generous bucket count into a cost paid
     * on every single edit.
     *
     * <p><b>What one edit costs downstream is not softened anywhere, deliberately.</b> Each row woken here
     * is a document re-drawn and written out whole, so one edit to a row a hundred thousand documents name
     * is a hundred thousand documents rewritten. <b>The throttle does not help and must not be assumed to:
     * its window is opened per document</b>, and the documents woken by one edit are a different document
     * each - every one of them with no window open and nothing to fold with, so a hundred thousand windows
     * open and a hundred thousand documents go. The three ways to soften it were each considered and each
     * gives up something worse - folding across documents means gathering whole documents into one place;
     * sending only the changed field means every sink downstream has to apply field-level edits; not
     * propagating at all is the behaviour this whole direction exists to fix. So the only thing bounding
     * this is the ceiling on how many rows may point at one, which fails the job outright rather than
     * letting it quietly grind: what is being refused there is the rewrite, not the storage.
     */
    private void wake(List<Object> key, Envelope event) {
        for (Set<Object> referrers : references.loadAll(bucketsOf(key)).values()) {
            for (Object referrer : referrers) {
                outgoing.add(new NestTouch(referrer, event.ts(), event.positions()));
            }
        }
    }

    /** Every bucket one identity's referrers are spread over, to be asked for in one request. */
    private static Collection<Object> bucketsOf(List<Object> key) {
        Collection<Object> buckets = new ArrayList<>(NestLookup.BUCKETS);
        for (int bucket = 0; bucket < NestLookup.BUCKETS; bucket++) {
            buckets.add(NestLookup.bucketKey(key, bucket));
        }
        return buckets;
    }


    /**
     * Records that {@code row} points at what its reference columns name - or, where the row has been
     * deleted, that it no longer points anywhere.
     *
     * <p><b>On every event of that stream, and never subject to a switch.</b> Hanging this on the
     * before-image switch would be the mistake worth naming: that switch is off by default, and with it off
     * there would be no record at all rather than a stale one.
     *
     * <p><b>A deletion is handled here rather than on the departure edge, and that placement is what keeps
     * it correct.</b> Being deleted is how a document stops pointing at something far more often than being
     * re-pointed is, so leaving it to an edge that only exists when key tracking is on would mean the record
     * never came down at all for most trees. And the two edges cannot both act on a deletion: they would be
     * adding and removing the same identity in the same bucket, from two deliveries the engine is free to
     * hand over in either order, so which one won would be a coin toss nothing reports.
     */
    private void register(Map<String, Object> row, boolean deleted) {
        List<Object> referenced = NestKeys.valuesOf(row, lookup.referenceFields());
        List<Object> referrer = NestKeys.valuesOf(row, lookup.referrerIdentity());
        Object bucket = NestLookup.bucketKey(referenced, NestLookup.bucketOf(referrer));
        if (deleted) {
            references.remove(bucket, referrer);
            reclaimIfNothingPointsAtIt(referenced);
        } else {
            references.add(bucket, referrer);
        }
    }

    /**
     * Takes {@code row}'s identity out of what it used to point at, where it now points somewhere else.
     *
     * <p>This copy of the row was keyed by where it was pointing, so the entry to take it out of is this
     * instance's own - which is the whole reason for a second delivery rather than a reach across members
     * from wherever the row's new home is.
     *
     * <p>Every row of that stream travels this edge, not only the ones that moved: an edge cannot filter.
     * What tells them apart is the two keys differing, and for a row that stayed put they do not - it landed
     * beside its twin, which has already recorded it where it still belongs.
     *
     * <p>A deletion falls out of that same test rather than needing one of its own, and has to: what a
     * deletion carries as the row it was and what it carries as its row are one and the same, so the keys
     * are equal and nothing happens here. Taking a deleted row out belongs to the other edge, where it
     * happens whether or not this one was drawn at all.
     */
    private void unregister(Envelope event, Map<String, Object> row) {
        Map<String, Object> was = event.before();
        if (was == null) {
            return;
        }
        List<Object> left = NestKeys.valuesOf(was, lookup.referenceFields());
        if (left.equals(NestKeys.valuesOf(row, lookup.referenceFields()))) {
            return;
        }
        List<Object> referrer = NestKeys.valuesOf(was, lookup.referrerIdentity());
        references.remove(NestLookup.bucketKey(left, NestLookup.bucketOf(referrer)), referrer);
        reclaimIfNothingPointsAtIt(left);
    }

    /**
     * Drops the record that a row was deleted, once no document points at it any longer.
     *
     * <p>That record is kept for one purpose - answering a document that still names the row, so it renders
     * without the field and goes, rather than waiting for ever on an arrival that has already happened. Once
     * nothing names it there is nobody left to answer, and kept anyway every deletion the source ever makes
     * leaves one behind for as long as the job runs.
     *
     * <p><b>A row that is still there is deliberately not dropped, and that is a departure from the shape
     * this was first written as.</b> "Nothing points at it" is a statement about the documents seen so far,
     * never about the ones still to come. A source sends a row once and then only when it changes, so a copy
     * let go of because it was unwanted at that moment is a copy nothing can ask for again: the next document
     * naming it renders with the field simply missing, which is indistinguishable from a document that has
     * none, and no count anywhere moves. What keeping it costs is one entry per row of a table this tree was
     * already holding one entry per row of.
     *
     * <p>Only reached from a re-point, and only after a single read that ends it for a row that is still
     * there - so the sweep of all the buckets is paid on the rare edit that leaves a deleted row behind.
     */
    private void reclaimIfNothingPointsAtIt(List<Object> key) {
        Map<String, Object> filed = store.load(key);
        if (filed == null || !filed.isEmpty()) {
            return;
        }
        if (references.loadAll(bucketsOf(key)).isEmpty()) {
            store.remove(key);
        }
    }

    /**
     * Passes on what this vertex may promise about the chain the pointed-at rows arrive on. <b>This is the
     * only place in the whole tree that chain is ever spoken for.</b> Those rows reach no document by
     * arriving, so before there was an edge out of here nothing downstream was compiled to carry that
     * chain at all, and its bound was worked out by nobody.
     *
     * <p>Only that edge's bounds are passed on. The rows pointing at these arrive here a second time, but
     * they reach their documents by their own path and are spoken for there; saying anything about them
     * from here would be a second answer about a chain that already has one, and the level below is not
     * compiled to expect it.
     *
     * <p>Nothing is held back: a row is filed and everything pointing at it woken within the drain, and the
     * engine calls this only once a drain has returned. The queue is emptied first all the same - a bound
     * sent ahead of the words it is meant to sit behind would say those documents had gone when they are
     * still in the outbox.
     */
    @Override
    public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
        if (bounds == null || ordinal != ROWS) {
            return true;
        }
        if (!flush()) {
            return false;
        }
        return bounds.advance(ordinal, watermark, this::tryEmit);
    }

    /**
     * Refuses to pass on a bound that arrived with no edge attached to it - already combined across both
     * edges feeding this vertex, and so a claim about the rows pointing at these as much as about these.
     * See the per-edge callback above for which of the two this vertex actually answers for.
     */
    @Override
    public boolean tryProcessWatermark(Watermark watermark) {
        return true;
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
}
