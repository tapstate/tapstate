package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.SettledPositions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
 * <p><b>One outbound edge, and two kinds of thing on it.</b> A row landing here changes no document by
 * arriving - which documents refer to it is not knowable from the row - so what goes out is worked out
 * from what was recorded about who points where: a word to each of them that the row they point at has
 * changed. It carries no fields; the document reads what to show out of the namespace this just wrote.
 * The second kind is for the rows that word goes to nobody about: filed, named by no document, and so
 * carried to a sink by nothing - see {@link #sayWhatOwesNothing()} for why a chain needs telling.
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
    private final long referrersAllowed;
    private final LevelBounds bounds;
    private final Queue<Object> outgoing = new ArrayDeque<>();
    // The highest position per chain among the rows this drain filed that woke nothing, folded as they are
    // met and said in one word when the drain ends. Kept across a drain that stopped early on a full
    // outbox: the word is queued at the end of a later drain, or where the next bound is passed on.
    private final Map<String, ChainPosition> owingNothing = new LinkedHashMap<>();
    private NestFailureRecording failures = NestFailureRecording.of(null);

    LookupProcessor(NestLookup lookup, NestStore<Map<String, Object>> store,
            NestStore<Set<Object>> references, long referrersAllowed, LevelBounds bounds) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.store = Objects.requireNonNull(store, "store");
        this.references = Objects.requireNonNull(references, "references");
        this.referrersAllowed = referrersAllowed;
        this.bounds = bounds;
    }

    /** Resolves where this vertex writes down what killed it; see {@link NestFailureRecording}. */
    @Override
    protected void init(Processor.Context context) {
        this.failures = NestFailureRecording.of(context);
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
        failures.recording(() -> {
            processRecording(ordinal, inbox);
            return null;
        });
    }

    private void processRecording(int ordinal, Inbox inbox) {
        if (!flush()) {
            return;
        }
        Map<Object, Map<String, Object>> filed =
                ordinal == REGISTRATIONS ? rowsNamedIn(inbox) : Map.of();
        for (Object item; (item = inbox.peek()) != null; ) {
            handle(ordinal, (Envelope) item, filed);
            inbox.remove();
            if (!flush()) {
                return;
            }
        }
        sayWhatOwesNothing();
        flush();
    }

    /**
     * Queues one word for the rows this drain filed that woke nothing, and forgets them.
     *
     * <p><b>Nothing else downstream will ever speak for them.</b> A row here reaches a sink only inside the
     * documents naming it, so a row nobody names produces no record at all - which is the shape of this
     * direction working, not a fault. But the chain it arrived on is then never advanced past it: the sink
     * learns a position is durable by writing what carries it, and there is nothing to write. Left alone
     * the chain stops at the last row some document happened to name and stays there for the life of the
     * job, with every document correct and a restart replaying that table from further back every day.
     *
     * <p><b>Once per drain rather than once per row.</b> On a table nothing points at, every row takes this
     * path, so a word each would put as many records on the edge as the stream has rows. Only the highest
     * position per chain is of any use downstream - a frontier advances to the highest it is given at or
     * below its bound - so the drain's are folded into one and the rest cost nothing to drop.
     *
     * <p>Queued rather than emitted, like everything else this vertex produces: it must sit behind the
     * words already in the queue, and being in the queue is what puts it behind them.
     */
    private void sayWhatOwesNothing() {
        if (owingNothing.isEmpty()) {
            return;
        }
        outgoing.add(new SettledPositions(owingNothing));
        owingNothing.clear();
    }

    /**
     * Which of the rows this drain's registrations name are filed already, asked for in one request.
     *
     * <p><b>Once per drain rather than once per row, which is the difference between one round trip and
     * as many as the drain is long.</b> Behind this namespace is a store, and a read of it per arrival of
     * the pointing stream is exactly the degeneration nothing else here is allowed either - and one that
     * reads identically to a batch from every angle but a count of the trips.
     *
     * <p>Taken before the drain and used throughout it, which is sound because the two edges are drained
     * separately: nothing files a row while this ordinal is being worked, so the reading cannot go stale
     * inside the loop. A row filed after this drain arrives to a bucket that now names the registration,
     * and is answered by the wake on its own arrival - the two paths meet exactly, with no gap and no
     * overlap.
     */
    private Map<Object, Map<String, Object>> rowsNamedIn(Inbox inbox) {
        Collection<Object> named = new LinkedHashSet<>();
        for (Object item : inbox) {
            named.add(NestKeys.valuesOf(NestKeys.rowOf((Envelope) item), lookup.referenceFields()));
        }
        return store.loadAll(named);
    }

    private void handle(int ordinal, Envelope event, Map<Object, Map<String, Object>> filed) {
        Map<String, Object> row = NestKeys.rowOf(event);
        if (ordinal == REGISTRATIONS) {
            register(event, row, filed);
            return;
        }
        if (ordinal == DEPARTED_REGISTRATIONS) {
            unregister(event, row);
            return;
        }
        List<Object> key = NestKeys.valuesOf(row, lookup.partitionKey());
        // Left standing as an empty row rather than taken out. A document pointing at a row that is not
        // here has to know which kind of not-here it is: one that has not arrived is worth waiting for and
        // one that has been deleted never will be, and an absent entry cannot say which. And never taken
        // away afterwards, on any path: which documents name a row is a statement about the ones seen so
        // far and never about the ones to come, so a record dropped because nothing wanted it at that
        // moment is the answer missing for the next document that does - and that document waits for an
        // arrival already in the past, for the life of the job, with nothing thrown and no count moved.
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
     *
     * <p><b>That ceiling is weighed here, and it costs nothing to weigh.</b> Every bucket is in hand
     * already and every identity in them is about to be walked, so counting them first is a second walk
     * over what is on the heap rather than a second reach into the state layer - which is what lets the
     * limit sit on the edit that pays for it rather than on the arrival of each row that registers, where
     * it would have cost a read of every bucket per row of the pointing stream.
     */
    private void wake(List<Object> key, Envelope event) {
        Collection<Set<Object>> buckets = references.loadAll(bucketsOf(key)).values();
        long referrers = 0;
        for (Set<Object> bucket : buckets) {
            referrers += bucket.size();
        }
        // Before the words are built rather than after, so a row past the limit allocates none of the queue
        // it was about to throw away. Only that: they would not have gone out either way, since an
        // exception leaves before the flush that empties this queue - so nothing observes the ordering, and
        // no case here asserts it.
        NestLimits.refuseFanout(lookup, key, referrers, referrersAllowed);
        if (referrers == 0) {
            // Filed, and owed to nobody: no document names this row, so no record downstream will ever
            // carry where it sat. Kept until the drain ends, where the drain's are said in one word.
            SettledPositions.fold(owingNothing, event.positions());
            return;
        }
        for (Set<Object> bucket : buckets) {
            for (Object referrer : bucket) {
                outgoing.add(new NestTouch(referrer, event.ts(), event.positions(), false));
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
    private void register(Envelope event, Map<String, Object> row,
            Map<Object, Map<String, Object>> filed) {
        List<Object> referenced = NestKeys.valuesOf(row, lookup.referenceFields());
        List<Object> referrer = NestKeys.valuesOf(row, lookup.referrerIdentity());
        Object bucket = NestLookup.bucketKey(referenced, NestLookup.bucketOf(referrer));
        if (NestKeys.isDeletion(event)) {
            references.remove(bucket, referrer);
            return;
        }
        references.add(bucket, referrer);
        if (filed.containsKey(referenced)) {
            tellItWhatItMissed(referrer, event);
        }
    }

    /**
     * Tells one row that what it points at is here already, where it is.
     *
     * <p><b>The two deliveries race and only one order of them was answered.</b> A row and the identities
     * pointing at it arrive on separate edges from separate streams, so the engine hands them over in
     * whichever order it likes. Waking on the row's arrival answers the order where the pointing came
     * first; in the other order the row is filed while no bucket names anybody, it says nothing, and the
     * registration that follows records an identity and goes quiet. The document is then parked waiting
     * for an arrival that has already happened, and no later event is coming to say so.
     *
     * <p><b>Nothing reports it.</b> The job stays running, nothing is thrown, no count moves, no row is
     * discarded, and every document that did assemble is correct - so the only trace is one document that
     * never appears. Measured over twenty runs of the job-level witness, three hung this way.
     *
     * <p><b>An entry that is here but empty is an answer too, and the one that ends the wait.</b> A row
     * deleted before anything pointed at it is left standing as an empty row for exactly this, so the
     * document renders without the field and goes rather than waiting for ever. Present-and-empty and
     * absent are different answers here, which is why the test is against absence and not against content.
     *
     * <p><b>What it carries is this arrival, not the one it missed.</b> The change that lets the document
     * be drawn now is this registration, and its positions are the ones a frontier must not pass before
     * the document has gone. The earlier arrival needs none: filing a row nothing pointed at owed nothing
     * to any chain, which is why that chain was never held back for it.
     *
     * <p><b>It goes as the word that is dropped where nothing was waiting, and that mark is what makes it
     * affordable.</b> On the ordinary path the row has been filed since long before the stream pointing at
     * it started, so every one of these finds a document that resolved the row on its own. Sent as an
     * edit, each would draw and send that document a second time - measured over two hundred, twice the
     * records downstream and two and a half times the reach into the assembling vertex's state, all of it
     * on the path where nothing is ever waiting.
     *
     * <p>What it does cost is the reading of the row namespace this drain already took in one request.
     * That is the price of the one ordering no other place can answer: whether the row is here is not
     * knowable from the pointing row, and the vertex that does know it is waiting cannot be told without
     * an edge back to this one.
     */
    private void tellItWhatItMissed(List<Object> referrer, Envelope event) {
        outgoing.add(new NestTouch(referrer, event.ts(), event.positions(), true));
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
     * <p><b>Its own identity counts as one of those keys.</b> A row that kept its reference and changed the
     * column identifying it is recorded by the twin under the new identity and, if only the reference were
     * compared, left recorded under the old one as well - in the same bucket, for the life of the job. That
     * stale identity is woken on every edit to the row pointed at, addressed to a document that does not
     * exist, and counts for ever against the ceiling on how many rows may point at one.
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
        List<Object> referrer = NestKeys.valuesOf(was, lookup.referrerIdentity());
        if (left.equals(NestKeys.valuesOf(row, lookup.referenceFields()))
                && referrer.equals(NestKeys.valuesOf(row, lookup.referrerIdentity()))) {
            return;
        }
        references.remove(NestLookup.bucketKey(left, NestLookup.bucketOf(referrer)), referrer);
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
        // Ahead of the flush, so a drain that stopped early on a full outbox still says what it filed
        // before the bound that covers it goes past. Without this the word waits for the next row of a
        // stream that may have gone quiet, which is exactly the stream this is for.
        sayWhatOwesNothing();
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
