package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * <p>Nothing is emitted. A row landing here changes no document by itself: which documents refer to it is
 * not knowable from the row, and the documents that do are woken by what remembers that, not by this.
 */
final class LookupProcessor extends AbstractProcessor {

    /** The edge carrying the rows this namespace holds. */
    static final int ROWS = 0;

    /** The edge carrying the rows pointing at them, keyed by what they point at rather than by themselves. */
    static final int REGISTRATIONS = 1;

    private final NestLookup lookup;
    private final NestStore<Map<String, Object>> store;
    private final NestStore<Set<Object>> references;

    LookupProcessor(NestLookup lookup, NestStore<Map<String, Object>> store,
            NestStore<Set<Object>> references) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.store = Objects.requireNonNull(store, "store");
        this.references = Objects.requireNonNull(references, "references");
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

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        Envelope event = (Envelope) item;
        Map<String, Object> row = NestKeys.rowOf(event);
        if (ordinal == REGISTRATIONS) {
            register(row);
            return true;
        }
        List<Object> key = NestKeys.valuesOf(row, lookup.partitionKey());
        if (NestKeys.isDeletion(event)) {
            // Taken out rather than kept as an empty row. A document naming a row that no longer exists
            // renders without that field, which is what an absent entry already produces - so the two ways
            // of saying "it is gone" are one, and there is no second one to disagree with the first.
            store.remove(key);
            return true;
        }
        store.save(key, row);
        return true;
    }

    /**
     * Records that {@code row} points at what its reference columns name.
     *
     * <p>Unconditional, on every event of that stream, whatever it does: an insert, an update and a
     * deletion all say the same thing about where the row was pointing when it arrived. Taking one back is
     * the other half and it is not this - it needs the row as it was, which not every source sends, and
     * hanging the recording on that would leave the whole index empty wherever the taking-back is off
     * rather than merely stale. An identity recorded for a row that has since gone is an entry pointing at
     * a document that is not there, which costs a wake-up that finds nothing.
     */
    private void register(Map<String, Object> row) {
        List<Object> referenced = NestKeys.valuesOf(row, lookup.referenceFields());
        List<Object> referrer = NestKeys.valuesOf(row, lookup.referrerIdentity());
        references.add(NestLookup.bucketKey(referenced, NestLookup.bucketOf(referrer)), referrer);
    }
}
