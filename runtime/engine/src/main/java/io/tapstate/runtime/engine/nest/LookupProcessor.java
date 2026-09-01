package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
 * <p>Nothing is emitted. A row landing here changes no document by itself: which documents refer to it is
 * not knowable from the row, and the documents that do are woken by what remembers that, not by this.
 */
final class LookupProcessor extends AbstractProcessor {

    private final NestLookup lookup;
    private final NestStore<Map<String, Object>> store;

    LookupProcessor(NestLookup lookup, NestStore<Map<String, Object>> store) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        Envelope event = (Envelope) item;
        Map<String, Object> row = NestKeys.rowOf(event);
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
}
