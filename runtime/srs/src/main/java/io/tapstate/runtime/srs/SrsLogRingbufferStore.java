package io.tapstate.runtime.srs;

import com.hazelcast.ringbuffer.RingbufferStore;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The hook the change ring calls to write a change down, bound to one ring. It carries no logic of its
 * own beyond naming the ring and converting between the ring's item and the log's record: the store
 * behind it decides how a change is persisted, and the ring decides when.
 *
 * <p><strong>The ring calls this before it admits the change</strong>, on both of its write paths, and a
 * throw here keeps the change out. That ordering is what lets "in the ring" mean "already written down"
 * with no reconciliation pass of our own -- and it is the ring's own guarantee, not one this class
 * arranges.
 *
 * <p>The position crosses as its opaque token. The item holds it as a source position; the log holds the
 * token alone, because a record outlives the process that wrote it and only the connector that issued
 * the offset can interpret it.
 *
 * <p><strong>Typed over {@code Object}, and it has to be.</strong> The ring's batch path builds its own
 * array and hands it over as the store's element type, but the array it built is not of that type -- so a
 * store declared over the item type has the compiler insert a cast that fails on every batch write, while
 * the single-item path works. Measured against Hazelcast 5.7.0: a batch of three items reaches a
 * {@code RingbufferStore<SrsItem>} as an array whose runtime component type is not {@code SrsItem}, and
 * the call dies inside the ring rather than here. Declaring the element type as {@code Object} and
 * casting each item is what makes the batch path usable at all, and the batch path is the whole point:
 * the cost of writing a change down is per call, not per byte.
 */
final class SrsLogRingbufferStore implements RingbufferStore<Object> {

    private final SrsLogStore log;
    private final String ring;

    SrsLogRingbufferStore(SrsLogStore log, String ring) {
        this.log = Objects.requireNonNull(log, "log");
        this.ring = Objects.requireNonNull(ring, "ring");
    }

    @Override
    public void store(long sequence, Object data) {
        log.store(ring, sequence, toRecord((SrsItem) data));
    }

    @Override
    public void storeAll(long firstItemSequence, Object[] items) {
        List<SrsLogRecord> records = new ArrayList<>(items.length);
        for (Object item : items) {
            records.add(toRecord((SrsItem) item));
        }
        log.storeAll(ring, firstItemSequence, records);
    }

    @Override
    public Object load(long sequence) {
        return log.load(ring, sequence).map(SrsLogRingbufferStore::toItem).orElse(null);
    }

    @Override
    public long getLargestSequence() {
        return log.largestSequence(ring);
    }

    private static SrsLogRecord toRecord(SrsItem item) {
        SourcePosition position = item.srcPos();
        return new SrsLogRecord(
                position == null ? null : position.token(),
                item.op(),
                item.ts(),
                item.before(),
                item.after(),
                item.schemaVer());
    }

    private static SrsItem toItem(SrsLogRecord record) {
        return new SrsItem(
                record.srcToken() == null ? null : new SourcePosition(record.srcToken()),
                record.op(),
                record.ts(),
                record.before(),
                record.after(),
                record.schemaVer());
    }
}
