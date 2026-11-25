package io.tapstate.app;

import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsLogBatch;
import io.tapstate.spi.store.SrsLogStore;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The change log held in memory, for a test that needs the log to answer rather than to persist. It
 * keeps what the real one keeps -- one record per (ring, sequence) -- and answers the two questions that
 * are not exact lookups off the same ordered map the real store answers them off an index.
 */
final class InMemorySrsLogStore implements SrsLogStore {

    private final Map<String, NavigableMap<Long, SrsLogRecord>> rings = new ConcurrentHashMap<>();
    private final Map<String, SrsLogBounds> bounds = new ConcurrentHashMap<>();

    @Override
    public synchronized void store(String ring, long seq, SrsLogRecord record) {
        rings.computeIfAbsent(ring, name -> new ConcurrentSkipListMap<>()).put(seq, record);
        bounds.compute(ring, (name, prior) -> new SrsLogBounds(
                Math.max(seq, prior == null ? -1 : prior.largestSequence()),
                prior == null ? -1 : prior.trimmedThrough()));
    }

    @Override
    public synchronized void storeAll(String ring, long firstSeq, List<SrsLogRecord> records) {
        long seq = firstSeq;
        for (SrsLogRecord record : records) {
            store(ring, seq++, record);
        }
    }

    @Override
    public synchronized Optional<SrsLogRecord> load(String ring, long seq) {
        NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
        return entries == null ? Optional.empty() : Optional.ofNullable(entries.get(seq));
    }

    @Override
    public synchronized long largestSequence(String ring) {
        return bounds(ring).largestSequence();
    }

    @Override
    public synchronized SrsLogBounds bounds(String ring) {
        return bounds.getOrDefault(ring, new SrsLogBounds(-1L, -1L));
    }

    @Override
    public synchronized SrsLogBatch readBatch(String ring, long firstSeq, int maxSize) {
        if (firstSeq < 0 || maxSize < 1) {
            throw new IllegalArgumentException("an SRS log read requires a non-negative sequence and positive size");
        }
        SrsLogBounds observed = bounds(ring);
        Map<Long, SrsLogRecord> records = new LinkedHashMap<>();
        NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
        if (entries != null) {
            long lastSeq = maxSize - 1L > Long.MAX_VALUE - firstSeq ? Long.MAX_VALUE : firstSeq + maxSize - 1L;
            records.putAll(entries.subMap(firstSeq, true, lastSeq, true));
        }
        return new SrsLogBatch(observed, records);
    }

    @Override
    public synchronized void trim(String ring, long throughSeq) {
        SrsLogBounds after = bounds.compute(ring, (name, prior) -> {
            SrsLogBounds current = prior == null ? new SrsLogBounds(-1L, -1L) : prior;
            return new SrsLogBounds(current.largestSequence(), Math.max(current.trimmedThrough(),
                    Math.min(throughSeq, current.largestSequence())));
        });
        NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
        if (entries != null) {
            entries.headMap(after.trimmedThrough(), true).clear();
        }
    }
}
