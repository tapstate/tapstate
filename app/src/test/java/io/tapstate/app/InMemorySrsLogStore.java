package io.tapstate.app;

import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogStore;

import java.util.List;
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

    @Override
    public void store(String ring, long seq, SrsLogRecord record) {
        rings.computeIfAbsent(ring, name -> new ConcurrentSkipListMap<>()).put(seq, record);
    }

    @Override
    public void storeAll(String ring, long firstSeq, List<SrsLogRecord> records) {
        long seq = firstSeq;
        for (SrsLogRecord record : records) {
            store(ring, seq++, record);
        }
    }

    @Override
    public Optional<SrsLogRecord> load(String ring, long seq) {
        NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
        return entries == null ? Optional.empty() : Optional.ofNullable(entries.get(seq));
    }

    @Override
    public long largestSequence(String ring) {
        NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
        return entries == null || entries.isEmpty() ? -1L : entries.lastKey();
    }

    @Override
    public void trim(String ring, long throughSeq) {
        NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
        if (entries != null) {
            entries.headMap(throughSeq, true).clear();
        }
    }
}
