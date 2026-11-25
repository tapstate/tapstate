package io.tapstate.spi.store;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A bounded durable read and the log bounds it observed. Records retain their exact stored sequence keys,
 * including any holes, and iterate in ascending sequence order. Neither a missing record nor an absent
 * original capture generation is replaced with an invented value.
 */
public record SrsLogBatch(SrsLogBounds bounds, Map<Long, SrsLogRecord> records) {

    public SrsLogBatch {
        Objects.requireNonNull(bounds, "bounds");
        Objects.requireNonNull(records, "records");
        TreeMap<Long, SrsLogRecord> ordered = new TreeMap<>();
        records.forEach((sequence, record) -> ordered.put(
                Objects.requireNonNull(sequence, "sequence"), Objects.requireNonNull(record, "record")));
        records = Collections.unmodifiableMap(new LinkedHashMap<>(ordered));
    }
}
