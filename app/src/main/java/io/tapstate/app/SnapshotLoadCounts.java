package io.tapstate.app;

import io.tapstate.spi.store.KeyedStateStore;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

/** The rows one pipeline actually read for each table of a chain-backed initial load. */
final class SnapshotLoadCounts {

    private static final String NAMESPACE_PREFIX = "snapshot.load.counts.";

    private SnapshotLoadCounts() {
    }

    static String namespaceOf(String pipelineId) {
        return NAMESPACE_PREFIX + pipelineId;
    }

    static void save(KeyedStateStore store, String pipelineId, String chainId, String table, long rows) {
        if (rows < 0L) {
            throw new IllegalArgumentException("snapshot rows must not be negative");
        }
        store.save(namespaceOf(pipelineId), key(chainId, table),
                Long.toString(rows).getBytes(StandardCharsets.US_ASCII));
    }

    static OptionalLong read(KeyedStateStore store, String pipelineId, String chainId, String table) {
        return store.load(namespaceOf(pipelineId), key(chainId, table))
                .map(bytes -> Long.parseLong(new String(bytes, StandardCharsets.US_ASCII)))
                .map(value -> {
                    if (value < 0L) {
                        throw new IllegalStateException("stored snapshot rows must not be negative");
                    }
                    return OptionalLong.of(value);
                })
                .orElseGet(OptionalLong::empty);
    }

    private static String key(String chainId, String table) {
        return chainId.length() + ":" + chainId + table;
    }
}
