package io.tapstate.app;

import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * An in-memory {@link SchemaStore} for the assembly-layer tests: a latest-only upsert keyed by connection id.
 * Enough to seed a source's discovered model so the target-model resolution has a model to read, without a
 * store backend.
 */
final class InMemorySchemaStore implements SchemaStore {

    private final Map<String, DiscoveredSourceModel> byConnection = new LinkedHashMap<>();
    private int reads;

    @Override
    public void save(DiscoveredSourceModel discovered) {
        byConnection.put(discovered.connectionId(), discovered);
    }

    @Override
    public Optional<DiscoveredSourceModel> get(String connectionId) {
        reads++;
        return Optional.ofNullable(byConnection.get(connectionId));
    }

    /**
     * How many times anything has read a connection's discovery. Counted rather than left to a case to
     * assert it "worked": a resolution that reads the whole model once per table and one that reads it
     * once produce the same answer, and the difference only shows on a source with many tables - which
     * is where it costs.
     */
    int reads() {
        return reads;
    }
}
