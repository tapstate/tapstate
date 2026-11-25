package io.tapstate.app;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An in-memory {@link ArtifactStore} for the assembly-layer tests: a batch upsert keyed by top-level id,
 * with the single-artifact {@code save} inherited from the port. Enough for the data-plane tests to seed a
 * pipeline and its referenced sources without a store backend.
 */
final class InMemoryArtifactStore implements ArtifactStore {

    private final Map<String, Resource> byId = new LinkedHashMap<>();
    /** Synchronized because members read one of these from their own threads, and a lost count lies. */
    private final Map<String, Integer> reads = Collections.synchronizedMap(new HashMap<>());

    @Override
    public synchronized void saveAll(List<Resource> artifacts) {
        for (Resource artifact : artifacts) {
            byId.put(artifact.id(), artifact);
        }
    }

    @Override
    public synchronized Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
        for (Map.Entry<String, String> expected : expectedContentHashes.entrySet()) {
            Resource stored = byId.get(expected.getKey());
            if (stored == null || !expected.getValue().equals(CanonicalHash.of(stored))) {
                return Optional.of(expected.getKey());
            }
        }
        saveAll(artifacts);
        return Optional.empty();
    }

    /**
     * Inserts only when the id is free, mirroring the backing store's atomic create. The distinction from
     * {@code saveAll} is the whole point for a caller that must not overwrite what somebody else declared,
     * so a double that upserted here would let such a caller pass while overwriting in production.
     */
    @Override
    public synchronized ArtifactMutation create(Resource artifact) {
        if (byId.containsKey(artifact.id())) {
            return ArtifactMutation.ALREADY_EXISTS;
        }
        byId.put(artifact.id(), artifact);
        return ArtifactMutation.CREATED;
    }

    @Override
    public synchronized ArtifactMutation replace(String id, String expectedContentHash, Resource replacement) {
        if (!id.equals(replacement.id())) throw new IllegalArgumentException("replacement id must equal the artifact id");
        Resource observed = byId.get(id);
        if (observed == null) return ArtifactMutation.NOT_FOUND;
        if (!CanonicalHash.of(observed).equals(expectedContentHash)) return ArtifactMutation.VERSION_CONFLICT;
        byId.put(id, replacement);
        return ArtifactMutation.REPLACED;
    }

    @Override
    public synchronized Optional<Resource> get(String id) {
        reads.merge(id, 1, Integer::sum);
        return Optional.ofNullable(byId.get(id));
    }

    /**
     * How many times each id has been read. Counted rather than left to a case to assert that it
     * "worked": reading a source once for every pipeline naming it and reading it once give the same
     * answer, and the difference only shows with many pipelines over few sources -- which is where it
     * costs.
     */
    Map<String, Integer> reads() {
        synchronized (reads) {
            return new HashMap<>(reads);
        }
    }

    @Override
    public synchronized List<Resource> list() {
        return List.copyOf(byId.values());
    }
}
