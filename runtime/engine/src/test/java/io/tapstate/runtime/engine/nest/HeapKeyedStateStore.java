package io.tapstate.runtime.engine.nest;

import io.tapstate.spi.store.KeyedStateStore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** A store that keeps what it is given, so an evicted entry has somewhere to have gone. */
final class HeapKeyedStateStore implements KeyedStateStore {

    private final Map<String, Map<String, byte[]>> byNamespace = new LinkedHashMap<>();

    @Override
    public Optional<byte[]> load(String namespace, String key) {
        Map<String, byte[]> entries = byNamespace.get(namespace);
        return entries == null ? Optional.empty() : Optional.ofNullable(entries.get(key));
    }

    @Override
    public void save(String namespace, String key, byte[] state) {
        byNamespace.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>()).put(key, state);
    }

    @Override
    public void delete(String namespace, String key) {
        Map<String, byte[]> entries = byNamespace.get(namespace);
        if (entries != null) {
            entries.remove(key);
        }
    }

    @Override
    public void dropNamespace(String namespace) {
        byNamespace.remove(namespace);
    }

    @Override
    public long count(String namespace) {
        return byNamespace.getOrDefault(namespace, Map.of()).size();
    }
}
