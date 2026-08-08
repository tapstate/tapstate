package io.tapstate.app;

import io.tapstate.spi.store.KeyedStateStore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory {@link KeyedStateStore} for the assembly-layer tests: what a stateful operator would read
 * back from, without a database behind it. It keeps namespaces apart the same way a real one does, which
 * is the property the tests using it actually rely on.
 */
final class InMemoryKeyedStateStore implements KeyedStateStore {

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
