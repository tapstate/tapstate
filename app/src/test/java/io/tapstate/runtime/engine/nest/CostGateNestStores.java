package io.tapstate.runtime.engine.nest;

import io.tapstate.runtime.engine.StateStoreCostProbe;
import io.tapstate.runtime.engine.StateStoreCostStats;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Test-only cold stores that use the production state bridge and its completed codec observations. */
public final class CostGateNestStores implements NestBinding.NestStores, AutoCloseable {
    private static final long serialVersionUID = 1L;

    public record Costs(long loads, long batchLoads, long saves, long conditionalSaves,
            long deletes, long encodes, long decodes) { }

    private static final Map<String, State> STATES = new ConcurrentHashMap<>();
    private final String scope = UUID.randomUUID().toString();

    private static final class State {
        private final StateStoreCostStats costs = new StateStoreCostStats();
        private final Bytes bytes = new Bytes();
        private final Map<String, NestStore<?>> stores = new LinkedHashMap<>();
        private final Map<String, NestStateMapStore> bridges = new LinkedHashMap<>();
        private final Set<String> measured = new LinkedHashSet<>();
        private boolean redundantRead;
        private boolean redundantEncoding;
        private State(boolean redundantRead, boolean redundantEncoding) {
            this.redundantRead = redundantRead;
            this.redundantEncoding = redundantEncoding;
        }
    }

    public CostGateNestStores(boolean redundantRead, boolean redundantEncoding) {
        STATES.put(scope, new State(redundantRead, redundantEncoding));
    }

    private static State state(String scope) {
        State current = STATES.get(scope);
        if (current == null) { throw new IllegalStateException("cold cost fixture was accessed after its scope ended"); }
        return current;
    }

    @Override public void close() { STATES.remove(scope); }

    @Override public NestStore<ResolverState> forResolver(NestVertex vertex) { return store(vertex.mapName()); }
    @Override public NestStore<RootAssembly> forAssembler(NestVertex vertex) { return store(vertex.mapName()); }
    @Override public NestStore<ParkedSubtree> forParking(NestVertex vertex) { return store(vertex.parkingMapName()); }
    @Override public NestStore<Map<String, Object>> forLookup(NestLookup lookup) { return store(lookup.mapName()); }
    @Override public NestStore<Set<Object>> forReferences(NestLookup lookup) { return store(lookup.referencesMapName()); }

    @SuppressWarnings("unchecked")
    private <S> NestStore<S> store(String namespace) {
        State current = state(scope);
        current.measured.add(namespace);
        return (NestStore<S>) current.stores.computeIfAbsent(namespace, key -> new Cold<>(scope, namespace));
    }

    public Costs snapshot() {
        long encodes = 0;
        long decodes = 0;
        State current = state(scope);
        for (String namespace : current.measured) {
            var reading = current.costs.reading(namespace);
            if (reading.isPresent()) {
                var encoded = reading.orElseThrow().codecs().get(StateStoreCostProbe.Codec.ENCODE);
                var decoded = reading.orElseThrow().codecs().get(StateStoreCostProbe.Codec.DECODE);
                encodes += encoded == null ? 0 : encoded.completed();
                decodes += decoded == null ? 0 : decoded.completed();
            }
        }
        Bytes bytes = current.bytes;
        return new Costs(bytes.loads, bytes.batchLoads, bytes.saves, bytes.conditionalSaves, bytes.deletes, encodes, decodes);
    }

    private static final class Cold<S> implements NestStore<S> {
        private static final long serialVersionUID = 1L;
        private final String scope;
        private final String namespace;

        private Cold(String scope, String namespace) {
            this.scope = scope;
            this.namespace = namespace;
        }

        private NestStateMapStore bridge() {
            State current = state(scope);
            return current.bridges.computeIfAbsent(namespace,
                    key -> new NestStateMapStore(namespace, current.bytes, current.costs));
        }

        @Override @SuppressWarnings("unchecked") public S load(Object key) {
            S loaded = (S) bridge().load(key);
            State current = state(scope);
            if (current.redundantRead) {
                current.redundantRead = false;
                // The repeated read executes the real bridge and underlying SPI, then discards its result.
                bridge().load(key);
            }
            return loaded;
        }

        @Override @SuppressWarnings("unchecked") public Map<Object, S> loadAll(Collection<Object> keys) {
            return (Map<Object, S>) (Map<?, ?>) bridge().loadAll(keys);
        }

        @Override public void save(Object key, S state) {
            bridge().store(key, state);
            State current = state(scope);
            if (current.redundantEncoding) {
                current.redundantEncoding = false;
                String discarded = namespace + ".discarded-encoding";
                current.measured.add(discarded);
                // Encode the same real state again through the production bridge. No extra cold write
                // is charged: the deliberately unused bytes are discarded at this separate SPI boundary.
                new NestStateMapStore(discarded, DISCARD, current.costs).store(key, state);
            }
        }

        @Override public void remove(Object key) { bridge().delete(key); }
        @Override public long count() { return state(scope).bytes.count(namespace); }
    }

    private static final KeyedStateStore DISCARD = new KeyedStateStore() {
        @Override public Optional<byte[]> load(String namespace, String key) { return Optional.empty(); }
        @Override public void save(String namespace, String key, byte[] state) { }
        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) { return Optional.empty(); }
        @Override public void delete(String namespace, String key) { }
        @Override public void dropNamespace(String namespace) { }
        @Override public long count(String namespace) { return 0; }
    };

    private static final class Bytes implements KeyedStateStore {
        private final Map<String, Map<String, byte[]>> byNamespace = new LinkedHashMap<>();
        private long loads;
        private long batchLoads;
        private long saves;
        private long conditionalSaves;
        private long deletes;

        @Override public Optional<byte[]> load(String namespace, String key) {
            loads++;
            return Optional.ofNullable(byNamespace.getOrDefault(namespace, Map.of()).get(key));
        }

        @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            batchLoads++;
            Map<String, byte[]> found = new LinkedHashMap<>();
            Map<String, byte[]> values = byNamespace.getOrDefault(namespace, Map.of());
            for (String key : keys) {
                if (values.containsKey(key)) {
                    found.put(key, values.get(key));
                }
            }
            return found;
        }

        @Override public void save(String namespace, String key, byte[] state) {
            saves++;
            byNamespace.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>()).put(key, state);
        }

        @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            conditionalSaves++;
            byte[] prior = byNamespace.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>()).putIfAbsent(key, state);
            return Optional.ofNullable(prior);
        }

        @Override public void delete(String namespace, String key) {
            deletes++;
            Map<String, byte[]> values = byNamespace.get(namespace);
            if (values != null) {
                values.remove(key);
            }
        }

        @Override public void dropNamespace(String namespace) { byNamespace.remove(namespace); }
        @Override public long count(String namespace) { return byNamespace.getOrDefault(namespace, Map.of()).size(); }
    }
}
