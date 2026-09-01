package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nest stores that keep everything on the heap of the member running the vertex, for cases whose subject
 * is not durability.
 *
 * <p>It sits in test sources on purpose. A running system has nowhere to get this shape from: the member
 * declares state maps only where there is a store behind them, so the compiler - not a convention - is
 * what keeps a pipeline off state that dies with the process.
 *
 * <p><b>What is shared between vertices is held in a registry outside the factory, not in a field of
 * it.</b> A job carries one supplier per vertex and each is serialized on its own, so two vertices holding
 * "the same" factory arrive on the member as two copies of it - and anything kept in a field is then two
 * maps that cannot see each other. That is invisible where a store is written and read by one vertex, and
 * it is exactly the case where one vertex writes and another reads: every read finds nothing, every
 * document renders without the field, and nothing fails. A distributed map has no such seam - it is
 * reached by name from wherever - so this is the double catching up with what it stands in for.
 */
public final class HeapNestStores {

    /** Stores shared across the vertices of one binding, keyed by that binding and the namespace. */
    private static final Map<String, NestStore<?>> SHARED = new ConcurrentHashMap<>();

    private HeapNestStores() {
    }

    /** Stores that keep everything on the heap of the member running the vertex, and outlive nothing. */
    public static NestBinding.NestStores onHeap() {
        // One per binding rather than one per process: two cases running in one JVM would otherwise read
        // each other's entries wherever they happened to name their pipelines alike.
        return new SharedHeapStores(UUID.randomUUID().toString());
    }

    private static final class SharedHeapStores implements NestBinding.NestStores {

        private static final long serialVersionUID = 1L;

        private final String binding;

        private SharedHeapStores(String binding) {
            this.binding = binding;
        }

        @Override
        public NestBinding.NestStores bind(HazelcastInstance member) {
            return this;
        }

        /** Fresh per ask: a resolver reads and writes only its own keys, so nothing depends on sharing. */
        @Override
        public NestStore<ResolverState> forResolver(NestVertex vertex) {
            return new HeapNestStore<>();
        }

        @Override
        public NestStore<RootAssembly> forAssembler(NestVertex vertex) {
            return new HeapNestStore<>();
        }

        /**
         * Shared per name, unlike the two above. A parked subtree is written by whichever processor held
         * the document it left and read by whichever holds the one gaining it, so a fresh store per ask
         * would mean the hand-over never arrives - and the map behind this in a running system is one map,
         * reached by name from anywhere.
         */
        @Override
        public NestStore<ParkedSubtree> forParking(NestVertex vertex) {
            return shared("parking|" + vertex.parkingMapName());
        }

        /**
         * Shared for the same reason and more strictly: the vertex carrying the stream writes these and the
         * assembler reads them, so they are never once touched by the same processor.
         */
        @Override
        public NestStore<Map<String, Object>> forLookup(NestLookup lookup) {
            return shared("lookup|" + lookup.mapName());
        }

        @SuppressWarnings("unchecked")
        private <S> NestStore<S> shared(String namespace) {
            return (NestStore<S>) SHARED.computeIfAbsent(binding + "|" + namespace,
                    name -> new HeapNestStore<>());
        }
    }
}
