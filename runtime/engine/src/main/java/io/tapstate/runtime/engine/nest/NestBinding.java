package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.runtime.engine.ReplayFloorFactory;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * What the assembly root supplies for a nest node: where each embedded stream's table key comes from,
 * where each vertex keeps its state, where changes that can never reach a document go, and how to read
 * back where a restart would resume.
 *
 * <p>These are things the engine deliberately does not decide. Resolving an alias to a table would
 * mean knowing the table universe; choosing a store would mean choosing between a heap and a disk;
 * choosing what happens to an unassemblable change is a product question, not a graph one; and the durable
 * position a restart resumes from lives in a store the engine ring is not allowed to reach for itself.
 *
 * <p>{@code tables} and {@code ledger} are asked while the graph is built and stay behind; the other three
 * are carried to the member that runs each vertex, which is why they are serializable.
 */
public record NestBinding(
        Function<String, NestTable> tables,
        NestStores stores,
        NestDeadLetter deadLetter,
        ReplayFloorFactory replayFloor,
        NestStateLedger ledger,
        NestSettings settings,
        NestClock clock) implements Serializable {

    /** A binding on the system clock, which is what everything but a test runs on. */
    public NestBinding(Function<String, NestTable> tables, NestStores stores, NestDeadLetter deadLetter,
            ReplayFloorFactory replayFloor, NestStateLedger ledger, NestSettings settings) {
        this(tables, stores, deadLetter, replayFloor, ledger, settings, NestClock.SYSTEM);
    }

    /**
     * A binding with nothing to read a durable resume position through, for a job assembled without one.
     * Deleted roots then keep their record for as long as the job runs, which costs memory and never
     * correctness - forgetting one is an optimisation, and not taking it is always allowed.
     */
    public NestBinding(Function<String, NestTable> tables, NestStores stores, NestDeadLetter deadLetter) {
        this(tables, stores, deadLetter, ReplayFloorFactory.NONE);
    }

    /**
     * A binding that remembers nothing about where this nest last kept its state, and so compares nothing
     * on the way up. It is the right default only where the state does not outlive the job: a heap-held
     * nest starts empty every time, and there is nothing a renamed path could abandon.
     */
    public NestBinding(Function<String, NestTable> tables, NestStores stores, NestDeadLetter deadLetter,
            ReplayFloorFactory replayFloor) {
        this(tables, stores, deadLetter, replayFloor, NestStateLedger.NONE);
    }

    /** A binding whose levels are held to {@code settings} and which reads back no resume position. */
    public NestBinding(Function<String, NestTable> tables, NestStores stores, NestDeadLetter deadLetter,
            NestSettings settings) {
        this(tables, stores, deadLetter, ReplayFloorFactory.NONE, NestStateLedger.NONE, settings);
    }

    /**
     * A binding on the default limits, which is every level held to the same number. Right where the
     * capacity of one level against another has not been thought about yet, and wrong as soon as it has:
     * levels of one tree differ in what they hold by orders of magnitude.
     */
    public NestBinding(Function<String, NestTable> tables, NestStores stores, NestDeadLetter deadLetter,
            ReplayFloorFactory replayFloor, NestStateLedger ledger) {
        this(tables, stores, deadLetter, replayFloor, ledger, NestSettings.defaults());
    }

    /** Where each kind of nest vertex keeps its state. */
    public interface NestStores extends Serializable {

        /**
         * Binds this factory to the member whose vertices it is about to serve, returning what to ask from
         * there on. It exists because the factory is serialized when the job is submitted while the thing a
         * distributed store is reached through is not: only coordinates travel, and the live handle is
         * picked up once the vertex is where it will run. A factory that needs nothing from the member
         * answers itself, which is why the default is to do so.
         */
        default NestStores bind(HazelcastInstance member) {
            return this;
        }

        /** The store for one resolver's mappings and the children waiting in it. */
        NestStore<ResolverState> forResolver(NestVertex vertex);

        /** The store for the assembler's documents. */
        NestStore<RootAssembly> forAssembler(NestVertex vertex);

        /**
         * The store for the rows a level points at, one entry per row and keyed by what identifies it.
         * Written by the vertex carrying that stream and read by the assembler rendering the documents
         * that refer to it, which is the one place a nest reaches outside its own partition - so it is a
         * store like the others rather than anything the reading side keeps for itself.
         *
         * <p>Defaulted to a refusal rather than to an empty store. A binding with nowhere to keep these
         * cannot serve a tree that points at anything, and a store that quietly kept nothing would render
         * every such document with the field missing - which is precisely the failure this whole shape
         * exists to end, and it would look like ordinary absent data.
         */
        default NestStore<Map<String, Object>> forLookup(NestLookup lookup) {
            throw new IllegalStateException(
                    "these nest stores have nowhere to keep the rows " + lookup.name() + " points at");
        }

        /**
         * The store for which identities point at each of the rows a level points at, spread over a fixed
         * number of buckets per row so that no one entry grows with the fanout. Written by the same vertex
         * that files the rows themselves, and by nothing else: both are partitioned by the identity of the
         * row being pointed at, so one member owns everything ever said about that row.
         *
         * <p>Defaulted to a refusal for the same reason the one above is. A binding with nowhere to keep
         * this cannot serve a tree that points at anything, and a store quietly keeping nothing would
         * leave a row that nothing appeared to point at - so a change to it would reach no document, and
         * every document would still look right until one of them was changed.
         */
        default NestStore<Set<Object>> forReferences(NestLookup lookup) {
            throw new IllegalStateException("these nest stores have nowhere to keep what points at the rows "
                    + lookup.name() + " holds");
        }

        /**
         * The store for subtrees between documents. Separate from the documents themselves because what is
         * kept there is not one: it belongs to no key of this vertex until the document gaining it takes it,
         * and mixing the two would put entries under keys the sweeps here walk.
         */
        NestStore<ParkedSubtree> forParking(NestVertex vertex);

        /**
         * Binds as above, with somewhere for the stores to leave their namespaces' readings. Only the
         * supplier building a vertex's processors calls this, because only there is it known that a job is
         * what these are for; every other caller gets stores that report nothing, which is the honest
         * answer where there is no run for a reading to belong to.
         *
         * <p>A new gauge per store, never one shared between them. A vertex runs as several processors on
         * one member and each is a thread of its own, so a gauge holding the handles it has already looked
         * up would be a plain map written from all of them at once - measured, that takes the whole job
         * down mid-run rather than merely losing a reading.
         */
        default NestStores bind(HazelcastInstance member, Supplier<NestStateGauge> gauges) {
            return bind(member);
        }
    }

    /**
     * Stores backed by one distributed map per vertex, the map named by whatever name the topology computed
     * for that vertex. That name has until now been derived and never consulted; here is where it starts to
     * decide which entries a vertex addresses, and so where two vertices stop being able to answer each
     * other's keys.
     *
     * <p>The returned factory carries no member and must be {@link NestStores#bind bound} to one before it
     * is asked for a store. Asking too early is a wiring mistake rather than anything a user did, so it
     * fails on the spot rather than degrading to a store that quietly keeps nothing.
     */
    public static NestStores onMap() {
        return new MapNestStores(null);
    }

    /** The factory behind {@link #onMap()}: coordinates while it travels, a live handle once it lands. */
    private static final class MapNestStores implements NestStores {

        private static final long serialVersionUID = 1L;

        private final transient HazelcastInstance member;
        private final transient Supplier<NestStateGauge> gauges;

        private MapNestStores(HazelcastInstance member) {
            this(member, () -> NestStateGauge.NONE);
        }

        private MapNestStores(HazelcastInstance member, Supplier<NestStateGauge> gauges) {
            this.member = member;
            this.gauges = gauges;
        }

        @Override
        public NestStores bind(HazelcastInstance member) {
            return new MapNestStores(Objects.requireNonNull(member, "member"));
        }

        @Override
        public NestStores bind(HazelcastInstance member, Supplier<NestStateGauge> gauges) {
            return new MapNestStores(Objects.requireNonNull(member, "member"),
                    Objects.requireNonNull(gauges, "gauges"));
        }

        @Override
        public NestStore<ResolverState> forResolver(NestVertex vertex) {
            return metered(mapOf(vertex));
        }

        @Override
        public NestStore<RootAssembly> forAssembler(NestVertex vertex) {
            return metered(mapOf(vertex));
        }

        /**
         * A store that leaves its namespace's readings where the run's own statistics are collected. The
         * gauge is chosen here rather than inside the store because here is where it is known that a job is
         * what this is being built for: a store built anywhere else has no job statistics to leave any in,
         * and asking for them there fails rather than quietly recording nothing.
         */
        private <S> NestStore<S> metered(IMap<Object, S> map) {
            NestStateGauge gauge = gauges.get();
            return gauge == NestStateGauge.NONE
                    ? new MapNestStore<>(map)
                    : new MapNestStore<>(map, NestStateStats.of(member), gauge);
        }

        @Override
        public NestStore<ParkedSubtree> forParking(NestVertex vertex) {
            return metered(mapNamed(vertex.name(), vertex.parkingMapName()));
        }

        @Override
        public NestStore<Map<String, Object>> forLookup(NestLookup lookup) {
            return metered(mapNamed(lookup.name(), lookup.mapName()));
        }

        @Override
        public NestStore<Set<Object>> forReferences(NestLookup lookup) {
            return metered(mapNamed(lookup.name(), lookup.referencesMapName()));
        }

        private <S> IMap<Object, S> mapOf(NestVertex vertex) {
            return mapNamed(vertex.name(), vertex.mapName());
        }

        private <S> IMap<Object, S> mapNamed(String askedFor, String name) {
            if (member == null) {
                throw new IllegalStateException(
                        "nest stores were asked for " + askedFor + " before being bound to a member");
            }
            return member.getMap(name);
        }
    }
}
