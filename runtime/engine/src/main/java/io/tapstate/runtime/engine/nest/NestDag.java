package io.tapstate.runtime.engine.nest;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.LevelBounds;
import io.tapstate.runtime.engine.PassthroughProcessor;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.ReplayFloorFactory;
import io.tapstate.runtime.engine.SettledPositions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Draws the vertices and edges one nest node compiles to. The shape was settled while compiling; this
 * only realises it, which is why nothing here decides anything about the tree.
 *
 * <p>Every edge into a vertex is partitioned by the same key and distributed, so the two kinds of thing
 * that meet on one key - the row that declares a mapping and the rows that ask about it - land on the
 * same member and the same partition without any vertex ever reaching across for state. The key is read
 * differently per edge: off a named field for rows arriving from a source, and off the routing key for
 * changes an upstream vertex already resolved.
 *
 * <p>Inbound ordinals are the compiled ones rather than the next free one, because the ordinal is how a
 * processor tells its own embed's rows from a child's. Where one alias resolves to several producers the
 * edge would have to fan in and the ordinal would stop being unique, so those are merged first and the
 * nest vertex still sees exactly one edge per stream.
 */
public final class NestDag {

    private NestDag() {
    }

    /**
     * Builds the node into {@code dag} and returns the vertex the rest of the pipeline reads from. A
     * passthrough nest builds one identity vertex fed by the root stream: it assembles nothing, so it
     * takes no state, no map and no thread of its own.
     */
    public static Vertex attach(DAG dag, NestTopology topology, String nodeId, String rootAlias,
            String outputStream, Function<String, List<Vertex>> upstream, NestBinding binding,
            ToIntFunction<Vertex> nextOutbound, NestFrontier frontier) {
        if (topology.isPassthrough()) {
            List<Vertex> sources = upstream.apply(rootAlias);
            Vertex passthrough = dag.newVertex(nodeId, gathering(frontier, rootAlias, sources.size()));
            int ordinal = 0;
            for (Vertex source : sources) {
                dag.edge(Edge.from(source, nextOutbound.applyAsInt(source)).to(passthrough, ordinal++));
            }
            return passthrough;
        }

        Map<List<String>, Vertex> built = new LinkedHashMap<>();
        Map<List<String>, List<String>> carried = new LinkedHashMap<>();
        Vertex assembler = null;
        for (NestVertex spec : topology.vertices()) {
            Vertex vertex = dag.newVertex(spec.name(), processorFor(spec, topology, binding, outputStream,
                    frontier, chainsInto(spec, carried, frontier)));
            built.put(spec.pathId(), vertex);
            for (NestInbound edge : spec.inbound()) {
                connect(dag, vertex, edge, built, upstream, nextOutbound, frontier);
            }
            assembler = vertex;
        }
        // Drawn after the assembler and never mistaken for it: a lookup takes no part in assembly, and the
        // vertex the rest of the pipeline reads from is the one that renders documents.
        for (NestLookup lookup : topology.lookups()) {
            attachLookup(dag, lookup, built, upstream, binding, nextOutbound, frontier);
        }
        return assembler;
    }

    /**
     * Draws the vertex that files away the rows one level points at, fed by that level's own stream and
     * partitioned by what identifies those rows. Partitioning by the same key the entries are filed under
     * is what makes one member the only writer of each of them, which is the premise the read from the
     * assembler rests on.
     *
     * <p>One outbound edge, back into the level doing the pointing. A row arriving here reaches no document
     * by arriving - which documents refer to it is not something the row says - so what leaves is worked out
     * from what was recorded about who points where, and is addressed by the pointing row's own identity so
     * it climbs to the document exactly as that row would. That is also the only edge over which the
     * pointed-at stream's chain reaches a document at all, which is what lets a bound on it ever be worked
     * out. It closes no cycle: everything that arrives here comes from a source, never from the assembler.
     *
     * <p>Two edges in, from two different streams. The rows themselves arrive on the first; the second
     * carries the rows of the level doing the pointing, delivered a second time and keyed by the row they
     * name rather than by their own identity, so that each of them is recorded here against what it points
     * at. <b>That second delivery leaves from the same source vertex the assembly one does.</b> Having the
     * vertex that assembles documents register them instead is the obvious reading and the one that cannot
     * be built: the changes to a pointed-at row have to reach that vertex, so an edge back into this one
     * closes a cycle, and Jet refuses the whole job at submission rather than the edge at drawing.
     */
    private static void attachLookup(DAG dag, NestLookup lookup, Map<List<String>, Vertex> built,
            Function<String, List<Vertex>> upstream, NestBinding binding,
            ToIntFunction<Vertex> nextOutbound, NestFrontier frontier) {
        List<Vertex> sources = upstream.apply(lookup.alias());
        if (sources == null || sources.isEmpty()) {
            throw new IllegalStateException("nest alias '" + lookup.alias() + "' resolved to no vertex");
        }
        Vertex vertex = dag.newVertex(lookup.name(), ProcessorMetaSupplier.of(
                new NestLookupSupplier(lookup, binding.stores(),
                        binding.settings().referrersAllowedIn(lookup.mapName()),
                        frontier == null ? null : frontier.axes(), chainsIntoLookup(lookup, frontier))));
        Vertex source = sources.size() == 1
                ? sources.get(0)
                : gatheredInto(dag, vertex, lookup.alias(), sources, nextOutbound, frontier);
        draw(dag, source, vertex, LookupProcessor.ROWS, fieldKey(lookup.partitionKey()), nextOutbound);

        List<Vertex> referrers = upstream.apply(lookup.referrerAlias());
        if (referrers == null || referrers.isEmpty()) {
            throw new IllegalStateException(
                    "nest alias '" + lookup.referrerAlias() + "' resolved to no vertex");
        }
        Vertex referrer = referrers.size() == 1
                ? referrers.get(0)
                : gatheredInto(dag, vertex, lookup.referrerAlias(), referrers, nextOutbound, frontier);
        draw(dag, referrer, vertex, LookupProcessor.REGISTRATIONS,
                fieldKey(lookup.referenceFields()), nextOutbound);
        if (lookup.referrerTracksKeyChanges()) {
            // The same rows a second time, keyed by what they pointed at before, so a row that now names
            // something else lands where the entry recording the old one is held. Only drawn where those
            // rows carry what they replace - without that there is nothing to key this copy by.
            draw(dag, referrer, vertex, LookupProcessor.DEPARTED_REGISTRATIONS,
                    leavingKey(lookup.referenceFields()), nextOutbound);
        }

        Vertex pointing = built.get(lookup.referrerPathId());
        if (pointing == null) {
            throw new IllegalStateException("nothing was built for the level pointing at "
                    + lookup.pathId() + ", so word of an edit has nowhere to go");
        }
        draw(dag, vertex, pointing, lookup.touchOrdinal(), routedKey(), nextOutbound);
    }

    /**
     * Which chains arrive on each edge into a lookup vertex. Only the rows' own edge is named: what it is
     * allowed to promise is about the stream it files, and the stream pointing at it is spoken for on the
     * path that stream takes to its own document. A vertex told to promise about both would be answering
     * twice for the second, into a level compiled to hear that answer once.
     */
    private static Map<Integer, List<String>> chainsIntoLookup(NestLookup lookup, NestFrontier frontier) {
        return frontier == null ? null
                : Map.of(LookupProcessor.ROWS, frontier.chainsOfAlias(lookup.alias()));
    }

    /** One passthrough gathering several producers of an alias, so the vertex below sees a single edge. */
    private static Vertex gatheredInto(DAG dag, Vertex destination, String alias, List<Vertex> sources,
            ToIntFunction<Vertex> nextOutbound, NestFrontier frontier) {
        Vertex merge = dag.newVertex(destination.getName() + ":" + alias,
                gathering(frontier, alias, sources.size()));
        int ordinal = 0;
        for (Vertex source : sources) {
            dag.edge(Edge.from(source, nextOutbound.applyAsInt(source)).to(merge, ordinal++));
        }
        return merge;
    }

    /**
     * Which chains arrive on each of {@code spec}'s inbound ordinals, and - recorded into {@code carried} -
     * which reach the vertex at all. An edge from a source carries whatever its alias reads; a cascading
     * edge carries everything its whole subtree does, which is why a vertex can only be worked out after
     * the ones feeding it and why the compiled order matters here as much as it does for drawing edges.
     *
     * <p>This is what a level waits on before it promises anything about a chain. It is taken from the
     * compiled tree rather than from what arrives, because an edge that has not spoken yet and one that
     * never carries the chain are indistinguishable at runtime, and treating the first as the second
     * promises changes that are still in flight.
     */
    private static Map<Integer, List<String>> chainsInto(NestVertex spec,
            Map<List<String>, List<String>> carried, NestFrontier frontier) {
        if (frontier == null) {
            return null;
        }
        Map<Integer, List<String>> byOrdinal = new LinkedHashMap<>();
        Set<String> reaching = new LinkedHashSet<>();
        for (NestInbound edge : spec.inbound()) {
            List<String> chains = edge.isCascade()
                    ? carried.get(edge.pathId())
                    : frontier.chainsOfAlias(edge.alias());
            if (chains == null) {
                throw new IllegalStateException("cascade into " + spec.name() + " knows no chain for "
                        + edge.pathId() + "; it is being wired before the vertex that feeds it");
            }
            byOrdinal.put(edge.ordinal(), chains);
            reaching.addAll(chains);
        }
        carried.put(spec.pathId(), List.copyOf(reaching));
        return byOrdinal;
    }

    private static void connect(DAG dag, Vertex destination, NestInbound edge, Map<List<String>, Vertex> built,
            Function<String, List<Vertex>> upstream, ToIntFunction<Vertex> nextOutbound,
            NestFrontier frontier) {
        if (edge.carriesTouches()) {
            // Drawn with the vertex that sends it, which does not exist yet: lookups are built after every
            // assembly vertex, so that the vertex a word of an edit lands on is already there to draw to.
            return;
        }
        if (edge.isCascade()) {
            Vertex source = built.get(edge.pathId());
            if (source == null) {
                throw new IllegalStateException("cascade into " + destination.getName()
                        + " has no vertex for " + edge.pathId());
            }
            draw(dag, source, destination, edge.ordinal(), routedKey(), nextOutbound);
            return;
        }
        List<Vertex> sources = upstream.apply(edge.alias());
        if (sources == null || sources.isEmpty()) {
            throw new IllegalStateException("nest alias '" + edge.alias() + "' resolved to no vertex");
        }
        Vertex source = sources.size() == 1
                ? sources.get(0)
                : merged(dag, destination, edge, sources, nextOutbound, frontier);
        draw(dag, source, destination, edge.ordinal(),
                edge.carriesDepartures() ? leavingKey(edge.keyFields()) : fieldKey(edge.keyFields()),
                nextOutbound);
    }

    /**
     * One passthrough that gathers several producers of one stream, so the nest vertex still sees a
     * single edge on the ordinal the compiler gave that stream.
     */
    private static Vertex merged(DAG dag, Vertex destination, NestInbound edge, List<Vertex> sources,
            ToIntFunction<Vertex> nextOutbound, NestFrontier frontier) {
        Vertex merge = dag.newVertex(destination.getName() + ":" + edge.alias(),
                gathering(frontier, edge.alias(), sources.size()));
        int ordinal = 0;
        for (Vertex source : sources) {
            dag.edge(Edge.from(source, nextOutbound.applyAsInt(source)).to(merge, ordinal++));
        }
        return merge;
    }

    /**
     * The vertex that gathers the {@code producers} of {@code alias} into one stream. It is where the
     * alias's several producers stop being several, and the only place that knows which chain arrives on
     * which of its edges - the level above it sees one edge and could never tell them apart.
     *
     * <p>A producer count that disagrees with the chains known for the alias means the graph is being
     * drawn from one reading and its frontier from another; the edges would then be told about chains
     * that arrive elsewhere, so it tears down rather than compiling a promise nobody can keep.
     */
    private static ProcessorMetaSupplier gathering(NestFrontier frontier, String alias, int producers) {
        if (frontier == null) {
            return PassthroughProcessor.metaSupplier();
        }
        List<List<String>> chains = frontier.chainsOfAliasByProducer().apply(alias);
        if (chains.size() != producers) {
            throw new IllegalStateException("alias '" + alias + "' is wired from " + producers
                    + " producers but its chains are known for " + chains.size());
        }
        Map<Integer, List<String>> byOrdinal = new LinkedHashMap<>();
        for (int ordinal = 0; ordinal < chains.size(); ordinal++) {
            byOrdinal.put(ordinal, chains.get(ordinal));
        }
        return PassthroughProcessor.metaSupplier(frontier.axes(), byOrdinal);
    }

    private static void draw(DAG dag, Vertex source, Vertex destination, int ordinal,
            FunctionEx<Object, Object> key, ToIntFunction<Vertex> nextOutbound) {
        dag.edge(Edge.from(source, nextOutbound.applyAsInt(source)).to(destination, ordinal)
                .partitioned(key).distributed());
    }

    private static ProcessorMetaSupplier processorFor(NestVertex spec, NestTopology topology,
            NestBinding binding, String outputStream, NestFrontier frontier,
            Map<Integer, List<String>> chainsByOrdinal) {
        NestBinding.NestStores stores = binding.stores();
        NestDeadLetter deadLetter = binding.deadLetter();
        List<EmbedSlot> slots = topology.slots();
        ChainAxes axes = frontier == null ? null : frontier.axes();
        // The window is named per namespace in the settings, which is where everything a deployment sets
        // about a nest is named. An append root takes no window at all, and no folding either.
        NestSendPolicy sending = topology.foldingAllowed()
                ? NestSendPolicy.within(binding.settings().sendWindowIn(spec.mapName()))
                : NestSendPolicy.everyChange();
        return ProcessorMetaSupplier.of(new NestVertexSupplier(spec, slots, stores, deadLetter, outputStream,
                axes, chainsByOrdinal, binding.replayFloor(), binding.settings(), binding.clock(), sending,
                topology.lookups()));
    }

    /**
     * Supplies the processors of one lookup vertex. Separate from the one that supplies assembly vertices
     * because it needs none of what that one binds: no replay floor, no dead letter, no parking, no clock -
     * a row is filed under its key and that is the whole of it.
     */
    private static final class NestLookupSupplier implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final NestLookup lookup;
        private final NestBinding.NestStores stores;

        /**
         * How many rows may point at one of these. Carried as the one number rather than as the settings it
         * was read out of: what is decided where the job is assembled has to travel to the member that
         * enforces it, and this vertex wants nothing else from them.
         */
        private final long referrersAllowed;

        private final ChainAxes axes;
        private final Map<Integer, List<String>> chainsByOrdinal;
        private transient NestBinding.NestStores bound;

        private NestLookupSupplier(NestLookup lookup, NestBinding.NestStores stores, long referrersAllowed,
                ChainAxes axes, Map<Integer, List<String>> chainsByOrdinal) {
            this.lookup = lookup;
            this.stores = stores;
            this.referrersAllowed = referrersAllowed;
            this.axes = axes;
            this.chainsByOrdinal = chainsByOrdinal;
        }

        @Override
        public void init(Context context) {
            bound = stores.bind(context.hazelcastInstance(), JetNestStateGauge::new);
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                // A level's bounds are worked out per instance, so each processor gets its own: sharing
                // one would combine what different partitions have seen into a single promise.
                processors.add(new LookupProcessor(lookup, bound.forLookup(lookup),
                        bound.forReferences(lookup), referrersAllowed, axes == null ? null
                                : new LevelBounds(chainsByOrdinal, axes, LevelBounds.HOLDS_NOTHING)));
            }
            return processors;
        }
    }

    /** Reads the key off the fields a row carries it in. */
    private static FunctionEx<Object, Object> fieldKey(List<String> fields) {
        return item -> NestKeys.valuesOf(NestKeys.rowOf((Envelope) item), fields);
    }

    /**
     * The key a row is leaving, so it arrives where the state it is leaving is held rather than where the
     * state it is joining is. A row carrying no earlier image is not leaving anywhere and takes the key it
     * already has, which lands it on the same partition as its twin - where the two keys being equal is
     * exactly what says there is no departure to make.
     */
    private static FunctionEx<Object, Object> leavingKey(List<String> fields) {
        return item -> {
            Envelope event = (Envelope) item;
            Map<String, Object> was = event.before();
            return NestKeys.valuesOf(was == null ? NestKeys.rowOf(event) : was, fields);
        };
    }

    /**
     * Reads the key an upstream vertex already resolved and routed by. Two things travel keyed that way and
     * both answer to this: a change climbing towards the document it belongs in, and word that a row one
     * level points at was edited, climbing the very same way. The second is why this asks what it is holding
     * rather than casting - a cascade carries whatever its level sends up, and one level's own rows are no
     * longer all of that.
     */
    private static FunctionEx<Object, Object> routedKey() {
        return item -> {
            if (item instanceof NestTouch word) {
                return word.key();
            }
            // Word that a chain got past some rows with nothing to deliver for them belongs to no document,
            // so there is no key it could be routed on and any one instance will do. A constant sends every
            // one of them to the same instance, which is what makes it a hand-off rather than a broadcast:
            // it is passed straight up to the sink, and a copy per instance would be a copy per instance of
            // the same claim. It is one word per drain of a lookup, so the instance it lands on is not
            // carrying a share of the stream's volume.
            if (item instanceof SettledPositions) {
                return SETTLED_POSITIONS_LANE;
            }
            return ((KeyedElement) item).key();
        };
    }

    /** The one key every {@link SettledPositions} is routed on; see {@link #routedKey()}. */
    private static final String SETTLED_POSITIONS_LANE = "settled-positions";

    /**
     * Supplies one nest vertex's processors on one member. It exists rather than a plain supplier because
     * the seam that reads back where a restart would resume is bound to a store that lives on the member
     * and cannot be serialized onto the graph: only the coordinates travel, and they are turned into the
     * real thing here, once, for every processor this member runs.
     *
     * <p>Both kinds go through it. Both keep something a deletion leaves behind - a root's record of being
     * deleted, a key's tombstoned mapping - and both drop it on the same terms, so neither is the one that
     * quietly did without.
     */
    private static final class NestVertexSupplier implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final NestVertex spec;
        private final List<EmbedSlot> slots;
        private final NestBinding.NestStores stores;
        private final NestDeadLetter deadLetter;
        private final String outputStream;
        private final ChainAxes axes;
        private final Map<Integer, List<String>> chainsByOrdinal;
        private final ReplayFloorFactory replayFloor;
        private final NestSettings settings;
        private final NestClock clock;
        private final NestSendPolicy sending;
        private final List<NestLookup> lookups;
        private transient ReplayFloor floor;
        private transient NestBinding.NestStores bound;
        private transient NestDeadLetter boundDeadLetter;

        private NestVertexSupplier(NestVertex spec, List<EmbedSlot> slots, NestBinding.NestStores stores,
                NestDeadLetter deadLetter, String outputStream, ChainAxes axes,
                Map<Integer, List<String>> chainsByOrdinal, ReplayFloorFactory replayFloor,
                NestSettings settings, NestClock clock, NestSendPolicy sending, List<NestLookup> lookups) {
            this.lookups = lookups;
            this.clock = clock;
            this.sending = sending;
            this.spec = spec;
            this.slots = slots;
            this.stores = stores;
            this.deadLetter = deadLetter;
            this.outputStream = outputStream;
            this.axes = axes;
            this.chainsByOrdinal = chainsByOrdinal;
            this.replayFloor = replayFloor;
            this.settings = settings;
        }

        @Override
        public void init(Context context) {
            floor = replayFloor.resolve(context.hazelcastInstance());
            // Metered from here and nowhere else: this is the one place a job is what the stores are
            // being bound for, and a reading can only be left from a thread running its processors.
            bound = stores.bind(context.hazelcastInstance(), JetNestStateGauge::new);
            // Bound for the same reason and at the same moment: somewhere to keep what cannot be assembled
            // is reached through a handle that does not travel with the graph.
            boundDeadLetter = deadLetter.bind(context.hazelcastInstance());
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(spec.isAssembler()
                        ? new AssemblerProcessor(spec, slots, bound.forAssembler(spec), outputStream,
                                axes, chainsByOrdinal, floor, settings, clock, sending,
                                bound.forParking(spec), boundDeadLetter, referenced())
                        : new ResolverProcessor(spec, bound.forResolver(spec), boundDeadLetter, axes,
                                chainsByOrdinal, floor, clock, settings, bound.forParking(spec)));
            }
            return processors;
        }

        /**
         * The stores holding the rows this tree's levels point at, by the namespace a slot names. Bound
         * here rather than reached for while rendering: what a distributed store is reached through does
         * not travel with the graph, and a render that looked one up per document would pay for it there.
         */
        private Map<String, NestStore<Map<String, Object>>> referenced() {
            Map<String, NestStore<Map<String, Object>>> byNamespace = new LinkedHashMap<>();
            for (NestLookup lookup : lookups) {
                byNamespace.put(lookup.mapName(), bound.forLookup(lookup));
            }
            return byNamespace;
        }
    }
}
