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
import io.tapstate.runtime.engine.PassthroughProcessor;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.ReplayFloorFactory;

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
            attachLookup(dag, lookup, upstream, binding, nextOutbound, frontier);
        }
        return assembler;
    }

    /**
     * Draws the vertex that files away the rows one level points at, fed by that level's own stream and
     * partitioned by what identifies those rows. Partitioning by the same key the entries are filed under
     * is what makes one member the only writer of each of them, which is the premise the read from the
     * assembler rests on.
     *
     * <p>It has no outbound edge, because a row arriving here reaches no document by arriving. Which
     * documents refer to it is not something the row says.
     */
    private static void attachLookup(DAG dag, NestLookup lookup, Function<String, List<Vertex>> upstream,
            NestBinding binding, ToIntFunction<Vertex> nextOutbound, NestFrontier frontier) {
        List<Vertex> sources = upstream.apply(lookup.alias());
        if (sources == null || sources.isEmpty()) {
            throw new IllegalStateException("nest alias '" + lookup.alias() + "' resolved to no vertex");
        }
        Vertex vertex = dag.newVertex(lookup.name(),
                ProcessorMetaSupplier.of(new NestLookupSupplier(lookup, binding.stores())));
        Vertex source = sources.size() == 1
                ? sources.get(0)
                : gatheredInto(dag, vertex, lookup.alias(), sources, nextOutbound, frontier);
        draw(dag, source, vertex, 0, fieldKey(lookup.partitionKey()), nextOutbound);
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
        private transient NestBinding.NestStores bound;

        private NestLookupSupplier(NestLookup lookup, NestBinding.NestStores stores) {
            this.lookup = lookup;
            this.stores = stores;
        }

        @Override
        public void init(Context context) {
            bound = stores.bind(context.hazelcastInstance(), JetNestStateGauge::new);
        }

        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(new LookupProcessor(lookup, bound.forLookup(lookup)));
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

    /** Reads the key an upstream vertex already resolved and routed by. */
    private static FunctionEx<Object, Object> routedKey() {
        return item -> ((KeyedElement) item).key();
    }

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
