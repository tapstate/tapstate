package io.tapstate.runtime.engine;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestDag;
import io.tapstate.runtime.engine.nest.NestFrontier;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateLedger;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.SinkWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Compiles one pipeline into one Jet DAG: source vertices to a linear transform chain to serve
 * sink vertices, wired by explicit edges. The builder is topology only - it submits nothing and
 * re-judges nothing - so a caller can assert the graph shape without running a job.
 */
public final class PipelineDagBuilder {

    /**
     * The name prefix every serve-sink vertex carries. The engine picks serve sinks out of a job's
     * metrics by this prefix to sum the records that reached them, so the prefix is a shared contract
     * between the builder that stamps it and the engine that reads it.
     */
    static final String SERVE_VERTEX_PREFIX = "serve.";

    private PipelineDagBuilder() {
    }

    /**
     * Every namespace this pipeline's nests keep state in, empty for a pipeline that has none.
     *
     * <p>Answered by compiling the tree rather than by reading back anything a run left behind: the run
     * that writes the state takes the names from the compiled tree, so compiling it is asking the same
     * question of the same source. The pipeline handed in must be the one the run is built from — compiled
     * from a later revision this names where the <em>next</em> run would write, not where this one does —
     * which is why a run is asked as it starts and what it answers is written down, rather than the
     * question being put again once the run is over and the pipeline may have been edited since.
     *
     * <p>Which steps are nests is decided by {@link #nestOf}, the same way the build decides it. Two
     * walks that judged that differently would drop the namespaces of one set of steps while a run wrote
     * to another's, and nothing would report the difference.
     */
    public static Set<String> nestStateNamespaces(PipelineResource pipeline, Function<String, NestTable> tables) {
        if (pipeline.transforms() == null) {
            return Set.of();
        }
        Set<String> namespaces = new LinkedHashSet<>();
        for (Step step : pipeline.transforms()) {
            TransformBody.Nest nest = nestOf(step);
            if (nest != null) {
                namespaces.addAll(NestTopology.compile(pipeline.id(), step.id(), nest, tables).stateNamespaces());
            }
        }
        return namespaces;
    }

    /**
     * What this pipeline's nests are held to: {@code base} - the numbers the deployment was started with -
     * with whatever its author wrote on the nest steps themselves laid over it.
     *
     * <p>The two knobs are laid on differently because they bound different things, and reading them the
     * same way is the mistake available here. A per-document limit is filed under the namespace of the
     * nest that wrote it, so two nests in one pipeline keep two limits; the memory budget is one number
     * for the pipeline, because what it bounds is memory all of its levels share. Two steps naming
     * different budgets is refused before this, where the author can be told which two numbers disagree.
     *
     * <p>A pipeline that wrote neither is answered with {@code base} itself. Rebuilding an identical copy
     * would be harmless and is avoided anyway: identity is the cheapest way for a caller to see that an
     * artifact authored before these fields existed changed nothing about how it runs.
     */
    public static NestSettings nestSettings(PipelineResource pipeline, Function<String, NestTable> tables,
            NestSettings base) {
        if (pipeline.transforms() == null) {
            return base;
        }
        NestSettings settings = base;
        for (Step step : pipeline.transforms()) {
            TransformBody.Nest nest = nestOf(step);
            if (nest == null) {
                continue;
            }
            if (nest.entriesInMemory() != null) {
                settings = settings.withEntriesHeldInMemory(nest.entriesInMemory());
            }
            if (nest.maxElementsPerDocument() != null) {
                // The document level of this nest and no other. The name comes from the compiler rather
                // than from anything spelled out here, for the same reason the namespaces do: two places
                // deriving it would eventually derive it differently, and the limit would then be filed
                // under a namespace no vertex ever asks about - unenforced, and reported as configured.
                String documents = NestTopology.compile(pipeline.id(), step.id(), nest, tables)
                        .assembler().mapName();
                settings = settings.withElementLimit(documents, nest.maxElementsPerDocument());
            }
        }
        return settings;
    }

    /**
     * Whether this pipeline nests at all. Distinct from having namespaces: a root declaring no embeds
     * compiles to none yet is still a nest, and still writes down the shape it compiled to - so a caller
     * clearing up after a pipeline has to know the difference between "kept no state" and "is not a nest".
     */
    public static boolean hasNest(PipelineResource pipeline) {
        if (pipeline.transforms() == null) {
            return false;
        }
        for (Step step : pipeline.transforms()) {
            if (nestOf(step) != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * The nest this step declares, or {@code null} where the step is anything else. One place answers it
     * so that everything walking a pipeline's transforms agrees on what a nest is. A step it answers for
     * is necessarily a {@link Step.Inline}, which is what lets a caller narrow to one on a non-null answer.
     */
    private static TransformBody.Nest nestOf(Step step) {
        return step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest ? nest : null;
    }

    /** Builds the Jet DAG for a validated pipeline against the given leaf and reference bindings. */
    public static DAG build(PipelineResource pipeline, DagBindings bindings) {
        return build(pipeline, bindings, null);
    }

    /**
     * Builds the Jet DAG for a validated pipeline. When {@code sinkAck} is present each serve sink
     * advances a durable sink-acked watermark through it; when it is null the sinks are the no-ack
     * variant, so an ack-less run still builds. The topology is identical either way — the ack only
     * changes which sink vertex the serve block wires.
     */
    public static DAG build(PipelineResource pipeline, DagBindings bindings, SinkAckFactory sinkAck) {
        return build(pipeline, bindings, sinkAck, null);
    }

    /**
     * Builds the Jet DAG for a validated pipeline. When {@code frontier} is present each level is told
     * which chains reach it over which of its edges, and so can work out how far it may let the frontier
     * go; when it is null no level propagates a bound at all, which is a frontier that does not advance
     * rather than one that advances too far. The topology is identical either way.
     */
    public static DAG build(PipelineResource pipeline, DagBindings bindings, SinkAckFactory sinkAck,
            FrontierBinding frontier) {
        DAG dag = new DAG();
        Map<String, Vertex> byKey = new HashMap<>();
        // Whether anything in this graph gathers several chains into one stream. It settles which shape of
        // frontier the sinks are given, and it is a property of the graph rather than of what flows through
        // it, so it is answered here and never re-judged while running.
        boolean assembled = false;
        // Jet rejects two edges that share a source or destination ordinal, so every edge takes the
        // next free ordinal on each of its endpoints; a fan-in (union) and a fan-out (multi-sink)
        // then wire without collision.
        Map<Vertex, Integer> outboundOrdinal = new HashMap<>();
        Map<Vertex, Integer> inboundOrdinal = new HashMap<>();
        PipelineChains chains = frontier == null ? null : new PipelineChains();

        for (String sourceId : pipeline.sources()) {
            byKey.put(sourceId, dag.newVertex(sourceId, bindings.sourceVertices().apply(sourceId)));
            if (chains != null) {
                chains.source(sourceId, frontier.chainOf(sourceId));
            }
        }
        // The numbering comes from the binding rather than from what was accumulated here, because the
        // assembler stamping at its sources needs the same answer: an axis meaning one chain in the graph
        // and another at a source would have unrelated promises combined as though they were one.
        ChainAxes axes = frontier == null ? null : frontier.axes();

        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                TransformBody.Nest nest = nestOf(step);
                if (nest != null) {
                    Step.Inline inline = (Step.Inline) step;
                    // A nest draws its own vertices and edges: their count, their keys and the ordinal
                    // each stream arrives on were all settled while compiling the tree.
                    if (bindings.nest() == null) {
                        throw new IllegalStateException("transform step '" + step.id()
                                + "' is a nest, but no nest binding was supplied to the builder");
                    }
                    NestTopology topology =
                            NestTopology.compile(pipeline.id(), step.id(), nest, bindings.nest().tables());
                    // Before a vertex is drawn, not after: the paths a tree keeps its state under are
                    // named by the tree, so a job built first and refused afterwards would already have
                    // decided which entries it was about to read - and every one of them would be the
                    // wrong entry, silently, with nothing downstream able to tell.
                    NestStateLedger.reconcile(bindings.nest().ledger(), pipeline.id(), step.id(),
                            topology.statePaths());
                    byKey.put(step.id(), NestDag.attach(dag, topology,
                            step.id(), nest.root().from(), step.id(),
                            alias -> verticesOf(aliasUpstream(inline.from(), alias, bindings), byKey),
                            bindings.nest(),
                            vertex -> outboundOrdinal.merge(vertex, 1, Integer::sum) - 1,
                            chains == null ? null : new NestFrontier(axes,
                                    alias -> chains.perProducer(
                                            aliasUpstream(inline.from(), alias, bindings)))));
                    if (chains != null) {
                        chains.derived(step.id(), nestUpstream(inline.from(), bindings));
                    }
                    assembled = true;
                    continue;
                }
                List<String> upstream = resolveClause(step.from(), bindings);
                Vertex vertex = transformVertex(dag, step, bindings, axes,
                        chains == null ? null : chains.perOrdinal(upstream));
                byKey.put(step.id(), vertex);
                if (chains != null) {
                    chains.derived(step.id(), upstream);
                }
                connect(dag, verticesOf(upstream, byKey), vertex, outboundOrdinal, inboundOrdinal);
            }
        }

        if (pipeline.serve() instanceof ServeBlock.Use) {
            throw new IllegalArgumentException(
                    "serve block is a use-reference; resolve it to an inline serve first");
        }
        if (pipeline.serve() instanceof ServeBlock.Inline serve && serve.sync() != null) {
            List<Vertex> upstream = verticesOf(resolve(serve.from(), bindings), byKey);
            List<SyncElement> sync = serve.sync();
            for (int i = 0; i < sync.size(); i++) {
                SyncElement element = sync.get(i);
                String name = SERVE_VERTEX_PREFIX + (element.id() != null ? element.id() : i);
                Vertex vertex = dag.newVertex(name,
                        sinkVertex(bindings.sinkWriters().apply(element), sinkAck, axes, assembled));
                connect(dag, upstream, vertex, outboundOrdinal, inboundOrdinal);
            }
        }

        return dag;
    }

    /**
     * The sink vertex for one serve.sync element: the ack-bearing sink when a sink-ack factory is present
     * (advancing a durable watermark), otherwise the no-ack sink. Both wrap the same writer factory in the
     * one generic sink adapter, pinned to total parallelism one.
     *
     * <p>{@code assembled} picks the shape of frontier the ack-bearing sink runs. Where the graph gathers
     * several chains into one stream, what arrives can no longer say by itself how far a chain has
     * travelled, and the sink goes by the bound the engine combines across its input queues instead. Where
     * it does not, the events of one chain arrive in their own order and the settled prefix is the whole
     * answer; a bound reaching such a sink is discarded rather than acted on, because the source stamps
     * bounds whatever the graph does with them.
     *
     * <p>An assembling graph built without a chain numbering gets the same shape with nothing to attribute
     * a bound to, so its frontier stands still. That is the direction to fail in: reading a stream of
     * several chains as though it were one would ack positions whose changes are still in flight.
     */
    private static ProcessorMetaSupplier sinkVertex(SupplierEx<? extends SinkWriter> writerFactory,
            SinkAckFactory sinkAck, ChainAxes axes, boolean assembled) {
        if (sinkAck == null) {
            return SinkProcessor.metaSupplier(writerFactory);
        }
        SupplierEx<SinkFrontier> frontier = assembled
                ? () -> new SettledFloor(axes, SettledFloor.DEFAULT_MAX_ENTRIES_PER_CHAIN)
                : ContiguousPrefix::new;
        return SinkProcessor.metaSupplier(writerFactory, sinkAck, frontier);
    }

    /**
     * The vertex for one transform step. Only the linear family is in scope here: a {@code union} is
     * topology, not a transform - a passthrough vertex whose several inbound edges are the merge, so
     * the transform-port binding is never asked for one; every other stateless step (filter / map / a
     * scripted row transform) runs the one generic adapter over the port the binding supplies. A
     * {@code nest} never reaches here: it draws a sub-graph of its own instead of a single vertex. A
     * {@code join} or an unresolved {@code use:} reference is out of this builder's scope and is
     * refused; extending to them replaces the refusal, not the seam.
     */
    private static Vertex transformVertex(DAG dag, Step step, DagBindings bindings, ChainAxes axes,
            Map<Integer, List<String>> chainsByOrdinal) {
        if (!(step instanceof Step.Inline inline)) {
            throw new IllegalArgumentException(
                    "transform step '" + step.id() + "' is a use-reference; resolve it to an inline step first");
        }
        TransformBody body = inline.body();
        if (body instanceof TransformBody.Join) {
            throw new IllegalArgumentException(
                    "transform step '" + step.id() + "' is a stateful " + body.type()
                            + "; the linear DAG builder does not carry it");
        }
        if (body instanceof TransformBody.Union) {
            // The merge is the topology, so nothing is transformed here - but the frontier still has to be
            // worked out per edge. The combined bound the engine would forward is never delivered at all
            // for a chain only one of the merged streams carries, which is the whole shape a union is.
            return dag.newVertex(step.id(), PassthroughProcessor.metaSupplier(axes, chainsByOrdinal));
        }
        return dag.newVertex(step.id(), TransformProcessor.metaSupplier(
                bindings.transformPorts().apply(step), axes, chainsByOrdinal));
    }

    /**
     * The producer keys behind one alias of a nest / join {@code from:} map. A validated pipeline
     * always names a declared alias, so an unknown one is a builder invariant violation rather than a
     * user error.
     */
    private static List<String> aliasUpstream(FromClause from, String alias, DagBindings bindings) {
        if (!(from instanceof FromClause.Aliases aliases)) {
            throw new IllegalStateException("a nest step must carry an alias-map from:");
        }
        FromRef ref = aliases.aliases().get(alias);
        if (ref == null) {
            throw new IllegalStateException("nest alias '" + alias + "' is not declared on the step");
        }
        return resolve(ref, bindings);
    }

    /** Every producer key feeding a nest step, over all of its aliases at once. */
    private static List<String> nestUpstream(FromClause from, DagBindings bindings) {
        List<String> keys = new ArrayList<>();
        if (from instanceof FromClause.Aliases aliases) {
            for (String alias : aliases.aliases().keySet()) {
                keys.addAll(aliasUpstream(from, alias, bindings));
            }
        }
        return keys;
    }

    /** Resolves every reference in a streaming {@code from:} list to the producer keys upstream. */
    private static List<String> resolveClause(FromClause from, DagBindings bindings) {
        List<String> upstream = new ArrayList<>();
        if (from instanceof FromClause.Flow flow) {
            for (FromRef ref : flow.refs()) {
                upstream.addAll(resolve(ref, bindings));
            }
        }
        return upstream;
    }

    /**
     * Resolves a {@code from:} reference to the producer keys it names. A validated pipeline always
     * resolves, so an empty result is a builder invariant violation, not a user error - it bare-throws
     * rather than emitting a broken DAG.
     */
    private static List<String> resolve(FromRef ref, DagBindings bindings) {
        List<String> keys = bindings.upstreams().apply(ref);
        if (keys == null || keys.isEmpty()) {
            throw new IllegalStateException("reference " + ref + " resolved to no upstream vertex");
        }
        return keys;
    }

    /** The vertices those producer keys name, refusing a key no vertex was built for. */
    private static List<Vertex> verticesOf(List<String> keys, Map<String, Vertex> byKey) {
        List<Vertex> vertices = new ArrayList<>();
        for (String key : keys) {
            Vertex vertex = byKey.get(key);
            if (vertex == null) {
                throw new IllegalStateException("reference resolved to unknown vertex '" + key + "'");
            }
            vertices.add(vertex);
        }
        return vertices;
    }

    /** Draws one edge from each upstream vertex into the destination, on fresh ordinals per endpoint. */
    private static void connect(DAG dag, List<Vertex> upstream, Vertex destination,
            Map<Vertex, Integer> outboundOrdinal, Map<Vertex, Integer> inboundOrdinal) {
        for (Vertex source : upstream) {
            int from = outboundOrdinal.merge(source, 1, Integer::sum) - 1;
            int to = inboundOrdinal.merge(destination, 1, Integer::sum) - 1;
            dag.edge(Edge.from(source, from).to(destination, to));
        }
    }
}
