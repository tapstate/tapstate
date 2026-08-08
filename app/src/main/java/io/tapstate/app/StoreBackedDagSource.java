package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.adapters.transform.MapSpec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.DagBindings;
import io.tapstate.runtime.engine.FrontierBinding;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.srs.SrsReadCursorPublisherFactory;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds the Jet topology a pipeline runs from its stored artifact. It loads the pipeline and the source and
 * target artifacts it references, turns each into the leaf and reference bindings the engine's DAG builder
 * needs, and returns the DAG the builder assembles - source vertices to a linear transform chain to serve
 * sink vertices. The builder owns the topology; this owns how each leaf resolves from the store.
 *
 * <p>Only serializable coordinates cross onto the DAG: a source vertex carries its resolved change-ring name
 * and stream name, a transform vertex carries the port's serializable shape, and a sink vertex carries the
 * resolved connector coordinates and resolves the connector on the member that opens it. The reference and
 * leaf resolution itself runs here, on the assembly side, so nothing store-bound is shipped.
 *
 * <p>The sink-writer factory is a constructor seam: production binds the PDK factory that resolves the
 * connector member-side, while a data-flow test can bind a capturing sink so the topology runs without a real
 * connector. Deriving the source's change-ring identity is delegated to the shared source resolution so the
 * reader built here and the capture side that fills the ring land on the same ring.
 *
 * <p>L1 shape: each source reads exactly one table, and a serve.sync element names a source id as its target
 * connection supplier. Start position defaults to the earliest buffered change.
 */
final class StoreBackedDagSource implements DagSource {

    private final StorePort storePort;
    private final SinkWriterBinder sinkWriterBinder;
    private final TargetModelResolver targetModelResolver;
    private final NestSettings nestSettings;

    StoreBackedDagSource(StorePort storePort) {
        this(storePort, PdkSinkWriterFactory::new);
    }

    /** A source whose nests are held to what {@code nestSettings} allows, and the member configured from. */
    StoreBackedDagSource(StorePort storePort, NestSettings nestSettings) {
        this(storePort, PdkSinkWriterFactory::new, nestSettings);
    }

    StoreBackedDagSource(StorePort storePort, SinkWriterBinder sinkWriterBinder) {
        this(storePort, sinkWriterBinder, NestSettings.defaults());
    }

    StoreBackedDagSource(StorePort storePort, SinkWriterBinder sinkWriterBinder, NestSettings nestSettings) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.sinkWriterBinder = Objects.requireNonNull(sinkWriterBinder, "sinkWriterBinder");
        this.targetModelResolver = new TargetModelResolver(this.storePort);
        this.nestSettings = Objects.requireNonNull(nestSettings, "nestSettings");
    }

    @Override
    public DAG dagFor(String pipelineId) {
        PipelineResource pipeline = StoredArtifacts.requirePipeline(artifacts(), pipelineId);
        TargetTable target = targetModelResolver.resolve(pipeline).orElse(null);
        FrontierBinding frontier = frontierBinding(pipeline);
        return PipelineDagBuilder.build(pipeline, bindings(pipeline, sourceIdByTable(pipeline), target, frontier),
                sinkAckFactory(pipeline, pipelineId), frontier);
    }

    /**
     * Where this pipeline's nests keep state: the namespace each compiled vertex holds its entries in,
     * plus the one its shape was written down in. The record goes with the state it describes - kept
     * behind, it would refuse the next start of a pipeline that has nothing left to abandon, naming paths
     * that no longer address anything.
     *
     * <p>The tree is compiled again here rather than remembered from the build, for the same reason the
     * build compiles it rather than reading it back: the names come from the tree, so the tree is what is
     * asked. A pipeline with no nest step keeps nothing and is named nothing, which is what leaves an
     * ordinary pipeline's stop untouched by any of this.
     */
    @Override
    public Set<String> stateNamespacesOf(String pipelineId) {
        PipelineResource pipeline = StoredArtifacts.requirePipeline(artifacts(), pipelineId);
        if (!PipelineDagBuilder.hasNest(pipeline)) {
            return Set.of();
        }
        Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable(pipeline));
        Set<String> namespaces =
                new LinkedHashSet<>(PipelineDagBuilder.nestStateNamespaces(pipeline, byAlias::get));
        namespaces.add(StoreBackedNestStateLedger.namespaceOf(pipelineId));
        return namespaces;
    }

    /**
     * Which chain each of the pipeline's sources reads: the table it is resolved to, which is the same
     * stream name that source projects into every change it emits. Reading it from the same resolution the
     * source vertex is built from is what keeps the two the same string - a chain named anything else would
     * reach a level that was compiled to carry a different one, and the level tears the job down rather
     * than widening itself to fit.
     */
    private FrontierBinding frontierBinding(PipelineResource pipeline) {
        Map<String, String> chainBySource = new LinkedHashMap<>();
        for (String sourceId : pipeline.sources()) {
            chainBySource.put(sourceId,
                    SourceCaptureResolution.of(StoredArtifacts.requireSource(artifacts(), sourceId)).table());
        }
        return new FrontierBinding(chainBySource);
    }

    /**
     * The source id to reach for each table the pipeline's sources read. A reference into a source names the
     * table, while the vertex reading it is keyed by the source id, so this is what carries one to the other.
     * A table read by two of one pipeline's sources cannot occur here: the reference rules reject the
     * ambiguity before a pipeline is ever stored.
     */
    private Map<String, String> sourceIdByTable(PipelineResource pipeline) {
        Map<String, String> byTable = new LinkedHashMap<>();
        for (String sourceId : pipeline.sources()) {
            SourceCaptureResolution resolution =
                    SourceCaptureResolution.of(StoredArtifacts.requireSource(artifacts(), sourceId));
            byTable.put(resolution.table(), sourceId);
        }
        return byTable;
    }

    /**
     * The sink-ack wiring that closes the durable frontier: as a sink confirms writes it advances the
     * pipeline consumer's durable sink-acked position through this. The sink knows a chain only by the
     * {@code src} stream name (a table at L1), so this carries a table-to-chain map resolved from every
     * source the pipeline reads. The map is built here, on the assembly side; only serializable
     * coordinates ship.
     */
    private SinkAckFactory sinkAckFactory(PipelineResource pipeline, String pipelineId) {
        return new StoreBackedSinkAckFactory(chainIdByTable(pipeline), pipelineId);
    }

    /**
     * The mining chain behind each table the pipeline's sources read. Both directions of the durable
     * frontier are keyed by it - the sink writes its confirmed position under it, and an operator upstream
     * reads that position back - so both are resolved the same way, from the same source resolution the
     * source vertex itself is built from.
     */
    private Map<String, String> chainIdByTable(PipelineResource pipeline) {
        Map<String, String> chainIdByTable = new LinkedHashMap<>();
        for (String sourceId : pipeline.sources()) {
            SourceCaptureResolution resolution =
                    SourceCaptureResolution.of(StoredArtifacts.requireSource(artifacts(), sourceId));
            chainIdByTable.put(resolution.table(), resolution.chainId().value());
        }
        return chainIdByTable;
    }

    /**
     * The leaf and reference bindings for the builder. The four functions run on the assembly side as the
     * builder walks the topology; only the vertex suppliers they return travel onto the DAG, so they may
     * reach the store freely while what they produce stays serializable.
     */
    private DagBindings bindings(PipelineResource pipeline, Map<String, String> sourceIdByTable, TargetTable target,
            FrontierBinding frontier) {
        ChainAxes axes = frontier.axes();
        return new DagBindings(
                sourceId -> sourceVertex(sourceId, axes),
                StoreBackedDagSource::transformPort,
                element -> sinkWriter(element, target),
                ref -> upstreams(ref, sourceIdByTable),
                nestBinding(pipeline, sourceIdByTable));
    }

    /**
     * What a nest node needs that the engine will not decide: the table behind each embedded alias, where
     * each vertex keeps its state, and where a change that can never reach a document goes.
     *
     * <p>Supplied whether or not the pipeline has a nest in it. It costs a walk of the transforms and
     * nothing else, and the alternative — deciding here that a pipeline has no nest — is a second place
     * that has to agree with the builder about what a nest is.
     *
     * <p>State goes in a map of the member's own, one per vertex, named by what the topology computed for
     * that vertex - so a vertex addresses the same entries across restarts and across the several processor
     * instances a vertex is run as. Whether those entries outlive the member is decided where the member is
     * configured: with a store behind the maps a restart reads a key back as it is asked for, and without
     * one the state is rebuilt by replay, which is what the earlier build promised and no more. Dropped
     * changes are counted and warned about rather than routed anywhere, because where they should go has
     * not been decided; counting them is the part that is not in question.
     *
     * <p>It also carries the read side of the durable frontier, which is how an assembler learns that a
     * root it deleted can no longer be built back by a replay and its record may be dropped. Without it
     * every deletion would leave something behind for as long as the job runs.
     *
     * <p>The last of the five is what makes the state layer's own name a checked thing rather than an
     * assumed one: the paths a nest keeps state under are written down as it is built and compared against
     * on the way up. Editing an embed's path is otherwise silent - it renames where the state is kept
     * without moving anything into it, and the tree rebuilds from empty while the pipeline reports that it
     * resumed.
     */
    /**
     * The numbers this pipeline's nests are held to, and the maps they apply to. Compiled here from the
     * same tree the topology and the teardown names come from, so a budget cannot end up on a namespace no
     * vertex writes to while the ones that are written to run on the deployment's number.
     */
    @Override
    public NestCapacity capacityOf(String pipelineId) {
        PipelineResource pipeline = StoredArtifacts.requirePipeline(artifacts(), pipelineId);
        if (!PipelineDagBuilder.hasNest(pipeline)) {
            return NestCapacity.none();
        }
        Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable(pipeline));
        return new NestCapacity(PipelineDagBuilder.nestStateNamespaces(pipeline, byAlias::get),
                PipelineDagBuilder.nestSettings(pipeline, byAlias::get, nestSettings));
    }

    private NestBinding nestBinding(PipelineResource pipeline, Map<String, String> sourceIdByTable) {
        Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable);
        return new NestBinding(byAlias::get, NestBinding.onMap(), new CountingNestDeadLetter(),
                new StoreBackedReplayFloorFactory(chainIdByTable(pipeline), pipeline.id()),
                new StoreBackedNestStateLedger(storePort.keyedState()),
                // What the deployment was started with, with what this pipeline's author wrote over it.
                // Laid on here rather than held as one value for the process because the shape each
                // number bounds is the pipeline's, not the process's: one tree is deep and narrow and
                // the next is shallow and wide, and a single number covers neither.
                PipelineDagBuilder.nestSettings(pipeline, byAlias::get, nestSettings));
    }

    /**
     * The table behind every alias the pipeline's nest steps declare.
     *
     * <p>An alias naming a table resolves to that table and the key its discovery model declares; one
     * naming a step resolves to a table with no key, as does one whose source was never discovered. The
     * empty key is not a failure here: it is only ever read to fill in an embed that left {@code arrayKey}
     * out, and an embed that needs it and cannot get it is the author's to fix — the engine says so with a
     * code. Resolving it to nothing instead would turn that into a crash.
     *
     * <p>Aliases are declared per step but asked for pipeline-wide, so two steps declaring one alias over
     * different tables cannot both be answered. That is refused rather than silently resolved one way.
     */
    private Map<String, NestTable> nestTablesByAlias(
            PipelineResource pipeline, Map<String, String> sourceIdByTable) {
        Map<String, NestTable> byAlias = new LinkedHashMap<>();
        if (pipeline.transforms() == null) {
            return byAlias;
        }
        for (Step step : pipeline.transforms()) {
            if (!(step instanceof Step.Inline inline) || !(inline.body() instanceof TransformBody.Nest)) {
                continue;
            }
            if (!(inline.from() instanceof FromClause.Aliases aliases)) {
                continue;
            }
            aliases.aliases().forEach((alias, ref) -> {
                NestTable resolved = nestTable(ref, sourceIdByTable);
                NestTable existing = byAlias.putIfAbsent(alias, resolved);
                if (existing != null && !existing.name().equals(resolved.name())) {
                    throw new IllegalStateException("alias '" + alias + "' names table '" + existing.name()
                            + "' on one nest step and '" + resolved.name() + "' on another; a nest binding "
                            + "answers per alias, so the two cannot both be resolved");
                }
            });
        }
        return byAlias;
    }

    /** One alias's table: its discovered key when the reference names a table, no key otherwise. */
    private NestTable nestTable(FromRef ref, Map<String, String> sourceIdByTable) {
        if (!(ref instanceof FromRef.Literal literal)) {
            // A regex names many upstreams and so no single table key; an embed over one declares its own.
            return new NestTable(String.valueOf(ref), List.of());
        }
        String table = literal.ref();
        String sourceId = sourceIdByTable.get(table);
        if (sourceId == null) {
            // A step id: the stream is another step's output, which has no table key to fall back on.
            return new NestTable(table, List.of());
        }
        return new NestTable(table, storePort.schemas().get(sourceId)
                .map(DiscoveredSourceModel::model)
                .flatMap(model -> model.tables().stream().filter(t -> t.name().equals(table)).findFirst())
                .map(SourceTable::primaryKey)
                .orElse(List.of()));
    }

    /**
     * The source vertex for one source id: it resolves the source's connector, config and single table into
     * the per-table change ring the capture side writes. The stream name projected into each event is the
     * table name. Resolving the same ring identity the capture side resolves is what points the reader at the
     * ring the writer fills.
     */
    private ProcessorMetaSupplier sourceVertex(String sourceId, ChainAxes axes) {
        SourceCaptureResolution resolution =
                SourceCaptureResolution.of(StoredArtifacts.requireSource(artifacts(), sourceId));
        // The ring knows a generation and a sequence; which axis this stream travels on and how the pair
        // packs into the one long a bound rides are properties of the whole job, so they are closed over
        // here rather than reached for from the source.
        String chain = resolution.table();
        byte axis = axes.axisOf(chain);
        return SrsSourceProcessor.metaSupplier(
                resolution.ringName(), resolution.table(), StartFrom.earliest(),
                ringGeneration(resolution), SrsReadCursorPublisherFactory.NONE,
                order -> new Watermark(FrontierOrders.pack(chain, order), axis));
    }

    /**
     * The generation the source's ring is open under, read once while the job is assembled, and zero for a
     * source that reads no chain of its own.
     *
     * <p>Only a read with an incremental tail through the shared ring opens a chain, so a snapshot-only or
     * srs-disabled read has no record here and no ring anyone fills — its rows reach the sink from the
     * snapshot buffer rather than the ring, and there is no stream of changes for them to be ordered
     * against. Reading the record rather than re-deriving the plan keeps one answer to that question: the
     * capture run writes the record, so its presence is what "this source reads a shared ring" means.
     */
    private long ringGeneration(SourceCaptureResolution resolution) {
        return storePort.meta().read(resolution.chainId().value()).map(SrsMeta::epoch).orElse(0L);
    }

    /**
     * The port factory for one linear transform step. The builder only asks this for an inline stateless
     * step (filter / map / a scripted row transform); a union it merges itself and a stateful step it
     * refuses, so neither reaches here. The returned factory captures only the step body's serializable
     * shape - an expression string, a projection spec, a script - so it ships and rebuilds the port on the
     * member.
     */
    private static SupplierEx<? extends TransformPort> transformPort(Step step) {
        if (!(step instanceof Step.Inline inline)) {
            throw new IllegalStateException("transform step '" + step.id() + "' is not inline");
        }
        TransformBody body = inline.body();
        return switch (body) {
            case TransformBody.Filter filter -> {
                String expr = filter.expr();
                yield (SupplierEx<TransformPort>) () -> StatelessTransforms.filter(expr);
            }
            case TransformBody.MapProjection projection -> {
                MapSpec spec = MapSpec.from(projection);
                yield (SupplierEx<TransformPort>) () -> StatelessTransforms.map(spec);
            }
            case TransformBody.Js js -> {
                String script = js.script();
                yield (SupplierEx<TransformPort>) () -> StatelessTransforms.js(script);
            }
            default -> throw new IllegalStateException("transform step '" + step.id()
                    + "' has a body the linear builder does not carry: " + body.type());
        };
    }

    /**
     * The sink-writer factory for one serve.sync element. The element names a source id as its target
     * connection supplier, so the connector and config come from that source; the write mode and ddl policy
     * come from the element, defaulting to upsert and fail. The resolved target model - the table the sink
     * creates and the key an upsert matches on, resolved from the pipeline source's discovered model - travels
     * with the binding, or is null when no model was discovered. The bound factory carries only these
     * serializable coordinates and opens the connector on the member that runs the sink.
     */
    private SupplierEx<? extends SinkWriter> sinkWriter(SyncElement element, TargetTable target) {
        SourceResource sink = StoredArtifacts.requireSource(artifacts(), element.source());
        return sinkWriterBinder.bind(
                sink.connector(), sink.config(), writeMode(element.writeMode()), ddl(element.ddl()), target);
    }

    /**
     * The producer vertex keys a reference names.
     *
     * <p>A reference reaching a source names the <em>table</em> that source reads, not the source itself,
     * while the vertex reading it is keyed by the source id — so a token naming one of the pipeline's tables
     * resolves to that source's vertex. Any other literal is already a vertex key: a step id, or a source id
     * where the source declares no table to be addressed by instead. Translating here is what this binding is
     * for: the builder is told which vertices a reference produces and cannot know that a table implies one.
     *
     * <p>A regex reference expands the source universe and is not carried by the linear L1 builder.
     */
    private static List<String> upstreams(FromRef ref, Map<String, String> sourceIdByTable) {
        if (ref instanceof FromRef.Literal literal) {
            return List.of(sourceIdByTable.getOrDefault(literal.ref(), literal.ref()));
        }
        throw new IllegalStateException("regex from: reference is not carried by the linear L1 builder: " + ref);
    }

    private static WriteMode writeMode(io.tapstate.core.model.WriteMode mode) {
        io.tapstate.core.model.WriteMode resolved = mode != null ? mode : io.tapstate.core.model.WriteMode.UPSERT;
        return switch (resolved) {
            case UPSERT -> WriteMode.UPSERT;
            case APPEND -> WriteMode.APPEND;
        };
    }

    private static DdlPolicy ddl(io.tapstate.core.model.DdlPolicy policy) {
        io.tapstate.core.model.DdlPolicy resolved = policy != null ? policy : io.tapstate.core.model.DdlPolicy.FAIL;
        return switch (resolved) {
            case APPLY -> DdlPolicy.APPLY;
            case IGNORE -> DdlPolicy.IGNORE;
            case FAIL -> DdlPolicy.FAIL;
        };
    }

    private ArtifactStore artifacts() {
        return storePort.artifacts();
    }

    /**
     * The seam that binds a serve.sync target's resolved connector coordinates to the sink-writer supplier
     * shipped onto the DAG. Production binds the PDK factory that resolves the connector member-side; a test
     * can bind a capturing sink so the topology runs without a real connector.
     */
    @FunctionalInterface
    interface SinkWriterBinder {

        SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                TargetTable target);
    }
}
