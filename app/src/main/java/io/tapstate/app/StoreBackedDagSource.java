package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.adapters.transform.MapSpec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ChainAxes;
import io.tapstate.runtime.engine.DagBindings;
import io.tapstate.runtime.engine.FrontierBinding;
import io.tapstate.runtime.engine.FrontierOrders;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.engine.nest.DurableNestDeadLetter;
import io.tapstate.runtime.engine.join.JoinBinding;
import io.tapstate.runtime.engine.join.JoinStoresBinding;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestClock;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
 * <p>A source may read several tables, and a serve.sync element names a source id as its target connection
 * supplier. Start position defaults to the earliest buffered change.
 */
final class StoreBackedDagSource implements DagSource {

    private final StorePort storePort;
    private final SinkWriterBinder sinkWriterBinder;
    private final TargetModelResolver targetModelResolver;
    private final NestSettings nestSettings;
    private final StoreReachability storeReachability;

    StoreBackedDagSource(StorePort storePort) {
        this(storePort, assembledSinkWriterBinder());
    }

    /** A source whose nests are held to what {@code nestSettings} allows, and the member configured from. */
    StoreBackedDagSource(StorePort storePort, NestSettings nestSettings) {
        this(storePort, assembledSinkWriterBinder(), nestSettings);
    }

    /** A source that holds a view's managed store to answering before the pipeline is built. */
    StoreBackedDagSource(StorePort storePort, StoreReachability storeReachability) {
        this(storePort, assembledSinkWriterBinder(), NestSettings.defaults(), storeReachability);
    }

    /** The assembled source: nests held to {@code nestSettings}, the store probed before a view is built. */
    StoreBackedDagSource(
            StorePort storePort, NestSettings nestSettings, StoreReachability storeReachability) {
        this(storePort, assembledSinkWriterBinder(), nestSettings, storeReachability);
    }

    /**
     * The binder the product is assembled with, named rather than written inline at each construction.
     *
     * <p>It has to be this one and not a method reference to the factory. A method reference binds to the
     * interface's single abstract method, which is the shape taking one model, and the default that adapts
     * a model-per-stream call to it has to pick one of them - so it picks none as soon as there is more than
     * one, and a pipeline reading two tables reaches its sink with no model at all.
     */
    static SinkWriterBinder assembledSinkWriterBinder() {
        return new PdkSinkWriterBinder();
    }

    /** The binder this source will bind its sinks through. */
    SinkWriterBinder sinkWriterBinder() {
        return sinkWriterBinder;
    }

    @Override
    public void validateStart(String pipelineId) {
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        if (pipeline.serve() instanceof ServeBlock.Inline serve
                && serve.sync() != null && !serve.sync().isEmpty()) {
            Map<String, SourceVertex> sourceVertices = sourceVertices(pipeline);
            Map<String, String> sourceKeyByTable = sourceKeyByTable(sourceVertices);
            Map<String, List<String>> sourceKeysById = sourceKeysById(sourceVertices);
            targetModelResolver.requireAllDiscovered(sourceIdsReaching(
                    pipeline, serve.from(), sourceKeyByTable, sourceKeysById, sourceVertices,
                    stepIds(pipeline)));
        }
    }

    StoreBackedDagSource(StorePort storePort, SinkWriterBinder sinkWriterBinder) {
        this(storePort, sinkWriterBinder, NestSettings.defaults());
    }

    StoreBackedDagSource(StorePort storePort, SinkWriterBinder sinkWriterBinder, NestSettings nestSettings) {
        // No prober: the store is taken at its word. Every construction that means to check one passes it.
        this(storePort, sinkWriterBinder, nestSettings, StoreReachability.assumingReachable());
    }

    StoreBackedDagSource(
            StorePort storePort, SinkWriterBinder sinkWriterBinder, NestSettings nestSettings,
            StoreReachability storeReachability) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.sinkWriterBinder = Objects.requireNonNull(sinkWriterBinder, "sinkWriterBinder");
        this.targetModelResolver = new TargetModelResolver(this.storePort);
        this.nestSettings = Objects.requireNonNull(nestSettings, "nestSettings");
        this.storeReachability = Objects.requireNonNull(storeReachability, "storeReachability");
    }

    @Override
    public DAG dagFor(String pipelineId) {
        // Expanded before anything reads the blocks, so every later step - target resolution included -
        // sees one shape rather than having to know a reference from a body.
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        Map<String, SourceVertex> sourceVertices = sourceVertices(pipeline);
        Map<String, String> sourceKeyByTable = sourceKeyByTable(sourceVertices);
        Map<String, List<String>> sourceKeysById = sourceKeysById(sourceVertices);
        Set<String> stepIds = stepIds(pipeline);
        // Resolve every selected table once. The terminal-specific stream sets below then narrow this map to
        // what can actually reach each sink, so an unrelated source neither gains a discovery obligation nor
        // a target binding.
        Map<String, TargetTable> bySourceTable = targetModelResolver.resolveAll(pipeline);
        // Compiled once, here, and handed to both the targets below and the join binding. Compiling it
        // twice would mean two answers to the same question with nothing comparing them.
        Map<String, CompiledJoin> compiledJoins =
                compiledJoins(pipeline, sourceIdByTable(sourceVertices));
        // A nest or a join emits under the id of the step that produced it rather than under a table name,
        // so the resolution above - which answers per source table - says nothing about it. Registering it
        // here is what lets the sink key its upsert and name the table it writes; without it the sink falls
        // back to a bare name carrying neither.
        Map<String, TargetTable> assembled =
                assembledTargets(pipeline, bySourceTable, sourceVertices, compiledJoins);
        Map<String, TargetTable> targets = new LinkedHashMap<>(bySourceTable);
        targets.putAll(assembled);
        Set<String> serveStreams = pipeline.serve() instanceof ServeBlock.Inline serve
                && serve.sync() != null && !serve.sync().isEmpty()
                ? streamsReaching(pipeline, serve.from(), sourceKeyByTable, sourceKeysById,
                        sourceVertices, stepIds)
                : Set.of();
        Set<String> viewStreams = pipeline.view() instanceof ViewBlock.Inline view
                ? streamsReaching(pipeline, view.from(), sourceKeyByTable, sourceKeysById,
                        sourceVertices, stepIds)
                : Set.of();
        FrontierBinding frontier = frontierBinding(sourceVertices);
        return PipelineDagBuilder.build(
                pipeline,
                bindings(pipeline, sourceVertices, sourceKeyByTable, sourceKeysById, targets,
                        serveStreams, viewStreams, stepIds, frontier, compiledJoins),
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
        Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable(sourceVertices(pipeline)));
        Set<String> namespaces =
                new LinkedHashSet<>(PipelineDagBuilder.nestStateNamespaces(pipeline, byAlias::get));
        namespaces.add(StoreBackedNestStateLedger.namespaceOf(pipelineId));
        return namespaces;
    }

    private record SourceVertex(
            String pipelineId, String sourceId, String table, SourceCaptureResolution resolution) {
    }

    /**
     * Which chain each of the pipeline's source vertices reads: the table it is resolved to, which is the
     * same stream name that vertex projects into every change it emits. Reading it from the same resolution
     * the source vertex is built from is what keeps the two the same string - a chain named anything else
     * would reach a level that was compiled to carry a different one, and the level tears the job down
     * rather than widening itself to fit.
     *
     * <p>Keyed per vertex and not per source: a source selecting several tables reads a chain per table, so
     * one entry for the source would name one of its tables and leave the changes of the rest outside every
     * promise the job makes about how far what it read has travelled.
     */
    private static FrontierBinding frontierBinding(Map<String, SourceVertex> sourceVertices) {
        Map<String, String> chainByVertex = new LinkedHashMap<>();
        sourceVertices.forEach((key, vertex) -> chainByVertex.put(key, vertex.table()));
        return new FrontierBinding(chainByVertex);
    }

    /**
     * The source id behind each table the pipeline's sources read. The nest side resolves an alias by asking
     * the store for that source's discovered model, so it needs the source itself - which a vertex key no
     * longer names once a source reading several tables keys its vertices by table.
     */
    private static Map<String, String> sourceIdByTable(Map<String, SourceVertex> sourceVertices) {
        Map<String, String> byTable = new LinkedHashMap<>();
        for (SourceVertex vertex : sourceVertices.values()) {
            byTable.putIfAbsent(vertex.table(), vertex.sourceId());
        }
        return byTable;
    }

    /**
     * The source id to reach for each table the pipeline's sources read. A reference into a source names the
     * table, while the vertex reading it is keyed by the source id, so this is what carries one to the other.
     * A table read by two of one pipeline's sources cannot occur here: the reference rules reject the
     * ambiguity before a pipeline is ever stored.
     */
    private Map<String, SourceVertex> sourceVertices(PipelineResource pipeline) {
        Map<String, SourceVertex> vertices = new LinkedHashMap<>();
        for (String sourceId : pipeline.sources()) {
            SourceResource source = StoredArtifacts.requireSource(artifacts(), sourceId);
            SourceCaptureResolution resolution = SourceCaptureResolution.of(source, SourceDiscovery.model(storePort, source));
            for (String table : resolution.tables()) {
                String key = resolution.tables().size() == 1 ? sourceId : sourceId + "." + table;
                vertices.put(key, new SourceVertex(pipeline.id(), sourceId, table, resolution));
            }
        }
        return vertices;
    }

    private Map<String, String> sourceKeyByTable(Map<String, SourceVertex> sourceVertices) {
        Map<String, String> byTable = new LinkedHashMap<>();
        Map<String, SourceVertex> firstByTable = new LinkedHashMap<>();
        for (Map.Entry<String, SourceVertex> entry : sourceVertices.entrySet()) {
            SourceVertex previous = firstByTable.putIfAbsent(entry.getValue().table(), entry.getValue());
            if (previous != null && !previous.sourceId().equals(entry.getValue().sourceId())) {
                throw new TapstateException(
                        ActuationError.SOURCE_TABLE_AMBIGUOUS,
                        Map.of(
                                "table", entry.getValue().table(),
                                "sources", previous.sourceId() + ", " + entry.getValue().sourceId()),
                        null);
            }
            byTable.putIfAbsent(entry.getValue().table(), entry.getKey());
        }
        return byTable;
    }

    /**
     * The write-side model for what each nest in this pipeline emits, keyed by the stream it emits under.
     *
     * <p>A nest's documents are the root's rows with the assembled children hanging off them, so the table
     * they land in and the columns they carry are the root table's. What they are matched on is not: a
     * document is addressed by the nest root's key, which the author writes and which need not be the root
     * table's primary key.
     *
     * <p>A nest whose root table was never discovered still contributes the table its documents land in,
     * which the topology knows without a discovery having run; only the columns are left out. Contributing
     * nothing at all would drop the assembled stream from the set of streams that reach a sink, and a sink
     * asked about a stream it was never told of falls back to a descriptor built from the bare stream name
     * - putting the documents under the transform step's name rather than the root's, silently. A nest
     * whose root alias resolves to no table at all is a different case and still contributes nothing.
     */
    private Map<String, TargetTable> assembledTargets(
            PipelineResource pipeline, Map<String, TargetTable> bySourceTable,
            Map<String, SourceVertex> sourceVertices, Map<String, CompiledJoin> compiledJoins) {
        Map<String, TargetTable> assembled = new LinkedHashMap<>();
        if (pipeline.transforms() == null) {
            return assembled;
        }
        Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable(sourceVertices));
        for (Step step : pipeline.transforms()) {
            if (!(step instanceof Step.Inline inline)) {
                continue;
            }
            if (inline.body() instanceof TransformBody.Join) {
                assembled.put(step.id(),
                        joinTarget(step, compiledJoins.get(step.id()), bySourceTable));
                continue;
            }
            if (!(inline.body() instanceof TransformBody.Nest nest)) {
                continue;
            }
            NestTable root = byAlias.get(nest.root().from());
            if (root == null) {
                continue;
            }
            TargetTable model = bySourceTable.get(root.name());
            assembled.put(step.id(), model != null
                    ? TargetModelResolver.keyedOn(model, nest.root().key())
                    : new TargetTable(root.name(), List.of()));
        }
        return assembled;
    }

    /**
     * The target model one join step's widened rows are written under: the fact table's name, the
     * join's own output columns, keyed on the fact table's primary key under the names the projection
     * publishes it by.
     *
     * <p><b>The key is the fact key alone, and the dimension keys are deliberately not in it.</b> The
     * driver publishes exactly one row per fact row - its dimension mirror holds one row per join key,
     * so a fact row matching two dimension rows is a fan-out this release does not state - which makes
     * the fact key already unique over the result set, and a dimension key added to it discriminates
     * nothing. It does cost two things. An outer-joined dimension publishes a null key for a fact row
     * it did not match, and a SQL target compares nulls as distinct: measured on postgres 16 and mysql
     * 8.0, the same logical row upserted twice under a unique index over a null key column leaves two
     * rows rather than one, unbounded in the number of republications and indistinguishable from
     * ordinary output; the same column in a PRIMARY KEY is refused outright by both.
     *
     * <p>The fields carry the type the source declared for a column published verbatim, and none for a
     * column computed by an expression - the connector infers that one, the way the view path already
     * treats a type it cannot resolve.
     */
    private TargetTable joinTarget(Step step, CompiledJoin compiled,
            Map<String, TargetTable> bySourceTable) {
        io.tapstate.core.sql.JoinPlan plan = compiled.plan();
        String factName = plan.factSource().name();
        String factTable = compiled.tableByName().getOrDefault(factName, plan.factSource().table());
        List<String> key = new ArrayList<>();
        for (String column : compiled.factKeyColumns()) {
            String published = plan.publishedAs(factName, column);
            if (published == null) {
                throw new TapstateException(ActuationError.JOIN_OUTPUT_KEY_NOT_PUBLISHED,
                        Map.of("step", step.id(), "table", factTable, "column", column), null);
            }
            key.add(published);
        }
        List<TargetField> fields = new ArrayList<>();
        for (String name : key) {
            fields.add(new TargetField(name, joinFieldType(compiled, name, bySourceTable), true));
        }
        for (io.tapstate.core.sql.OutputField field : plan.outputFields()) {
            if (!key.contains(field.name())) {
                fields.add(new TargetField(
                        field.name(), joinFieldType(compiled, field.name(), bySourceTable), false));
            }
        }
        return new TargetTable(factTable, fields);
    }

    /** The declared type of the column one output field publishes verbatim, or null where it computes one. */
    private static String joinFieldType(CompiledJoin compiled, String output,
            Map<String, TargetTable> bySourceTable) {
        for (io.tapstate.core.sql.OutputField field : compiled.plan().outputFields()) {
            if (!field.name().equals(output)
                    || !(field.from() instanceof io.tapstate.core.sql.Expr.Column reference)) {
                continue;
            }
            TargetTable source =
                    bySourceTable.get(compiled.tableByName().get(reference.ref().source()));
            if (source == null) {
                return null;
            }
            for (TargetField candidate : source.fields()) {
                if (candidate.name().equals(reference.ref().column())) {
                    return candidate.type();
                }
            }
            return null;
        }
        return null;
    }

    private Map<String, List<String>> sourceKeysById(Map<String, SourceVertex> sourceVertices) {
        Map<String, List<String>> keys = new LinkedHashMap<>();
        for (Map.Entry<String, SourceVertex> entry : sourceVertices.entrySet()) {
            keys.computeIfAbsent(entry.getValue().sourceId(), ignored -> new java.util.ArrayList<>())
                    .add(entry.getKey());
        }
        return keys;
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
            SourceResource source = StoredArtifacts.requireSource(artifacts(), sourceId);
            SourceCaptureResolution resolution = SourceCaptureResolution.of(source, SourceDiscovery.model(storePort, source));
            for (String table : resolution.tables()) {
                chainIdByTable.put(table, resolution.chainId().value());
            }
        }
        return chainIdByTable;
    }

    /**
     * The leaf and reference bindings for the builder. The binding functions run on the assembly side as the
     * builder walks the topology; only the vertex suppliers they return travel onto the DAG, so they may
     * reach the store freely while what they produce stays serializable.
     */
    private DagBindings bindings(
            PipelineResource pipeline,
            Map<String, SourceVertex> sourceVertices,
            Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, TargetTable> targets,
            Set<String> serveStreams,
            Set<String> viewStreams,
            Set<String> stepIds,
            FrontierBinding frontier,
            Map<String, CompiledJoin> compiledJoins) {
        ChainAxes axes = frontier.axes();
        return new DagBindings(
                key -> sourceVertex(sourceVertices.get(key), axes),
                StoreBackedDagSource::transformPort,
                element -> sinkWriter(element, targets, serveStreams),
                ref -> upstreams(ref, sourceKeyByTable, sourceKeysById, sourceVertices, stepIds),
                sourceKeysById::get,
                view -> viewSink(pipeline, view, targets, viewStreams, sourceKeysById),
                nestBinding(pipeline, sourceIdByTable(sourceVertices)),
                joinBinding(compiledJoins));
    }

    /**
     * The sink-writer factory for a pipeline's view. It differs from a serve.sync element in exactly one
     * place: the element names the source it writes to, while a view does not name one at all - the
     * deployment's managed state store is resolved on the view's behalf. Everything after that is the
     * same seam the sync path uses, so a view is written by the same writer over the same binding.
     *
     * <p>Write mode and ddl policy take the sync defaults. A view converges on its key, which is what
     * upsert means; and the ddl policy governs how an incoming schema change is handled, not whether the
     * target may be created, so refusing to drift costs the materialization nothing.
     */
    private SupplierEx<? extends SinkWriter> viewSink(
            PipelineResource pipeline, ViewBlock view, Map<String, TargetTable> targets,
            Set<String> viewStreams, Map<String, List<String>> tablesBySourceId) {
        if (!(view instanceof ViewBlock.Inline inline)) {
            throw new IllegalArgumentException(
                    "view block is a use-reference; resolve it to an inline view first");
        }
        // Resolve first: it holds the simpler facts - a missing key among them - and a view without a
        // key has nothing for the identity gate to compare. Review found the reverse order turning the
        // coded missing-key refusal into a bare NullPointerException inside the gate.
        ViewTargetResolver.ViewTarget target = ViewTargetResolver.resolve(inline);
        requireKeyIsTheFeedIdentity(pipeline, inline, targets, tablesBySourceId);
        // Coded rather than bare, unlike a source the author named: this store is the deployment's, so
        // its absence is a condition an operator acts on rather than a defect on this side.
        SourceResource store = artifacts().get(target.sourceId())
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .orElseThrow(() -> new TapstateException(ActuationError.VIEW_STORE_NOT_CONFIGURED,
                        Map.of("store", target.sourceId()), null));
        // Resolved by id alone, so a source the author happened to give that id would satisfy the lookup
        // - and be written into. Capture settings are what tells an authored source from the store; a
        // plain connection under the id is indistinguishable today, which is a narrower, recorded gap.
        if (store.mode() != null || store.tables() != null) {
            throw new TapstateException(ActuationError.VIEW_STORE_IS_A_CAPTURE_SOURCE,
                    Map.of("store", target.sourceId()), null);
        }
        // Last of the three, and in this order deliberately: the two above are answered from the store's
        // own record and cost nothing, so a misconfiguration is named without ever touching the network.
        // Only once the resource is known to be the deployment's store is it worth asking whether it
        // answers.
        storeReachability.requireReachable(
                target.sourceId(), store.connector(), store.config());
        // Keyed by every source table that can reach the view, all answering with the one collection.
        // The sink resolves a target by the table a row came from, so a view - which collapses those
        // tables into a single object - has to answer to each of their names. Keyed by the view's own
        // name instead, every lookup misses and the rows land under the source table: the right rows,
        // silently in the wrong collection, which no topology assertion can see.
        Map<String, TargetTable> bySourceTable = new LinkedHashMap<>();
        for (String sourceTable : viewStreams) {
            bySourceTable.put(sourceTable,
                    viewTargetTable(target, targets == null ? null : targets.get(sourceTable)));
        }
        return sinkWriterBinder.bind(
                store.connector(), store.config(), WriteMode.UPSERT, DdlPolicy.FAIL, bySourceTable);
    }

    /**
     * Refuses a view whose single key is not the identity of what feeds it, before anything binds.
     *
     * <p>The view sink upserts every stream on the view's declared key and indexes it uniquely, so the
     * key has to be what the feed converges on. Two shapes break that and neither says anything at
     * write time: an assembly keyed on more columns than the view's key collapses distinct roots onto
     * one document, and several tables feeding one view take turns overwriting each other wherever
     * their key values coincide. Both land rows in the right collection with a right-looking count on
     * any single snapshot, which is why they are refused here by name instead.
     *
     * <p>What feeds the view is resolved by walking its from-reference down to leaves: a nest step is
     * one assembled stream carrying its root's key, a source id is each of its tables, anything else
     * is one table. A regex names many upstreams by construction and is refused as such.
     */
    private static void requireKeyIsTheFeedIdentity(PipelineResource pipeline, ViewBlock.Inline view,
            Map<String, TargetTable> targets, Map<String, List<String>> tablesBySourceId) {
        List<String> streams = new ArrayList<>();
        List<TransformBody.Nest> assemblies = new ArrayList<>();
        collectFeed(pipeline, view.from(), tablesBySourceId, streams, assemblies, new HashSet<>());
        if (streams.size() + assemblies.size() > 1) {
            throw new TapstateException(ActuationError.VIEW_FED_BY_MANY_TABLES,
                    Map.of("view", view.id(), "tables", String.join(", ", streams)), null);
        }
        if (assemblies.size() == 1) {
            requireKeyIs(view, assemblies.getFirst().root().key());
            return;
        }
        // A single table: its identity is whatever discovery recorded. An undiscovered table has no
        // identity on record, and the view's own key is then the only identity there is - which is the
        // path that lets materialization run before any discovery has.
        if (streams.size() == 1 && targets != null) {
            TargetTable model = targets.get(streams.getFirst());
            if (model != null) {
                List<String> identity = model.fields().stream()
                        .filter(TargetField::primaryKey).map(TargetField::name).toList();
                if (!identity.isEmpty()) {
                    requireKeyIs(view, identity);
                }
            }
        }
    }

    /** One refusal for every feed shape: the view's single key must be exactly this identity. */
    private static void requireKeyIs(ViewBlock.Inline view, List<String> identity) {
        if (identity == null || !identity.equals(List.of(view.primaryKey()))) {
            throw new TapstateException(ActuationError.VIEW_KEY_NOT_FEED_IDENTITY,
                    Map.of("view", view.id(), "key", String.valueOf(view.primaryKey()),
                            "identity", identity == null ? "(none)" : String.join(", ", identity)),
                    null);
        }
    }

    /** Resolves one from-reference to the leaf streams it names; see the gate above for the reading. */
    private static void collectFeed(PipelineResource pipeline, FromRef from,
            Map<String, List<String>> tablesBySourceId, List<String> streams,
            List<TransformBody.Nest> assemblies, Set<String> visited) {
        if (!(from instanceof FromRef.Literal literal)) {
            // A regex is many upstreams by construction; two entries make the count say so.
            streams.add(from.toString());
            streams.add(from.toString());
            return;
        }
        String ref = literal.ref();
        if (!visited.add(ref)) {
            return;
        }
        Step step = stepOf(pipeline, ref);
        if (step != null) {
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest) {
                assemblies.add(nest);
                return;
            }
            // A join is one stream of its own, not the tables under it. Passing through counted its
            // sources instead and refused every join that feeds a view as "fed by many tables" - a
            // shape the product's own valid corpus writes, so the validator accepted a pipeline the
            // builder then would not build. Its identity is the target model registered for the step,
            // which the branch below reads like any other single stream's.
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Join) {
                streams.add(step.id());
                return;
            }
            // A plain transform passes through whatever feeds it.
            for (FromRef upstream : refsOf(step.from())) {
                collectFeed(pipeline, upstream, tablesBySourceId, streams, assemblies, visited);
            }
            return;
        }
        List<String> sourceTables = tablesBySourceId.get(ref);
        if (sourceTables != null) {
            streams.addAll(sourceTables);
            return;
        }
        streams.add(ref);
    }

    private static Step stepOf(PipelineResource pipeline, String id) {
        if (pipeline.transforms() == null) {
            return null;
        }
        for (Step step : pipeline.transforms()) {
            if (step.id().equals(id)) {
                return step;
            }
        }
        return null;
    }

    private static List<FromRef> refsOf(FromClause from) {
        if (from instanceof FromClause.Flow flow) {
            return flow.refs();
        }
        if (from instanceof FromClause.Aliases aliases) {
            return List.copyOf(aliases.aliases().values());
        }
        return List.of();
    }

    /** The source artifacts whose rows can reach one terminal reference. */
    private static Set<String> sourceIdsReaching(
            PipelineResource pipeline,
            FromRef from,
            Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, SourceVertex> sourceVertices,
            Set<String> stepIds) {
        Set<String> sourceIds = new LinkedHashSet<>();
        collectSourceIds(pipeline, from, sourceKeyByTable, sourceKeysById, sourceVertices,
                stepIds, sourceIds, new HashSet<>());
        return sourceIds;
    }

    /** Walks a terminal reference backwards through transforms and views to its source leaves. */
    private static void collectSourceIds(
            PipelineResource pipeline,
            FromRef from,
            Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, SourceVertex> sourceVertices,
            Set<String> stepIds,
            Set<String> sourceIds,
            Set<String> visiting) {
        ViewBlock.Inline view = inlineViewNamed(pipeline, from);
        if (view != null) {
            collectSourceIds(pipeline, view.from(), sourceKeyByTable, sourceKeysById, sourceVertices,
                    stepIds, sourceIds, visiting);
            return;
        }
        for (String key : upstreams(
                from, sourceKeyByTable, sourceKeysById, sourceVertices, stepIds)) {
            SourceVertex source = sourceVertices.get(key);
            if (source != null) {
                sourceIds.add(source.sourceId());
                continue;
            }
            Step step = stepOf(pipeline, key);
            if (step != null && visiting.add(key)) {
                for (FromRef upstream : refsOf(step.from())) {
                    collectSourceIds(pipeline, upstream, sourceKeyByTable, sourceKeysById,
                            sourceVertices, stepIds, sourceIds, visiting);
                }
            }
        }
    }

    /** The stream ids a terminal sink can receive: source tables, or a nest step's assembled stream id. */
    private static Set<String> streamsReaching(
            PipelineResource pipeline,
            FromRef from,
            Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, SourceVertex> sourceVertices,
            Set<String> stepIds) {
        Set<String> streams = new LinkedHashSet<>();
        collectStreams(pipeline, from, sourceKeyByTable, sourceKeysById, sourceVertices,
                stepIds, streams, new HashSet<>());
        return streams;
    }

    /** Resolves the stream names preserved through stateless steps and replaced by a nest assembly. */
    private static void collectStreams(
            PipelineResource pipeline,
            FromRef from,
            Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, SourceVertex> sourceVertices,
            Set<String> stepIds,
            Set<String> streams,
            Set<String> visiting) {
        ViewBlock.Inline view = inlineViewNamed(pipeline, from);
        if (view != null) {
            collectStreams(pipeline, view.from(), sourceKeyByTable, sourceKeysById, sourceVertices,
                    stepIds, streams, visiting);
            return;
        }
        for (String key : upstreams(
                from, sourceKeyByTable, sourceKeysById, sourceVertices, stepIds)) {
            SourceVertex source = sourceVertices.get(key);
            if (source != null) {
                streams.add(source.table());
                continue;
            }
            Step step = stepOf(pipeline, key);
            if (step == null) {
                continue;
            }
            // A nest and a join both replace what feeds them with a stream of their own, emitted under
            // the step's id. Passing through to the upstream tables instead would hand the sink their
            // models - the wrong shape and the wrong key - for rows that are neither.
            if (step instanceof Step.Inline inline
                    && (inline.body() instanceof TransformBody.Nest
                            || inline.body() instanceof TransformBody.Join)) {
                streams.add(step.id());
                continue;
            }
            if (visiting.add(key)) {
                for (FromRef upstream : refsOf(step.from())) {
                    collectStreams(pipeline, upstream, sourceKeyByTable, sourceKeysById,
                            sourceVertices, stepIds, streams, visiting);
                }
            }
        }
    }

    /** A declared view is a data alias for what it reads, not a producer vertex of its own. */
    private static ViewBlock.Inline inlineViewNamed(PipelineResource pipeline, FromRef from) {
        if (from instanceof FromRef.Literal literal
                && pipeline.view() instanceof ViewBlock.Inline view
                && view.id().equals(literal.ref())) {
            return view;
        }
        return null;
    }

    /**
     * The target model one stream materializes under: the view's resolved collection and indexes, carrying
     * that stream's own fields. Answered per stream rather than once for the view, because the fields are
     * where the key lives and the key is what an upsert converges on - and the streams reaching one view
     * do not share one. A nest's assembled documents are keyed on the root's key, which is a different
     * column list from any single source table's, so one descriptor shared across every stream can carry
     * at most one of them right. Collapsing them instead costs the key entirely: the documents land in the
     * right collection with nothing to match on, and every re-sent root accumulates beside the one it
     * should have replaced. This is the shape the serve path already resolves per stream.
     */
    private static TargetTable viewTargetTable(
            ViewTargetResolver.ViewTarget target, TargetTable stream) {
        List<TargetField> streamFields = stream == null ? List.of() : stream.fields();
        List<TargetField> fields = new ArrayList<>(streamFields.size() + 1);
        // The key first and always, carrying the stream's type for it when the stream declares one. A
        // view names its own key, so it has one to be matched on before any discovery has run - and a
        // type it could not resolve is left for the connector to infer rather than standing in the way.
        fields.add(new TargetField(target.primaryKey(), typeOf(streamFields, target.primaryKey()), true));
        for (TargetField field : streamFields) {
            if (!field.name().equals(target.primaryKey())) {
                fields.add(new TargetField(field.name(), field.type(), false));
            }
        }
        return new TargetTable(target.collection(), fields, target.indexes());
    }

    /** The stream's own type token for one column, or null when the stream does not declare it. */
    private static String typeOf(List<TargetField> fields, String name) {
        for (TargetField field : fields) {
            if (field.name().equals(name)) {
                return field.type();
            }
        }
        return null;
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
        Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable(sourceVertices(pipeline)));
        return new NestCapacity(PipelineDagBuilder.nestStateNamespaces(pipeline, byAlias::get),
                PipelineDagBuilder.nestSettings(pipeline, byAlias::get, nestSettings));
    }

    /**
     * What a join node needs that the engine will not work out: its SQL compiled into a plan, the key
     * the driving source's rows are identified by, and where the state lives.
     *
     * <p>Compiled here rather than on the member, and once rather than per vertex. The library that
     * parses and validates SQL is granted to one core module and the runtime ring cannot see it, so a
     * plan built member-side would mean putting that library where the ring rules say it may not go -
     * and building it twice would mean two answers to the same question with nothing comparing them.
     *
     * <p>A pipeline with no join step compiles nothing and asks nothing of the schema store.
     */
    private JoinBinding joinBinding(Map<String, CompiledJoin> byStep) {
        return new JoinBinding(
                step -> compiledJoin(byStep, step).plan(),
                step -> compiledJoin(byStep, step).factKeyColumns(),
                JoinStoresBinding.onTheCluster());
    }

    /** Every join step of this pipeline, compiled, keyed by step id; empty where there is no join. */
    private Map<String, CompiledJoin> compiledJoins(
            PipelineResource pipeline, Map<String, String> sourceIdByTable) {
        Map<String, CompiledJoin> byStep = new LinkedHashMap<>();
        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                if (step instanceof Step.Inline inline
                        && inline.body() instanceof TransformBody.Join join) {
                    byStep.put(step.id(), compileJoin(inline, join, sourceIdByTable));
                }
            }
        }
        return byStep;
    }

    private static CompiledJoin compiledJoin(Map<String, CompiledJoin> byStep, Step step) {
        CompiledJoin compiled = byStep.get(step.id());
        if (compiled == null) {
            throw new IllegalStateException("no join was compiled for step '" + step.id() + "'");
        }
        return compiled;
    }

    /**
     * One join step's plan and the key its driving rows are filed under.
     *
     * <p>Each alias the step declares is registered under both the name the author aliased it to and
     * the table it reads, because the SQL may name either: {@code FROM orders o} and {@code FROM o}
     * are both written, and the plan's source name comes out as the alias in both spellings - which is
     * what the graph then resolves the upstream vertex by.
     *
     * <p>Columns are reported nullable whatever the source said, because a discovered field carries no
     * nullability. That widens the output row's declared types and never narrows them: claiming NOT
     * NULL for a column that turns out to hold one is the direction that produces a wrong promise.
     */
    private CompiledJoin compileJoin(Step.Inline step, TransformBody.Join join,
            Map<String, String> sourceIdByTable) {
        Map<String, List<String>> keyByTable = new LinkedHashMap<>();
        Map<String, String> tableByName = new LinkedHashMap<>();
        List<io.tapstate.core.sql.SourceTable> tables = new ArrayList<>();
        if (step.from() instanceof FromClause.Aliases aliases) {
            aliases.aliases().forEach((alias, ref) -> {
                NestTable resolved = nestTable(ref, sourceIdByTable);
                List<io.tapstate.core.sql.SourceColumn> columns =
                        columnsOf(resolved.name(), sourceIdByTable);
                keyByTable.put(resolved.name(), resolved.primaryKey());
                keyByTable.put(alias, resolved.primaryKey());
                // Both spellings answer with the real table, because the plan calls a source whichever
                // of the two the SQL wrote and the target has to be named after the table either way.
                tableByName.put(resolved.name(), resolved.name());
                tableByName.put(alias, resolved.name());
                tables.add(new io.tapstate.core.sql.SourceTable(alias, columns));
                if (!alias.equals(resolved.name())) {
                    tables.add(new io.tapstate.core.sql.SourceTable(resolved.name(), columns));
                }
            });
        }
        io.tapstate.core.sql.JoinPlan plan =
                io.tapstate.core.sql.SqlFrontEnd.derive(join.sql(), List.copyOf(tables));
        String driving = plan.factSource().table();
        List<String> key = keyByTable.getOrDefault(driving, List.of());
        if (key.isEmpty()) {
            throw new TapstateException(ActuationError.JOIN_SOURCE_KEY_MISSING,
                    Map.of("step", step.id(), "table", driving), null);
        }
        return new CompiledJoin(plan, key, Map.copyOf(tableByName));
    }

    /** The columns of one table, in the shared type vocabulary the plan is derived against. */
    private List<io.tapstate.core.sql.SourceColumn> columnsOf(String table,
            Map<String, String> sourceIdByTable) {
        String sourceId = sourceIdByTable.get(table);
        if (sourceId == null) {
            return List.of();
        }
        return storePort.schemas().get(sourceId)
                .map(DiscoveredSourceModel::model)
                .flatMap(model -> model.tables().stream()
                        .filter(t -> t.name().equals(table)).findFirst())
                .map(t -> t.fields().stream()
                        .map(f -> new io.tapstate.core.sql.SourceColumn(f.name(), f.type(), true))
                        .toList())
                .orElse(List.of());
    }

    /**
     * One join step's plan, the key its driving rows are filed under, and the real table behind each
     * name the plan calls a source by - alias and table name both, since the SQL may write either.
     */
    private record CompiledJoin(io.tapstate.core.sql.JoinPlan plan, List<String> factKeyColumns,
            Map<String, String> tableByName) {
    }

    private NestBinding nestBinding(PipelineResource pipeline, Map<String, String> sourceIdByTable) {
        Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable);
        return new NestBinding(byAlias::get, NestBinding.onMap(),
                new LoggingNestDeadLetter(new DurableNestDeadLetter()),
                new StoreBackedReplayFloorFactory(chainIdByTable(pipeline), pipeline.id()),
                new StoreBackedNestStateLedger(storePort.keyedState()),
                // What the deployment was started with, with what this pipeline's author wrote over it.
                // Laid on here rather than held as one value for the process because the shape each
                // number bounds is the pipeline's, not the process's: one tree is deep and narrow and
                // the next is shallow and wide, and a single number covers neither.
                PipelineDagBuilder.nestSettings(pipeline, byAlias::get, nestSettings),
                NestClock.SYSTEM);
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
     * The source vertex for one selected table: it resolves the source's connector, config and per-table
     * change ring the capture side writes. The stream name projected into each event is the table name.
     * Resolving the same ring identity the capture side resolves is what points the reader at the ring the
     * writer fills.
     */
    private ProcessorMetaSupplier sourceVertex(SourceVertex vertex, ChainAxes axes) {
        if (vertex == null) {
            throw new IllegalStateException("source vertex binding is missing");
        }
        // The ring knows a generation and a sequence; which axis this stream travels on and how the pair
        // packs into the one long a bound rides are properties of the whole job, so they are closed over
        // here rather than reached for from the source.
        String chain = vertex.table();
        byte axis = axes.axisOf(chain);
        return SrsSourceProcessor.metaSupplier(
                vertex.resolution().ringName(vertex.table()), vertex.table(), StartFrom.earliest(),
                ringGeneration(vertex.resolution()),
                CaptureRunUnit.readCursorPublisher(
                        vertex.resolution().chainId().value(), vertex.pipelineId(), vertex.table()),
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
     * come from the element, defaulting to upsert and fail. The resolved target models are narrowed to the
     * streams that can reach this serve block; the start precondition guarantees each has a discovered model.
     * The bound factory carries only these serializable coordinates and opens the connector on the member
     * that runs the sink.
     */
    private SupplierEx<? extends SinkWriter> sinkWriter(
            SyncElement element, Map<String, TargetTable> targets, Set<String> serveStreams) {
        SourceResource sink = StoredArtifacts.requireSource(artifacts(), element.source());
        return sinkWriterBinder.bind(
                sink.connector(), sink.config(), writeMode(element.writeMode()), ddl(element.ddl()),
                TargetModelResolver.renameAll(targets, serveStreams, element.rename()));
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
     * <p>A regex reference expands selected source tables and transform step ids in declaration order. A
     * regex that expands to no producer is rejected before a broken DAG can be assembled.
     */
    private static List<String> upstreams(
            FromRef ref,
            Map<String, String> sourceIdByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, SourceVertex> sourceVertices,
            Set<String> stepIds) {
        if (ref instanceof FromRef.Literal literal) {
            List<String> sourceKeys = sourceKeysById.get(literal.ref());
            if (sourceKeys != null) {
                return List.copyOf(sourceKeys);
            }
            if (sourceVertices.containsKey(literal.ref())) {
                return List.of(literal.ref());
            }
            int dot = literal.ref().indexOf('.');
            if (dot > 0) {
                String sourceId = literal.ref().substring(0, dot);
                String table = literal.ref().substring(dot + 1);
                List<String> qualified = sourceKeysById.get(sourceId);
                if (qualified != null) {
                    List<String> matches = qualified.stream()
                            .filter(key -> sourceVertices.get(key).table().equals(table))
                            .toList();
                    if (matches.isEmpty()) {
                        throw new TapstateException(
                                ActuationError.SOURCE_TABLE_NOT_DISCOVERED,
                                Map.of("source", sourceId, "table", table), null);
                    }
                    return matches;
                }
            }
            return List.of(sourceIdByTable.getOrDefault(literal.ref(), literal.ref()));
        }
        FromRef.Regex regex = (FromRef.Regex) ref;
        final Pattern pattern;
        try {
            pattern = Pattern.compile(regex.pattern());
        } catch (PatternSyntaxException exception) {
            throw new TapstateException(
                    ActuationError.FROM_REGEX_INVALID, Map.of("regex", regex.pattern()), exception);
        }
        LinkedHashSet<String> matches = new LinkedHashSet<>();
        for (Map.Entry<String, SourceVertex> entry : sourceVertices.entrySet()) {
            SourceVertex vertex = entry.getValue();
            if (pattern.matcher(vertex.table()).matches()
                    || pattern.matcher(entry.getKey()).matches()
                    || pattern.matcher(vertex.sourceId()).matches()) {
                matches.add(entry.getKey());
            }
        }
        for (String stepId : stepIds) {
            if (pattern.matcher(stepId).matches()) {
                matches.add(stepId);
            }
        }
        if (matches.isEmpty()) {
            throw new TapstateException(ActuationError.FROM_REGEX_EMPTY, Map.of("regex", regex.pattern()), null);
        }
        return List.copyOf(matches);
    }

    private static Set<String> stepIds(PipelineResource pipeline) {
        if (pipeline.transforms() == null) {
            return Set.of();
        }
        return pipeline.transforms().stream().map(Step::id).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
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

        default SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                Map<String, TargetTable> targets) {
            return bind(connectorId, settings, writeMode, ddl,
                    targets.size() == 1 ? targets.values().iterator().next() : null);
        }
    }

    static final class PdkSinkWriterBinder implements SinkWriterBinder {

        @Override
        public SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                TargetTable target) {
            return new PdkSinkWriterFactory(connectorId, settings, writeMode, ddl, target);
        }

        @Override
        public SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                Map<String, TargetTable> targets) {
            return new PdkSinkWriterFactory(connectorId, settings, writeMode, ddl, targets);
        }
    }
}
