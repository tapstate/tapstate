package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Watermark;
import io.tapstate.adapters.pdk.ConnectorStateNamespace;
import io.tapstate.adapters.transform.MapSpec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.core.lifecycle.PipelineStateInventory;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineNode;
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
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkPreparationNamespace;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
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
import java.util.function.Function;
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
    private final JoinSchemaDrift joinSchemaDrift;
    private final SourceSchemaCopy sourceSchemaCopy;
    private final StepSchemaRecord stepSchemaRecord;

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
        this.joinSchemaDrift = new JoinSchemaDrift(this.storePort.derivedSchemas());
        this.sourceSchemaCopy = new SourceSchemaCopy(this.storePort.derivedSchemas());
        this.stepSchemaRecord = new StepSchemaRecord(this.storePort.derivedSchemas());
    }

    @Override
    public DAG dagFor(String pipelineId) {
        // Expanded before anything reads the blocks, so every later step - target resolution included -
        // sees one shape rather than having to know a reference from a body.
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        Map<String, SourceVertex> sourceVertices = sourceVertices(pipeline);
        // The pipeline takes its own copy of what discovery found for each table it reads, before anything
        // downstream is worked out from it. Reading the discovery directly instead would let a
        // re-discovery change the shape of this run's input while the run is already using it.
        List<String> derivedSteps = new ArrayList<>(copySourceSchemas(pipelineId, sourceVertices));
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
        // Held to the columns it was last recorded producing, before anything is built out of them. A
        // join's output columns follow from the SELECT and from what the sources say their columns are,
        // so the same pipeline can start one day producing a differently shaped row than the day before
        // - and every write of that row succeeds, which is why the difference has to be caught here or
        // not at all.
        compiledJoins.forEach((stepId, compiled) -> joinSchemaDrift.checkAndRecord(
                pipelineId, stepId, compiled.body(), compiled.plan(), compiled.tables()));
        // What this run will be holding on to, written down now that every step's shape is recorded and
        // the gate above has let the start through. Nothing re-reads a derived schema once the job is
        // submitted, so without this note a reader asking what the running pipeline produces answers
        // with whatever was recorded most recently instead - the two agree right up to the moment
        // somebody records a new shape, which is the only moment the question is worth asking.
        derivedSteps.addAll(compiledJoins.keySet());
        // Every other step's own model, worked out from the copies above and from each other. Without
        // this a pipeline records what it reads and what it joins, and nothing for the steps in
        // between - so a reader asking what it produces at one of those cannot tell a step whose model
        // was never derived from a step that could not be described.
        derivedSteps.addAll(recordStepSchemas(pipelineId,
                deriveSteps(pipeline, sourceVertices, sourceKeyByTable, sourceKeysById, stepIds,
                        compiledJoins, vertex -> copiedColumns(pipelineId, vertex))));
        pinWhatThisRunHolds(pipelineId, derivedSteps);
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
        // Each stream a sink receives, narrowed to what the pipeline actually publishes on it rather
        // than to what its source table holds. Only the serve terminal is narrowed here: a view
        // composes its own descriptor around the key it names, and a stream reaching both terminals
        // would otherwise be answered twice with nothing saying which answer the map holds.
        if (pipeline.serve() instanceof ServeBlock.Inline serving && !serveStreams.isEmpty()) {
            targets.putAll(publishedTargets(pipelineId, pipeline, serving.from(), serveStreams,
                    bySourceTable, sourceVertices, sourceKeyByTable, sourceKeysById, stepIds));
        }
        requireFactKeyPublishedWhereAWriteMatchesOnIt(pipeline, compiledJoins, serveStreams);
        FrontierBinding frontier = frontierBinding(sourceVertices);
        return PipelineDagBuilder.build(
                pipeline,
                bindings(pipeline, sourceVertices, sourceKeyByTable, sourceKeysById, targets,
                        serveStreams, viewStreams, stepIds, frontier, compiledJoins),
                sinkAckFactory(pipeline, pipelineId), frontier);
    }

    /**
     * Where this pipeline keeps state: the namespaces its compiled nest and join vertices hold entries
     * in, the nest-shape record, and each PDK connector's exact node namespace. Operator records go with
     * the state they describe - kept behind, they would make the next use of this pipeline id inherit
     * rows and shapes the current sources no longer hold.
     *
     * <p>The tree is compiled again here rather than remembered from the build, for the same reason the
     * build compiles it rather than reading it back: the names come from the tree, so the tree is what is
     * asked. A pipeline with no nest or join has no operator-state holding, but still names connector
     * state for its capture sources and sinks because those connectors can keep notes between runs.
     */
    @Override
    public List<PipelineStateHolding> stateHeldBy(String pipelineId) {
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        List<PipelineStateHolding> holdings = new ArrayList<>();

        // Read joins off the wiring before resolving source models. A stop must still name their mirrors
        // and reverse indexes when the sources behind the query have gone undiscoverable.
        Set<String> operatorNamespaces = new LinkedHashSet<>(PipelineDagBuilder.joinStateNamespaces(pipeline));
        if (PipelineDagBuilder.hasNest(pipeline)) {
            Map<String, NestTable> byAlias = nestTablesByAlias(pipeline, sourceIdByTable(sourceVertices(pipeline)));
            operatorNamespaces.addAll(PipelineDagBuilder.nestStateNamespaces(pipeline, byAlias::get));
            operatorNamespaces.add(StoreBackedNestStateLedger.namespaceOf(pipelineId));
        }
        if (!operatorNamespaces.isEmpty()) {
            holdings.add(PipelineStateInventory.OPERATOR_STATE.in(operatorNamespaces));
        }
        holdings.add(PipelineStateInventory.CONNECTOR_STATE.in(connectorStateNamespaces(pipeline)));
        return List.copyOf(holdings);
    }

    /**
     * The exact PDK state namespaces the assembled pipeline can open: its capture sources and every sink
     * the DAG builds. The PDK owns the namespace spelling, while this layer owns which pipeline nodes the
     * runtime can open; combining the two keeps a stop's inventory identical to the runtime's identities.
     */
    private static Set<String> connectorStateNamespaces(PipelineResource pipeline) {
        Set<String> namespaces = new LinkedHashSet<>();
        pipeline.sources().forEach(source -> namespaces.add(
                ConnectorStateNamespace.of(new PipelineNode(pipeline.id(), source.id()))));
        if (pipeline.view() instanceof ViewBlock.Inline view) {
            PipelineNode node = new PipelineNode(pipeline.id(), view.id());
            namespaces.add(ConnectorStateNamespace.of(node));
            namespaces.add(SinkPreparationNamespace.of(node));
        }
        if (pipeline.serve() instanceof ServeBlock.Inline serve && serve.sync() != null) {
            serve.sync().forEach(sync -> {
                PipelineNode node = new PipelineNode(pipeline.id(), syncNodeId(sync));
                namespaces.add(ConnectorStateNamespace.of(node));
                namespaces.add(SinkPreparationNamespace.of(node));
            });
        }
        return namespaces;
    }

    private record SourceVertex(
            String pipelineId, String sourceId, String table, SourceCaptureResolution resolution) {
    }

    /**
     * Records this pipeline's own copy of each source table it reads. One vertex is one selected table, so
     * a source reading several tables leaves one copy per table rather than one for the source - the same
     * reason the chain binding is keyed per vertex, and the reason the id carries the table.
     *
     * <p>A table nothing has discovered is skipped rather than recorded empty. Authoring against an
     * undiscovered source is allowed, and a start that actually needs the model refuses by name before it
     * binds anything.
     */
    private List<String> copySourceSchemas(String pipelineId, Map<String, SourceVertex> sourceVertices) {
        List<String> copied = new ArrayList<>();
        for (SourceVertex vertex : sourceVertices.values()) {
            SourceResource source = StoredArtifacts.requireSource(artifacts(), vertex.sourceId());
            SourceModel discovered = SourceDiscovery.model(storePort, source);
            if (sourceSchemaCopy.copy(pipelineId, vertex.sourceId(), vertex.table(),
                    discovered == null ? null : discoveredTable(discovered, vertex.table()))) {
                copied.add(SourceSchemaCopy.nodeId(vertex.sourceId(), vertex.table()));
            }
        }
        return copied;
    }

    /**
     * Re-copies the physical model of every table this pipeline reads, without assembling anything else.
     * This is the whole of what a re-copy is - the physical model is the truth, and a pipeline's source
     * nodes hold a copy of it - so the paths that re-copy outside a start share this one rather than
     * each walking the pipeline their own way.
     *
     * <p>Nothing is pinned here. A pin says what a run is holding, and none of the callers of this is a
     * run: pinning from one would tell a reader that a job which never saw these columns is producing
     * them.
     */
    List<String> copySourceSchemas(String pipelineId) {
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        List<String> copied = new ArrayList<>();
        for (String sourceId : pipeline.sourceIds()) {
            SourceResource source = StoredArtifacts.requireSource(artifacts(), sourceId);
            SourceModel discovered = SourceDiscovery.model(storePort, source);
            // Applying an undiscovered source records nothing, including when discovery is needed
            // to expand an omitted or regex table selector. A start still requires capture resolution.
            if (discovered == null) {
                continue;
            }
            for (String table : SourceTableSelection.resolve(source, discovered)) {
                if (sourceSchemaCopy.copy(pipelineId, sourceId, table, discoveredTable(discovered, table))) {
                    copied.add(SourceSchemaCopy.nodeId(sourceId, table));
                }
            }
        }
        return copied;
    }

    /**
     * Writes down which recorded version of each derived step this run was assembled from. Read back
     * only while a run exists; overwritten by the next assembly, so nothing has to clear it.
     */
    private void pinWhatThisRunHolds(String pipelineId, List<String> stepIds) {
        for (String stepId : stepIds) {
            storePort.derivedSchemas().latest(pipelineId, stepId).ifPresent(recorded ->
                    storePort.derivedSchemas().pin(pipelineId, stepId, recorded.version()));
        }
    }

    /** The named table in a discovered model, or null when the model does not carry it. */
    private static SourceTable discoveredTable(SourceModel model, String table) {
        return model.tables().stream().filter(t -> t.name().equals(table)).findFirst().orElse(null);
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
        return sourceVertices(pipeline, false);
    }

    private Map<String, SourceVertex> sourceVertices(PipelineResource pipeline, boolean skipUndiscovered) {
        Map<String, SourceVertex> vertices = new LinkedHashMap<>();
        for (String sourceId : pipeline.sourceIds()) {
            SourceResource source = StoredArtifacts.requireSource(artifacts(), sourceId);
            SourceModel discovered = SourceDiscovery.model(storePort, source);
            if (skipUndiscovered && discovered == null) {
                continue;
            }
            SourceCaptureResolution resolution = SourceCaptureResolution.of(source, discovered);
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
     * The targets a serve block's sinks are built from: each stream as the pipeline publishes it, not as
     * its source table holds it.
     *
     * <p><b>A target is created to the shape the rows arriving at it actually have.</b> Built from the
     * source table instead, it carries a column for every column the table has - including the ones a
     * step between them stopped forwarding - and nothing reports that: every row still arrives, every
     * write still succeeds, and the column simply sits empty in a table somebody later reads as data.
     * The columns and their order come from the pipeline's own copy of the source model, worked forward
     * through each step the stream passes, which is what makes this the copy's first reader.
     *
     * <p>A stream nobody can describe is left as it was. An unknown is not a claim that the rows are
     * shapeless - it is the absence of one - and narrowing a target to it would remove every column
     * from a table whose rows still carry them.
     */
    private Map<String, TargetTable> publishedTargets(
            String pipelineId, PipelineResource pipeline, FromClause from, Set<String> streams,
            Map<String, TargetTable> bySourceTable, Map<String, SourceVertex> sourceVertices,
            Map<String, String> sourceKeyByTable, Map<String, List<String>> sourceKeysById,
            Set<String> stepIds) {
        Map<String, TargetTable> published = new LinkedHashMap<>();
        for (String stream : streams) {
            TargetTable base = bySourceTable.get(stream);
            if (base == null) {
                continue;
            }
            Map<String, NodeColumns> inputs = new LinkedHashMap<>();
            for (FromRef ref : refsOf(from)) {
                NodeColumns columns = streamColumnsAt(pipelineId, pipeline, ref, stream, sourceVertices,
                        sourceKeyByTable, sourceKeysById, stepIds, new HashSet<>(), base);
                if (columns != null) {
                    inputs.put(Integer.toString(inputs.size()), columns);
                }
            }
            NodeColumns produced = inputs.size() == 1 ? inputs.values().iterator().next()
                    : NodeColumns.of(new TransformBody.Union(), inputs, null);
            if (produced != null && produced.known()) {
                published.put(stream, publishedAs(base, produced));
            } else if (produced != null) {
                // A script can replace values under their old names. Unknown lineage cannot reuse
                // source type attributes or secondary uniqueness merely because names remain unchanged.
                published.put(stream, TargetModelResolver.keyedOn(new TargetTable(base.name(), base.fields().stream()
                        .map(field -> new TargetField(field.name(), field.type(), field.primaryKey(), field.inferredType()))
                        .toList(), List.of()), base.fields().stream().filter(TargetField::primaryKey)
                        .map(TargetField::name).toList()));
            }
        }
        return published;
    }

    /**
     * What one source table's rows carry where a terminal reference reads them, or null when that
     * stream does not reach it as itself.
     *
     * <p><b>Worked out per stream rather than per node.</b> A step that merges several streams has one
     * model, but the sink resolves a target by the table a row came from - and a merge forwards each
     * row as it arrives rather than reshaping it to the merged model - so the shape that matters here
     * is what this one table's rows look like at that point, which is the merged model's answer only
     * when the merge has one input.
     *
     * <p>A nest and a join publish a stream of their own under the step id, so a source table's stream
     * does not travel past one as itself; those two are registered separately from their compiled
     * output and are not reached from here.
     */
    private NodeColumns streamColumnsAt(
            String pipelineId, PipelineResource pipeline, FromRef from, String stream,
            Map<String, SourceVertex> sourceVertices, Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById, Set<String> stepIds, Set<String> visiting, TargetTable base) {
        ViewBlock.Inline view = inlineViewNamed(pipeline, from);
        if (view != null) {
            NodeColumns upstream = streamColumnsAt(pipelineId, pipeline, view.from(), stream,
                    sourceVertices, sourceKeyByTable, sourceKeysById, stepIds, visiting, base);
            return upstream == null ? null : NodeColumns.of(view, upstream);
        }
        List<NodeColumns> reached = new ArrayList<>();
        for (String key : upstreams(from, sourceKeyByTable, sourceKeysById, sourceVertices, stepIds)) {
            SourceVertex vertex = sourceVertices.get(key);
            if (vertex != null) {
                if (vertex.table().equals(stream)) {
                    NodeColumns copied = copiedColumns(pipelineId, vertex);
                    if (copied != null) {
                        Map<String, io.tapstate.core.common.NumericType> numbers = new LinkedHashMap<>();
                        base.fields().stream().filter(field -> field.numericType() != null)
                                .forEach(field -> numbers.put(field.name(), field.numericType()));
                        Map<String, io.tapstate.core.common.StringType> strings = new LinkedHashMap<>();
                        base.fields().stream().filter(field -> field.stringType() != null)
                                .forEach(field -> strings.put(field.name(), field.stringType()));
                        reached.add(copied.withNumericTypes(numbers).withStringTypes(strings));
                    }
                }
                continue;
            }
            if (!(stepOf(pipeline, key) instanceof Step.Inline inline)
                    || inline.body() instanceof TransformBody.Nest
                    || inline.body() instanceof TransformBody.Join
                    || !visiting.add(key)) {
                continue;
            }
            try {
                Map<String, NodeColumns> inputs = new LinkedHashMap<>();
                for (FromRef upstreamRef : refsOf(inline.from())) {
                    NodeColumns upstream = streamColumnsAt(pipelineId, pipeline, upstreamRef, stream,
                            sourceVertices, sourceKeyByTable, sourceKeysById, stepIds, visiting, base);
                    if (upstream != null) {
                        inputs.put(Integer.toString(inputs.size()), upstream);
                    }
                }
                if (!inputs.isEmpty()) {
                    reached.add(NodeColumns.of(inline.body(), inputs, null));
                }
            } finally {
                // A shared ancestor must be visited again from another fork. Only cycles on the
                // current path are excluded, otherwise a later branch can silently lose its model.
                visiting.remove(key);
            }
        }
        return reached.isEmpty() ? null : NodeColumns.merged(reached);
    }

    /**
     * Derives what every transform step of this pipeline produces and files it beside the pipeline,
     * answering the step ids that got a record.
     *
     * <p><b>A second walk over the same graph, deliberately.</b> The one above answers what a single
     * source table's rows look like where a terminal reads them, because that is what a target table is
     * built to; this one answers what a node produces, which is what a reader asking about the
     * pipeline's shape is asking. A step merging two streams has one answer to this question and one
     * per stream to the other, so neither walk can be made to serve both without one of them becoming
     * wrong.
     *
     * <p><b>A join is derived here and recorded elsewhere.</b> The drift check above records it, having
     * first held it to what it produced before; recording it again here would append a second version
     * of a shape that did not move. It is still derived, because a step reading a join needs its
     * columns.
     *
     * <p><b>Fixed point rather than a topological sort.</b> Steps are declared in whatever order the
     * author wrote them, so a single pass can meet a step before its upstream. Passing until nothing
     * new resolves reaches the same answer without anything having to order the graph, and leaves a
     * step whose inputs never resolve simply unrecorded - which is the same reading a source table
     * nothing has discovered gets.
     */
    // ponytail: O(steps^2) worst case; a pipeline with enough steps for that to be felt wants a
    // topological order instead.
    private StepDerivations deriveSteps(
            PipelineResource pipeline, Map<String, SourceVertex> sourceVertices,
            Map<String, String> sourceKeyByTable, Map<String, List<String>> sourceKeysById,
            Set<String> stepIds, Map<String, CompiledJoin> compiledJoins,
            Function<SourceVertex, NodeColumns> sourceColumns) {
        return deriveSteps(pipeline, sourceVertices, sourceKeyByTable, sourceKeysById,
                stepIds, compiledJoins, sourceColumns, false);
    }

    private StepDerivations deriveSteps(
            PipelineResource pipeline, Map<String, SourceVertex> sourceVertices,
            Map<String, String> sourceKeyByTable, Map<String, List<String>> sourceKeysById,
            Set<String> stepIds, Map<String, CompiledJoin> compiledJoins,
            Function<SourceVertex, NodeColumns> sourceColumns, boolean incompleteSources) {
        List<Step.Inline> steps = new ArrayList<>();
        for (Step step : pipeline.transforms() == null ? List.<Step>of() : pipeline.transforms()) {
            if (step instanceof Step.Inline inline) {
                steps.add(inline);
            }
        }
        Map<String, NodeColumns> derived = new LinkedHashMap<>();
        // Seeded with the joins so that a step reading one can be worked out; they carry no entry in
        // the inputs below and so are not recorded again here.
        compiledJoins.forEach((stepId, compiled) ->
                derived.put(stepId, NodeColumns.of(compiled.body(), Map.of(), compiled.plan())));
        Map<String, Recordable> recordable = new LinkedHashMap<>();
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            for (Step.Inline step : steps) {
                if (derived.containsKey(step.id()) || step.body() instanceof TransformBody.Join) {
                    continue;
                }
                Map<String, NodeColumns> inputs = inputsOf(refsOf(step.from()), sourceVertices,
                        sourceKeyByTable, sourceKeysById, stepIds, derived, sourceColumns, incompleteSources);
                if (inputs == null
                        || (step.body() instanceof TransformBody.Nest nest
                                && !inputs.containsKey(nest.root().from()))) {
                    continue;
                }
                NodeColumns columns = NodeColumns.of(step.body(), inputs, null);
                // A node that cannot say what it emits carries what reached it, so the chain of models
                // does not stop here and take everything below it with it. Only where the answer is
                // unknown: a node that did work its columns out keeps them.
                boolean carried = !columns.known();
                derived.put(step.id(), carried ? NodeColumns.merged(inputs.values()) : columns);
                recordable.put(step.id(), new Recordable(inputs, step.body(), carried));
                progressed = true;
            }
        }
        // The view is a block beside the steps rather than one of them, so the loop above never
        // reaches it - and it stores the rows it is handed, which is a model of its own. Worked out
        // after the loop because every step it could read from has settled by then.
        if (pipeline.view() instanceof ViewBlock.Inline view) {
            Map<String, NodeColumns> inputs = inputsOf(List.of(view.from()), sourceVertices,
                    sourceKeyByTable, sourceKeysById, stepIds, derived, sourceColumns, incompleteSources);
            if (inputs != null) {
                derived.put(view.id(), NodeColumns.of(view, NodeColumns.merged(inputs.values())));
                recordable.put(view.id(), new Recordable(inputs, view, false));
            }
        }
        // Push delivery is not assembled, so serve.push definitions have no executing node to record.
        // Its format rules in NodeColumns do not imply runtime model coverage.
        return new StepDerivations(derived, recordable);
    }

    /**
     * Records each non-join node after an apply or explicit acceptance, using the same walk as start.
     * Source copies and join baselines have their own writers; this refresh neither rewrites those
     * baselines nor moves the versions held by an assembled run. Undiscovered inputs leave only the
     * affected branches unresolved, so an independent discovered branch still refreshes.
     */
    void refreshStepSchemas(String pipelineId) {
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        Map<String, SourceVertex> vertices = sourceVertices(pipeline, true);
        Map<String, String> keysByTable = sourceKeyByTable(vertices);
        Map<String, List<String>> keysBySource = sourceKeysById(vertices);
        Set<String> steps = stepIds(pipeline);
        boolean incomplete = keysBySource.size() < pipeline.sourceIds().size();
        Map<String, CompiledJoin> compiled = new LinkedHashMap<>();
        for (Step step : pipeline.transforms() == null ? List.<Step>of() : pipeline.transforms()) {
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Join join
                    && inputsOf(refsOf(inline.from()), vertices, keysByTable, keysBySource, steps,
                            Map.of(), vertex -> copiedColumns(pipelineId, vertex), incomplete) != null) {
                compiled.put(step.id(), compileJoin(inline, join, sourceIdByTable(vertices)));
            }
        }
        recordStepSchemas(pipelineId, deriveSteps(pipeline, vertices, keysByTable, keysBySource,
                steps, compiled, vertex -> copiedColumns(pipelineId, vertex), incomplete));
    }

    /**
     * What each of this pipeline's nodes works out today, keyed the way the record is keyed: source
     * nodes under the qualified table id, steps under their own. Read-only - nothing here records, which
     * is what lets the read face ask the same question as the assembly without writing an answer to it.
     */
    Map<String, NodeColumns> derivedNodesOf(String pipelineId) {
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        Map<String, SourceVertex> sourceVertices = sourceVertices(pipeline);
        Map<String, NodeColumns> steps = deriveSteps(pipeline, sourceVertices,
                sourceKeyByTable(sourceVertices), sourceKeysById(sourceVertices), stepIds(pipeline),
                compiledJoins(pipeline, sourceIdByTable(sourceVertices)), this::discoveredColumns)
                .columns();
        Map<String, NodeColumns> nodes = new LinkedHashMap<>();
        for (SourceVertex vertex : sourceVertices.values()) {
            NodeColumns columns = discoveredColumns(vertex);
            if (columns != null) {
                nodes.put(SourceSchemaCopy.nodeId(vertex.sourceId(), vertex.table()), columns);
            }
        }
        // In the order the pipeline declares them, which is the order the face promises - the walk
        // above resolves them in whatever order their inputs came ready.
        for (Step step : pipeline.transforms() == null ? List.<Step>of() : pipeline.transforms()) {
            NodeColumns columns = steps.get(step.id());
            if (columns != null) {
                nodes.put(step.id(), columns);
            }
        }
        if (pipeline.view() instanceof ViewBlock.Inline view && steps.get(view.id()) != null) {
            nodes.put(view.id(), steps.get(view.id()));
        }
        return nodes;
    }

    /**
     * What one source table holds in the world right now, rather than what this pipeline copied of it.
     *
     * <p>The distinction is the whole of the read face: a report answering from the copy would compare
     * the copy with itself and agree every time, which is the one answer that must not be produced by
     * a source having moved.
     */
    private NodeColumns discoveredColumns(SourceVertex vertex) {
        SourceResource source = StoredArtifacts.requireSource(artifacts(), vertex.sourceId());
        SourceModel discovered = SourceDiscovery.model(storePort, source);
        SourceTable table = discovered == null ? null : discoveredTable(discovered, vertex.table());
        return table == null ? null : NodeColumns.known(SourceSchemaCopy.columnsOf(table));
    }

    /** What every node of a pipeline derives, and how the ones this walk worked out were reached. */
    private record StepDerivations(
            Map<String, NodeColumns> columns, Map<String, Recordable> recordable) {
    }

    /**
     * One node's derivation as the record needs it: what reached it, what the author wrote, and whether
     * the columns are what this node produces or only what reached it.
     */
    private record Recordable(Map<String, NodeColumns> inputs, Object authored, boolean carried) {
    }

    /**
     * Records what the walk above worked out, answering the step ids that got a record. Separate from
     * the walk because the read face runs the same walk and must write nothing.
     */
    private List<String> recordStepSchemas(String pipelineId, StepDerivations derived) {
        List<String> recorded = new ArrayList<>();
        derived.recordable().forEach((nodeId, node) -> {
            if (stepSchemaRecord.record(pipelineId, nodeId, derived.columns().get(nodeId),
                    node.inputs(), node.authored(), node.carried())) {
                recorded.add(nodeId);
            }
        });
        return recorded;
    }

    /**
     * What reaches one step, keyed by the reference the author wrote it under, or null while any of
     * those references cannot be answered yet.
     *
     * <p><b>Keyed by the reference rather than by what it resolves to</b>, because a nest names its root
     * by that reference and has to find it here. One reference resolving to several source tables - a
     * multi-table source, a regex - arrives as their merge, which is what a node reading that reference
     * receives.
     *
     * <p>A nest whose root is not among these keys is left unrecorded rather than derived: this walk
     * only writes down what the assembly already works out, and a recorder is not the thing that should
     * refuse an assembly that otherwise builds.
     */
    private Map<String, NodeColumns> inputsOf(
            List<FromRef> refs, Map<String, SourceVertex> sourceVertices,
            Map<String, String> sourceKeyByTable, Map<String, List<String>> sourceKeysById,
            Set<String> stepIds, Map<String, NodeColumns> derived,
            Function<SourceVertex, NodeColumns> sourceColumns, boolean incompleteSources) {
        Map<String, NodeColumns> inputs = new LinkedHashMap<>();
        for (FromRef ref : refs) {
            // A regex could include tables not discovered yet. Recording only its known matches
            // would turn an incomplete input into a falsely complete model.
            if (incompleteSources && ref instanceof FromRef.Regex) {
                return null;
            }
            List<NodeColumns> reached = new ArrayList<>();
            for (String key : upstreams(ref, sourceKeyByTable, sourceKeysById, sourceVertices, stepIds)) {
                SourceVertex vertex = sourceVertices.get(key);
                NodeColumns columns = vertex != null ? sourceColumns.apply(vertex) : derived.get(key);
                if (columns == null) {
                    return null;
                }
                reached.add(columns);
            }
            if (reached.isEmpty()) {
                return null;
            }
            inputs.put(referenceOf(ref), NodeColumns.merged(reached));
        }
        return inputs.isEmpty() ? null : inputs;
    }

    /** The text a reference was written as, which is the name a nest's root is declared under. */
    private static String referenceOf(FromRef ref) {
        return ref instanceof FromRef.Literal literal ? literal.ref() : ((FromRef.Regex) ref).pattern();
    }

    /** This pipeline's own copy of one source table's model, as a node's columns. */
    private NodeColumns copiedColumns(String pipelineId, SourceVertex vertex) {
        return storePort.derivedSchemas()
                .latest(pipelineId, SourceSchemaCopy.nodeId(vertex.sourceId(), vertex.table()))
                .map(recorded -> NodeColumns.known(recorded.schema()))
                .orElse(null);
    }

    /**
     * One published stream's target: the columns the pipeline produces, in that order, each carrying
     * what the source declared for it and nothing where the source has no such column.
     *
     * <p>The inferred portable type follows the produced model. Source spelling is carried separately
     * for diagnostics; the PDK adapter converts the inferred type through the target connector.
     *
     * <p>The key is whatever of the table's own key still travels, key columns leading, because that
     * is the order the sink matches an upsert in. A projection that drops a key column publishes rows
     * with nothing to match on; that reaches the sink as a key one column short and is reported there,
     * against the table being written, rather than being invented back here.
     */
    private static TargetTable publishedAs(TargetTable base, NodeColumns produced) {
        Map<String, TargetField> declared = new LinkedHashMap<>();
        base.fields().forEach(field -> declared.put(field.name(), field));
        List<TargetField> fields = new ArrayList<>(produced.columns().size());
        List<String> key = new ArrayList<>();
        for (String column : produced.columns().keySet()) {
            TargetField carried = declared.get(column);
            fields.add(new TargetField(column, carried == null ? null : carried.type(), false,
                    JoinSchemaDrift.typeOf(produced.columns().get(column)), produced.numericTypes().get(column), produced.stringTypes().get(column)));
            if (carried != null && carried.primaryKey()) {
                key.add(column);
            }
        }
        return TargetModelResolver.keyedOn(
                new TargetTable(base.name(), fields, base.indexes().stream()
                        .filter(index -> produced.columns().keySet().containsAll(index.fields()))
                        // A same-named computed value does not inherit source uniqueness.
                        .filter(index -> !index.unique() || produced.unchangedFields().containsAll(index.fields()))
                        .toList()), key);
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
     * <p>A column published verbatim carries both its source spelling and its inferred portable type,
     * so the adapter can translate it through the target connector. A computed column keeps its
     * unresolved type. The unique index covers only the complete published fact key.
     */
    private TargetTable joinTarget(Step step, CompiledJoin compiled,
            Map<String, TargetTable> bySourceTable) {
        io.tapstate.core.sql.JoinPlan plan = compiled.plan();
        String factTable = compiled.factTable();
        List<String> key = publishedFactKey(compiled);
        List<TargetField> fields = new ArrayList<>();
        for (String name : key) {
            fields.add(joinField(compiled, name, true, bySourceTable));
        }
        for (io.tapstate.core.sql.OutputField field : plan.outputFields()) {
            if (!key.contains(field.name())) {
                fields.add(joinField(compiled, field.name(), false, bySourceTable));
            }
        }
        List<TargetIndex> indexes = key.isEmpty() ? List.of() : List.of(new TargetIndex(key, true));
        return new TargetTable(factTable, fields, indexes);
    }

    /**
     * The fact key under the names the projection publishes it by, or empty where it does not publish
     * all of it.
     *
     * <p><b>Empty rather than partial.</b> A key one column short still looks like a key and still
     * matches writes to rows, so it merges rows the query says are distinct - the same silent failure
     * as no key at all, wearing a key's clothes. Whether an empty one is allowed to reach a sink is
     * decided by what that sink does with it, which is not known here.
     */
    private static List<String> publishedFactKey(CompiledJoin compiled) {
        String factName = compiled.plan().factSource().name();
        List<String> key = new ArrayList<>();
        for (String column : compiled.factKeyColumns()) {
            String published = compiled.plan().publishedAs(factName, column);
            if (published == null) {
                return List.of();
            }
            key.add(published);
        }
        return key;
    }

    /** The first fact key column the projection does not publish, or null where it publishes them all. */
    private static String unpublishedFactKeyColumn(CompiledJoin compiled) {
        String factName = compiled.plan().factSource().name();
        for (String column : compiled.factKeyColumns()) {
            if (compiled.plan().publishedAs(factName, column) == null) {
                return column;
            }
        }
        return null;
    }

    /**
     * Refuses a join whose projection does not publish its driving table's key - but only where
     * something actually matches a write to an existing row on that key.
     *
     * <p><b>An append is not judged, and that is the whole reason this is not decided where the target
     * is built.</b> Append never matches a write to an existing row, so it has no use for a key at
     * all; refusing one for want of a key would refuse a pipeline that is not broken. This is the same
     * reading the source-side key rule takes, and the two must not disagree about what a keyless write
     * means. Measured while this was still unconditional: a join feeding an append-mode sync was
     * refused by name for a key it never needed.
     *
     * <p>Read across the whole serve block rather than per element, the way the source-side rule reads
     * it: one keyed write anywhere in it is enough, because every element is fed by the same streams.
     * A view is not judged here - it declares its own key and its own gate compares that key against
     * what feeds it.
     */
    private static void requireFactKeyPublishedWhereAWriteMatchesOnIt(
            PipelineResource pipeline, Map<String, CompiledJoin> compiledJoins,
            Set<String> serveStreams) {
        if (!(pipeline.serve() instanceof ServeBlock.Inline serve) || serve.sync() == null) {
            return;
        }
        boolean matchesOnAKey = false;
        for (SyncElement sync : serve.sync()) {
            matchesOnAKey |= sync.writeMode() == null
                    || sync.writeMode() == io.tapstate.core.model.WriteMode.UPSERT;
        }
        if (!matchesOnAKey) {
            return;
        }
        for (String stream : serveStreams) {
            CompiledJoin compiled = compiledJoins.get(stream);
            if (compiled == null) {
                continue;
            }
            String missing = unpublishedFactKeyColumn(compiled);
            if (missing != null) {
                throw new TapstateException(ActuationError.JOIN_OUTPUT_KEY_NOT_PUBLISHED,
                        Map.of("step", stream, "table", compiled.factTable(), "column", missing),
                        null);
            }
        }
    }


    /** Carries source metadata through a column alias; computed output keeps its unresolved type. */
    private static TargetField joinField(CompiledJoin compiled, String output, boolean primaryKey,
            Map<String, TargetTable> bySourceTable) {
        for (io.tapstate.core.sql.OutputField field : compiled.plan().outputFields()) {
            if (!field.name().equals(output)) {
                continue;
            }
            if (!(field.from() instanceof io.tapstate.core.sql.Expr.Column reference)) {
                return field.type() == io.tapstate.core.common.TapstateType.DECIMAL
                        ? new TargetField(output, null, primaryKey, field.type())
                        : new TargetField(output, null, primaryKey);
            }
            TargetTable source =
                    bySourceTable.get(compiled.tableByName().get(reference.ref().source()));
            if (source == null) {
                return new TargetField(output, null, primaryKey);
            }
            for (TargetField candidate : source.fields()) {
                if (candidate.name().equals(reference.ref().column())) {
                    return new TargetField(output, candidate.type(), primaryKey, candidate.inferredType(), candidate.numericType(), candidate.stringType());
                }
            }
            return new TargetField(output, null, primaryKey);
        }
        return new TargetField(output, null, primaryKey);
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
        for (String sourceId : pipeline.sourceIds()) {
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
                element -> sinkWriter(pipeline, element, targets, serveStreams),
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
                store.connector(), store.config(), WriteMode.UPSERT, DdlPolicy.FAIL, bySourceTable,
                new PipelineNode(pipeline.id(), inline.id()));
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
            FromClause from,
            Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, SourceVertex> sourceVertices,
            Set<String> stepIds) {
        Set<String> sourceIds = new LinkedHashSet<>();
        for (FromRef ref : refsOf(from)) {
            collectSourceIds(pipeline, ref, sourceKeyByTable, sourceKeysById, sourceVertices,
                    stepIds, sourceIds, new HashSet<>());
        }
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
            FromClause from,
            Map<String, String> sourceKeyByTable,
            Map<String, List<String>> sourceKeysById,
            Map<String, SourceVertex> sourceVertices,
            Set<String> stepIds) {
        Set<String> streams = new LinkedHashSet<>();
        for (FromRef ref : refsOf(from)) {
            collectStreams(pipeline, ref, sourceKeyByTable, sourceKeysById, sourceVertices,
                    stepIds, streams, new HashSet<>());
        }
        return streams;
    }

    /** The view form remains a single reference; keep its terminal walk scalar. */
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
        TargetField streamKey = streamFields.stream().filter(field -> field.name().equals(target.primaryKey()))
                .findFirst().orElse(null);
        fields.add(streamKey == null ? new TargetField(target.primaryKey(), null, true)
                : new TargetField(streamKey.name(), streamKey.type(), true, streamKey.inferredType(), streamKey.numericType(), streamKey.stringType()));
        for (TargetField field : streamFields) {
            if (!field.name().equals(target.primaryKey())) {
                fields.add(new TargetField(field.name(), field.type(), false, field.inferredType(), field.numericType(), field.stringType()));
            }
        }
        return new TargetTable(target.collection(), fields, target.indexes());
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

    /**
     * Every join step of a stored pipeline, compiled exactly the way a start compiles it, keyed by step
     * id; empty where the pipeline has no join.
     *
     * <p>Offered rather than reimplemented next door on purpose. Whoever reports what a join produces
     * has to be answering the same question a start answers, and a second implementation of a
     * derivation is two answers with nothing comparing them - which here would mean a report saying the
     * columns are fine and a start refusing them, or the reverse.
     */
    Map<String, CompiledJoin> compiledJoinsOf(String pipelineId) {
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(artifacts(), pipelineId), artifacts());
        return compiledJoins(pipeline, sourceIdByTable(sourceVertices(pipeline)));
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
        List<io.tapstate.core.sql.SourceTable> derivedFrom = List.copyOf(tables);
        io.tapstate.core.sql.JoinPlan plan =
                io.tapstate.core.sql.SqlFrontEnd.derive(join.sql(), derivedFrom);
        // Every source the plan carries has to be one the step declared, because the alias is what the
        // wiring resolves an upstream through. The SQL may spell a source either way - the alias, or the
        // table it stands for, both of which were registered above so that the front end accepts what an
        // author writes - and only one of the two spellings survives into the plan's source names.
        Set<String> declaredAliases = step.from() instanceof FromClause.Aliases declared
                ? declared.aliases().keySet()
                : Set.of();
        for (io.tapstate.core.sql.JoinTree.Source source : plan.from().sources()) {
            if (!declaredAliases.contains(source.name())) {
                throw new TapstateException(ActuationError.JOIN_SOURCE_NOT_DECLARED,
                        Map.of("step", step.id(), "name", source.name()), null);
            }
        }
        String driving = plan.factSource().table();
        List<String> key = keyByTable.getOrDefault(driving, List.of());
        if (key.isEmpty()) {
            throw new TapstateException(ActuationError.JOIN_SOURCE_KEY_MISSING,
                    Map.of("step", step.id(), "table", driving), null);
        }
        return new CompiledJoin(plan, key, Map.copyOf(tableByName), join, derivedFrom);
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
     *
     * <p>The two inputs the plan was derived from are carried alongside it. The plan states the answer;
     * whoever has to say why the answer moved needs what it was worked out from, and needs the author's
     * input and the world's kept apart - an edited query producing new columns is what the author asked
     * for, while an untouched query producing new columns is the world having moved under it.
     */
    record CompiledJoin(io.tapstate.core.sql.JoinPlan plan, List<String> factKeyColumns,
            Map<String, String> tableByName, TransformBody.Join body,
            List<io.tapstate.core.sql.SourceTable> tables) {

        /** What the author wrote, which is what a change of statement is fingerprinted from. */
        String sql() {
            return body.sql();
        }

        /** The real table the join is driven from, under its own name rather than the SQL's alias. */
        String factTable() {
            String name = plan.factSource().name();
            return tableByName.getOrDefault(name, plan.factSource().table());
        }
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
        return storePort.schemas().get(sourceId)
                .map(DiscoveredSourceModel::model)
                .flatMap(model -> model.tables().stream().filter(t -> t.name().equals(table)).findFirst())
                .map(discovered -> new NestTable(table, discovered.primaryKey(), uniqueIndexesOf(discovered)))
                .orElseGet(() -> new NestTable(table, List.of()));
    }

    /**
     * The columns of each unique index on a discovered table, in the order the source reported them. It
     * is the last place a level's identity can come from when the table declares no primary key, so the
     * non-unique indexes are dropped here rather than downstream: an index that does not identify a row
     * is not a candidate, and carrying it further would only give the compiler more to reject.
     */
    private static List<List<String>> uniqueIndexesOf(SourceTable table) {
        List<List<String>> unique = new ArrayList<>();
        for (SourceIndex index : table.indexes()) {
            if (index.unique() && !index.fields().isEmpty()) {
                unique.add(index.fields());
            }
        }
        return unique;
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
            PipelineResource pipeline, SyncElement element, Map<String, TargetTable> targets,
            Set<String> serveStreams) {
        SourceResource sink = StoredArtifacts.requireSource(artifacts(), element.source());
        return sinkWriterBinder.bind(
                sink.connector(), sink.config(), writeMode(element.writeMode()), ddl(element.ddl()),
                TargetModelResolver.renameAll(targets, serveStreams, element.rename()),
                new PipelineNode(pipeline.id(), syncNodeId(element)),
                element.onFullLoad() == null ? OnFullLoad.APPEND : OnFullLoad.valueOf(element.onFullLoad().name()),
                freshFullLoad(pipeline));
    }

    /** A CDC-only read and any previously delivered load suppress destructive target preparation. */
    private boolean freshFullLoad(PipelineResource pipeline) {
        if (pipeline.settings() != null
                && pipeline.settings().readMode() == io.tapstate.core.model.ReadMode.CDC_ONLY) {
            return false;
        }
        return sourceVertices(pipeline).values().stream().noneMatch(vertex ->
                storePort.meta().read(vertex.resolution().chainId().value())
                        .map(meta -> meta.consumerOffsets().stream().anyMatch(
                                consumer -> consumer.pipelineId().equals(pipeline.id())))
                        .orElse(false));
    }

    /**
     * What names a serve.sync element as a node of its pipeline: its own id, which authoring generates
     * for an element that declares none and holds unique across everything a pipeline names inside
     * itself. That uniqueness is what makes it usable as a node id at all — the ids of two sinks of one
     * pipeline have to differ or their connectors share one set of notes.
     *
     * <p>The fall back to the source written to is for an artifact that reached the store without going
     * through authoring, where the id is still what the model says it is: optional. It names something
     * rather than leaving the sink with no node at all. Two id-less elements writing to one target would
     * name the same node, which is why authoring generating the ids is what this rests on rather than
     * the fallback.
     */
    private static String syncNodeId(SyncElement element) {
        return element.id() != null && !element.id().isBlank() ? element.id() : element.source();
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
                TargetTable target, PipelineNode node);

        default SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                Map<String, TargetTable> targets, PipelineNode node) {
            return bind(connectorId, settings, writeMode, ddl,
                    targets.size() == 1 ? targets.values().iterator().next() : null, node);
        }
        default SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                Map<String, TargetTable> targets, PipelineNode node, OnFullLoad onFullLoad, boolean fullLoad) {
            return bind(connectorId, settings, writeMode, ddl, targets, node);
        }
    }

    static final class PdkSinkWriterBinder implements SinkWriterBinder {

        @Override
        public SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                TargetTable target, PipelineNode node) {
            return new PdkSinkWriterFactory(connectorId, settings, writeMode, ddl, target, node);
        }

        @Override
        public SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                Map<String, TargetTable> targets, PipelineNode node) {
            return new PdkSinkWriterFactory(connectorId, settings, writeMode, ddl, targets, node);
        }
        @Override
        public SupplierEx<? extends SinkWriter> bind(
                String connectorId, Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                Map<String, TargetTable> targets, PipelineNode node, OnFullLoad onFullLoad, boolean fullLoad) {
            return new PdkSinkWriterFactory(connectorId, settings, writeMode, ddl, targets, node,
                    onFullLoad, fullLoad);
        }
    }
}
