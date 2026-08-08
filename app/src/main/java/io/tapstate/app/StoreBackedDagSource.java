package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import io.tapstate.adapters.transform.MapSpec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.DagBindings;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.runtime.engine.SinkAckBinding;
import io.tapstate.runtime.srs.SrsReadCursorPublisherFactory;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    StoreBackedDagSource(StorePort storePort) {
        this(storePort, PdkSinkWriterFactory::new);
    }

    StoreBackedDagSource(StorePort storePort, SinkWriterBinder sinkWriterBinder) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.sinkWriterBinder = Objects.requireNonNull(sinkWriterBinder, "sinkWriterBinder");
        this.targetModelResolver = new TargetModelResolver(this.storePort);
    }

    @Override
    public DAG dagFor(String pipelineId) {
        PipelineResource pipeline = StoredArtifacts.requirePipeline(artifacts(), pipelineId);
        TargetModelResolver.ResolvedTarget resolved = targetModelResolver.resolve(pipeline)
                .orElseGet(() -> new TargetModelResolver.ResolvedTarget(sourceTable(pipeline), null));
        return PipelineDagBuilder.build(
                pipeline,
                bindings(sourceIdByTable(pipeline), resolved.target(), resolved.sourceTable()),
                sinkAckBinding(pipeline, pipelineId));
    }

    private String sourceTable(PipelineResource pipeline) {
        String sourceId = pipeline.sources().getFirst();
        return SourceCaptureResolution.of(StoredArtifacts.requireSource(artifacts(), sourceId)).table();
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
     * {@code src} stream name (a table at L1), so the binding carries a table-to-chain map resolved from
     * every source the pipeline reads, and the sink-side order matches the capture watermark's order so the
     * two cannot drift. The map is built here, on the assembly side; only serializable coordinates ship.
     */
    private SinkAckBinding sinkAckBinding(PipelineResource pipeline, String pipelineId) {
        Map<String, String> chainIdByTable = new LinkedHashMap<>();
        for (String sourceId : pipeline.sources()) {
            SourceCaptureResolution resolution =
                    SourceCaptureResolution.of(StoredArtifacts.requireSource(artifacts(), sourceId));
            chainIdByTable.put(resolution.table(), resolution.chainId().value());
        }
        return new SinkAckBinding(
                new StoreBackedSinkAckFactory(chainIdByTable, pipelineId), MockPositionOrder.INSTANCE);
    }

    /**
     * The leaf and reference bindings for the builder. The four functions run on the assembly side as the
     * builder walks the topology; only the vertex suppliers they return travel onto the DAG, so they may
     * reach the store freely while what they produce stays serializable.
     */
    private DagBindings bindings(Map<String, String> sourceIdByTable, TargetTable target, String sourceTable) {
        return new DagBindings(
                this::sourceVertex,
                StoreBackedDagSource::transformPort,
                element -> sinkWriter(element, TargetModelResolver.rename(target, sourceTable, element.rename())),
                ref -> upstreams(ref, sourceIdByTable));
    }

    /**
     * The source vertex for one source id: it resolves the source's connector, config and single table into
     * the per-table change ring the capture side writes. The stream name projected into each event is the
     * table name. Resolving the same ring identity the capture side resolves is what points the reader at the
     * ring the writer fills.
     */
    private ProcessorMetaSupplier sourceVertex(String sourceId) {
        SourceCaptureResolution resolution =
                SourceCaptureResolution.of(StoredArtifacts.requireSource(artifacts(), sourceId));
        return SrsSourceProcessor.metaSupplier(
                resolution.ringName(), resolution.table(), StartFrom.earliest(), SrsReadCursorPublisherFactory.NONE);
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
