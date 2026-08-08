package io.tapstate.runtime.engine;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;
import java.util.function.Function;

/**
 * The leaf and reference bindings the DAG builder needs, supplied by the assembly root. The builder
 * knows the pipeline topology; the app knows how each leaf becomes a Jet vertex and how a reference
 * resolves to its producer. Keeping both on this seam is what lets the engine build a running DAG
 * while depending on the core and spi rings only - no adapter or connector type crosses here.
 *
 * <ul>
 *   <li>{@code sourceVertices} - a source id to the Jet source vertex that reads it. The vertex is
 *       opaque: config resolution and the member-side event build stay behind it.
 *   <li>{@code transformPorts} - a stateless step (filter / map / a scripted row transform) to the
 *       factory of the pure port it runs. A bare port factory, not a vertex: the builder alone wraps
 *       it in the one generic adapter, so no caller can substitute a per-operator processor.
 *   <li>{@code sinkWriters} - one {@code serve.sync} element to the factory of the sink writer that
 *       writes it. A bare writer factory, not a vertex: the builder alone wraps it in the one generic
 *       sink adapter, so no caller can substitute a per-connector sink processor. Write mode, ddl
 *       policy and the target connector fold in behind the writer the factory opens on the member.
 *   <li>{@code upstreams} - a resolved {@code from:} reference to the producer vertex keys it names
 *       (source ids or step ids). Reference resolution against the source universe lives with the
 *       caller, so the engine never sees the table universe.
 * </ul>
 */
public record DagBindings(
        Function<String, ProcessorMetaSupplier> sourceVertices,
        Function<Step, SupplierEx<? extends TransformPort>> transformPorts,
        Function<SyncElement, SupplierEx<? extends SinkWriter>> sinkWriters,
        Function<FromRef, List<String>> upstreams,
        Function<String, List<String>> sourceKeys) {

    public DagBindings(
            Function<String, ProcessorMetaSupplier> sourceVertices,
            Function<Step, SupplierEx<? extends TransformPort>> transformPorts,
            Function<SyncElement, SupplierEx<? extends SinkWriter>> sinkWriters,
            Function<FromRef, List<String>> upstreams) {
        this(sourceVertices, transformPorts, sinkWriters, upstreams, sourceId -> List.of(sourceId));
    }
}
