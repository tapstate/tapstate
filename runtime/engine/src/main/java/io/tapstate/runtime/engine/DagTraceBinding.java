package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Optional observer vertices attached to logical source outputs and transform inputs and outputs. */
public record DagTraceBinding(
        Function<String, ProcessorMetaSupplier> vertices,
        BiFunction<String, String, ProcessorMetaSupplier> inputs) {

    public DagTraceBinding {
        Objects.requireNonNull(vertices, "vertices");
        Objects.requireNonNull(inputs, "inputs");
    }

    public DagTraceBinding(Function<String, ProcessorMetaSupplier> vertices) {
        this(vertices, (nodeId, alias) -> vertices.apply(nodeId + "\u0000input\u0000" + alias));
    }

    String vertexName(String nodeId) {
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(nodeId.getBytes(StandardCharsets.UTF_8));
        return "preview.trace." + encoded;
    }

    String inputVertexName(String nodeId, String alias) {
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString((nodeId + "\u0000" + alias).getBytes(StandardCharsets.UTF_8));
        return "preview.trace.input." + encoded;
    }
}
