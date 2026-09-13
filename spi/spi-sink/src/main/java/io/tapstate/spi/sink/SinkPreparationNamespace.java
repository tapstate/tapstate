package io.tapstate.spi.sink;

import io.tapstate.core.model.PipelineNode;

/** Durable preparation receipts owned by one sink, removed with its pipeline's rerun state. */
public final class SinkPreparationNamespace {
    private SinkPreparationNamespace() { }

    /** The preparation namespace for a named sink, or null for an unscoped write. */
    public static String of(PipelineNode node) {
        return node == null ? null : "sink.prepare." + node.pipelineId() + "." + node.nodeId();
    }
}
