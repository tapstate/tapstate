package io.tapstate.control.core;

import io.tapstate.core.model.PipelineNode;
import java.util.List;

/** Exact effective execution artifacts, read independently from the currently installed default. */
public interface ConnectorUsageQuery {
    record Version(String nodeId, PipelineNode.Purpose purpose, String connectorId, String version,
            String sha256, String captureId, String chainId) { }

    record Pipeline(String pipelineId, String name, List<Version> connectors) {
        public Pipeline { connectors = List.copyOf(connectors); }
    }

    record Page(int page, int pageSize, int total, List<Pipeline> pipelines) {
        public Page { pipelines = List.copyOf(pipelines); }
    }

    List<Version> forPipeline(String pipelineId);

    Page usages(String connectorId, String sha256, String search, int page, int pageSize);
}
