package io.tapstate.control.core;

import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.LogLine;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;

import java.util.List;
import java.util.Objects;

/**
 * The pipeline logs read side. It tails the node-local log sink for one pipeline — the process's own
 * captured log output, keyed by pipeline id. This is the read face that is not store-backed: it reads
 * an in-process sink directly rather than the published observation doc, because logs are node-local
 * and not fanned into a shared store. A read of a pipeline with no captured lines is a benign empty
 * tail, never a coded error.
 */
public final class PipelineLogQueryService {

    public enum Scope {
        CURRENT,
        INCARNATION
    }

    private final LogSink logs;
    private final ArtifactStore artifacts;
    private final ExecutionGenerationStore generations;
    private final String clusterId;

    public PipelineLogQueryService(LogSink logs) {
        this.logs = Objects.requireNonNull(logs, "logs");
        this.artifacts = null;
        this.generations = null;
        this.clusterId = null;
    }

    public PipelineLogQueryService(LogSink logs, ArtifactStore artifacts,
            ExecutionGenerationStore generations, String clusterId) {
        this.logs = Objects.requireNonNull(logs, "logs");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.generations = Objects.requireNonNull(generations, "generations");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
    }

    /** The pipeline's most recent log lines, oldest to newest; empty when it has logged nothing here. */
    public PipelineLogs logs(String pipelineId) {
        return logs(pipelineId, Scope.CURRENT);
    }

    public PipelineLogs logs(String pipelineId, Scope scope) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(scope, "scope");
        return new PipelineLogs(pipelineId, selectedLines(pipelineId, scope));
    }

    /** The pipeline's most recent {@code limit} lines, oldest to newest. */
    public PipelineLogs logs(String pipelineId, int limit) {
        return logs(pipelineId, limit, Scope.CURRENT);
    }

    public PipelineLogs logs(String pipelineId, int limit, Scope scope) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(scope, "scope");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        List<LogLine> lines = selectedLines(pipelineId, scope);
        int from = Math.max(0, lines.size() - limit);
        return new PipelineLogs(pipelineId, lines.subList(from, lines.size()));
    }

    private List<LogLine> selectedLines(String pipelineId, Scope scope) {
        if (artifacts == null) {
            return logs.tail(pipelineId);
        }
        if (artifacts.get(pipelineId).filter(artifact -> "pipeline".equals(artifact.kind())).isEmpty()) {
            return List.of();
        }
        var incarnation = artifacts.pipelineIncarnationId(pipelineId);
        if (incarnation.isEmpty()) {
            return logs.tail(pipelineId);
        }
        if (scope == Scope.INCARNATION) {
            return logs.tailIncarnation(pipelineId, incarnation.get());
        }
        var generation = generations.currentGeneration(clusterId, pipelineId);
        return generation.isEmpty() ? List.of() : logs.tail(pipelineId,
                new LogSink.Scope(incarnation.get(), generation.getAsLong()));
    }
}
