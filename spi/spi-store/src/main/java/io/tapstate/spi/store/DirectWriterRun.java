package io.tapstate.spi.store;

import java.util.Objects;

/** Writer accounting cannot cross either the original private producer epoch or Pipeline execution. */
public record DirectWriterRun(long epoch,WorkloadClaimFence commandFence,WriterRun accounting) {
    public DirectWriterRun {
        Objects.requireNonNull(commandFence,"commandFence"); Objects.requireNonNull(accounting,"accounting");
        if (epoch < 1 || epoch == Long.MAX_VALUE || commandFence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || commandFence.executionGeneration() < 1 || accounting.runId().isBlank() || accounting.expected().isEmpty())
            throw new IllegalArgumentException("invalid original direct writer run");
        for (var entry : accounting.expected().entrySet()) {
            if (entry.getKey().isBlank() || entry.getValue().isEmpty() || new java.util.HashSet<>(entry.getValue()).size() != entry.getValue().size()
                    || entry.getValue().stream().anyMatch(name -> name == null || name.isBlank())) throw new IllegalArgumentException("invalid direct writer roster");
        }
        if (accounting.snapshotEpoch() != null && (accounting.snapshotEpoch() < 1 || accounting.snapshotEpoch() > epoch))
            throw new IllegalArgumentException("invalid direct snapshot generation");
        if (!accounting.expected().keySet().containsAll(accounting.progress().keySet())) throw new IllegalArgumentException("unexpected direct progress table");
        accounting.progress().forEach((table,writers) -> {
            if (!accounting.expected().get(table).containsAll(writers.keySet()) || writers.values().stream().anyMatch(Objects::isNull))
                throw new IllegalArgumentException("unexpected direct progress writer");
            writers.values().forEach(point -> {
                var order = point.durableThrough();
                if (order.epoch() < 0 || order.epoch() > epoch || order.seq() < 0 && order.seq() != io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ)
                    throw new IllegalArgumentException("direct writer progress belongs to another capture epoch");
                var token = point.lastTokened();
                if (token != null && (token.order().epoch() < 1 || token.order().epoch() > epoch || token.order().seq() == Long.MAX_VALUE
                        || token.order().seq() < 0 && token.order().seq() != io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ || token.token().isBlank()))
                    throw new IllegalArgumentException("invalid direct writer recovery token");
            });
        });
    }
}
