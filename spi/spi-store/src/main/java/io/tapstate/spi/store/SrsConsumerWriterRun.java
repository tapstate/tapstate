package io.tapstate.spi.store;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Current writer accounting belongs to one original membership and complete Pipeline execution. */
public record SrsConsumerWriterRun(long membershipRevision,WorkloadClaimFence commandFence,String runId,
        Map<String,List<String>> expected,Map<String,Map<String,WriterProgress>> progress) {
    public SrsConsumerWriterRun {
        Objects.requireNonNull(commandFence,"commandFence");
        var copied = new WriterRun(runId,expected,progress,null); runId = copied.runId(); expected = copied.expected(); progress = copied.progress();
        if (membershipRevision < 1 || membershipRevision == Long.MAX_VALUE || commandFence.executionGeneration() < 1
                || commandFence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION || runId.isBlank() || expected.isEmpty())
            throw new IllegalArgumentException("writer accounting requires one live original execution and membership");
        for (var entry : expected.entrySet()) {
            if (entry.getKey().isBlank() || entry.getValue().isEmpty() || entry.getValue().stream().anyMatch(writer -> writer == null || writer.isBlank())
                    || new HashSet<>(entry.getValue()).size() != entry.getValue().size())
                throw new IllegalArgumentException("writer accounting requires distinct named writers for every expected table");
        }
        if (!expected.keySet().containsAll(progress.keySet())) throw new IllegalArgumentException("writer progress names an unexpected table");
        for (var entry : progress.entrySet()) if (!expected.get(entry.getKey()).containsAll(entry.getValue().keySet())
                || entry.getValue().values().stream().anyMatch(Objects::isNull)) throw new IllegalArgumentException("writer progress names an unexpected writer");
    }
    /** Snapshot order belongs to the owning cursor and may become available after this run starts. */
    public WriterRun withSnapshotEpoch(Long epoch) { return new WriterRun(runId,expected,progress,epoch); }
}
