package io.tapstate.spi.store;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One run's writer accounting for one pipeline on one mining chain: which writers each table's changes are
 * expected to reach, and how far each of them has durably landed them.
 *
 * <p>{@code runId} names the run the accounting belongs to. A writer of another run - one still winding down
 * after a replacement started - writes nothing into it, so its progress cannot stand for a writer of this run.
 *
 * <p>{@code snapshotEpoch} is the generation the pipeline's initial load was read under, or null where the
 * pipeline recorded no load: it is what says whether a table's progress has passed the load's rows.
 */
public record WriterRun(
        String runId,
        Map<String, List<String>> expected,
        Map<String, Map<String, WriterProgress>> progress,
        Long snapshotEpoch) {

    public WriterRun {
        Objects.requireNonNull(runId, "runId");
        Map<String, List<String>> expectedCopy = new LinkedHashMap<>();
        Objects.requireNonNull(expected, "expected").forEach((table, writers) ->
                expectedCopy.put(table, List.copyOf(writers)));
        expected = Collections.unmodifiableMap(expectedCopy);
        Map<String, Map<String, WriterProgress>> progressCopy = new LinkedHashMap<>();
        Objects.requireNonNull(progress, "progress").forEach((table, byWriter) ->
                progressCopy.put(table, Collections.unmodifiableMap(new LinkedHashMap<>(byWriter))));
        progress = Collections.unmodifiableMap(progressCopy);
    }

    /** The writers {@code table}'s changes are expected to reach; empty where the run expects none. */
    public List<String> expectedFor(String table) {
        return expected.getOrDefault(table, List.of());
    }

    /** Each writer's progress on {@code table}; a writer that has reported nothing is absent. */
    public Map<String, WriterProgress> progressFor(String table) {
        return progress.getOrDefault(table, Map.of());
    }
}
