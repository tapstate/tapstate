package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Current-run display facts only; neither recovery inputs nor target completion authority. */
public record InitialLoad(String runKey, Instant observedAt, String readMode, Map<String, Table> tables) {
    public enum Stage { PENDING, READING, LANDING, LANDED }
    public enum Phase { SNAPSHOTTING, MIXED, STREAMING, COMPLETED, UNKNOWN }

    public record Table(Stage stage, Long rowsRead, Long rowsTotalEstimate) {
        public Table {
            Objects.requireNonNull(stage, "stage");
            if (rowsRead != null && rowsRead < 0 || rowsTotalEstimate != null && rowsTotalEstimate < 0) {
                throw new IllegalArgumentException("initial-load row counts must be non-negative");
            }
        }
    }

    public InitialLoad {
        Objects.requireNonNull(runKey, "runKey");
        Objects.requireNonNull(observedAt, "observedAt");
        if (!Set.of("snapshot_and_cdc", "snapshot_only", "cdc_only").contains(readMode)) {
            throw new IllegalArgumentException("unrecognized initial-load read mode");
        }
        tables = Map.copyOf(Objects.requireNonNull(tables, "tables"));
    }

    public static String executionKey(ExecutionPlan plan) {
        return "execution:" + plan.pipelineId() + ":" + plan.executionGeneration() + ":" + plan.plannedAt();
    }

    public boolean matches(ExecutionPlan plan) {
        return plan != null && runKey.equals(executionKey(plan));
    }

    public InitialLoad at(Instant at) {
        return new InitialLoad(runKey, at, readMode, tables);
    }

    public long landedTables() {
        return tables.values().stream().filter(table -> table.stage() == Stage.LANDED).count();
    }

    public Phase phase(PipelineState state) {
        if (state != PipelineState.RUNNING && state != PipelineState.COMPLETED) return Phase.UNKNOWN;
        if (readMode.equals("cdc_only")) return state == PipelineState.RUNNING ? Phase.STREAMING : Phase.UNKNOWN;
        if (tables.isEmpty()) return Phase.UNKNOWN;
        long landed = landedTables();
        if (landed == tables.size()) {
            if (readMode.equals("snapshot_only")) return state == PipelineState.COMPLETED ? Phase.COMPLETED : Phase.SNAPSHOTTING;
            return state == PipelineState.RUNNING ? Phase.STREAMING : Phase.UNKNOWN;
        }
        return landed > 0 && readMode.equals("snapshot_and_cdc") ? Phase.MIXED : Phase.SNAPSHOTTING;
    }
}
