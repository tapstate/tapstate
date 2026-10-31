package io.tapstate.spi.store;

import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationRunSpec;
import java.util.Objects;
import java.util.UUID;

/** CONFIG one-time report birth. Requests describe original commands; they do not grant authority. */
public interface ValidationReportAdmission {
    record Task(String clusterId, String pipelineId, String taskId, UUID generation) {
        public Task {
            clusterId = identifier(clusterId, "task.clusterId");
            pipelineId = identifier(pipelineId, "task.pipelineId");
            taskId = identifier(taskId, "task.taskId");
            Objects.requireNonNull(generation, "generation");
        }
    }
    record Start(Task task, long configurationRevision, long sequence, String commandId, ValidationRunIdentity run) {
        public Start {
            Objects.requireNonNull(task, "task"); Objects.requireNonNull(run, "run");
            commandId = identifier(commandId, "start.commandId");
            if (configurationRevision < 1 || sequence < 1 || sequence == Long.MAX_VALUE
                    || !task.clusterId().equals(run.owner().clusterId())
                    || !task.pipelineId().equals(run.owner().pipelineId()) || !task.taskId().equals(run.taskId()))
                throw invalid("start.identity");
        }
        public ValidationResultStore.Report report() { return new ValidationResultStore.Report(run, sequence); }
    }
    record Open(Start start, ValidationRunSpec snapshot) {
        public Open {
            Objects.requireNonNull(start); Objects.requireNonNull(snapshot);
            if (!start.task().pipelineId().equals(snapshot.task().pipelineId()) || start.run().mode() != snapshot.mode()
                    || !start.run().specHash().equals(snapshot.fingerprint()) || start.run().keyMappingRevision() != snapshot.task().mappingRevision()
                    || !start.run().comparisonRulesVersion().equals(snapshot.task().comparisonRulesVersion())) throw invalid("report.specSnapshot");
        }
    }
    private static String identifier(String value, String field) {
        if (value == null || value.isBlank()) throw invalid(field);
        ValidationError.fieldName(value, field); return value;
    }
    private static io.tapstate.core.common.TapstateException invalid(String field) {
        return new io.tapstate.core.common.TapstateException(ValidationError.INVALID_CONTRACT, java.util.Map.of("field", field), null);
    }
    enum Reservation { RESERVED, DUPLICATE, MODE_BUSY, CLOSED, STALE }
    enum Birth { CREATED, DUPLICATE, CLOSED, STALE }
    enum Closure { CLOSED, ALREADY_CLOSED, STALE }

    /** Atomically advances the exact mode sequence and stores OPEN; a prior nonclosed permission blocks it. */
    Reservation reserve(Start start);
    /** Original OPEN -> BORN plus original report insert in one CONFIG transaction; missing BORN report is loss. */
    Birth open(Start start);
    /** Original immutable comparison snapshot; implementations may not replace it from latest task settings. */
    default Birth open(Open command) { throw invalid("report.specSnapshotUnsupported"); }
    /** Competes with birth: revokes OPEN, or seals the exact BORN report, and stores CLOSED atomically. */
    Closure close(Start start);
}
