package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationMethod;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationSchedule;
import io.tapstate.core.validation.ValidationTaskSpec;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** CONFIG task definitions only. Saving is not run admission or confirmation of a runtime delay. */
public interface ValidationTaskStore {
    record Full(ValidationMethod method, int maxConcurrentTables, ValidationSchedule schedule, boolean scheduleEnabled, int historyLimit) {
        public Full {
            Objects.requireNonNull(method, "method");
            if (maxConcurrentTables < 1 || maxConcurrentTables > ValidationTaskSpec.MAX_MAPPINGS || scheduleEnabled && schedule == null)
                throw invalid("task.fullSettings");
            history(historyLimit);
        }
    }
    record Incremental(long firstCheckDelayMillis, int historyLimit) {
        public Incremental {
            if (firstCheckDelayMillis < 0 || firstCheckDelayMillis > 86_400_000) throw invalid("task.incrementalDelay");
            history(historyLimit);
        }
    }
    record Settings(Full full, Incremental incremental) {
        public Settings { Objects.requireNonNull(full); Objects.requireNonNull(incremental); }
        public static Settings defaults(ValidationMethod method, long firstCheckDelayMillis) {
            return new Settings(new Full(method, 1, null, false, 10), new Incremental(firstCheckDelayMillis, 10));
        }
    }
    record Create(ValidationReportAdmission.Task task, ValidationTaskSpec comparison, Settings settings, UUID commandId) {
        public Create { definition(task, comparison, settings); Objects.requireNonNull(commandId); }
    }
    record Update(ValidationReportAdmission.Task task, long expectedRevision, ValidationTaskSpec comparison, Settings settings, UUID commandId) {
        public Update {
            definition(task, comparison, settings); Objects.requireNonNull(commandId);
            if (expectedRevision < 1 || expectedRevision >= Long.MAX_VALUE - 1) throw invalid("task.expectedRevision");
        }
    }
    enum Phase { OPEN, BORN, CLOSED }
    record Permission(ValidationReportAdmission.Start start, Phase phase) {
        public Permission { Objects.requireNonNull(start); Objects.requireNonNull(phase); }
    }
    record Mode(long nextStartSequence, Permission permission) {
        public Mode {
            if (nextStartSequence < 1 || permission == null && nextStartSequence != 1
                    || permission != null && permission.start().sequence() + 1 != nextStartSequence) throw invalid("task.mode");
        }
    }
    record Snapshot(ValidationReportAdmission.Task task, long revision, UUID commandId, ValidationTaskSpec comparison, Settings settings,
                    long schedulingRevision, UUID schedulingCommandId, boolean closed, Mode full, Mode incremental) {
        public Snapshot {
            definition(task, comparison, settings); Objects.requireNonNull(commandId); Objects.requireNonNull(schedulingCommandId);
            Objects.requireNonNull(full); Objects.requireNonNull(incremental);
            if (revision < 1 || revision == Long.MAX_VALUE || schedulingRevision < 1 || schedulingRevision > revision) throw invalid("task.revisions");
            ValidationTaskStore.mode(task, revision, full, ValidationMode.FULL);
            ValidationTaskStore.mode(task, revision, incremental, ValidationMode.INCREMENTAL);
        }
        public Mode mode(ValidationMode selected) { return Objects.requireNonNull(selected) == ValidationMode.FULL ? full : incremental; }
    }
    enum Write { APPLIED, DUPLICATE, CONFLICT, CLOSED }

    /** Missing definitions are not initialized on read. Returned descriptors never grant execution. */
    Optional<Snapshot> read(String pipelineId, String taskId);
    /** Explicit first create; a conflicting task lifetime is never overwritten. Replay uses the same command and body. */
    Write create(Create command);
    /** Exact task lifetime/revision CAS. Existing start sequences and permissions remain unchanged. */
    Write update(Update command);
    /** CONFIG only: blocks new starts and seals original permissions/reports; CACHE termination remains separate. */
    ValidationReportAdmission.Closure close(ValidationReportAdmission.Task task);

    private static void definition(ValidationReportAdmission.Task task, ValidationTaskSpec comparison, Settings settings) {
        Objects.requireNonNull(task); Objects.requireNonNull(comparison); Objects.requireNonNull(settings);
        if (!task.pipelineId().equals(comparison.pipelineId())) throw invalid("task.pipeline");
        comparison.requireFor(settings.full().method(), ValidationMode.FULL);
    }
    private static void mode(ValidationReportAdmission.Task task, long revision, Mode mode, ValidationMode expected) {
        if (mode.permission() != null && (!mode.permission().start().task().equals(task)
                || mode.permission().start().run().mode() != expected || mode.permission().start().configurationRevision() > revision))
            throw invalid("task.permissionScope");
    }
    private static void history(int value) { if (value < 1 || value > 100) throw invalid("task.historyLimit"); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
}
