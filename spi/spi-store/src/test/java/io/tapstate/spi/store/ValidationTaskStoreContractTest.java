package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.spi.store.ValidationTaskStore.*;

class ValidationTaskStoreContractTest {
    @Test void countConfigurationAndSnapshotNeedNoInventedKeyOrCodecBinding() {
        var task = task(); var comparison = comparison("a"); var command = UUID.randomUUID();
        var settings = Settings.defaults(ValidationMethod.COUNT, 1_000);
        assertThatCode(() -> new Create(task, comparison, settings, command)).doesNotThrowAnyException();
        var snapshot = new Snapshot(task, 1, command, comparison, settings, 1, command, false, new Mode(1, null), new Mode(1, null));
        assertThat(snapshot.comparison().mappings().getFirst().keys()).isNull();
        assertThat(snapshot.mode(ValidationMode.FULL).nextStartSequence()).isOne();
        assertThat(snapshot.settings().full().maxConcurrentTables()).isOne();
        assertThat(snapshot.settings().full().historyLimit()).isEqualTo(10);
        assertThat(snapshot.settings().incremental().historyLimit()).isEqualTo(10);
    }

    @Test void aLaterDefinitionDoesNotRewriteItsOlderOriginalActiveStart() {
        var task = task(); var before = comparison("a"); var current = comparison("d");
        var original = start(task, before, 1, 1); var command = UUID.randomUUID();
        var snapshot = new Snapshot(task, 2, command, current, Settings.defaults(ValidationMethod.COUNT, 2_000), 2, command, false,
                new Mode(2, new Permission(original, Phase.BORN)), new Mode(1, null));
        assertThat(snapshot.full().permission().start()).isSameAs(original);
        assertThat(snapshot.full().permission().start().run().specHash()).isNotEqualTo(new ValidationRunSpec(current, ValidationMode.FULL, ValidationMethod.COUNT).fingerprint());
        assertThat(snapshot.full().permission().start().configurationRevision()).isOne();
        assertThat(snapshot.schedulingRevision()).isEqualTo(2);
    }

    @Test void snapshotPermissionsCannotCrossTaskLifetimesModesOrFutureRevisions() {
        var task = task(); var comparison = comparison("a"); var command = UUID.randomUUID(); var settings = Settings.defaults(ValidationMethod.COUNT, 1_000);
        var original = start(task, comparison, 1, 1); var held = new Mode(2, new Permission(original, Phase.OPEN));
        var replaced = new ValidationReportAdmission.Task(task.clusterId(), task.pipelineId(), task.taskId(), UUID.randomUUID());
        assertThatThrownBy(() -> new Snapshot(replaced, 1, command, comparison, settings, 1, command, false, held, new Mode(1, null)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Snapshot(task, 1, command, comparison, settings, 1, command, false, new Mode(1, null), held))
                .isInstanceOf(TapstateException.class);
        var future = new Mode(2, new Permission(start(task, comparison, 3, 1), Phase.OPEN));
        assertThatThrownBy(() -> new Snapshot(task, 2, command, comparison, settings, 1, command, false, future, new Mode(1, null)))
                .isInstanceOf(TapstateException.class);
    }

    @Test void exhaustedStartSequenceRemainsReadableAndCannotOverflowIntoAnotherPermission() {
        var task = task(); var comparison = comparison("a"); var original = start(task, comparison, 1, Long.MAX_VALUE - 1);
        var held = new Mode(Long.MAX_VALUE, new Permission(original, Phase.CLOSED)); var command = UUID.randomUUID();
        var snapshot = new Snapshot(task, 1, command, comparison, Settings.defaults(ValidationMethod.COUNT, 1_000), 1, command, false, held, new Mode(1, null));
        assertThat(snapshot.full().nextStartSequence()).isEqualTo(Long.MAX_VALUE);
        assertThatThrownBy(() -> new Mode(Long.MIN_VALUE, held.permission())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> start(task, comparison, 1, Long.MAX_VALUE)).isInstanceOf(TapstateException.class);
    }

    private static ValidationReportAdmission.Task task() { return new ValidationReportAdmission.Task("cluster", "flow", "task", UUID.randomUUID()); }
    private static ValidationTaskSpec comparison(String pipelineHash) {
        return new ValidationTaskSpec("flow", Map.of("flow", pipelineHash.repeat(64), "source", "b".repeat(64), "views", "c".repeat(64)), 1, "rules-v1",
                List.of(new ValidationTaskSpec.Mapping("mapping", ValidationTaskSpec.OutputKind.SYNC, "output",
                        new ValidationTaskSpec.Table("input", "source", "db", null, "orders", null, null),
                        new ValidationTaskSpec.Table("output", "views", "db", null, "renamed", null, null), null)));
    }
    private static ValidationReportAdmission.Start start(ValidationReportAdmission.Task task, ValidationTaskSpec comparison, long revision, long sequence) {
        var spec = new ValidationRunSpec(comparison, ValidationMode.FULL, ValidationMethod.COUNT);
        var owner = new ValidationOwner(task.clusterId(), task.pipelineId(), "node", "boot", 1, 1, 1);
        return new ValidationReportAdmission.Start(task, revision, sequence, "start-" + sequence,
                new ValidationRunIdentity(owner, task.taskId(), "run-" + sequence, spec.mode(), spec.fingerprint(), comparison.mappingRevision(), comparison.comparisonRulesVersion()));
    }
}
