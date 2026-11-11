package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.validation.ValidationTiming.*;

class ValidationMinimumContractsTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Domain A = new Domain("source-a", "partition-1", "member-a", "boot-a", "epoch-a", "native-v1");
    private static final Domain B = new Domain("source-b", "partition-2", "member-b", "boot-b", "epoch-b", "native-v2");
    private static final Set<Milestone> ALL = Set.of(Milestone.values());
    private static final Set<Milestone> CAPTURE = Set.of(Milestone.CAPTURED, Milestone.HINTS_DELIVERED, Milestone.IMPACTS_SETTLED);

    @Test void everySourceMustCompleteL0ButL1NeverWaitsForCandidateOrTargetCommits() {
        Scope scope = scope(2);
        var pending = coverage(scope, Phase.L0, Map.of(A, new Progress(ALL, false), B, new Progress(CAPTURE, false)));
        var l1 = coverage(scope, Phase.L1, Map.of(A, new Progress(CAPTURE, false), B, new Progress(CAPTURE, false)));
        assertThat(window(pending, l1, true, true).settled()).isFalse();
        assertThat(window(coverage(scope, Phase.L0, complete(false)), l1, true, true).settled()).isTrue();
        for (Milestone omitted : Milestone.values()) {
            var partial = new java.util.HashSet<>(ALL); partial.remove(omitted);
            assertThat(window(coverage(scope, Phase.L0, Map.of(A, new Progress(partial, false), B, new Progress(ALL, false))),
                    l1, true, true).settled()).as(omitted.name()).isFalse();
        }
    }

    @Test void equalSourceReadsCannotHideAbaOrMissingFreshness() {
        var scope = scope(2); var l0 = coverage(scope, Phase.L0, complete(false));
        var l1 = coverage(scope, Phase.L1, complete(true));
        assertThat(window(l0, l1, true, true).settled()).isFalse();
        l1 = coverage(scope, Phase.L1, complete(false));
        assertThat(window(l0, l1, false, true).settled()).isFalse();
        assertThat(window(l0, l1, true, false).settled()).isFalse();
        assertThat(new Capabilities(A, Set.of(Capability.CURRENT_COMMITTED_BOUNDARY)).supportsStrictWindow()).isFalse();
    }

    @Test void domainsEpochsAttemptsAndObservationRevisionsCannotBeMixed() {
        var original = scope(2); var l0 = coverage(original, Phase.L0, complete(false));
        assertInvalid(() -> new Batch(original, "bad", Phase.L0, START, Map.of(A, "9")));
        var changedEpoch = new Domain(B.sourceId(), B.captureDomain(), B.memberId(), "new-boot", "new-epoch", B.semanticsVersion());
        var foreign = new Scope(original.observation(), original.execution(), original.attemptId(), "dependencies",
                List.of(A, changedEpoch), START, 10_000);
        var foreignL1 = coverage(foreign, Phase.L1, Map.of(A, new Progress(ALL, false), changedEpoch, new Progress(ALL, false)));
        assertInvalid(() -> window(l0, foreignL1, true, true));
        assertInvalid(() -> window(l0, coverage(scope(3), Phase.L1, complete(false)), true, true));
        var differentAttempt = new Scope(original.observation(), original.execution(), "attempt-2", "dependencies",
                original.domains(), START, 10_000);
        assertInvalid(() -> window(l0, coverage(differentAttempt, Phase.L1, complete(false)), true, true));
    }

    @Test void windowsRequireRegisteredOrderedDistinctFreshReadsWithinBoundedLifetime() {
        var scope = scope(2); var l0 = coverage(scope, Phase.L0, complete(false)); var l1 = coverage(scope, Phase.L1, complete(false));
        assertInvalid(() -> new Batch(scope, "early", Phase.L0, START.minusNanos(1), Map.of(A, "9", B, "10")));
        assertInvalid(() -> new Batch(scope, "late", Phase.L1, START.plusSeconds(11), Map.of(A, "9", B, "10")));
        assertInvalid(() -> new Window(l0, l1, read("same", 1), read("same", 3), read("s2", 5), true, true));
        assertInvalid(() -> new Window(l0, l1, read("s1", 4), read("target", 3), read("s2", 5), true, true));
        assertInvalid(() -> new Window(l0, l1, read("s1", -1), read("target", 3), read("s2", 5), true, true));
    }

    @Test void l0EvidenceObtainedAfterTargetReadCannotRetroactivelySettleTheWindow() {
        var scope = scope(2);
        var onTime = coverage(scope, Phase.L0, complete(false));
        var late = new Coverage(onTime.batch(), START.plusSeconds(7), onTime.domains());
        assertInvalid(() -> window(late, coverage(scope, Phase.L1, complete(false)), true, true));
    }

    @Test void mappingConfirmationBindsRenamedActualKeysAndRunVersions() {
        var source = binding("source", "source_id"); var target = binding("target", "renamed_id");
        var mapping = new ValidationConfirmedMapping("mapping", "output", 1, "confirmed-by-request", "spec", "rules",
                new ValidationConfirmedMapping.Side(source, "source-model", "source-codec"),
                new ValidationConfirmedMapping.Side(target, "target-model", "target-codec"));
        mapping.requireObservation(observation(2));
        assertThat(source.read(row(Map.of("source_id", 7)))).isEqualTo(target.read(row(Map.of("renamed_id", 7L))));
        assertThat(target.read(row(Map.of("source_id", 7)))).isInstanceOf(ValidationKeyBinding.Invalid.class);
        var run = observation(2).run();
        var stale = new ValidationRunIdentity(run.owner(), run.taskId(), run.runId(), run.mode(), "different-spec", 1, "rules");
        assertInvalid(() -> mapping.requireObservation(new ValidationObservation(stale, observation(2).root(), 2)));
        assertInvalid(() -> new ValidationConfirmedMapping("mapping", "output", 1, "", "spec", "rules", mapping.source(), mapping.target()));
    }

    @Test void unknownAckCannotBePromotedByAgreementAndOldAckCannotCompleteNewRepair() {
        var old = attempt("old"); var current = attempt("new");
        var unknown = ack(current, ValidationRepair.WriteStatus.UNKNOWN);
        assertThat(ValidationRepair.afterRecheck(current, "submission-new", unknown, agreement(current.current(), 8)))
                .isEqualTo(ValidationRepair.Outcome.UNKNOWN);
        assertInvalid(() -> ValidationRepair.afterRecheck(current, "submission-new", ack(old, ValidationRepair.WriteStatus.COMMITTED),
                agreement(current.current(), 8)));
        assertInvalid(() -> ValidationRepair.afterRecheck(current, "wrong-submission", ack(current, ValidationRepair.WriteStatus.COMMITTED),
                agreement(current.current(), 8)));
        assertThat(ValidationRepair.afterRecheck(current, "submission-new", ack(current, ValidationRepair.WriteStatus.COMMITTED),
                agreement(current.current(), 8))).isEqualTo(ValidationRepair.Outcome.REPAIRED);
        for (var status : List.of(ValidationRepair.WriteStatus.FAILED, ValidationRepair.WriteStatus.NO_OUTPUT)) {
            assertThat(ValidationRepair.afterRecheck(current, "submission-new", ack(current, status), null)).isEqualTo(ValidationRepair.Outcome.FAILED);
        }
    }

    @Test void committedWriteStillNeedsThisObservationFullRecheckAfterAck() {
        var attempt = attempt("new"); var ack = ack(attempt, ValidationRepair.WriteStatus.COMMITTED);
        assertInvalid(() -> ValidationRepair.afterRecheck(attempt, ack.submissionId(), ack, agreement(observation(3), 8)));
        assertInvalid(() -> ValidationRepair.afterRecheck(attempt, ack.submissionId(), ack, agreement(observation(2), 6)));
        var scope = scope(2);
        ValidationRepair.requireWindow(attempt, window(coverage(scope, Phase.L0, complete(false)), coverage(scope, Phase.L1, complete(false)), true, true));
        assertInvalid(() -> ValidationRepair.requireWindow(attempt,
                window(coverage(scope, Phase.L0, complete(false)), coverage(scope, Phase.L1, complete(true)), true, true)));
        assertInvalid(() -> new ValidationRepair.Snapshot(attempt, physical("wrong", "id"), row(Map.of("id", 7)), read("source", 7)));
    }

    private static ValidationKeyBinding binding(String id, String field) {
        return new ValidationKeyBinding(id, List.of(new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.EXACT_NUMBER, Set.of())),
                Set.of(), new ValidationValues.Limits(4096, 32, 4));
    }
    private static ValidationValues.Row row(Map<String, ?> value) { return ValidationValues.ownRow(value, new ValidationValues.Limits(4096, 32, 4)); }
    private static ValidationObservation observation(long revision) {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "boot", 1, 1, 0),
                "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
        return new ValidationObservation(run, new ValidationRoot("mapping", "output", key(), List.of()), revision);
    }
    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(7))); }
    private static ValidationPhysicalIdentity physical(String connection, String field) {
        return new ValidationPhysicalIdentity(connection, "db", null, "table", List.of(List.of(field)), key());
    }
    private static Scope scope(long revision) {
        return new Scope(observation(revision), new Execution("cluster", "pipeline", "definition", "production", 1),
                "attempt", "dependencies", List.of(A, B), START, 10_000);
    }
    private static Map<Domain, Progress> complete(boolean changed) { return Map.of(A, new Progress(ALL, changed), B, new Progress(ALL, false)); }
    private static Coverage coverage(Scope scope, Phase phase, Map<Domain, Progress> progress) {
        var tokens = new java.util.HashMap<Domain, String>(); scope.domains().forEach(domain -> tokens.put(domain, "opaque-9"));
        return new Coverage(new Batch(scope, "batch-" + phase, phase, START.plusSeconds(phase == Phase.L0 ? 0 : 6), tokens), START.plusSeconds(phase == Phase.L0 ? 0 : 6), progress);
    }
    private static Window window(Coverage l0, Coverage l1, boolean fresh, boolean same) {
        return new Window(l0, l1, read("s1", 1), read("target", 3), read("s2", 5), fresh, same);
    }
    private static ValidationCheckEvent.ReadBoundary read(String id, int second) {
        return new ValidationCheckEvent.ReadBoundary(id, "read-v1", START.plusSeconds(second), START.plusSeconds(second).plusMillis(10));
    }
    private static ValidationRepair.Attempt attempt(String id) {
        return new ValidationRepair.Attempt("request-" + id, id, observation(1), observation(2), scope(2).execution(),
                physical("source", "source_id"), physical("target", "renamed_id"));
    }
    private static ValidationRepair.Ack ack(ValidationRepair.Attempt attempt, ValidationRepair.WriteStatus status) {
        return new ValidationRepair.Ack(attempt, "submission-" + attempt.repairId(), "output", attempt.target(), status, START.plusSeconds(7));
    }
    private static ValidationRootCheck agreement(ValidationObservation observation, int second) {
        try (var builder = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, ValidationRootCheck.TargetPresence.PRESENT,
                new ValidationRootCheck.Limits(32768, 10, 10, 10, 0))) {
            builder.read(ValidationRootCheck.Side.SOURCE, read("source-check", second));
            builder.read(ValidationRootCheck.Side.TARGET, read("target-check", second));
            for (var check : ValidationRootCheck.Check.values()) builder.checked(check);
            return builder.finish();
        }
    }
    private static void assertInvalid(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOf(TapstateException.class)
                .satisfies(error -> assertThat(((TapstateException) error).code()).isEqualTo(ValidationError.INVALID_CONTRACT));
    }
}
