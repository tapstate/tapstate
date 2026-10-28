package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationRunLifecycleTest {
    @Test void stoppingBeforeSubmissionRetainsTheRememberedHandlesActualResources() {
        var f=new Fixture();f.loseRunningReply=true;assertThat(f.lifecycle.advance().controlUnknown()).isTrue();
        f.lifecycle.stop();var stopped=f.lifecycle.advance();
        assertThat(f.execution.starts).isZero();assertThat(stopped.retired()).isTrue();
        assertThat(stopped.drained()).isFalse();assertThat(f.dataReturned).isZero();assertThat(f.processorsReturned).isZero();
        f.execution.processors=true;f.lifecycle.observePhysicalDrain();assertThat(f.processorsReturned).isEqualTo(1);assertThat(f.dataReturned).isZero();
        f.execution.readers=true;f.lifecycle.observePhysicalDrain();assertThat(f.dataReturned).isEqualTo(1);
    }
    @Test void initialWaitingCreatesNoJobReaderOrHintAndCancelsTheOriginalUnstartedRun() {
        var f = new Fixture(); f.waiting = true;
        var status = f.lifecycle.advance();
        assertThat(status.state()).isEqualTo(ValidationRunState.STARTING); assertThat(status.waiting()).isTrue();
        assertThat(f.created).isZero(); assertThat(f.execution.starts).isZero(); assertThat(f.open).isTrue();
        f.lifecycle.stop(); assertThat(f.sealed).isTrue(); assertThat(f.closed).isFalse();
        status = f.lifecycle.advance();
        assertThat(status.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(status.retired()).isTrue(); assertThat(status.drained()).isTrue();
        assertThat(f.closed).isTrue(); assertThat(f.created).isZero(); assertThat(f.processorsReturned).isEqualTo(1); assertThat(f.dataReturned).isEqualTo(1);
    }

    @Test void aLostRunningTransitionReplyReconcilesTheOriginalBeforeExactlyOneSubmission() {
        var f = new Fixture(); f.loseRunningReply = true;
        assertThat(f.lifecycle.advance().controlUnknown()).isTrue();
        assertThat(f.execution.starts).isZero(); assertThat(f.created).isEqualTo(1);
        assertThat(f.lifecycle.advance().state()).isEqualTo(ValidationRunState.RUNNING);
        f.lifecycle.advance(); assertThat(f.execution.starts).isEqualTo(1); assertThat(f.created).isEqualTo(1);
        assertThat(f.starts).hasSize(1); assertThat(f.starts.getFirst()).isEqualTo(f.original.admission());
    }

    @Test void unknownClosureRetainsRunAndActualDrainOutlivesKnownTerminalState() {
        var f = new Fixture(); f.lifecycle.advance(); f.loseCloseReply = true;
        f.lifecycle.stop(); var unknown = f.lifecycle.advance();
        assertThat(unknown.controlUnknown()).isTrue(); assertThat(unknown.retired()).isFalse();
        assertThat(f.dataReturned).isZero(); assertThat(f.processorsReturned).isZero();
        var terminal = f.lifecycle.advance(); assertThat(terminal.state()).isEqualTo(ValidationRunState.STOPPED);
        assertThat(terminal.retired()).isTrue(); assertThat(terminal.drained()).isFalse();
        f.execution.processors = true; f.lifecycle.advance(); assertThat(f.processorsReturned).isEqualTo(1); assertThat(f.dataReturned).isZero();
        f.execution.readers = true; assertThat(f.lifecycle.advance().drained()).isTrue(); assertThat(f.dataReturned).isEqualTo(1);
        f.lifecycle.advance(); assertThat(f.processorsReturned).isEqualTo(1); assertThat(f.dataReturned).isEqualTo(1);
    }

    @Test void jetAndReaderExitWithoutBusinessCompletionCannotProduceCompleted() {
        var f = new Fixture(); f.lifecycle.advance(); f.execution.processors = true; f.execution.readers = true;
        assertThat(f.lifecycle.advance().state()).isEqualTo(ValidationRunState.RUNNING); assertThat(f.closed).isFalse();
        f.execution.complete = true;
        var status = f.lifecycle.advance(); assertThat(status.state()).isEqualTo(ValidationRunState.COMPLETED); assertThat(status.retired()).isTrue();
        assertThat(status.drained()).isTrue(); assertThat(f.closed).isTrue();
    }

    @Test void failedSubmissionIsNeverRetriedAndUnknownExecutionRetainsPhysicalReservations() {
        var f = new Fixture(); f.execution.throwStart = true;
        var status = f.lifecycle.advance(); assertThat(status.state()).isEqualTo(ValidationRunState.FAILED); assertThat(status.retired()).isTrue();
        assertThat(status.drained()).isFalse(); f.lifecycle.advance(); assertThat(f.execution.starts).isEqualTo(1);
        assertThat(f.dataReturned).isZero(); assertThat(f.processorsReturned).isZero();
        assertThat(status.failure()).isSameAs(f.execution.failure);
    }

    @Test void unknownRetirementRetriesItsOriginalTerminalWithoutAnIllegalTerminalTransition() {
        var f = new Fixture(); f.lifecycle.advance(); f.loseRetireReply = true;
        f.lifecycle.stop(); var unknown = f.lifecycle.advance();
        assertThat(unknown.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(unknown.controlUnknown()).isTrue(); assertThat(unknown.retired()).isFalse();
        assertThat(f.lifecycle.advance().retired()).isTrue(); assertThat(f.actual).isEqualTo(ValidationRunState.STOPPED); assertThat(f.execution.starts).isEqualTo(1);
    }

    @Test void stopAfterLostRunningReplyFirstReconcilesThatOriginalTransition() {
        var f = new Fixture(); f.loseRunningReply = true; assertThat(f.lifecycle.advance().controlUnknown()).isTrue();
        f.lifecycle.stop(); var status = f.lifecycle.advance();
        assertThat(status.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(status.retired()).isTrue(); assertThat(f.closed).isTrue(); assertThat(f.execution.starts).isZero();
    }

    @Test void failureAfterLostRunningReplyFirstReconcilesThatOriginalTransition() {
        var f = new Fixture(); f.loseRunningReply = true; f.lifecycle.advance(); f.lifecycle.fail(unknown());
        assertThat(f.lifecycle.advance().state()).isEqualTo(ValidationRunState.FAILED); assertThat(f.lifecycle.status().retired()).isTrue();
    }

    @Test void failureAfterAnExternalStopLatchUsesTheActualOriginalStoppingState() {
        var f = new Fixture(); f.lifecycle.advance(); f.actual = ValidationRunState.STOPPING; f.lifecycle.fail(unknown());
        var status = f.lifecycle.advance(); assertThat(status.state()).isEqualTo(ValidationRunState.FAILED); assertThat(status.retired()).isTrue();
    }

    @Test void aDefinitiveJoinRefusalClosesOriginalConfigAndFencesLateJoinWithoutInventingAJoin() {
        var f = new Fixture(); f.joinAnswer = ValidationControlStore.Join.GENERATION_CLOSED;
        var status = f.lifecycle.advance();
        assertThat(f.closed).isTrue(); assertThat(status.state()).isEqualTo(ValidationRunState.FAILED);
        assertThat(status.admitted()).isFalse(); assertThat(status.retired()).isTrue(); assertThat(f.abandoned).isEqualTo(1);
        assertThat(f.created).isZero(); assertThat(f.execution.starts).isZero(); assertThat(f.processorsReturned).isEqualTo(1);
    }

    @Test void actualPhysicalExitCanReturnDataAndCpuAfterOwnerLossWithoutPretendingControlRetired() {
        var f = new Fixture(); f.lifecycle.advance(); f.lifecycle.stop(); f.ownerLive = false;
        f.execution.processors = true; f.execution.readers = true; var status = f.lifecycle.advance();
        assertThat(status.retired()).isFalse(); assertThat(f.closed).isFalse(); assertThat(status.drained()).isTrue();
        assertThat(f.processorsReturned).isEqualTo(1); assertThat(f.dataReturned).isEqualTo(1);
    }

    @Test void aKnownStopWinsAgainstFullBusinessCompletionWithoutAnImpossibleCompletionRetry() {
        var f = new Fixture(); f.lifecycle.advance(); f.actual = ValidationRunState.STOPPING; f.execution.complete = true;
        var status = f.lifecycle.advance();
        assertThat(status.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(status.retired()).isTrue(); assertThat(status.controlUnknown()).isFalse();
    }

    @Test void cancellationOfAnUnconfirmedJoinSealsItEvenWhenJoiningNowThrowsForClosedConfig() {
        var f = new Fixture(); f.joinClosesConfigAndThrows = true;
        assertThat(f.lifecycle.advance().controlUnknown()).isTrue();
        f.closed = true; f.lifecycle.stop(); var status = f.lifecycle.advance();
        assertThat(status.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(status.retired()).isTrue(); assertThat(f.abandoned).isEqualTo(1);
        assertThat(f.created).isZero(); assertThat(f.execution.starts).isZero();
    }

    @Test void aDefinitiveBirthRefusalEndsTheJoinedOriginalWithoutAnExternalStop() {
        var f = new Fixture(); f.closeAfterJoin = true; var status = f.lifecycle.advance();
        assertThat(status.state()).isEqualTo(ValidationRunState.FAILED); assertThat(status.retired()).isTrue();
        assertThat(status.admitted()).isTrue(); assertThat(f.created).isZero(); assertThat(f.execution.starts).isZero();
    }

    private static final class Fixture implements ValidationRunLifecycle.Activation, ValidationRunLifecycle.Local, ValidationReportAdmission, ValidationControlStore {
        final ValidationRunIdentity identity = new ValidationRunIdentity(new ValidationOwner("cluster", "flow", "node", "boot", 1, 1, 1),
                "task", UUID.randomUUID().toString(), ValidationMode.FULL, spec().fingerprint(), 1, "rules");
        final ValidationControlStore.Run original = new ValidationControlStore.Run(identity, 1,
                new ValidationReportAdmission.Start(new ValidationReportAdmission.Task("cluster", "flow", "task", UUID.randomUUID()), 1, 1, UUID.randomUUID().toString(), identity), spec());
        final List<Start> starts = new ArrayList<>(); final Work execution = new Work();
        final ValidationRunLifecycle lifecycle = new ValidationRunLifecycle(original, this, this, this, this);
        ValidationRunState actual = ValidationRunState.STARTING;
        boolean waiting, open, closed, sealed, loseRunningReply, loseCloseReply, loseRetireReply, joinClosesConfigAndThrows, closeAfterJoin, ownerLive = true; int created, processorsReturned, dataReturned, abandoned;
        Join joinAnswer = Join.JOINED;
        @Override public ValidationRunLifecycle.Readiness readiness() { return waiting ? ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD : ValidationRunLifecycle.Readiness.READY; }
        @Override public ValidationRunLifecycle.Execution create() { created++; return execution; }
        @Override public void requireOwner() { if (!ownerLive) throw unknown(); }
        @Override public void requireRun() { if (sealed) throw new IllegalStateException("publication already sealed"); }
        @Override public void sealPublication() { sealed = true; }
        @Override public void retired(ValidationRunState terminal) { assertThat(closed).isTrue(); assertThat(actual).isEqualTo(terminal); }
        @Override public void processorsExited() { processorsReturned++; }
        @Override public void drained() { dataReturned++; }
        @Override public Reservation reserve(Start start) { assertThat(start).isEqualTo(original.admission()); if (closed) return Reservation.CLOSED; if (starts.isEmpty()) starts.add(start); return Reservation.RESERVED; }
        @Override public Birth open(Start start) { throw new AssertionError("snapshot required"); }
        @Override public Birth open(Open command) { assertThat(command.start()).isEqualTo(original.admission()); if (closed) return Birth.CLOSED; open = true; return Birth.CREATED; }
        @Override public Closure close(Start start) { assertThat(start).isEqualTo(original.admission()); closed = true; if (loseCloseReply) { loseCloseReply = false; throw unknown(); } return Closure.CLOSED; }
        @Override public Join join(ValidationControlStore.Run run) { assertThat(run).isEqualTo(original); if (joinClosesConfigAndThrows) throw unknown(); if (closeAfterJoin) closed = true; return joinAnswer; }
        @Override public Optional<ValidationControlStore.Status> read(ValidationOwner owner, ValidationControlStore.Run run) {
            assertThat(run).isEqualTo(original); return Optional.of(new ValidationControlStore.Status(original, actual, abandoned > 0));
        }
        @Override public Mutation abandonClosedStart(Abandonment command) { assertThat(closed).isTrue(); abandoned++; actual = command.terminal(); return Mutation.APPLIED; }
        @Override public Mutation transition(Transition change) {
            assertThat(change.run()).isEqualTo(original);
            if (actual == change.next()) return Mutation.ALREADY_APPLIED;
            if (actual != change.expected()) return Mutation.STALE; if (change.next().terminal()) assertThat(closed).isTrue();
            actual = change.next();
            if (actual == ValidationRunState.RUNNING && loseRunningReply) { loseRunningReply = false; throw unknown(); }
            return Mutation.APPLIED;
        }
        @Override public Mutation latchProblemLimit(ValidationControlStore.Run run) { throw new AssertionError(); }
        @Override public Mutation retire(Retirement command) { assertThat(closed).isTrue(); if (loseRetireReply) { loseRetireReply = false; throw unknown(); } return Mutation.APPLIED; }
        @Override public Mutation terminateLostOwner(LostOwner command) { throw new AssertionError(); }
        final class Work implements ValidationRunLifecycle.Execution {
            int starts; boolean complete, processors, readers, throwStart; final TapstateException failure = unknown();
            @Override public ValidationRunIdentity identity() { return Fixture.this.identity; }
            @Override public void start() { starts++; if (throwStart) throw failure; }
            @Override public ValidationRunLifecycle.Progress progress() { return new ValidationRunLifecycle.Progress(complete, processors, readers, null); }
            @Override public void cancel() {}
        }
    }
    private static TapstateException unknown() { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "test.unknownResponse"), null); }
    private static ValidationRunSpec spec() {
        var key = List.of(new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number")));
        var task = new ValidationTaskSpec("flow", Map.of("flow", "a".repeat(64), "source", "b".repeat(64), "target", "c".repeat(64)), 1, "rules", List.of(
                new ValidationTaskSpec.Mapping("mapping", ValidationTaskSpec.OutputKind.SYNC, "out",
                        new ValidationTaskSpec.Table("source", "source", "database", null, "orders", "model", "codec"),
                        new ValidationTaskSpec.Table("out", "target", "database", null, "orders", "model", "codec"), new ValidationTaskSpec.Keys("confirmed", key, key))));
        return new ValidationRunSpec(task, ValidationMode.FULL, ValidationMethod.COUNT);
    }
}
