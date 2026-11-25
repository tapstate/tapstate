package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.containers.MongoDBContainer;
import static org.assertj.core.api.Assertions.*;

/** Actual CFG/CACHE closure and run CAS; no business validator or initial-output proof is substituted. */
@RequiresDocker
class NativeValidationRunLifecycleIT {
    @Container static final MongoDBContainer MONGO = NativeValidationOwnerReservationsIT.MONGO;
    @Test void originalStartingWaitIsCancelledAndRetiredWithoutAnyValidationJob() {
        try (var fixture = new NativeValidationOwnerReservationsIT.Fixture()) {
            var local = new Local(fixture, "FULL");
            var status = local.lifecycle.advance(); assertThat(status.waiting()).isTrue(); assertThat(status.state()).isEqualTo(ValidationRunState.STARTING);
            assertThat(local.access.control().read(local.run.identity().owner(), local.run).orElseThrow().retired()).isFalse();
            assertThat(fixture.member.getJet().getJobs()).isEmpty();
            local.reservation.close(); status = local.lifecycle.advance();
            assertThat(status.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(status.retired()).isTrue(); assertThat(status.drained()).isTrue();
            assertThat(local.access.control().read(local.run.identity().owner(), local.run).orElseThrow().retired()).isTrue();
            assertThat(fixture.tasks.read("flow", "task").orElseThrow().full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.CLOSED);
            assertThat(fixture.raw.getDatabase(fixture.configName).getCollection("validation_reports").find().first()).containsEntry("closed", true);
            assertThat(fixture.resources.snapshot().metadataFrames()).isZero(); assertThat(fixture.resources.snapshot().processors()).isZero();
        }
    }

    @Test void aRealSharedStopLatchRejectsJoiningThenFencesTheOriginalAgainstLateJoin() {
        try (var fixture = new NativeValidationOwnerReservationsIT.Fixture()) {
            var full = new Local(fixture, "FULL"); var incremental = new Local(fixture, "INCREMENTAL");
            incremental.lifecycle.advance();
            assertThat(incremental.access.control().latchProblemLimit(incremental.run)).isEqualTo(ValidationControlStore.Mutation.APPLIED);
            var status = full.lifecycle.advance();
            assertThat(status.state()).isEqualTo(ValidationRunState.FAILED); assertThat(status.admitted()).isFalse(); assertThat(status.retired()).isTrue();
            var stored = full.access.control().read(full.run.identity().owner(), full.run).orElseThrow();
            assertThat(stored.retired()).isTrue(); assertThat(stored.state()).isEqualTo(ValidationRunState.FAILED);
            assertThat(fixture.tasks.read("flow", "task").orElseThrow().full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.CLOSED);
            assertThat(full.access.control().abandonClosedStart(full.run)).isEqualTo(ValidationControlStore.Mutation.ALREADY_APPLIED);
            assertThat(full.access.control().read(full.run.identity().owner(), incremental.run).orElseThrow().state()).isEqualTo(ValidationRunState.STOPPING);
            assertThat(fixture.member.getJet().getJobs()).isEmpty();
            incremental.lifecycle.stop(); incremental.lifecycle.advance();
        }
    }

    @Test void anUnbornClosedStartFencesLateJoinWithoutAnOpenReportOrAnInvalidEmptyGeneration() {
        try (var fixture = new NativeValidationOwnerReservationsIT.Fixture()) {
            var local = new Local(fixture, "FULL");
            local.access.admissions().reserve(local.run.admission()); local.access.admissions().close(local.run.admission());
            assertThat(local.access.control().abandonClosedStart(local.run)).isEqualTo(ValidationControlStore.Mutation.APPLIED);
            var snapshot = local.access.control().snapshot(local.run.identity().owner());
            assertThat(snapshot.closed()).isTrue(); assertThat(snapshot.active()).isEmpty(); assertThat(snapshot.countingGeneration()).isEqualTo(1);
            assertThat(snapshot.retainedRuns()).isEqualTo(1);
            assertThat(fixture.raw.getDatabase(fixture.configName).getCollection("validation_reports").countDocuments()).isZero();
            assertThatThrownBy(() -> local.access.control().join(local.run)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            local.lifecycle.stop(); local.lifecycle.advance();
        }
    }

    @Test void aClosedConfigAfterAnUnconfirmedJoinIsStillCancelledAndFenced() {
        try (var fixture = new NativeValidationOwnerReservationsIT.Fixture()) {
            var local = new Local(fixture, "FULL", true);
            var status = local.lifecycle.advance();
            assertThat(status.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(status.retired()).isTrue(); assertThat(status.drained()).isTrue();
            assertThat(status.admitted()).isFalse(); assertThat(status.controlUnknown()).isFalse();
            var known = local.access.control().read(local.run.identity().owner(), local.run).orElseThrow();
            assertThat(known.state()).isEqualTo(ValidationRunState.STOPPED); assertThat(known.retired()).isTrue();
            assertThat(fixture.raw.getDatabase(fixture.configName).getCollection("validation_reports").countDocuments()).isZero();
            assertThat(fixture.member.getJet().getJobs()).isEmpty(); assertThat(fixture.resources.snapshot().metadataFrames()).isZero();
        }
    }

    @Test void originalConfigClosureAfterKnownJoinFailsTheUnbornRunWithoutAnExternalStop() {
        try (var fixture = new NativeValidationOwnerReservationsIT.Fixture()) {
            var local = new Local(fixture, "FULL", 2); var status = local.lifecycle.advance();
            assertThat(status.state()).isEqualTo(ValidationRunState.FAILED); assertThat(status.retired()).isTrue(); assertThat(status.admitted()).isTrue();
            assertThat(local.access.control().read(local.run.identity().owner(), local.run).orElseThrow().retired()).isTrue();
            assertThat(fixture.raw.getDatabase(fixture.configName).getCollection("validation_reports").countDocuments()).isZero();
            assertThat(fixture.member.getJet().getJobs()).isEmpty(); assertThat(fixture.resources.snapshot().metadataFrames()).isZero();
        }
    }

    private static final class Local implements ValidationRunLifecycle.Local, ValidationRunLifecycle.Activation {
        final NativeValidationOwnerReservations.Reservation reservation;
        final ValidationMemberResources.Run resources;
        final io.tapstate.adapters.mongostore.MongoConfigurationDomain.ValidationAccess access;
        final ValidationControlStore.Run run;
        final ValidationRunLifecycle lifecycle;
        boolean sealed;
        Local(NativeValidationOwnerReservationsIT.Fixture fixture, String mode) { this(fixture, mode, false); }
        Local(NativeValidationOwnerReservationsIT.Fixture fixture, String mode, boolean closeAfterJoin) { this(fixture, mode, closeAfterJoin ? 1 : 0); }
        Local(NativeValidationOwnerReservationsIT.Fixture fixture, String mode, int joinGate) {
            var request = fixture.request(mode, 1); var response = fixture.owners.reserve(request);
            assertThat(response.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec = new ValidationRunSpec(fixture.comparison, ValidationMode.valueOf(mode), mode.equals("FULL") ? ValidationMethod.COUNT : ValidationMethod.FULL_COLUMN);
            var identity = new ValidationRunIdentity(response.owner(), "task", request.run(), spec.mode(), spec.fingerprint(), 1, "rules");
            var start = new ValidationReportAdmission.Start(fixture.task, 1, 1, request.command().toString(), identity);
            reservation = fixture.owners.take(start); resources = reservation.resources(); access = reservation.access();
            run = new ValidationControlStore.Run(identity, reservation.countingGeneration(), start, spec);
            var nativeControl = access.control();
            var control = joinGate == 0 ? nativeControl : new Control(nativeControl) {
                @Override public Join join(Run run) {
                    var joined = super.join(run); assertThat(joined).isEqualTo(Join.JOINED);
                    access.admissions().close(run.admission());
                    if (joinGate == 1) { Local.this.lifecycle.stop(); return nativeControl.join(run); }
                    return joined;
                }
            };
            lifecycle = new ValidationRunLifecycle(run, access.admissions(), control, this, this); reservation.activate(lifecycle::stop);
        }
        @Override public void requireOwner() { reservation.requireOwner(); }
        @Override public void requireRun() { reservation.requireReserved(); if (sealed) throw new AssertionError("sealed run read"); }
        @Override public void sealPublication() { sealed = true; }
        @Override public void retired(ValidationRunState terminal) { reservation.releaseAfterKnownTerminal(); }
        @Override public void processorsExited() { resources.processorsExited(); }
        @Override public void drained() { resources.close(); }
        @Override public ValidationRunLifecycle.Readiness readiness() { return ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD; }
        @Override public ValidationRunLifecycle.Execution create() { throw new AssertionError("initial wait must create no Job, reader or hint registration"); }
    }

    private static class Control implements ValidationControlStore {
        final ValidationControlStore original;
        Control(ValidationControlStore original) { this.original = original; }
        @Override public Join join(Run run) { return original.join(run); }
        @Override public Mutation transition(Transition command) { return original.transition(command); }
        @Override public Mutation latchProblemLimit(Run run) { return original.latchProblemLimit(run); }
        @Override public Mutation retire(Retirement command) { return original.retire(command); }
        @Override public Mutation terminateLostOwner(LostOwner command) { return original.terminateLostOwner(command); }
        @Override public Mutation abandonClosedStart(Abandonment command) { return original.abandonClosedStart(command); }
        @Override public java.util.Optional<Status> read(ValidationOwner owner, Run run) { return original.read(owner, run); }
    }
}
