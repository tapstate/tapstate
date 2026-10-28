package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationControlStore;
import io.tapstate.spi.store.ValidationReportAdmission;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Serialized control for one original run. Invoke advance on admitted control I/O, never on a Jet
 * tasklet, source callback or deadline thread. Cancellation only closes local admission immediately;
 * durable closure and actual task/query drain remain separate, retryable original operations.
 */
final class ValidationRunLifecycle {
    enum Readiness { WAITING_FOR_INITIAL_LOAD, UNKNOWN, READY }
    interface Execution {
        ValidationRunIdentity identity();
        /** Submission is single attempt. A thrown response may still have created the original Job. */
        void start();
        /** Passive facts from the original execution; Jet completion alone is insufficient. */
        Progress progress();
        /** The actual admitted owner-local queue; absent for a full scan. No query runs here. */
        default java.util.Optional<io.tapstate.runtime.engine.validation.ValidationCandidateQueue> candidates() { return java.util.Optional.empty(); }
        /** Nonblocking local cancellation signal only, idempotent even before start. */
        void cancel();
    }
    record Progress(boolean businessComplete, boolean processorsExited, boolean readersReleased, Throwable failure) {}
    interface Activation {
        /** Metadata/control evidence only. Waiting must create no Job, reader or hint registration. */
        Readiness readiness();
        /** Recheck and reserve execution resources; uncertain evidence retains this STARTING run. */
        default Readiness prepare(){return readiness();}
        /** Construct a local handle after readiness, with no reader, subscription or Job side effect. */
        Execution create();
        /** Bind local control to the remembered handle before RUNNING or its single submission. */
        default void prepared(Execution execution) {}
    }
    interface Local {
        /** Known CACHE join, followed only by local quota admission; never a second store join. */
        default void joined() {}
        void requireOwner();
        void requireRun();
        void sealPublication();
        /** Known CFG refusal/closure and CACHE retirement/closed-start fencing; no physical drain claim. */
        void retired(ValidationRunState terminal);
        void processorsExited();
        /** Actual reader/row/task ownership has ended. Idempotent; no cancellation-as-drain. */
        void drained();
    }
    record Status(ValidationRunState state, boolean admitted, boolean waiting, boolean controlUnknown,
                  boolean retired, boolean drained, Throwable failure) {}

    private final ValidationControlStore.Run original;
    private final ValidationReportAdmission admissions;
    private final ValidationControlStore control;
    private final Activation activation;
    private final Local local;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private ValidationRunState state = ValidationRunState.STARTING;
    private boolean reserved, joined, born, waiting, attempted, unknown, retired, drained, processorReceipt, retirementNotified;
    private boolean desiredComplete;
    private ValidationRunState terminalChoice;
    private ValidationControlStore.Transition pendingTransition;
    private boolean admissionRejected, joinRejected;
    private volatile Execution execution;
    private Throwable controlFailure;

    ValidationRunLifecycle(ValidationControlStore.Run original, ValidationReportAdmission admissions,
            ValidationControlStore control, Activation activation, Local local) {
        this.original = Objects.requireNonNull(original); this.admissions = Objects.requireNonNull(admissions);
        this.control = Objects.requireNonNull(control); this.activation = Objects.requireNonNull(activation); this.local = Objects.requireNonNull(local);
        if (original.admission() == null || original.snapshot() == null) throw invalid("run.originalSnapshot");
    }
    ValidationControlStore.Run original() { return original; }

    /** Deadline/owner-loss safe: local fences and cancellation signal do not perform durable I/O. */
    void stop() { stopping.set(true); local.sealPublication(); var current = execution; if (current != null) current.cancel(); }
    void fail(Throwable reported) { failure.record(Objects.requireNonNull(reported)); stop(); }

    synchronized Status status() { return new Status(state, joined, waiting, unknown, retired, drained, failure.get() != null ? failure.get() : controlFailure); }

    synchronized Status advance() {
        unknown = false; controlFailure = null;
        try {
            if (!retired) {
                local.requireOwner();
                reconcileTransition();
                if (!reserved) {
                    var answer = admissions.reserve(original.admission());
                    if (answer == ValidationReportAdmission.Reservation.RESERVED || answer == ValidationReportAdmission.Reservation.DUPLICATE) reserved = true;
                    else { admissionRejected = true; fail(invalid("run.originalAdmission")); }
                }
                if (reserved && !joined && !joinRejected) {
                    if (stopping.get()) joinRejected = true;
                    else {
                        ValidationControlStore.Join answer;
                        try { answer = control.join(original); }
                        catch (TapstateException uncertain) {
                            var permission = admissions.reserve(original.admission());
                            if (permission != ValidationReportAdmission.Reservation.CLOSED) throw uncertain;
                            joinRejected = true; if (!stopping.get()) fail(uncertain); answer = null;
                        }
                    if (answer == ValidationControlStore.Join.JOINED || answer == ValidationControlStore.Join.ALREADY_JOINED) {
                        joined = true;
                        try { local.joined(); } catch (TapstateException refused) { fail(refused); finish(); return settled(); }
                    }
                        else if (answer != null) { joinRejected = true; fail(invalid("run.originalJoin")); }
                    }
                }
                if (stopping.get()) finish();
                else {
                    if (!born) {
                        var answer = admissions.open(new ValidationReportAdmission.Open(original.admission(), original.snapshot()));
                        if (answer != ValidationReportAdmission.Birth.CREATED && answer != ValidationReportAdmission.Birth.DUPLICATE) {
                            fail(invalid("run.originalBirth")); finish(); return settled();
                        }
                        born = true;
                    }
                    local.requireRun();
                    if (state == ValidationRunState.STARTING) {
                        try { var ready=activation.prepare();waiting=ready!=Readiness.READY;
                            if(ready==Readiness.UNKNOWN){unknown=true;controlFailure=invalid("run.initialEvidenceUnknown");return settled();}
                        }
                        catch (TapstateException refused) { fail(refused); finish(); return settled(); }
                        if (!waiting && !stopping.get()) {
                            if (execution == null) {
                                try { execution = Objects.requireNonNull(activation.create()); activation.prepared(execution); }
                                catch (TapstateException refused) { fail(refused); finish(); return settled(); }
                                if (!execution.identity().equals(original.identity())) throw invalid("run.executionIdentity");
                            }
                            // Keep STARTING if the response is unknown. The exact original transition
                            // is reconciled before the one submission attempt; no replacement is built.
                            change(ValidationRunState.RUNNING);
                        }
                    }
                    if (state == ValidationRunState.RUNNING && !stopping.get()) {
                        if (!attempted) {
                            attempted = true;
                            try { execution.start(); }
                            catch (TapstateException failedStart) { fail(failedStart); }
                        }
                        var progress = Objects.requireNonNull(execution.progress());
                        if (progress.failure() != null) fail(progress.failure());
                        if (progress.businessComplete()) {
                            if (original.identity().mode() != ValidationMode.FULL) throw invalid("run.incrementalCompletion");
                            desiredComplete = true; stopping.set(true); local.sealPublication();
                        }
                        if (stopping.get()) finish();
                    }
                }
            }
            if (retired && !retirementNotified) { local.retired(state); retirementNotified = true; }
        } catch (TapstateException uncertain) {
            // Store transport, stale authority and unknown commits do not authorize another command,
            // report, Job, or resource release. A later advance only retries these original gates.
            unknown = true; controlFailure = uncertain;
        } catch (RuntimeException | Error defect) {
            failure.record(defect);
            try { stop(); } catch (RuntimeException | Error secondary) { failure.record(secondary); ValidationFailureLatch.addContext(failure.get(), secondary); }
            if (failure.get() instanceof Error fatal) throw fatal;
            throw defect;
        } finally {
            // Actual physical exit is useful even after authority loss prevents durable cleanup.
            // It releases only data/task ownership; unconfirmed control/quota facts stay retained.
            observeDrain();
        }
        return status();
    }

    private void finish() {
        waiting = false; local.sealPublication();
        if (admissionRejected) { state = ValidationRunState.FAILED; retired = true; return; }
        if (joinRejected) {
            if (admissions.close(original.admission()) == ValidationReportAdmission.Closure.STALE) throw invalid("run.originalClosure");
            var terminal = failure.get() == null ? ValidationRunState.STOPPED : ValidationRunState.FAILED;
            if (control.abandonClosedStart(new ValidationControlStore.Abandonment(original, terminal)) == ValidationControlStore.Mutation.STALE) throw invalid("run.originalAbandonment");
            var known = control.read(original.identity().owner(), original).orElseThrow(() -> invalid("run.originalAbandonment"));
            if (!known.retired() || !known.state().terminal()) throw invalid("run.originalAbandonment");
            state = known.state(); retired = true; return;
        }
        if (terminalChoice == null) terminalChoice = failure.get() != null ? ValidationRunState.FAILED : desiredComplete ? ValidationRunState.COMPLETED : ValidationRunState.STOPPED;
        if (!state.terminal() && terminalChoice == ValidationRunState.STOPPED && state != ValidationRunState.STOPPING) change(ValidationRunState.STOPPING);
        if (execution != null && !desiredComplete) execution.cancel();
        var closure = admissions.close(original.admission());
        if (closure == ValidationReportAdmission.Closure.STALE) throw invalid("run.originalClosure");
        for (int step = 0; !state.terminal() && step < 3; step++) change(terminalChoice);
        var terminal = terminalChoice;
        if (state != terminal) throw invalid("run.terminalChanged");
        var result = control.retire(new ValidationControlStore.Retirement(original, original.report(), terminal));
        if (result == ValidationControlStore.Mutation.STALE) throw invalid("run.originalRetirement");
        retired = true;
    }
    private Status settled() {
        if (retired && !retirementNotified) { local.retired(state); retirementNotified = true; }
        observeDrain(); return status();
    }
    private void change(ValidationRunState next) {
        for (int step = 0; step < ValidationRunState.values().length; step++) {
            pendingTransition = new ValidationControlStore.Transition(original, state, next);
            reconcileTransition();
            if (state == next || state.terminal()) return;
            if (next == ValidationRunState.COMPLETED && state == ValidationRunState.STOPPING && terminalChoice == ValidationRunState.STOPPED) return;
            if (!state.canTransitionTo(next, original.identity().mode())) throw invalid("run.originalTransition");
        }
        throw invalid("run.originalTransition");
    }
    private void reconcileTransition() {
        var pending = pendingTransition; if (pending == null) return;
        var result = control.transition(pending);
        if (result != ValidationControlStore.Mutation.STALE) { state = pending.next(); pendingTransition = null; return; }
        var known = control.read(original.identity().owner(), original).orElseThrow(() -> invalid("run.originalTransition"));
        if (known.state() == pending.expected()) throw invalid("run.originalTransition");
        state = known.state(); pendingTransition = null;
        if (state == ValidationRunState.STOPPING || state.terminal()) { stopping.set(true); local.sealPublication(); }
        if (state == ValidationRunState.STOPPING && terminalChoice == ValidationRunState.COMPLETED) { desiredComplete = false; terminalChoice = ValidationRunState.STOPPED; }
        if (state.terminal()) terminalChoice = state;
    }
    private void observeDrain() {
        if ((!retired && !stopping.get()) || drained) return;
        var progress = execution == null ? new Progress(false, true, true, null) : execution.progress();
        if (!processorReceipt && progress.processorsExited()) { local.processorsExited(); processorReceipt = true; }
        if (processorReceipt && progress.readersReleased()) { local.drained(); drained = true; }
    }
    /** Passive physical facts remain observable after failed/unknown control IO or owner loss. */
    synchronized void observePhysicalDrain(){observeDrain();}
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
}
