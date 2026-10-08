package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorEnd;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Converges a pipeline's actual state toward its desired intent. It reads the desired target, seeds
 * the actual checkpoint the first time a pipeline appears, and lands the target through the fencing
 * compare-and-swap — rebasing on the fresh epoch when a write is fenced, so a writer that lost its
 * place picks the current epoch back up before retrying. This is the desired/actual split at work:
 * the control side writes desired intent, this converge side writes actual state, and the two never
 * cross. A lost race is a fenced value the retry loop handles, not an error; the loop is bounded, so
 * a pipeline that is being written out from under it concedes the pass and lets the next one retry.
 */
public final class PipelineConverger {

    /** The retry ceiling for one pass: a fenced writer rebases up to this many times before conceding. */
    public static final int MAX_CAS_ATTEMPTS = 8;

    private final DesiredStore desired;
    private final StateStore state;
    private final LifecycleActuator actuator;
    private final Clock clock;
    private final RebuildAdmission rebuilds;

    /** A converge loop that never rebuilds a failed run, which is every run on a single node. */
    public PipelineConverger(DesiredStore desired, StateStore state, LifecycleActuator actuator, Clock clock) {
        this(desired, state, actuator, clock, RebuildAdmission.never());
    }

    public PipelineConverger(
            DesiredStore desired, StateStore state, LifecycleActuator actuator, Clock clock,
            RebuildAdmission rebuilds) {
        this.desired = Objects.requireNonNull(desired, "desired");
        this.state = Objects.requireNonNull(state, "state");
        this.actuator = Objects.requireNonNull(actuator, "actuator");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.rebuilds = Objects.requireNonNull(rebuilds, "rebuilds");
    }

    /** One factual worker-side decision, before any corresponding lifecycle action is attempted. */
    public record PendingDecision(DesiredState intent, Optional<CheckpointDoc> checkpoint, PendingAction action,
            Optional<StopReservation> boundHandoff) {
        public PendingDecision {
            Objects.requireNonNull(intent, "intent");
            Objects.requireNonNull(checkpoint, "checkpoint");
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(boundHandoff, "boundHandoff");
            if (boundHandoff.isPresent()) {
                StopReservation receipt = boundHandoff.orElseThrow();
                if (action != PendingAction.NONE || receipt.phase() != StopReservation.Phase.SUCCESSOR_BOUND
                        || !intent.equals(receipt.originalDesired())
                        || checkpoint.isEmpty() || !intent.pipelineId().equals(checkpoint.orElseThrow().pipelineId())
                        || checkpoint.orElseThrow().epoch() != receipt.reservedEpoch()) {
                    throw new IllegalArgumentException("a pending receipt must describe the exact bound handoff");
                }
            }
        }

        public PendingDecision(DesiredState intent, Optional<CheckpointDoc> checkpoint, PendingAction action) {
            this(intent, checkpoint, action, Optional.empty());
        }
    }

    public enum PendingAction { NONE, START, STOP }

    /** Drives the pipeline's actual state toward its current desired target, seeding it if new. */
    public ConvergeResult converge(String pipelineId) {
        return converge(pipelineId, ignored -> { });
    }

    /** The callback runs on this worker and cannot grant or repeat a lifecycle admission. */
    public ConvergeResult converge(String pipelineId, Consumer<PendingDecision> decisions) {
        Objects.requireNonNull(decisions, "decisions");
        Optional<DesiredState> intent = desired.read(pipelineId);
        Optional<StopReservation> stopping = state.supportsStopReservations()
                ? state.readStopReservation(pipelineId) : Optional.empty();
        if (stopping.isPresent()) {
            return intent.isEmpty() ? ConvergeResult.superseded()
                    : resumeStop(stopping.orElseThrow(), intent.orElseThrow(), false, decisions);
        }
        if (intent.isEmpty()) {
            return ConvergeResult.nothingToDo();
        }
        PipelineState target = intent.get().targetState();
        // Only what the user asked for clears anything. Every other road to a stopped job below --
        // a source that ran out, a job that died -- drives the same verb with this false, because
        // nobody asked for those and a run that ends on its own still has somewhere to carry on from.
        boolean purgeState = intent.get().purgeState();
        boolean reassemble = intent.get().reassemble();
        Optional<CheckpointDoc> actualDoc = state.read(pipelineId);
        PipelineState actual = actualDoc.map(doc -> StateJson.parse(doc.stateJson())).orElse(null);
        // The one intent that is carried out once rather than held true: a start superseding a stop
        // this side never got to read. It is owed only while the actual state still stands where it
        // stood when the intent was written -- and carrying it out is itself a fenced write, so the
        // epoch moves and the instruction is spent by having happened. Nothing has to come back and
        // take it off the intent, which is as well, because intent is the control layer's to write.
        //
        // An epoch that has already moved on is not a lost instruction: it means the stop was
        // converged after all, and then what is wanted is exactly the ordinary start below.
        Long stampedAt = intent.get().rebuiltAtStateEpoch();
        boolean rebuildOwed = reassemble && stampedAt != null
                && actualDoc.map(CheckpointDoc::epoch).orElse(-1L).equals(stampedAt);
        boolean resumeNeedsRebuild = target == PipelineState.RUNNING && actual == PipelineState.PAUSED
                && actuator.needsRebuildOnResume(pipelineId);
        boolean rebuild = resumeNeedsRebuild || (reassemble && (rebuildOwed
                || (stampedAt == null && actual == PipelineState.PAUSED)
                || (actual == PipelineState.STOPPED && actuator.isCarryingAJob(pipelineId))));

        if (target == PipelineState.RUNNING && actual == PipelineState.RUNNING) {
            // A pipeline believed running whose job has died converges to the observable FAILED state,
            // rather than reporting RUNNING over a dead job. The failure cause rides out on the result so
            // the driver can surface it. A converge-side transition, never a user verb.
            Optional<Throwable> failure = actuator.failure(pipelineId);
            if (failure.isPresent()) {
                decisions.accept(new PendingDecision(intent.orElseThrow(), actualDoc, PendingAction.STOP));
                ConvergeResult driven =
                        driveTo(pipelineId, PipelineState.FAILED, false, actualDoc.orElse(null), false);
                return driven.checkpoint()
                        .map(checkpoint -> ConvergeResult.failed(checkpoint, failure.get(),
                                driven.transitionFrom()))
                        .orElse(driven);
            }
            // Nothing failed and nothing is carrying it: this process has come up to a checkpoint an
            // earlier one wrote. The state already matches the intent, so the drive below would call
            // this converged and actuate nothing - which is how a pipeline ends up reporting RUNNING,
            // with no errors, over a data plane that does not exist. Put a job behind it instead.
            //
            // A start rather than a resume: a resume continues a job that is being held, and there is
            // no job here to continue. The fresh run re-reads its source position from the store, which
            // is where the previous process's progress was recorded, so this resumes the work without
            // resuming the job. Submitting is absent-safe, and the guard is "no job is carrying it"
            // rather than "this process did not start it", so the next tick actuates nothing.
            if (!actuator.isCarryingAJob(pipelineId) && !rebuild) {
                decisions.accept(new PendingDecision(intent.orElseThrow(), actualDoc, PendingAction.START));
                ConvergeResult.ExecutionBoundary submission = null;
                try (LifecycleActuator.PreparedStart prepared = actuator.prepareStart(pipelineId)) {
                    prepared.submit();
                    var source = prepared.submittedSource().orElse(null);
                    if (source != null && source.scope() != null && source.oldJob() != null) {
                        submission = new ConvergeResult.ExecutionBoundary(source.scope(), actualDoc.orElseThrow().epoch(),
                                PipelineState.RUNNING, clock.instant(), false);
                    }
                } catch (StartDeferred waiting) {
                    return ConvergeResult.startDeferred(actualDoc.orElseThrow(), waiting.reason());
                } catch (TapstateException refused) {
                    // Same refusal, third road. This one is the worst of the three to let escape: the
                    // checkpoint already says RUNNING, so an escaping throw leaves every read face
                    // answering healthy over a data plane that was never built, and the loop retries
                    // for the life of the process. A store that is unreachable when a process comes up
                    // is exactly the condition the coded refusal exists for.
                    return failedWith(pipelineId, refused, actualDoc.orElseThrow(), intent.orElseThrow());
                }
                ConvergeResult restored = ConvergeResult.converged(actualDoc.orElseThrow());
                return submission == null ? restored : restored.withExecutionBoundary(submission);
            }
        }

        if (target == PipelineState.PAUSED && actual == PipelineState.PAUSED) {
            Optional<Throwable> failure = actuator.lost(pipelineId);
            if (failure.isEmpty() && !actuator.isCarryingAJob(pipelineId)) {
                failure = actuator.failure(pipelineId).or(() -> Optional.of(
                        new TapstateException(LifecycleError.PAUSED_JOB_MISSING,
                                Map.of("pipeline", pipelineId), null)));
            }
            if (failure.isPresent()) {
                decisions.accept(new PendingDecision(intent.orElseThrow(), actualDoc, PendingAction.STOP));
                ConvergeResult driven = driveTo(
                        pipelineId, PipelineState.FAILED, false, actualDoc.orElse(null), false);
                Throwable cause = failure.get();
                return driven.checkpoint().map(checkpoint -> ConvergeResult.failed(checkpoint, cause,
                        driven.transitionFrom()))
                        .orElse(driven);
            }
        }

        if ((target == PipelineState.RUNNING || target == PipelineState.PAUSED)
                && actual == PipelineState.FAILED && !rebuildOwed) {
            rebuilds.recordFailure(pipelineId);
            // A run that died because the cluster changed under it is the one death this loop may answer
            // by itself, and it is asked here rather than where the death was observed so that the
            // failure is recorded and published first: whatever is decided next, nobody is left reading a
            // healthy pipeline over a dead job while it is being decided. The admission bounds itself --
            // a yes that never runs out is a restart loop wearing the word "recovery".
            if (target == PipelineState.RUNNING && rebuilds.admits(pipelineId)) {
                decisions.accept(new PendingDecision(intent.orElseThrow(), actualDoc, PendingAction.START));
                return driveTo(pipelineId, target, false, actualDoc.orElse(null), false, true, false, intent.get(), decisions)
                        .recoveringExecution();
            }
            // Otherwise a failed run stays failed: re-driving it toward RUNNING would restart the dead job
            // on every tick, and toward PAUSED would try every tick to hold a job that is gone, be refused,
            // and fail the pipeline over again. The user recovers by stopping it then starting a fresh
            // run -- which arrives as the one instruction above, and that is let through: it is somebody
            // saying so once, which is the whole difference from this loop noticing the same death every
            // second.
            // actual is FAILED only when the checkpoint was read and parsed, so
            // the doc is necessarily present; orElseThrow makes that invariant explicit and fail-loud.
            decisions.accept(new PendingDecision(intent.orElseThrow(), actualDoc, PendingAction.NONE));
            return ConvergeResult.converged(actualDoc.orElseThrow());
        }

        if (target == PipelineState.RUNNING && actual == PipelineState.COMPLETED && !rebuildOwed) {
            // A run whose bounded source ran out is over, and nobody writes an intent to say so: the
            // desired state still reads RUNNING because that is what was asked for and it was carried
            // out. Driving it would start the whole run again -- on this tick, and on every tick after
            // it, since each new run reaches the same end. Running it again is a user's stop and start,
            // which arrives as the one instruction handled above.
            decisions.accept(new PendingDecision(intent.orElseThrow(), actualDoc, PendingAction.NONE));
            return ConvergeResult.converged(actualDoc.orElseThrow());
        }

        PendingAction action = target == PipelineState.RUNNING && (actual != target || rebuild || rebuildOwed)
                ? PendingAction.START : target == PipelineState.STOPPED && actual != target
                        ? PendingAction.STOP : PendingAction.NONE;
        decisions.accept(new PendingDecision(intent.orElseThrow(), actualDoc, action));
        return driveTo(pipelineId, target, true, actualDoc.orElse(null), purgeState, rebuild, rebuildOwed,
                intent.get(), decisions);
    }

    /**
     * Marks a running pipeline terminal once its bounded source is exhausted — a converge-side
     * transition, never a user verb. The exhaustion signal that calls this comes from the execution
     * engine; a pipeline that has never run has no checkpoint and is left untouched. A pipeline marked
     * completed must then be dropped from the reconcile set (or its desired intent advanced to match),
     * or a later convergence pass would drive its actual state back toward a non-terminal desired target.
     */
    public ConvergeResult markCompleted(String pipelineId) {
        if (state.supportsStopReservations() && state.readStopReservation(pipelineId).isPresent()) {
            return ConvergeResult.superseded();
        }
        return driveTo(
                pipelineId, PipelineState.COMPLETED, false, state.read(pipelineId).orElse(null), false,
                false, false);
    }

    private ConvergeResult driveTo(
            String pipelineId, PipelineState target, boolean seedIfAbsent, CheckpointDoc current,
            boolean purgeState) {
        return driveTo(pipelineId, target, seedIfAbsent, current, purgeState, false, false);
    }

    private ConvergeResult driveTo(
            String pipelineId, PipelineState target, boolean seedIfAbsent, CheckpointDoc current,
            boolean purgeState, boolean rebuild, boolean evenIfAlreadyThere) {
        return driveTo(pipelineId, target, seedIfAbsent, current, purgeState, rebuild, evenIfAlreadyThere, null);
    }

    private ConvergeResult driveTo(
            String pipelineId, PipelineState target, boolean seedIfAbsent, CheckpointDoc current,
            boolean purgeState, boolean rebuild, boolean evenIfAlreadyThere, DesiredState stopIntent) {
        return driveTo(pipelineId, target, seedIfAbsent, current, purgeState, rebuild, evenIfAlreadyThere,
                stopIntent, ignored -> { });
    }

    private ConvergeResult driveTo(
            String pipelineId, PipelineState target, boolean seedIfAbsent, CheckpointDoc current,
            boolean purgeState, boolean rebuild, boolean evenIfAlreadyThere, DesiredState stopIntent,
            Consumer<PendingDecision> decisions) {
        String targetJson = StateJson.of(target);
        if (current == null) {
            if (!seedIfAbsent) {
                return ConvergeResult.nothingToDo();
            }
            state.create(pipelineId, StateJson.of(PipelineState.NEW), clock.instant());
            current = requireCheckpoint(pipelineId);
        }
        if (target == PipelineState.RUNNING && rebuild) {
            return rebuildToRunning(pipelineId, current, stopIntent, decisions);
        }
        // A requested rebuild was handled above; an ordinary state match needs no further actuation.
        if (current.stateJson().equals(targetJson) && !evenIfAlreadyThere) {
            return ConvergeResult.converged(current);
        }
        if (target == PipelineState.STOPPED) {
            return beginStop(pipelineId, current, false, stopIntent, decisions);
        }
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            PipelineState from = StateJson.parse(current.stateJson());
            LifecycleActuator.PreparedStart prepared = null;
            try {
                // Admission that can refuse without a job must finish before RUNNING is made durable.
                // A prepared run still submits only after the fenced checkpoint write succeeds.
                if (target == PipelineState.RUNNING && from != PipelineState.PAUSED && !rebuild) {
                    try {
                        noteAction(decisions, stopIntent, current, PendingAction.START);
                        prepared = actuator.prepareStart(pipelineId);
                    } catch (StartDeferred waiting) {
                        return ConvergeResult.startDeferred(current, waiting.reason());
                    } catch (TapstateException refused) {
                        return failedWith(pipelineId, refused, current, stopIntent);
                    }
                }
                CasOutcome outcome = state.compareAndSwap(pipelineId, current.epoch(), targetJson, clock.instant());
                if (outcome instanceof CasOutcome.Applied applied) {
                    if (target == PipelineState.FAILED) { rebuilds.recordFailure(pipelineId); }
                    // The store remains the state truth. A prepared start may reserve bounded inputs
                    // before this CAS, but submits its Jet job only after the CAS has landed.
                    try {
                        if (prepared != null) {
                            prepared.submit();
                        } else {
                            actuate(pipelineId, from, target, purgeState);
                        }
                    } catch (TapstateException refused) {
                        // A coded refusal after a transition is an observable pipeline failure. Internal
                        // admission waits are handled above, before the transition.
                        return failedWith(pipelineId, refused, applied.next(), stopIntent);
                    }
                    return ConvergeResult.converged(applied.next(), from);
                }
                // Fenced: another writer moved the epoch on. Re-read and rebase before retrying.
                if (state.supportsStopReservations() && state.readStopReservation(pipelineId).isPresent()) {
                    return ConvergeResult.superseded();
                }
                current = requireCheckpoint(pipelineId);
                if (current.stateJson().equals(targetJson)) {
                    return ConvergeResult.converged(current);
                }
                if (target == PipelineState.FAILED) {
                    // FAILED is a conclusion about a state just read, not an intent to retry over a
                    // newer checkpoint written by somebody else.
                    return ConvergeResult.superseded();
                }
            } finally {
                if (prepared != null) {
                    prepared.close();
                }
            }
        }
        return ConvergeResult.superseded();
    }

    /** Completes the reserved stop before one replacement submission can be admitted. */
    private ConvergeResult rebuildToRunning(String pipelineId, CheckpointDoc current, DesiredState stopIntent,
            Consumer<PendingDecision> decisions) {
        if (StateJson.parse(current.stateJson()) == PipelineState.STOPPED && !actuator.isCarryingAJob(pipelineId)) {
            return driveTo(pipelineId, PipelineState.RUNNING, false, current, false, false, false, stopIntent, decisions);
        }
        return beginStop(pipelineId, current, true, stopIntent, decisions);
    }

    private ConvergeResult beginStop(String pipelineId, CheckpointDoc current, boolean replacement, DesiredState intent,
            Consumer<PendingDecision> decisions) {
        if (intent == null || desired.read(pipelineId).filter(intent::equals).isEmpty()
                || Thread.currentThread().isInterrupted()) {
            return ConvergeResult.superseded();
        }
        if (!state.supportsStopReservations()) {
            return legacyStop(current, intent, replacement, decisions);
        }
        StopAuthority authority = actuator.stopAuthority(pipelineId).orElse(null);
        StopReservation.Source source = actuator.stopSource(pipelineId)
                .orElseThrow(() -> new IllegalStateException("durable stop binding supplied no factual source"));
        if (!Objects.equals(authority, actuator.stopAuthority(pipelineId).orElse(null))
                || source.scope() != null && (authority == null
                        || source.scope().executionGeneration() != authority.executionGeneration())) {
            return ConvergeResult.superseded();
        }
        StopReservation proposal = StopReservation.stopping(pipelineId, UUID.randomUUID().toString(), current.epoch(),
                intent, source, StopReservation.CounterPolicy.freeze(current, intent), authority);
        return state.reserveStop(current, proposal, clock.instant())
                .map(accepted -> resumeStop(accepted, intent, true, decisions)).orElseGet(ConvergeResult::superseded);
    }

    private ConvergeResult resumeStop(StopReservation marker, DesiredState intent, boolean firstAttempt,
            Consumer<PendingDecision> decisions) {
        String id = marker.pipelineId();
        StopAuthority authority = actuator.stopAuthority(id).orElse(null);
        if (marker.writerAuthority() != null && authority == null) { return ConvergeResult.superseded(); }
        boolean retiring = !marker.originalDesired().equals(intent);
        if (marker.legacy() && !retiring) {
            marker = state.promoteStopReservation(marker, intent, authority, clock.instant()).orElse(null);
            if (marker == null) { return ConvergeResult.superseded(); }
        }
        if (!retiring && !Objects.equals(marker.writerAuthority(), authority)) {
            marker = authority == null ? null : state.rebindStop(marker, authority, clock.instant()).orElse(null);
            if (marker == null) { return ConvergeResult.superseded(); }
        }
        StopReservation expected = marker;
        java.util.function.BooleanSupplier current = () -> currentMarker(expected, intent, authority);
        if (!current.getAsBoolean()) { return ConvergeResult.superseded(); }
        CheckpointDoc before = requireCheckpoint(id);
        if (StateJson.parse(before.stateJson()) == PipelineState.FAILED) { rebuilds.recordFailure(id); }
        if (retiring) {
            noteAction(decisions, intent, before, PendingAction.STOP);
            boolean continuing = continuesCounters(before, intent);
            if (!actuator.finishStop(expected, continuing, firstAttempt, true, current)) {
                return pendingOrSuperseded(id, current);
            }
            if (!current.getAsBoolean()) { return ConvergeResult.superseded(); }
            // A stamped START must survive the epoch reserved by the old unfinished operation.
            if (intent.targetState() == PipelineState.RUNNING && intent.reassemble()
                    && Objects.equals(intent.rebuiltAtStateEpoch(), marker.reservedEpoch())) {
                StopReservation.Source source = actuator.stopSource(id).orElseThrow(
                        () -> new IllegalStateException("durable stop binding supplied no factual source"));
                if (!current.getAsBoolean()) { return ConvergeResult.superseded(); }
                StopReservation replacement = StopReservation.stopping(id, UUID.randomUUID().toString(),
                        marker.reservedEpoch(), intent, source, StopReservation.CounterPolicy.freeze(before, intent), authority);
                return state.replaceStop(marker, replacement, clock.instant())
                        .map(accepted -> resumeStop(accepted, intent, true, decisions)).orElseGet(ConvergeResult::superseded);
            }
            state.retireStop(expected, intent, authority, clock.instant());
            return ConvergeResult.superseded();
        }
        return switch (expected.phase()) {
            case STOPPING -> {
                noteAction(decisions, intent, before, PendingAction.STOP);
                boolean continuing = expected.counterPolicy() == StopReservation.CounterPolicy.CONTINUE;
                if (!actuator.finishStop(expected, continuing, firstAttempt, false, current)) {
                    yield pendingOrSuperseded(id, current);
                }
                if (!current.getAsBoolean()) { yield ConvergeResult.superseded(); }
                if (intent.targetState() == PipelineState.RUNNING) {
                    yield state.markReplacementPending(expected, clock.instant())
                            .map(accepted -> resumeStop(accepted, intent, false, decisions))
                            .orElseGet(ConvergeResult::superseded);
                }
                yield state.completeStop(expected, clock.instant())
                        .map(done -> ConvergeResult.converged(done, StateJson.parse(before.stateJson())))
                        .orElseGet(ConvergeResult::superseded);
            }
            case REPLACEMENT_PENDING -> {
                noteAction(decisions, intent, before, PendingAction.START);
                yield admitReplacement(expected, intent, current, decisions);
            }
            case SUCCESSOR_ADMITTED, SUCCESSOR_BOUND -> recoverSuccessor(expected, intent, current, decisions);
        };
    }

    private static void noteAction(Consumer<PendingDecision> decisions, DesiredState intent,
            CheckpointDoc checkpoint, PendingAction action) {
        if (intent != null) { decisions.accept(new PendingDecision(intent, Optional.ofNullable(checkpoint), action)); }
    }

    /** Rechecks a local projection receipt without granting or repeating any lifecycle action. */
    public boolean currentHandoffDecision(PendingDecision decision) {
        StopReservation receipt = decision.boundHandoff().orElse(null);
        return receipt != null && !Thread.currentThread().isInterrupted()
                && desired.read(receipt.pipelineId()).filter(decision.intent()::equals).isPresent()
                && state.readStopReservation(receipt.pipelineId()).filter(receipt::equals).isPresent()
                && state.read(receipt.pipelineId()).equals(decision.checkpoint());
    }

    private static void noteBoundHandoff(Consumer<PendingDecision> decisions, DesiredState intent,
            CheckpointDoc checkpoint, StopReservation receipt) {
        decisions.accept(new PendingDecision(intent, Optional.of(checkpoint), PendingAction.NONE, Optional.of(receipt)));
    }

    private boolean currentMarker(StopReservation marker, DesiredState intent, StopAuthority authority) {
        return !Thread.currentThread().isInterrupted()
                && desired.read(marker.pipelineId()).filter(intent::equals).isPresent()
                && state.readStopReservation(marker.pipelineId()).filter(marker::equals).isPresent()
                && Objects.equals(authority, actuator.stopAuthority(marker.pipelineId()).orElse(null));
    }

    private ConvergeResult pendingOrSuperseded(String id, java.util.function.BooleanSupplier current) {
        return current.getAsBoolean() ? ConvergeResult.stopPending(requireCheckpoint(id))
                : ConvergeResult.superseded();
    }

    private ConvergeResult admitReplacement(StopReservation marker, DesiredState intent,
            java.util.function.BooleanSupplier current, Consumer<PendingDecision> decisions) {
        String id = marker.pipelineId();
        var admittedAttempt = new java.util.concurrent.atomic.AtomicReference<StopReservation>();
        try (LifecycleActuator.PreparedReplacement prepared = actuator.prepareReplacement(marker,
                (writer, incarnation, boot, executionMembers) -> {
                    if (!current.getAsBoolean() || !Objects.equals(marker.writerAuthority(), writer)) { return Optional.empty(); }
                    var accepted = state.admitSuccessor(marker, incarnation, boot, executionMembers, clock.instant());
                    accepted.ifPresent(value -> admittedAttempt.set(value.reservation()));
                    return accepted;
                },
                admitted -> currentMarker(admitted, intent, admitted.writerAuthority()))) {
            StopReservation admitted = prepared.admitted();
            if (!currentMarker(admitted, intent, admitted.writerAuthority())) { return ConvergeResult.superseded(); }
            prepared.submit();
            return prepared.submittedJob()
                    .flatMap(job -> bindSuccessorWithBoundary(admitted, job, intent, decisions))
                    .orElseGet(() -> ConvergeResult.stopPending(requireCheckpoint(id)));
        } catch (StartDeferred waiting) {
            return ConvergeResult.startDeferred(requireCheckpoint(id), waiting.reason());
        } catch (TapstateException refused) {
            StopReservation expected = Optional.ofNullable(admittedAttempt.get()).orElse(marker);
            java.util.function.BooleanSupplier stillCurrent = () -> currentMarker(expected, intent, expected.writerAuthority());
            if (!stillCurrent.getAsBoolean()) { return ConvergeResult.superseded(); }
            Optional<StopReservation.JobIdentity> job = Optional.empty();
            if (expected.successor() != null) {
                var actual = actuator.inspectSuccessor(expected, stillCurrent).orElse(null);
                if (actual == null || !stillCurrent.getAsBoolean()) { return ConvergeResult.superseded(); }
                job = actual.job();
            }
            var failed = state.failReplacement(expected, job, clock.instant()).orElse(null);
            if (failed == null) { return ConvergeResult.superseded(); }
            java.util.function.BooleanSupplier failureCurrent = () -> !Thread.currentThread().isInterrupted()
                    && desired.read(id).filter(intent::equals).isPresent()
                    && state.read(id).filter(failed::equals).isPresent()
                    && Objects.equals(expected.writerAuthority(), actuator.stopAuthority(id).orElse(null));
            actuator.observeReplacementFailure(expected, failureCurrent);
            return ConvergeResult.failed(failed, refused, Optional.of(PipelineState.STOPPED));
        }
    }

    private ConvergeResult recoverSuccessor(StopReservation marker, DesiredState intent,
            java.util.function.BooleanSupplier current, Consumer<PendingDecision> decisions) {
        String id = marker.pipelineId();
        var actual = actuator.inspectSuccessor(marker, current).orElse(null);
        if (actual == null || !current.getAsBoolean()) { return ConvergeResult.superseded(); }
        if (actual.job().isEmpty()) {
            CheckpointDoc concluded = requireCheckpoint(id);
            PipelineState concludedState = StateJson.parse(concluded.stateJson());
            if (marker.phase() == StopReservation.Phase.SUCCESSOR_BOUND
                    && (concludedState == PipelineState.FAILED || concludedState == PipelineState.COMPLETED)) {
                noteAction(decisions, intent, concluded, PendingAction.NONE);
                var ready = marker.counterPolicy() == StopReservation.CounterPolicy.RESET
                        ? Optional.of(marker.handoffIdentity()) : actuator.continuationReady(marker, current);
                return ready.isPresent() ? state.completeHandoff(marker, ready.orElseThrow(), clock.instant())
                        .map(ConvergeResult::converged).orElseGet(ConvergeResult::superseded)
                        : ConvergeResult.converged(concluded);
            }
            SuccessorEnd end;
            if (marker.phase() == StopReservation.Phase.SUCCESSOR_ADMITTED) {
                end = new SuccessorEnd.Absent(marker.successor().scope(), marker.successor().submissionBootId());
            } else {
                // Preserve the last bound target's floor before retiring its now absent physical job.
                noteAction(decisions, intent, concluded, PendingAction.STOP);
                if (!actuator.finishStop(marker, marker.counterPolicy() == StopReservation.CounterPolicy.CONTINUE,
                        false, false, current)) { return pendingOrSuperseded(id, current); }
                end = new SuccessorEnd.Terminal(marker.successor().scope(), marker.successor().job());
            }
            return state.retireSuccessor(marker, end, clock.instant())
                    .map(pending -> ConvergeResult.stopPending(requireCheckpoint(id)))
                    .orElseGet(ConvergeResult::superseded);
        }
        if (marker.phase() == StopReservation.Phase.SUCCESSOR_ADMITTED) {
            noteAction(decisions, intent, requireCheckpoint(id), PendingAction.START);
            return bindSuccessorWithBoundary(marker, actual.job().orElseThrow(), intent, decisions)
                    .orElseGet(ConvergeResult::superseded);
        }
        CheckpointDoc before = requireCheckpoint(id);
        if (actual.terminalState().isPresent()
                && StateJson.parse(before.stateJson()) != actual.terminalState().orElseThrow()) {
            PipelineState terminal = actual.terminalState().orElseThrow();
            var ended = state.recordSuccessorTerminal(marker,
                    new SuccessorEnd.Terminal(marker.successor().scope(), actual.job().orElseThrow()),
                    terminal, clock.instant()).orElse(null);
            if (ended == null) { return ConvergeResult.superseded(); }
            ConvergeResult driven = resumeStop(ended, intent, false, decisions);
            if (driven.checkpoint().isPresent()) {
                Optional<Throwable> cause = terminal == PipelineState.FAILED ? actuator.failure(id) : Optional.empty();
                if (cause.isPresent()) {
                    return ConvergeResult.failed(driven.checkpoint().orElseThrow(), cause.orElseThrow(),
                            Optional.of(StateJson.parse(before.stateJson())));
                }
                return ConvergeResult.converged(driven.checkpoint().orElseThrow(), StateJson.parse(before.stateJson()));
            }
            return driven;
        }
        noteBoundHandoff(decisions, intent, before, marker);
        if (!actuator.adoptSuccessor(marker, current) || !current.getAsBoolean()) {
            return ConvergeResult.superseded();
        }
        noteBoundHandoff(decisions, intent, before, marker);
        var ready = marker.counterPolicy() == StopReservation.CounterPolicy.RESET
                ? Optional.of(marker.handoffIdentity()) : actuator.continuationReady(marker, current);
        if (ready.isPresent()) {
            return state.completeHandoff(marker, ready.orElseThrow(), clock.instant())
                    .map(ConvergeResult::converged).orElseGet(ConvergeResult::superseded);
        }
        // The actual job is running. Telemetry completion stays independently pending in the marker.
        return ConvergeResult.converged(requireCheckpoint(id));
    }

    private Optional<ConvergeResult> bindSuccessorWithBoundary(StopReservation admitted, StopReservation.JobIdentity job,
            DesiredState intent, Consumer<PendingDecision> decisions) {
        String id = admitted.pipelineId();
        CheckpointDoc before = requireCheckpoint(id);
        if (before.epoch() != admitted.reservedEpoch() || StateJson.parse(before.stateJson()) != PipelineState.STOPPED) {
            return Optional.empty();
        }
        Instant boundAt = clock.instant();
        var bound = state.bindSuccessor(admitted, admitted.successor().scope(), job, boundAt);
        if (bound.isEmpty()) { return Optional.empty(); }
        StopReservation receipt = bound.orElseThrow();
        ConvergeResult continued = resumeStop(receipt, intent, false, decisions);
        if (continued.status() == ConvergeStatus.SUPERSEDED) { return Optional.of(continued); }
        return Optional.of(continued.withExecutionBoundary(new ConvergeResult.ExecutionBoundary(receipt.successor().scope(),
                receipt.reservedEpoch(), StateJson.parse(before.stateJson()), boundAt, false)));
    }

    /** A genuine resume carries known totals; a stamped restart or purge starts fresh counters. */
    private static boolean continuesCounters(CheckpointDoc before, DesiredState intent) {
        return StateJson.parse(before.stateJson()) == PipelineState.PAUSED
                && intent.targetState() == PipelineState.RUNNING
                && intent.rebuiltAtStateEpoch() == null && !intent.purgeState();
    }

    /** Compatibility for a binding that explicitly has no durable reservation capability. */
    private ConvergeResult legacyStop(CheckpointDoc before, DesiredState intent, boolean replacement,
            Consumer<PendingDecision> decisions) {
        String id = before.pipelineId();
        CasOutcome reserved = state.compareAndSwap(id, before.epoch(), before.stateJson(), clock.instant());
        if (!(reserved instanceof CasOutcome.Applied admitted)) { return ConvergeResult.superseded(); }
        noteAction(decisions, intent, admitted.next(), PendingAction.STOP);
        boolean continuing = replacement && continuesCounters(before, intent);
        if (continuing) {
            actuator.stopForRebuildingResume(id, intent.purgeState());
        } else {
            actuator.stop(id, intent.purgeState());
        }
        if (Thread.currentThread().isInterrupted() || desired.read(id).filter(intent::equals).isEmpty()) {
            return ConvergeResult.superseded();
        }
        CasOutcome finished = state.compareAndSwap(id, admitted.next().epoch(), StateJson.of(PipelineState.STOPPED),
                clock.instant());
        if (!(finished instanceof CasOutcome.Applied applied)) { return ConvergeResult.superseded(); }
        if (replacement) {
            return driveTo(id, PipelineState.RUNNING, false, applied.next(), false, false, false, intent, decisions);
        }
        return ConvergeResult.converged(applied.next(), StateJson.parse(before.stateJson()));
    }

    /**
     * Drives the job side to match a state transition the store just recorded. Start and resume both
     * land in RUNNING, so the origin state decides between them: RUNNING reached from PAUSED continues
     * the held job (resume), reached from anywhere else begins a fresh run (start). A pipeline seeded at
     * NEW is never a transition target here, so it drives nothing.
     *
     * <p>Rebuilding a job follows the separate STOPPED-to-RUNNING path above, preserving the same intent
     * while a replacement waits for admission.
     *
     * <p>{@code purgeState} reaches only the stop, and only ever carries what a user's own stop asked
     * for. A pipeline that completed or failed arrives at the same verb, and arrives with it false: an
     * ending nobody asked for is not permission to throw away where it had got to.
     */
    private void actuate(String pipelineId, PipelineState from, PipelineState target, boolean purgeState) {
        switch (target) {
            case RUNNING -> {
                if (from == PipelineState.PAUSED) {
                    actuator.resume(pipelineId);
                } else {
                    actuator.start(pipelineId);
                }
            }
            case PAUSED -> actuator.pause(pipelineId);
            case STOPPED, COMPLETED, FAILED -> actuator.stop(pipelineId, purgeState);
            case NEW -> {
                // The seed state is written through create(), never a compare-and-swap target, so it
                // never reaches this actuation path.
            }
        }
    }

    /**
     * Drives the pipeline to FAILED and carries {@code cause} out on the result, so the publisher renders
     * it as the observation's coded failure. Shared with the dead-job path, which reaches the same state
     * by a different road.
     */
    private ConvergeResult failedWith(String pipelineId, Throwable cause, CheckpointDoc expected,
            DesiredState originalIntent) {
        if (Thread.currentThread().isInterrupted() || originalIntent == null
                || desired.read(pipelineId).filter(originalIntent::equals).isEmpty()) {
            return ConvergeResult.superseded();
        }
        ConvergeResult driven =
                driveTo(pipelineId, PipelineState.FAILED, false, expected, false);
        return driven.checkpoint()
                .map(checkpoint -> ConvergeResult.failed(checkpoint, cause,
                        driven.transitionFrom()))
                .orElse(driven);
    }

    private CheckpointDoc requireCheckpoint(String pipelineId) {
        return state.read(pipelineId)
                .orElseThrow(() -> new IllegalStateException("checkpoint vanished for pipeline " + pipelineId));
    }
}
