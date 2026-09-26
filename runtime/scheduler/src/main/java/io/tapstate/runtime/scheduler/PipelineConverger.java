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

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;

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

    /** Drives the pipeline's actual state toward its current desired target, seeding it if new. */
    public ConvergeResult converge(String pipelineId) {
        Optional<DesiredState> intent = desired.read(pipelineId);
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
                ConvergeResult driven =
                        driveTo(pipelineId, PipelineState.FAILED, false, actualDoc.orElse(null), false);
                return driven.checkpoint()
                        .map(checkpoint -> ConvergeResult.failed(checkpoint, failure.get()))
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
                try (LifecycleActuator.PreparedStart prepared = actuator.prepareStart(pipelineId)) {
                    prepared.submit();
                } catch (StartDeferred waiting) {
                    return ConvergeResult.startDeferred(actualDoc.orElseThrow(), waiting.reason());
                } catch (TapstateException refused) {
                    // Same refusal, third road. This one is the worst of the three to let escape: the
                    // checkpoint already says RUNNING, so an escaping throw leaves every read face
                    // answering healthy over a data plane that was never built, and the loop retries
                    // for the life of the process. A store that is unreachable when a process comes up
                    // is exactly the condition the coded refusal exists for.
                    return failedWith(pipelineId, refused);
                }
                return ConvergeResult.converged(actualDoc.orElseThrow());
            }
        }

        if (target == PipelineState.PAUSED && actual == PipelineState.PAUSED) {
            Optional<Throwable> failure = actuator.failure(pipelineId);
            if (failure.isEmpty() && !actuator.isCarryingAJob(pipelineId)) {
                failure = Optional.of(new TapstateException(LifecycleError.PAUSED_JOB_MISSING,
                        Map.of("pipeline", pipelineId), null));
            }
            if (failure.isPresent()) {
                ConvergeResult driven = driveTo(
                        pipelineId, PipelineState.FAILED, false, actualDoc.orElse(null), false);
                Throwable cause = failure.get();
                return driven.checkpoint().map(checkpoint -> ConvergeResult.failed(checkpoint, cause))
                        .orElse(driven);
            }
        }

        if (target == PipelineState.PAUSED && actual == PipelineState.FAILED) {
            // The intent stays paused; the failed execution is not a job that can be paused again.
            return ConvergeResult.converged(actualDoc.orElseThrow());
        }

        if (target == PipelineState.RUNNING && actual == PipelineState.FAILED && !rebuildOwed) {
            // A run that died because the cluster changed under it is the one death this loop may answer
            // by itself, and it is asked here rather than where the death was observed so that the
            // failure is recorded and published first: whatever is decided next, nobody is left reading a
            // healthy pipeline over a dead job while it is being decided. The admission bounds itself --
            // a yes that never runs out is a restart loop wearing the word "recovery".
            if (rebuilds.admits(pipelineId)) {
                return driveTo(pipelineId, target, false, actualDoc.orElse(null), false, true, false);
            }
            // Otherwise a failed run stays failed: re-driving it toward RUNNING would restart the dead job
            // on every tick. The user recovers by stopping it then starting a fresh run -- which arrives
            // as the one instruction above, and that is let through: it is somebody saying so once,
            // which is the whole difference from this loop noticing the same death every second.
            // actual is FAILED only when the checkpoint was read and parsed, so
            // the doc is necessarily present; orElseThrow makes that invariant explicit and fail-loud.
            return ConvergeResult.converged(actualDoc.orElseThrow());
        }

        if (target == PipelineState.RUNNING && actual == PipelineState.COMPLETED && !rebuildOwed) {
            // A run whose bounded source ran out is over, and nobody writes an intent to say so: the
            // desired state still reads RUNNING because that is what was asked for and it was carried
            // out. Driving it would start the whole run again -- on this tick, and on every tick after
            // it, since each new run reaches the same end. Running it again is a user's stop and start,
            // which arrives as the one instruction handled above.
            return ConvergeResult.converged(actualDoc.orElseThrow());
        }

        return driveTo(pipelineId, target, true, actualDoc.orElse(null), purgeState, rebuild, rebuildOwed);
    }

    /**
     * Marks a running pipeline terminal once its bounded source is exhausted — a converge-side
     * transition, never a user verb. The exhaustion signal that calls this comes from the execution
     * engine; a pipeline that has never run has no checkpoint and is left untouched. A pipeline marked
     * completed must then be dropped from the reconcile set (or its desired intent advanced to match),
     * or a later convergence pass would drive its actual state back toward a non-terminal desired target.
     */
    public ConvergeResult markCompleted(String pipelineId) {
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
        String targetJson = StateJson.of(target);
        if (current == null) {
            if (!seedIfAbsent) {
                return ConvergeResult.nothingToDo();
            }
            state.create(pipelineId, StateJson.of(PipelineState.NEW), clock.instant());
            current = requireCheckpoint(pipelineId);
        }
        if (target == PipelineState.RUNNING && rebuild) {
            return rebuildToRunning(pipelineId, current, purgeState);
        }
        // A requested rebuild was handled above; an ordinary state match needs no further actuation.
        if (current.stateJson().equals(targetJson) && !evenIfAlreadyThere) {
            return ConvergeResult.converged(current);
        }
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            PipelineState from = StateJson.parse(current.stateJson());
            LifecycleActuator.PreparedStart prepared = null;
            try {
                // Admission that can refuse without a job must finish before RUNNING is made durable.
                // A prepared run still submits only after the fenced checkpoint write succeeds.
                if (target == PipelineState.RUNNING && from != PipelineState.PAUSED && !rebuild) {
                    try {
                        prepared = actuator.prepareStart(pipelineId);
                    } catch (StartDeferred waiting) {
                        return ConvergeResult.startDeferred(current, waiting.reason());
                    } catch (TapstateException refused) {
                        return failedWith(pipelineId, refused);
                    }
                }
                CasOutcome outcome = state.compareAndSwap(pipelineId, current.epoch(), targetJson, clock.instant());
                if (outcome instanceof CasOutcome.Applied applied) {
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
                        return failedWith(pipelineId, refused);
                    }
                    return ConvergeResult.converged(applied.next());
                }
                // Fenced: another writer moved the epoch on. Re-read and rebase before retrying.
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

    /**
     * Records the old job as stopped before replacing it. A new start that waits for capture capacity
     * leaves STOPPED in the checkpoint, so neither a reader nor a later pass mistakes it for RUNNING.
     * Both halves still consume the same desired instruction in one pass when admission succeeds.
     */
    private ConvergeResult rebuildToRunning(String pipelineId, CheckpointDoc current, boolean purgeState) {
        boolean stoppedHere = false;
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS
                && StateJson.parse(current.stateJson()) != PipelineState.STOPPED; attempt++) {
            boolean continuingPausedRun = StateJson.parse(current.stateJson()) == PipelineState.PAUSED;
            CasOutcome outcome = state.compareAndSwap(
                    pipelineId, current.epoch(), StateJson.of(PipelineState.STOPPED), clock.instant());
            if (outcome instanceof CasOutcome.Applied applied) {
                current = applied.next();
                try {
                    if (continuingPausedRun) {
                        actuator.stopForRebuildingResume(pipelineId, purgeState);
                    } else {
                        actuator.stop(pipelineId, purgeState);
                    }
                } catch (TapstateException refused) {
                    return failedWith(pipelineId, refused);
                }
                stoppedHere = true;
                break;
            }
            current = requireCheckpoint(pipelineId);
        }
        if (StateJson.parse(current.stateJson()) != PipelineState.STOPPED) {
            return ConvergeResult.superseded();
        }
        if (!stoppedHere && actuator.isCarryingAJob(pipelineId)) {
            try {
                actuator.stop(pipelineId, purgeState);
            } catch (TapstateException refused) {
                return failedWith(pipelineId, refused);
            }
        }
        return driveTo(pipelineId, PipelineState.RUNNING, false, current, false, false, false);
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
    private ConvergeResult failedWith(String pipelineId, TapstateException cause) {
        ConvergeResult driven =
                driveTo(pipelineId, PipelineState.FAILED, false, requireCheckpoint(pipelineId), false);
        return driven.checkpoint()
                .map(checkpoint -> ConvergeResult.failed(checkpoint, cause))
                .orElse(driven);
    }

    private CheckpointDoc requireCheckpoint(String pipelineId) {
        return state.read(pipelineId)
                .orElseThrow(() -> new IllegalStateException("checkpoint vanished for pipeline " + pipelineId));
    }
}
