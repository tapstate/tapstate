package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import io.tapstate.spi.store.StateStore;

import java.time.Clock;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;

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
    private final boolean requirePreparedState;
    private final Consumer<String> beforeCompletion;
    private final BooleanSupplier stillOriginalIntent;

    /** A converge loop that never rebuilds a failed run, which is every run on a single node. */
    public PipelineConverger(DesiredStore desired, StateStore state, LifecycleActuator actuator, Clock clock) {
        this(desired, state, actuator, clock, RebuildAdmission.never());
    }

    public PipelineConverger(
            DesiredStore desired, StateStore state, LifecycleActuator actuator, Clock clock,
            RebuildAdmission rebuilds) {
        this(desired, state, actuator, clock, rebuilds, pipeline -> { });
    }

    /** Records the final confirmed observation before completion releases the run's live readings. */
    public PipelineConverger(
            DesiredStore desired, StateStore state, LifecycleActuator actuator, Clock clock,
            RebuildAdmission rebuilds, Consumer<String> beforeCompletion) {
        this(desired, state, actuator, clock, rebuilds, false, beforeCompletion, () -> false);
    }

    private PipelineConverger(DesiredStore desired, StateStore state, LifecycleActuator actuator, Clock clock,
            RebuildAdmission rebuilds, boolean requirePreparedState, Consumer<String> beforeCompletion,
            BooleanSupplier stillOriginalIntent) {
        this.requirePreparedState = requirePreparedState;
        this.desired = Objects.requireNonNull(desired, "desired");
        this.state = Objects.requireNonNull(state, "state");
        this.actuator = Objects.requireNonNull(actuator, "actuator");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.rebuilds = Objects.requireNonNull(rebuilds, "rebuilds");
        this.beforeCompletion = Objects.requireNonNull(beforeCompletion, "beforeCompletion");
        this.stillOriginalIntent = Objects.requireNonNull(stillOriginalIntent, "stillOriginalIntent");
    }

    /** Tells the rebuild admission which pipelines are still desired, so it forgets the rest. */
    public void retain(Collection<String> pipelineIds) {
        rebuilds.retain(pipelineIds);
    }

    /** Drives the pipeline's actual state toward its current desired target, seeding it if new. */
    public ConvergeResult converge(String pipelineId) {
        return converge(pipelineId, desired.read(pipelineId));
    }

    /**
     * Consumes one captured CONFIG intent and this pass's bound ports. Retries read only those ports;
     * neither a newer intent nor another physical state can be adopted in the middle of this pass.
     * Initial checkpoints belong to controlled preparation, never ordinary scoped convergence.
     */
    public ConvergeResult convergeScoped(PipelineIntentSnapshot intent, StateStore scopedState,
            LifecycleActuator scopedActuator, RebuildAdmission scopedRebuilds) {
        // Without a current CONFIG authority, this entry cannot publish a terminal completion.
        return convergeScoped(intent, scopedState, scopedActuator, scopedRebuilds, ignored -> {}, () -> false);
    }

    /** Completion retains the final scoped reading and rejects a changed original CONFIG intent. */
    public ConvergeResult convergeScoped(PipelineIntentSnapshot intent, StateStore scopedState,
            LifecycleActuator scopedActuator, RebuildAdmission scopedRebuilds, Consumer<String> finalObservation,
            BooleanSupplier currentIntent) {
        Objects.requireNonNull(intent, "intent");
        return new PipelineConverger(desired, scopedState, scopedActuator, clock, scopedRebuilds, true, finalObservation, currentIntent)
                .converge(intent.scope().pipelineId(), Optional.ofNullable(intent.desired()));
    }

    private ConvergeResult converge(String pipelineId, Optional<DesiredState> intent) {
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
        boolean rebuildOwed = intent.get().rebuildOwedAt(actualDoc.orElse(null));
        boolean rebuild = reassemble && (stampedAt == null || rebuildOwed);

        // An owed restart must reach the fenced stop/start below before checking the old run.
        // Recording that run's failure would advance the epoch without carrying out the restart,
        // consuming its accepted start and any clearing the user requested.
        if (target == PipelineState.RUNNING && actual == PipelineState.RUNNING && !rebuildOwed) {
            return convergeRunning(pipelineId, actualDoc.orElseThrow(), purgeState, rebuild, intent.get());
        }

        if (target == PipelineState.PAUSED && actual == PipelineState.PAUSED) {
            // A paused pipeline's job is held, not run, so it is not asked for a failure the way a running
            // one is. It can still be lost: the data plane holding the job goes, and the job with it. The
            // pipeline would then go on reading PAUSED over a job that no longer exists, and turn FAILED
            // only once somebody resumed it. It fails now, with the cause, the way a dead running job does.
            Optional<Throwable> lost = actuator.lost(pipelineId);
            if (lost.isPresent()) {
                return failedWith(pipelineId, lost.get());
            }
        }

        if ((target == PipelineState.RUNNING || target == PipelineState.PAUSED)
                && actual == PipelineState.FAILED && !rebuildOwed) {
            return convergeFailed(pipelineId, target, actualDoc.orElseThrow());
        }

        if (target == PipelineState.RUNNING && actual == PipelineState.COMPLETED && !rebuildOwed) {
            // A run whose bounded source ran out is over, and nobody writes an intent to say so: the
            // desired state still reads RUNNING because that is what was asked for and it was carried
            // out. Driving it would start the whole run again -- on this tick, and on every tick after
            // it, since each new run reaches the same end. Running it again is a user's stop and start,
            // which arrives as the one instruction handled above.
            actuator.retryCompletion(pipelineId);
            return ConvergeResult.converged(actualDoc.orElseThrow());
        }

        return driveTo(pipelineId, target, true, actualDoc.orElse(null), purgeState, rebuild, rebuildOwed);
    }

    private ConvergeResult convergeRunning(
            String pipelineId, CheckpointDoc current, boolean purgeState, boolean rebuild, DesiredState intent) {
        // A pipeline believed running whose job has died converges to the observable FAILED state,
        // rather than reporting RUNNING over a dead job. The failure cause rides out on the result so
        // the driver can surface it. A converge-side transition, never a user verb.
        Optional<Throwable> failure = actuator.failure(pipelineId);
        if (failure.isPresent()) {
            ConvergeResult driven = driveTo(pipelineId, PipelineState.FAILED, false, current, false);
            return driven.checkpoint()
                    .map(checkpoint -> ConvergeResult.failed(checkpoint, failure.get()))
                    .orElse(driven);
        }
        Optional<LifecycleActuator.Completion> completion = actuator.completion(pipelineId);
        if (completion.isPresent()) {
            // Success belongs to this execution, not to an intent that can be retried on a new epoch.
            // Save the final reading while capture is still present, then prove the run and intent again.
            beforeCompletion.accept(pipelineId);
            if (!actuator.completion(pipelineId).equals(completion)
                    || !(requirePreparedState ? stillOriginalIntent.getAsBoolean()
                            : desired.read(pipelineId).filter(intent::equals).isPresent())) {
                return ConvergeResult.superseded();
            }
            return completeRun(pipelineId, current, completion.orElseThrow());
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
        if (!actuator.isCarryingAJob(pipelineId)) {
            try {
                actuator.start(pipelineId);
            } catch (TapstateException refused) {
                // Same refusal, third road. This one is the worst of the three to let escape: the
                // checkpoint already says RUNNING, so an escaping throw leaves every read face
                // answering healthy over a data plane that was never built, and the loop retries
                // for the life of the process. A store that is unreachable when a process comes up
                // is exactly the condition the coded refusal exists for.
                return failedWith(pipelineId, refused);
            }
            return ConvergeResult.converged(current);
        }
        return driveTo(pipelineId, PipelineState.RUNNING, true, current, purgeState, rebuild, false);
    }

    private ConvergeResult completeRun(
            String pipelineId, CheckpointDoc current, LifecycleActuator.Completion completion) {
        CasOutcome outcome = completion.fence() == null
                ? state.compareAndSwap(pipelineId, current.epoch(), StateJson.of(PipelineState.COMPLETED), clock.instant())
                : state.compareAndSwap(pipelineId, current.epoch(), StateJson.of(PipelineState.COMPLETED),
                        clock.instant(), completion.fence());
        if (!(outcome instanceof CasOutcome.Applied applied)) {
            return ConvergeResult.superseded();
        }
        // Cleanup failures are reconcile failures, not a new verdict about the successful data run.
        // A later completed pass retries cleanup without consuming another lifecycle epoch.
        actuator.complete(pipelineId, completion);
        return ConvergeResult.converged(applied.next());
    }

    private ConvergeResult convergeFailed(String pipelineId, PipelineState target, CheckpointDoc current) {
        // A run that died because the cluster changed under it is the one death this loop may answer
        // by itself, and it is asked here rather than where the death was observed so that the
        // failure is recorded and published first: whatever is decided next, nobody is left reading a
        // healthy pipeline over a dead job while it is being decided. The admission bounds itself --
        // a yes that never runs out is a restart loop wearing the word "recovery". It is asked only of
        // a pipeline meant to run: a rebuild starts a run, which is not what a paused intent asks for.
        if (target == PipelineState.RUNNING && rebuilds.admits(pipelineId)) {
            return driveTo(pipelineId, target, false, current, false, true, false);
        }
        // Otherwise a failed run stays failed: re-driving it toward RUNNING would restart the dead job
        // on every tick, and toward PAUSED would try every tick to hold a job that is gone, be refused,
        // and fail the pipeline over again. The user recovers by stopping it then starting a fresh
        // run -- which arrives as the one instruction above, and that is let through: it is somebody
        // saying so once, which is the whole difference from this loop noticing the same death every
        // second.
        return ConvergeResult.converged(current);
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
            if (requirePreparedState) throw missingCheckpoint(pipelineId);
            if (!seedIfAbsent) {
                return ConvergeResult.nothingToDo();
            }
            state.create(pipelineId, StateJson.of(PipelineState.NEW), clock.instant());
            current = requireCheckpoint(pipelineId);
        }
        // A rebuild is the one thing this comparison cannot see. It asks for the run behind the state
        // to be replaced, and the state already matching is exactly the condition it is asked for in,
        // so reading it as converged is how both halves of a stop and a start written together went
        // missing at once with nothing anywhere reporting it.
        if (current.stateJson().equals(targetJson) && !evenIfAlreadyThere) {
            return ConvergeResult.converged(current);
        }
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            PipelineState from = StateJson.parse(current.stateJson());
            CasOutcome outcome = state.compareAndSwap(pipelineId, current.epoch(), targetJson, clock.instant());
            if (outcome instanceof CasOutcome.Applied applied) {
                if (target == PipelineState.FAILED) {
                    rebuilds.recordFailure(pipelineId);
                }
                // Record first, then actuate: the store is the source of truth and Jet is subordinate, so the
                // fenced write lands the intent durably before the job side is driven to match it.
                try {
                    actuate(pipelineId, from, target, purgeState, rebuild);
                } catch (TapstateException refused) {
                    if(target==PipelineState.COMPLETED)return ConvergeResult.failed(applied.next(),refused);
                    // The job side refused with a diagnosis. Record it the way a job that died is recorded,
                    // because to everyone reading the product they are the same event: the pipeline is not
                    // going to run, and here is why. Letting it escape instead leaves the pass to abort
                    // before anything is published, so the read face keeps whatever state it last saw while
                    // the loop retries every tick -- a pipeline stuck at NEW, and the reason for it in the
                    // server log alone.
                    //
                    // Only coded refusals. An uncoded throw is a defect in this process rather than a
                    // condition of this pipeline, and recording it here would file it under the user's name
                    // and stop it crashing anything -- which is what makes a defect visible.
                    return failedWith(pipelineId, refused);
                }
                return ConvergeResult.converged(applied.next());
            }
            // Fenced: another writer moved the epoch on. Re-read it and rebase before retrying.
            current = requireCheckpoint(pipelineId);
            if (current.stateJson().equals(targetJson)) {
                return ConvergeResult.converged(current);
            }
            if (target == PipelineState.FAILED || target == PipelineState.COMPLETED) {
                // Every other target is an intent, and an intent survives being fenced: it is still what
                // is wanted, so rebasing onto the fresh epoch and asking again is right. A terminal result is
                // a conclusion about the state that was read, and being fenced means that
                // state is no longer there. Re-driving it would record a failure over whatever the other
                // writer just landed (a user's stop, most often), actuate a second stop for it, and be
                // corrected by the next pass: one tick of a pipeline reading failed that nothing failed,
                // which no reader can tell from one that did. Conceding costs nothing, because a job that
                // really is dead is still dead on the next pass and is failed then.
                return ConvergeResult.superseded();
            }
        }
        return ConvergeResult.superseded();
    }

    /**
     * Drives the job side to match a state transition the store just recorded. Start and resume both
     * land in RUNNING, so the origin state decides between them: RUNNING reached from PAUSED continues
     * the held job (resume), reached from anywhere else begins a fresh run (start). A pipeline seeded at
     * NEW is never a transition target here, so it drives nothing.
     *
     * <p>An intent that asks to be re-assembled overrides the first of those: the held job is torn down
     * and a fresh one submitted, keeping everything the pipeline has. That is what makes an edit made
     * while paused take effect, and it happens here rather than as two intents because this side samples
     * the latest intent instead of consuming every one written.
     *
     * <p>{@code purgeState} reaches only the stop, and only ever carries what a user's own stop asked
     * for. A pipeline that completed or failed arrives at the same verb, and arrives with it false: an
     * ending nobody asked for is not permission to throw away where it had got to.
     */
    private void actuate(
            String pipelineId, PipelineState from, PipelineState target, boolean purgeState,
            boolean rebuild) {
        switch (target) {
            case RUNNING -> {
                if (rebuild) {
                    // Both halves here, in one pass, because they cannot be two intents: this side reads
                    // the latest intent rather than consuming a queue of them, so a stop written and
                    // immediately overwritten by a start is never observed and neither half happens.
                    // Nothing is cleared -- carrying on from the recorded position is the whole point of
                    // re-assembling rather than re-reading the source.
                    actuator.stop(pipelineId, purgeState);
                    actuator.start(pipelineId);
                } else if (from == PipelineState.PAUSED) {
                    actuator.resume(pipelineId);
                } else {
                    actuator.start(pipelineId);
                }
            }
            case PAUSED -> actuator.pause(pipelineId);
            case STOPPED, FAILED -> actuator.stop(pipelineId, purgeState);
            case COMPLETED -> actuator.complete(pipelineId);
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
    private ConvergeResult failedWith(String pipelineId, Throwable cause) {
        ConvergeResult driven =
                driveTo(pipelineId, PipelineState.FAILED, false, requireCheckpoint(pipelineId), false);
        return driven.checkpoint()
                .map(checkpoint -> ConvergeResult.failed(checkpoint, cause))
                .orElse(driven);
    }

    private CheckpointDoc requireCheckpoint(String pipelineId) {
        return state.read(pipelineId)
                .orElseThrow(() -> missingCheckpoint(pipelineId));
    }

    private RuntimeException missingCheckpoint(String pipelineId) {
        return requirePreparedState
                ? new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId, "field", "checkpoint"), null)
                : new IllegalStateException("checkpoint vanished for pipeline " + pipelineId);
    }
}
