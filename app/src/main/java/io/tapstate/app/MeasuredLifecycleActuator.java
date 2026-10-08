package io.tapstate.app;

import io.tapstate.runtime.scheduler.LifecycleActuator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.HandoffIdentity;

/** Measures real engine/capture operations at the assembly seam, including failures and cleanup. */
final class MeasuredLifecycleActuator implements LifecycleActuator {

    private final LifecycleActuator delegate;
    private final LifecycleWorkDispatcher facts;

    MeasuredLifecycleActuator(LifecycleActuator delegate, LifecycleWorkDispatcher facts) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.facts = Objects.requireNonNull(facts, "facts");
    }

    @Override
    public PreparedStart prepareStart(String pipelineId) {
        return measuredStart(() -> delegate.prepareStart(pipelineId));
    }

    @Override
    public PreparedStart prepareStart(String pipelineId, io.tapstate.core.lifecycle.DesiredState desired,
            io.tapstate.core.lifecycle.CheckpointDoc checkpoint,
            java.util.function.Consumer<io.tapstate.spi.store.PreExecutionFailure.Attempt> captured) {
        return measuredStart(() -> delegate.prepareStart(pipelineId, desired, checkpoint, captured));
    }

    private PreparedStart measuredStart(java.util.function.Supplier<PreparedStart> preparation) {
        long began = System.nanoTime();
        try {
            PreparedStart prepared = preparation.get();
            long preparationNanos = System.nanoTime() - began;
            AtomicBoolean recorded = new AtomicBoolean();
            return new PreparedStart() {
                private long workNanos = preparationNanos;

                @Override public Optional<StopReservation.Source> submittedSource() { return prepared.submittedSource(); }

                @Override
                public void submit() {
                    long submitting = System.nanoTime();
                    try {
                        prepared.submit();
                    } finally {
                        workNanos += System.nanoTime() - submitting;
                    }
                }

                @Override
                public void close() {
                    long closing = System.nanoTime();
                    try {
                        prepared.close();
                    } finally {
                        workNanos += System.nanoTime() - closing;
                        if (recorded.compareAndSet(false, true)) {
                            facts.recordVerb(LifecycleWorkDispatcher.Verb.START, workNanos);
                        }
                    }
                }
            };
        } catch (RuntimeException | Error failure) {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.START, System.nanoTime() - began);
            throw failure;
        }
    }

    @Override
    public PreparedReplacement prepareReplacement(StopReservation reservation, ReplacementAdmission admission,
            Predicate<StopReservation> current) {
        return measuredReplacement(() -> delegate.prepareReplacement(reservation, admission, current));
    }

    @Override
    public PreparedReplacement prepareReplacement(StopReservation reservation, ReplacementAdmission admission,
            Predicate<StopReservation> current, io.tapstate.core.lifecycle.CheckpointDoc checkpoint,
            java.util.function.Consumer<io.tapstate.spi.store.PreExecutionFailure.Attempt> captured) {
        return measuredReplacement(() -> delegate.prepareReplacement(reservation, admission, current, checkpoint, captured));
    }

    private PreparedReplacement measuredReplacement(java.util.function.Supplier<PreparedReplacement> preparation) {
        long began = System.nanoTime();
        try {
            PreparedReplacement prepared = preparation.get();
            long preparationNanos = System.nanoTime() - began;
            return new PreparedReplacement() {
                private long workNanos = preparationNanos;
                private boolean recorded;

                @Override public StopReservation admitted() { return prepared.admitted(); }
                @Override public Optional<StopReservation.JobIdentity> submittedJob() { return prepared.submittedJob(); }
                @Override public Optional<StopReservation.Source> submittedSource() { return prepared.submittedSource(); }
                @Override public void submit() {
                    long submitting = System.nanoTime();
                    try { prepared.submit(); }
                    finally { workNanos += System.nanoTime() - submitting; }
                }
                @Override public void close() {
                    long closing = System.nanoTime();
                    try { prepared.close(); }
                    finally {
                        workNanos += System.nanoTime() - closing;
                        if (!recorded) {
                            recorded = true;
                            facts.recordVerb(LifecycleWorkDispatcher.Verb.START, workNanos);
                        }
                    }
                }
            };
        } catch (RuntimeException | Error failure) {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.START, System.nanoTime() - began);
            throw failure;
        }
    }

    @Override public boolean stillPreExecution(io.tapstate.spi.store.PreExecutionFailure.Attempt attempt) {
        return delegate.stillPreExecution(attempt);
    }

    @Override public Optional<SuccessorInspection> inspectSuccessor(StopReservation reservation,
            BooleanSupplier current) { return delegate.inspectSuccessor(reservation, current); }

    @Override public boolean adoptSuccessor(StopReservation reservation, BooleanSupplier current) {
        return delegate.adoptSuccessor(reservation, current);
    }

    @Override public Optional<HandoffIdentity> continuationReady(StopReservation reservation,
            BooleanSupplier current) { return delegate.continuationReady(reservation, current); }

    @Override public void observeReplacementFailure(StopReservation reservation, BooleanSupplier current) {
        delegate.observeReplacementFailure(reservation, current);
    }

    @Override
    public boolean needsRebuildOnResume(String pipelineId) {
        return delegate.needsRebuildOnResume(pipelineId);
    }

    @Override
    public void start(String pipelineId) {
        long began = System.nanoTime();
        try {
            delegate.start(pipelineId);
        } finally {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.START, System.nanoTime() - began);
        }
    }

    @Override
    public void pause(String pipelineId) {
        long began = System.nanoTime();
        try {
            delegate.pause(pipelineId);
        } finally {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.PAUSE, System.nanoTime() - began);
        }
    }

    @Override
    public void resume(String pipelineId) {
        long began = System.nanoTime();
        try {
            delegate.resume(pipelineId);
        } finally {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.RESUME, System.nanoTime() - began);
        }
    }

    @Override
    public void stop(String pipelineId, boolean purgeState) {
        long began = System.nanoTime();
        try {
            delegate.stop(pipelineId, purgeState);
        } finally {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.STOP, System.nanoTime() - began);
        }
    }

    @Override
    public void stopForRebuildingResume(String pipelineId, boolean purgeState) {
        long began = System.nanoTime();
        try {
            delegate.stopForRebuildingResume(pipelineId, purgeState);
        } finally {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.STOP, System.nanoTime() - began);
        }
    }

    @Override
    public Optional<Throwable> failure(String pipelineId) {
        return delegate.failure(pipelineId);
    }

    @Override
    public Optional<Throwable> lost(String pipelineId) {
        return delegate.lost(pipelineId);
    }

    @Override
    public boolean isCarryingAJob(String pipelineId) {
        return delegate.isCarryingAJob(pipelineId);
    }

    @Override
    public Optional<StopReservation.Subject> stopSubject(String pipelineId) {
        return delegate.stopSubject(pipelineId);
    }

    @Override
    public Optional<StopReservation.Source> stopSource(String pipelineId) {
        return delegate.stopSource(pipelineId);
    }

    @Override
    public Optional<StopAuthority> stopAuthority(String pipelineId) {
        return delegate.stopAuthority(pipelineId);
    }

    @Override
    public boolean finishStop(StopReservation reservation, boolean continuing, boolean firstAttempt,
            boolean retiring, BooleanSupplier current) {
        long began = System.nanoTime();
        try {
            return delegate.finishStop(reservation, continuing, firstAttempt, retiring, current);
        } finally {
            facts.recordVerb(LifecycleWorkDispatcher.Verb.STOP, System.nanoTime() - began);
        }
    }
}
