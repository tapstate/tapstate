package io.tapstate.app;

import io.tapstate.runtime.scheduler.LifecycleActuator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

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
        long began = System.nanoTime();
        try {
            PreparedStart prepared = delegate.prepareStart(pipelineId);
            long preparationNanos = System.nanoTime() - began;
            AtomicBoolean recorded = new AtomicBoolean();
            return new PreparedStart() {
                private long workNanos = preparationNanos;

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
    public boolean isCarryingAJob(String pipelineId) {
        return delegate.isCarryingAJob(pipelineId);
    }
}
