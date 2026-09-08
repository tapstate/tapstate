package io.tapstate.app;

import io.tapstate.runtime.scheduler.LifecycleActuator;

import java.util.Optional;

/**
 * A {@link LifecycleActuator} that drives nothing — lets a wiring test exercise the convergence loop
 * without standing up a Jet member, since those tests assert bean wiring and state convergence, not the
 * data-plane job side. It never reports a failure: with no real job there is none to observe.
 */
final class NoOpActuator implements LifecycleActuator {

    @Override
    public void start(String pipelineId) {
    }

    @Override
    public void pause(String pipelineId) {
    }

    @Override
    public void resume(String pipelineId) {
    }

    @Override
    public void stop(String pipelineId, boolean purgeState) {
    }

    @Override
    public Optional<Throwable> failure(String pipelineId) {
        return Optional.empty();
    }

    /** Always carrying: this double stands in for a data plane that is doing what it was told. */
    @Override
    public boolean isCarryingAJob(String pipelineId) {
        return true;
    }
}
