package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.SourcePosition;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds whether a source run's cdc stream has failed. The stream runs on its own thread and reports its
 * failure through the capture listener's error channel; this keeps the first such failure so a coordinator
 * polling the run can observe a dead tail. That failure is otherwise invisible above the run: the execution
 * job reading the change ring keeps running over a ring that has simply gone quiet, so its status never
 * reflects the tail's death.
 */
public final class CaptureHealth {

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /** The failure the run's cdc stream died with, or empty while it is healthy or opened no tail. */
    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure.get());
    }

    /**
     * Records the failure the cdc stream died with. The first is kept: a stream reports at most one, but a
     * compare-and-set keeps the record stable if that ever changes rather than letting a later error
     * overwrite the cause that stopped the tail.
     */
    public void fail(Throwable error) {
        failure.compareAndSet(null, error);
    }

    /** Wraps an event handler as a listener that records a stream failure on this health through {@link #fail}. */
    CaptureListener recording(CaptureListener onEvent) {
        return new CaptureListener() {
            @Override
            public void onEvent(Envelope event, Optional<SourcePosition> position) {
                onEvent.onEvent(event, position);
            }

            @Override
            public void onError(Throwable error) {
                fail(error);
            }
        };
    }
}
