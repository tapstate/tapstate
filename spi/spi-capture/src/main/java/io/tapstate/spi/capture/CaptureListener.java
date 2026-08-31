package io.tapstate.spi.capture;

import io.tapstate.core.event.Envelope;

import java.util.Optional;

/**
 * A sink for CDC events: it receives each change event the capture streams. The events are row and
 * schema mutations (ops {@code i} / {@code u} / {@code d} / {@code ddl}).
 */
@FunctionalInterface
public interface CaptureListener {

    /**
     * Called once per captured change event, with the position the source reported for it.
     *
     * <p>The position is present only on a change the source has stated a position <em>at or after</em> —
     * in practice the last change of a batch the source closed by naming where it had read to. It is
     * absent on the others, and that absence is load-bearing rather than a gap to fill in: a source names
     * one position for a run of changes, meaning "everything up to here has been handed over", and that
     * sentence is only true once the whole run has been. Stamping the earlier changes with it would say a
     * change had been read that had not, and a run interrupted in the middle would resume past the ones it
     * never delivered — losing them with nothing thrown and nothing logged.
     *
     * <p>So a recipient records a position when one arrives and simply carries on when none does. What
     * ranks two changes against each other is never this: a token is opaque and only equality is defined
     * on it, so ordering is the runtime's own, assigned as it reads.
     */
    void onEvent(Envelope event, Optional<SourcePosition> position);

    /**
     * Called when the capture stream fails, so the failure a background stream cannot return to its
     * caller is still delivered. The default is a no-op: a listener that does not observe stream health
     * ignores it. Delivered at most once, after which the stream has ended.
     */
    default void onError(Throwable error) {
    }
}
