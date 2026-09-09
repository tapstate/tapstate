package io.tapstate.spi.capture;

import io.tapstate.core.event.Envelope;

import java.util.List;
import java.util.Optional;

/**
 * A sink for CDC events: it receives each change event the capture streams. The events are row and
 * schema mutations (ops {@code i} / {@code u} / {@code d} / {@code ddl}).
 */
@FunctionalInterface
public interface CaptureListener {

    /**
     * Called once per run of changes the source hands over, with the position it reported for that run.
     *
     * <p><strong>The run is the source's own, and it is delivered whole.</strong> A source reads a batch
     * and names one position for it; splitting that batch into single changes on the way here throws away
     * the only grouping anyone downstream could have recovered, and every cost that is paid per act rather
     * than per change — a durable write above all — is then paid once per change instead of once per
     * batch. An empty run is possible and means the source handed over only events that carry no change.
     *
     * <p><strong>The position belongs to the run, not to any one change in it.</strong> It means
     * "everything up to here has been handed over", and that sentence is only true once the whole run has
     * been. It is absent when the source named none, and that absence is load-bearing rather than a gap to
     * fill in: inventing one would say changes had been read that had not, and a run interrupted in the
     * middle would resume past the ones it never delivered — losing them with nothing thrown and nothing
     * logged.
     *
     * <p>So a recipient records a position when one arrives and simply carries on when none does. What
     * ranks two changes against each other is never this: a token is opaque and only equality is defined
     * on it, so ordering is the runtime's own, assigned as it reads.
     */
    void onBatch(List<Envelope> events, Optional<SourcePosition> position);

    /**
     * Called when the capture stream fails, so the failure a background stream cannot return to its
     * caller is still delivered. The default is a no-op: a listener that does not observe stream health
     * ignores it. Delivered at most once, after which the stream has ended.
     */
    default void onError(Throwable error) {
    }
}
