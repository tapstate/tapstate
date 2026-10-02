package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The capture-health holder: it reports no failure while the tail is alive, keeps the first failure its cdc
 * stream reports, and its recording wrapper routes a listener's stream error onto it while passing events
 * through. Keeping the first matters because the cause that stopped the tail is the one worth surfacing; a
 * later straggler must not overwrite it.
 */
class CaptureHealthTest {

    @Test
    void isEmptyUntilAFailureIsRecorded() {
        assertThat(new CaptureHealth().failure()).isEmpty();
    }

    @Test
    void keepsTheFirstFailureWhenSeveralAreReported() {
        CaptureHealth health = new CaptureHealth();
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");

        health.fail(first);
        health.fail(second);

        // A stream reports one failure, but if several ever arrive the cause that stopped the tail is the one
        // kept -- a later straggler must not overwrite it.
        assertThat(health.failure()).contains(first);
    }

    @Test
    void theRecordingListenerPassesEventsThroughAndRecordsAnError() {
        CaptureHealth health = new CaptureHealth();
        Envelope[] delivered = new Envelope[1];
        CaptureListener listener = health.recording((events, pos) -> delivered[0] = events.get(0));
        assertThat(listener).isNotInstanceOf(CaptureStartedListener.class);
        Envelope event = Envelope.insert(1L, "orders", Map.of("id", 1), Map.of());

        listener.onBatch(List.of(event), Optional.empty());
        assertThat(delivered[0]).as("events pass through to the wrapped handler").isSameAs(event);
        assertThat(health.failure()).as("no failure while only events flow").isEmpty();

        RuntimeException boom = new RuntimeException("stream boom");
        listener.onError(boom);
        assertThat(health.failure()).as("an error on the listener is recorded on the health").contains(boom);
    }

    @Test
    void onlyAnOptedInDelegateReceivesItsStartingAnchorWithoutCountingItAsAChange() {
        CaptureHealth health = new CaptureHealth();
        SourcePosition[] observed = new SourcePosition[1];
        Envelope[] delivered = new Envelope[1];
        CaptureListener recorded = health.recording(new CaptureStartedListener() {
            @Override
            public void onStart(SourcePosition position) {
                observed[0] = position;
            }

            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                delivered[0] = events.get(0);
            }
        });
        assertThat(recorded).isInstanceOf(CaptureStartedListener.class);
        SourcePosition anchor = new SourcePosition("before-first-change");
        ((CaptureStartedListener) recorded).onStart(anchor);

        assertThat(observed[0]).isSameAs(anchor);
        assertThat(health.receivedRows()).isEmpty();
        Envelope change = Envelope.insert(1L, "orders", Map.of("id", 1), Map.of());
        recorded.onBatch(List.of(change), Optional.empty());
        assertThat(delivered[0]).isSameAs(change);
        assertThat(health.receivedRows()).isEqualTo(Map.of("orders", Map.of("i", 1L)));
        RuntimeException failure = new RuntimeException("capture failed");
        recorded.onError(failure);
        assertThat(health.failure()).contains(failure);
    }
}
