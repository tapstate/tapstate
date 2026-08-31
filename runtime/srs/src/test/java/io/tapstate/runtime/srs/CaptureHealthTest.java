package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import org.junit.jupiter.api.Test;

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
        CaptureListener listener = health.recording((e, pos) -> delivered[0] = e);
        Envelope event = Envelope.insert(1L, "orders", Map.of("id", 1), Map.of());

        listener.onEvent(event, Optional.empty());
        assertThat(delivered[0]).as("events pass through to the wrapped handler").isSameAs(event);
        assertThat(health.failure()).as("no failure while only events flow").isEmpty();

        RuntimeException boom = new RuntimeException("stream boom");
        listener.onError(boom);
        assertThat(health.failure()).as("an error on the listener is recorded on the health").contains(boom);
    }
}
