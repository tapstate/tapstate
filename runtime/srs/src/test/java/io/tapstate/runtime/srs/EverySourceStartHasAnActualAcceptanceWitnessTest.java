package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EverySourceStartHasAnActualAcceptanceWitnessTest {
    @Test
    void aPlainBatchDelegateCannotHideTheSourcesResolvedStartingAnchor() {
        CaptureHealth health = new CaptureHealth();
        assertThat(health.recording((events, position) -> { })).isInstanceOf(CaptureStartedListener.class);
    }

    @Test
    void anAnchorWithoutSuccessfulReaderDeliveryNeverCertifiesAcceptance() {
        CaptureHealth health = new CaptureHealth();
        var listener = (CaptureStartedListener) health.recording((events, position) -> { });
        listener.onStart(new SourcePosition("retained-before-failure"));
        assertThat(health.readerAccepted()).isFalse();
        RuntimeException expired = new RuntimeException("source refused the retained position");
        listener.onError(expired);
        assertThat(health.readerAccepted()).isFalse();
        assertThat(health.failure()).contains(expired);
        assertThat(health.resolvedStartingAnchor()).contains(new SourcePosition("retained-before-failure"));
    }

    @Test
    void aSuccessfulEmptyHeartbeatCertifiesTheActualReader() {
        CaptureHealth health = new CaptureHealth();
        var listener = (CaptureStartedListener) health.recording((events, position) -> { });
        listener.onStart(new SourcePosition("retained"));
        listener.onBatch(List.of(), Optional.empty());
        assertThat(health.readerAccepted()).isTrue();
        assertThat(health.receivedRows()).isEmpty();
    }

    @Test
    void aPreviousWideningCallbackCannotCertifyTheNewListener() {
        CaptureHealth health = new CaptureHealth();
        var prior = (CaptureStartedListener) health.recording((events, position) -> { });
        prior.onStart(new SourcePosition("old"));
        prior.onBatch(List.of(), Optional.empty());
        var current = (CaptureStartedListener) health.recording((events, position) -> { });
        prior.onStart(new SourcePosition("late-old"));
        prior.onBatch(List.of(), Optional.empty());
        prior.onError(new RuntimeException("old reader ended after widening"));
        assertThat(health.readerAccepted()).isFalse();
        assertThat(health.failure()).isEmpty();
        current.onStart(new SourcePosition("new"));
        current.onBatch(List.of(), Optional.empty());
        assertThat(health.readerAccepted()).isTrue();
        assertThat(health.resolvedStartingAnchor()).contains(new SourcePosition("new"));
    }
}
