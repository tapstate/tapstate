package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.tapstate.spi.capture.Subscription;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * The capture-run handle as a closeable teardown grip: closing it stops the running cdc stream through the
 * subscription it carries, and closing a run that opened no tail is a safe no-op. Closing is what tears the
 * capture daemon down, so it must reach the subscription when one is present and never fault when none is.
 */
class CaptureRunTest {

    @Test
    void closeStopsTheCdcSubscriptionWhenOneIsRunning() {
        AtomicBoolean closed = new AtomicBoolean(false);
        Subscription subscription = () -> closed.set(true);
        CaptureRun run = new CaptureRun(
                Optional.empty(), false, 0L, Optional.empty(), Optional.of(subscription), new CaptureHealth());

        run.close();

        assertThat(closed).as("closing the run closes its cdc subscription, stopping the capture").isTrue();
    }

    @Test
    void closeIsANoOpWhenThereIsNoCdcSubscription() {
        CaptureRun run = new CaptureRun(
                Optional.empty(), false, 0L, Optional.empty(), Optional.empty(), new CaptureHealth());

        // A snapshot-only or srs-disabled run may open no cdc tail; closing it must be a safe no-op, not a fault.
        assertThatCode(run::close).doesNotThrowAnyException();
    }

    @Test
    void failureSurfacesWhatTheStreamRecordedOnTheHealth() {
        CaptureHealth health = new CaptureHealth();
        RuntimeException boom = new RuntimeException("tail boom");
        // A stream failure lands on the health through the recording listener the phase installs.
        health.recording((e, pos) -> {
        }).onError(boom);
        CaptureRun run = new CaptureRun(
                Optional.empty(), false, 0L, Optional.empty(), Optional.empty(), health);

        assertThat(run.failure()).contains(boom);
    }

    @Test
    void failureIsEmptyForARunWhoseTailIsHealthy() {
        CaptureRun run = new CaptureRun(
                Optional.empty(), false, 0L, Optional.empty(), Optional.empty(), new CaptureHealth());

        assertThat(run.failure()).isEmpty();
    }

    @Test
    void closeIsIdempotentAcrossRepeatedCalls() {
        AtomicBoolean closed = new AtomicBoolean(false);
        Subscription subscription = () -> closed.set(true);
        CaptureRun run = new CaptureRun(
                Optional.empty(), false, 0L, Optional.empty(), Optional.of(subscription), new CaptureHealth());

        run.close();
        assertThatCode(run::close).doesNotThrowAnyException();
        assertThat(closed).isTrue();
    }
}
