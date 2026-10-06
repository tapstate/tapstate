package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real BackgroundLoad completion remains distinct from a bounded cancellation request. */
class BackgroundLoadCloseCompletionTest {
    @Test
    void aCancelledBackgroundLoadsLateNativeCloseFailureCannotBeHiddenByFinished() throws Exception {
        CountDownLatch opening = new CountDownLatch(1), release = new CountDownLatch(1);
        IllegalStateException original = new IllegalStateException("controlled late background tail close refusal");
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) {
                return new CaptureBatch() {
                    @Override public boolean hasNext() { return false; }
                    @Override public Envelope next() { throw new java.util.NoSuchElementException(); }
                    @Override public Optional<SourcePosition> seam() { return Optional.empty(); }
                    @Override public void close() { }
                };
            }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) { throw new AssertionError(); }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        };
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            var opened = SnapshotPhase.openChainless(source, new CaptureConfig("controlled", Map.of(), List.of("orders")), 1);
            CaptureHealth health = new CaptureHealth();
            BackgroundLoad load = new BackgroundLoad(opened, CaptureHandoff.of(row -> { }), () -> {
                opening.countDown();
                boolean interrupted = false;
                try {
                    while (release.getCount() != 0) {
                        try { if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("controlled tail open was not released"); } }
                        catch (InterruptedException requested) { interrupted = true; }
                    }
                    return Optional.of(() -> { throw original; });
                } finally { if (interrupted) { Thread.currentThread().interrupt(); } }
            }, health, workers.reserve().orElseThrow());
            CaptureRun run = new CaptureRun(Optional.empty(), false, Optional.empty(), health, load);
            load.start();
            try {
                assertThat(opening.await(5, TimeUnit.SECONDS)).isTrue();
                Thread.currentThread().interrupt(); run.close(); Thread.interrupted();
                release.countDown();
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> run.awaitLoaded(Duration.ofSeconds(5))).isSameAs(original);
                org.assertj.core.api.Assertions.assertThatThrownBy(run::close).isSameAs(original);
                assertThat(run.failure()).isEmpty();
            } finally { Thread.interrupted(); release.countDown(); }
        }
    }

    @Test
    void aReturnedCloseDoesNotEndAnActualBackgroundReaderThatIgnoredItsInterrupt() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        CaptureBatch batch = new CaptureBatch() {
            @Override public boolean hasNext() {
                entered.countDown();
                boolean interrupted = false;
                try {
                    while (release.getCount() != 0) {
                        try {
                            if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("controlled reader was not released"); }
                        } catch (InterruptedException requested) { interrupted = true; }
                    }
                    return false;
                } finally { if (interrupted) { Thread.currentThread().interrupt(); } }
            }
            @Override public Envelope next() { throw new java.util.NoSuchElementException(); }
            @Override public Optional<SourcePosition> seam() { return Optional.empty(); }
            @Override public void close() { }
        };
        CapturePort source = new CapturePort() {
            @Override public CaptureBatch snapshot(CaptureConfig config) { return batch; }
            @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                throw new AssertionError("this actual snapshot-only load has no tail");
            }
            @Override public ConnectionReport testConnection(CaptureConfig config) { throw new UnsupportedOperationException(); }
            @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new UnsupportedOperationException(); }
        };
        try (SnapshotWorkers workers = new SnapshotWorkers(1, 1)) {
            var opened = SnapshotPhase.openChainless(source, new CaptureConfig("controlled", Map.of(), List.of("orders")), 1);
            CaptureHealth health = new CaptureHealth();
            BackgroundLoad load = new BackgroundLoad(opened, CaptureHandoff.of(row -> { }), Optional::empty,
                    health, workers.reserve().orElseThrow());
            CaptureRun run = new CaptureRun(Optional.empty(), false, Optional.empty(), health, load);
            load.start();
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                // A real interrupted teardown caller must not turn cancel's bounded wait into terminal proof.
                Thread.currentThread().interrupt();
                run.close();
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                Thread.interrupted();
                assertThat(run.loading()).isTrue();
                assertThat(run.awaitLoaded(Duration.ZERO)).isFalse();
                release.countDown();
                assertThat(run.awaitLoaded(Duration.ofSeconds(5))).isTrue();
                run.close();
                assertThat(run.loading()).isFalse();
                assertThat(run.failure()).isEmpty();
            } finally { Thread.interrupted(); release.countDown(); run.awaitLoaded(Duration.ofSeconds(5)); run.close(); }
        }
    }
}
