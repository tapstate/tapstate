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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A load read behind its run, and the tail that follows it. */
class BackgroundLoadTest {

    @Test void aCloseFailureStillInterruptsAndJoinsTheOriginalReadingWorker() throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1);var worker=new AtomicReference<Thread>();
        var failure=new IllegalStateException("close invariant");var once=new java.util.concurrent.atomic.AtomicBoolean();
        CapturePort source=new CapturePort() {
            public CaptureBatch snapshot(CaptureConfig config) {
                return new CaptureBatch() {
                    public boolean hasNext() {
                        worker.set(Thread.currentThread());entered.countDown();
                        try { new java.util.concurrent.CountDownLatch(1).await();return false; }
                        catch(InterruptedException interrupted) { Thread.currentThread().interrupt();throw new java.util.concurrent.CancellationException("source interrupted"); }
                    }
                    public Envelope next() { throw new AssertionError(); }
                    public Optional<SourcePosition> seam() { return Optional.empty(); }
                    public void close() { if(once.compareAndSet(false,true))throw failure; }
                };
            }
            public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) { throw new AssertionError(); }
            public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
            public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
        };
        var load=SnapshotPhase.openChainless(source,new CaptureConfig("fixture",Map.of(),List.of("orders")),1);
        var background=new BackgroundLoad(load,CaptureHandoff.of(row -> {}),Optional::empty,new CaptureHealth(),"close-failure-worker");
        background.start();assertThat(entered.await(3,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        try {
            assertThatThrownBy(background::cancel).isSameAs(failure);
            assertThat(background.awaitFinished(Duration.ofMillis(500))).isTrue();
        } finally { worker.get().interrupt();assertThat(background.awaitFinished(Duration.ofSeconds(3))).isTrue(); }
    }

    @Test void aStillRunningOriginalLoadKeepsItsClosePendingUntilTheWorkerExits() throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
        CapturePort source=new CapturePort() {
            public CaptureBatch snapshot(CaptureConfig config) {
                return new CaptureBatch() {
                    public boolean hasNext() {
                        entered.countDown();while(release.getCount()!=0)try {release.await();}catch(InterruptedException stopping) { }
                        return false;
                    }
                    public Envelope next(){throw new AssertionError();}
                    public Optional<SourcePosition> seam(){return Optional.empty();}
                    public void close(){ }
                };
            }
            public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener){throw new AssertionError();}
            public ConnectionReport testConnection(CaptureConfig config){throw new AssertionError();}
            public DiscoveredSchema discoverSchema(CaptureConfig config){throw new AssertionError();}
        };
        var load=SnapshotPhase.openChainless(source,new CaptureConfig("fixture",Map.of(),List.of("orders")),1);
        var original=new BackgroundLoad(load,CaptureHandoff.of(row -> {}),Optional::empty,new CaptureHealth(),"held-original-load");
        original.start();assertThat(entered.await(3,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        try {
            assertThatThrownBy(original::cancel).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(CaptureError.STILL_ENDING));
            assertThat(original.finished()).isFalse();
        } finally {release.countDown();assertThat(original.awaitFinished(Duration.ofSeconds(3))).isTrue();original.cancel();}
    }

    /**
     * A run closed as its load ends never opens its tail. Opened anyway, the tail would set up on the source
     * what nothing is left to read through or let go of -- a replication slot, after the clearing that closed
     * the run had already let go of the chain's.
     */
    @Test
    void aRunClosedAsItsLoadEndsNeverOpensItsTail() throws InterruptedException {
        AtomicInteger tailsOpened = new AtomicInteger();
        AtomicReference<BackgroundLoad> run = new AtomicReference<>();
        SnapshotPhase.Load load = SnapshotPhase.openChainless(new OneRowSource(),
                new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders")), 1L);
        CaptureHandoff closingAsTheLoadEnds = new CaptureHandoff() {
            @Override
            public void accept(Envelope row) {
            }

            @Override
            public void loaded(String table) {
                // The last thing the load says before it ends; the run is closed from its own reading thread.
                run.get().cancel();
            }
        };
        BackgroundLoad background = new BackgroundLoad(load, closingAsTheLoadEnds, () -> {
            tailsOpened.incrementAndGet();
            return Optional.of(() -> { });
        }, new CaptureHealth(), "tapstate-test-load");
        run.set(background);

        background.start();

        assertThat(background.awaitFinished(Duration.ofSeconds(10))).isTrue();
        assertThat(tailsOpened).as("a tail opened after the run was closed").hasValue(0);
        assertThat(background.tail()).isEmpty();
    }

    /** A source whose bounded read yields one row of {@code orders}, with a seam. */
    private static final class OneRowSource implements CapturePort {

        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            Iterator<Envelope> rows = List.of(Envelope.read(1, "orders", Map.of("id", 1), Map.of())).iterator();
            return new CaptureBatch() {
                @Override
                public boolean hasNext() {
                    return rows.hasNext();
                }

                @Override
                public Envelope next() {
                    return rows.next();
                }

                @Override
                public Optional<SourcePosition> seam() {
                    return Optional.of(new SourcePosition("seam-0"));
                }

                @Override
                public void close() {
                }
            };
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }
}
