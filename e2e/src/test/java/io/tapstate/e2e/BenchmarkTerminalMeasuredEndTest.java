package io.tapstate.e2e;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Measured-end tokens come from unique ordered source rows and cannot be inferred at close. */
class BenchmarkTerminalMeasuredEndTest {

    private static final String TABLE = "bench_nest_items";

    @Test
    void missingConfiguredPhaseFailsAtClose() {
        var listener = holder(Map.of("cdc-update", 12_000L));
        assertThatThrownBy(listener::requireMeasuredComplete).isInstanceOf(AssertionError.class)
                .hasMessageContaining("before every configured measured phase ended");
    }

    @Test
    void capturedPhaseTokenIsKeptAndDuplicateOrOffsetlessMarkersFail() {
        var listener = holder(Map.of("cdc-update", 12_000L));
        listener.onBatch(List.of(row(1, Op.UPDATE)), Optional.of(new SourcePosition("warm")));
        listener.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.of(new SourcePosition("measured")));
        assertThat(listener.measuredToken("cdc-update")).isEqualTo("measured");
        listener.requireMeasuredComplete();
        listener.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.of(new SourcePosition("replayed")));
        assertThatThrownBy(listener::check).isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("measured-end row arrived out of phase order or twice");

        var noPosition = holder(Map.of("cdc-update", 12_000L));
        noPosition.onBatch(List.of(row(1, Op.UPDATE)), Optional.of(new SourcePosition("warm")));
        noPosition.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.empty());
        assertThatThrownBy(noPosition::check).isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("measured-end row had no source position");
    }

    @Test
    void dualPhaseMarkersMustArriveInColdThenCdcOrder() {
        var listener = holder(Map.of("cold-read", 312_000L, "cdc-update", 12_000L));
        listener.onBatch(List.of(row(1, Op.UPDATE)), Optional.of(new SourcePosition("warm")));
        listener.onBatch(List.of(row(312_000, Op.INSERT)), Optional.of(new SourcePosition("cold")));
        listener.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.of(new SourcePosition("cdc")));
        listener.requireMeasuredComplete();
        assertThat(listener.measuredToken("cold-read")).isEqualTo("cold");
        assertThat(listener.measuredToken("cdc-update")).isEqualTo("cdc");

        var outOfOrder = holder(Map.of("cold-read", 312_000L, "cdc-update", 12_000L));
        outOfOrder.onBatch(List.of(row(1, Op.UPDATE)), Optional.of(new SourcePosition("warm")));
        outOfOrder.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.of(new SourcePosition("early")));
        assertThatThrownBy(outOfOrder::check).isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("measured-end row arrived out of phase order or twice");
    }

    @Test
    void aMarkerBetweenThePredicateReadAndWaitCannotDelayTheWholeDeadline() throws Exception {
        var listener = holder(Map.of("cdc-update", 12_000L));
        listener.onBatch(List.of(row(1, Op.UPDATE)), Optional.of(new SourcePosition("warm")));
        assertThat(listener.measuredToken("cdc-update")).isNull();
        // The real callback can acquire the monitor after this predicate read but before wait.
        listener.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.of(new SourcePosition("measured")));
        var waiter = CompletableFuture.runAsync(() -> listener.waitForChange(Duration.ofSeconds(2).toNanos(),
                () -> listener.measuredToken("cdc-update") != null));
        try {
            waiter.get(500, TimeUnit.MILLISECONDS);
            assertThat(listener.measuredToken("cdc-update")).isEqualTo("measured");
        } finally {
            waiter.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void waitingForTheNextPhaseDoesNotReturnBecauseAnEarlierPhaseIsComplete() throws Exception {
        var listener = holder(Map.of("cold-read", 312_000L, "cdc-update", 12_000L));
        listener.onBatch(List.of(row(1, Op.UPDATE)), Optional.of(new SourcePosition("warm")));
        listener.onBatch(List.of(row(312_000, Op.INSERT)), Optional.of(new SourcePosition("cold")));
        var checked = new CountDownLatch(1);
        var waiter = CompletableFuture.runAsync(() -> listener.waitForChange(Duration.ofSeconds(2).toNanos(), () -> {
            boolean complete = listener.measuredToken("cdc-update") != null;
            checked.countDown();
            return complete;
        }));
        try {
            assertThat(checked.await(500, TimeUnit.MILLISECONDS)).isTrue();
            assertThatThrownBy(() -> waiter.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            listener.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.of(new SourcePosition("cdc")));
            waiter.get(500, TimeUnit.MILLISECONDS);
            assertThat(listener.measuredToken("cdc-update")).isEqualTo("cdc");
        } finally { waiter.get(3, TimeUnit.SECONDS); }
    }

    @Test
    void aFailureBeforeWaitRemainsAnErrorWithoutWaitingForAnotherNotification() throws Exception {
        var listener = holder(Map.of("cdc-update", 12_000L));
        listener.onBatch(List.of(row(1, Op.UPDATE)), Optional.of(new SourcePosition("warm")));
        listener.onBatch(List.of(row(12_000, Op.UPDATE)), Optional.empty());
        var waiter = CompletableFuture.runAsync(() -> listener.waitForChange(Duration.ofSeconds(2).toNanos(),
                () -> listener.measuredToken("cdc-update") != null));
        try {
            waiter.get(500, TimeUnit.MILLISECONDS);
            assertThatThrownBy(listener::check).isInstanceOf(AssertionError.class)
                    .hasRootCauseMessage("measured-end row had no source position");
        } finally { waiter.get(3, TimeUnit.SECONDS); }
    }

    private static BenchmarkTerminalCapture.Holder holder(Map<String, Long> markers) {
        return new BenchmarkTerminalCapture.Holder(TABLE, 1, 900_014, null, markers);
    }

    private static Envelope row(long id, Op operation) {
        return new Envelope(operation, 1, TABLE, null, Map.of("id", id), null);
    }
}
