package io.tapstate.e2e;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A sidecar must refuse boundary order and offset faults before its token is used as an ACK oracle. */
class BenchmarkTerminalCaptureBoundaryTest {

    private static final BenchmarkWorkloadDefinitions.Workload WORKLOAD =
            BenchmarkWorkloadDefinitions.byId("copy");
    private static final BenchmarkWorkloadDefinitions.SourceChain CHAIN = WORKLOAD.sourceChains().getFirst();
    private static final BenchmarkBoundaryWrites.Boundary BOUNDARY =
            BenchmarkBoundaryWrites.forChain(WORKLOAD, CHAIN);

    @Test
    void restoreBeforeChangedIsRefused() {
        var listener = holder();
        listener.onBatch(List.of(row(BOUNDARY.restoredValue())), Optional.of(new SourcePosition("restore")));
        assertThatThrownBy(listener::check).isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("sidecar saw restored boundary row out of order or twice");
    }

    @Test
    void replayedRestoreIsRefusedEvenAfterAValidFirstToken() {
        var listener = holder();
        listener.onBatch(List.of(row(BOUNDARY.changedValue())), Optional.empty());
        listener.onBatch(List.of(row(BOUNDARY.restoredValue())), Optional.of(new SourcePosition("restore")));
        listener.onBatch(List.of(row(BOUNDARY.restoredValue())), Optional.of(new SourcePosition("replayed")));
        assertThatThrownBy(listener::check).isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("sidecar saw restored boundary row out of order or twice");
    }

    @Test
    void restoredRowWithoutConnectorPositionIsRefused() {
        var listener = holder();
        listener.onBatch(List.of(row(BOUNDARY.changedValue())), Optional.empty());
        listener.onBatch(List.of(row(BOUNDARY.restoredValue())), Optional.empty());
        assertThatThrownBy(listener::check).isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("restored boundary row had no source position");
        assertThatThrownBy(listener::boundaryToken).isInstanceOf(AssertionError.class)
                .hasMessageContaining("no unique restored boundary source position");
    }

    @Test
    void measuredUpdatesOnTheBoundaryRowAreIgnoredOnlyAfterSealing() {
        var listener = holder();
        listener.onBatch(List.of(row(BOUNDARY.changedValue())), Optional.empty());
        listener.onBatch(List.of(row(BOUNDARY.restoredValue())), Optional.of(new SourcePosition("restore")));
        listener.sealBoundary();
        listener.onBatch(List.of(row(999L)), Optional.of(new SourcePosition("measured")));
        listener.check();
        assertThat(listener.boundaryToken()).isEqualTo("restore");
    }

    private static BenchmarkTerminalCapture.Holder holder() {
        return new BenchmarkTerminalCapture.Holder(CHAIN.table(),
                BenchmarkPreflightWrites.warmupRowId(WORKLOAD, CHAIN), CHAIN.terminalRowId(), BOUNDARY);
    }

    private static Envelope row(Object value) {
        return new Envelope(Op.UPDATE, 1, CHAIN.table(),
                Map.of("id", BOUNDARY.rowId(), BOUNDARY.field(), BOUNDARY.changedValue()),
                Map.of("id", BOUNDARY.rowId(), BOUNDARY.field(), value), null);
    }
}
