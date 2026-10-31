package io.tapstate.app;

import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAckFactory;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class ValidationInitialLoadStateTest {
    private static final ValidationInitialLoadState.Attempt ATTEMPT = new ValidationInitialLoadState.Attempt(31, 42);
    @Test void selectedOutputWaitsForAllOfItsActualWritersIncludingTheZeroRowWriter() {
        var state = state(1); state.source(ATTEMPT, "orders", producer(0), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.SNAPSHOT, 7));
        state.writerStarted(ATTEMPT, "serve.orders#0"); state.writerStarted(ATTEMPT, "serve.orders#1");
        state.bound(ATTEMPT, "orders", "serve.orders#0", SourceOrder.snapshotRow(7));
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.WAITING);
        state.bound(ATTEMPT, "orders", "serve.orders#1", SourceOrder.snapshotRow(7));
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.READY);
        assertThat(state.status(Set.of("serve.unrelated")).state()).isEqualTo(ValidationInitialLoadState.State.WAITING);
    }

    @Test void unrelatedSourceFailureDoesNotRejectTheSelectedOutput() {
        var state = state(1); state.source(ATTEMPT, "elsewhere", producer(0), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.UNKNOWN, 0));
        state.source(ATTEMPT, "orders", producer(0), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.NO_INITIAL_LOAD, 0));
        state.writerStarted(ATTEMPT, "serve.orders#0"); state.writerStarted(ATTEMPT, "serve.orders#1");
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.READY);
        assertThat(state.status(Set.of("serve.unrelated")).state()).isEqualTo(ValidationInitialLoadState.State.UNAVAILABLE);
    }

    @Test void noInitialLoadNeedsActualProducerAndWriterInitializationRatherThanAnEmptyResult() {
        var state = state(2); state.writerStarted(ATTEMPT, "serve.orders#0"); state.writerStarted(ATTEMPT, "serve.orders#1");
        state.source(ATTEMPT, "orders", producer(7), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.NO_INITIAL_LOAD, 0));
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.WAITING);
        state.source(ATTEMPT, "orders", producer(8), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.NO_INITIAL_LOAD, 0));
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.READY);
        state.source(ATTEMPT, "orders", producer(8), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.SNAPSHOT, 9));
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.UNAVAILABLE);
    }

    @Test void aCdcWatermarkOrAnotherEpochCannotProveSnapshotLanding() {
        var state = state(1); state.source(ATTEMPT, "orders", producer(0), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.SNAPSHOT, 7));
        state.writerStarted(ATTEMPT, "serve.orders#0"); state.writerStarted(ATTEMPT, "serve.orders#1");
        state.bound(ATTEMPT, "orders", "serve.orders#0", new SourceOrder(7, 100));
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.WAITING);
        state.bound(ATTEMPT, "orders", "serve.orders#0", SourceOrder.snapshotRow(8)); state.bound(ATTEMPT, "orders", "serve.orders#1", SourceOrder.snapshotRow(7));
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.UNAVAILABLE);
    }
    @Test void aPreviousJetAttemptCannotContributeProducerWriterOrCommitFacts() {
        var state = state(1); var stale = new ValidationInitialLoadState.Attempt(31, 41);
        assertThat(state.source(stale, "orders", producer(0), new ValidationInitialLoadState.Source(ValidationInitialLoadState.Mode.NO_INITIAL_LOAD, 0))).isFalse();
        assertThat(state.writerStarted(stale, "serve.orders#0")).isFalse();
        assertThat(state.bound(stale, "orders", "serve.orders#0", SourceOrder.snapshotRow(7))).isFalse();
        assertThat(state.status(Set.of("serve.orders")).state()).isEqualTo(ValidationInitialLoadState.State.WAITING);
    }

    private static ValidationInitialLoadState state(int count) {
        var writers = new SinkAckFactory.RunWriters(Map.of("orders", List.of("serve.orders#0", "serve.orders#1"), "elsewhere", List.of("serve.unrelated#0")),
                Map.of("serve.orders", Map.of("orders", List.of("serve.orders#0", "serve.orders#1")), "serve.unrelated", Map.of("elsewhere", List.of("serve.unrelated#0"))));
        return new ValidationInitialLoadState(new SinkAckFactory.ExecutionWriters(31, 42, writers), Map.of("orders", count, "elsewhere", 1), 65536, () -> {});
    }
    private static ValidationInitialLoadState.Producer producer(int index) { return new ValidationInitialLoadState.Producer("node", "boot", new UUID(1, 1), index); }
}
