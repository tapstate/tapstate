package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TuiOperationTest {

    @Test
    void ctrlCRequestsCancellationForAnActiveStream() {
        TuiOperation stream = TuiOperation.stream("op-stream", "pipeline status --watch");

        TuiOperation.CtrlCResult result = stream.onCtrlC();

        assertThat(result.action()).isEqualTo(TuiOperation.CtrlCAction.REQUEST_CANCELLATION);
        assertThat(result.operation().status()).isEqualTo(TuiOperation.Status.CANCELLATION_REQUESTED);
        assertThat(result.operation().id()).isEqualTo("op-stream");
        assertThat(result.remoteCancellationRequested()).isTrue();
        assertThat(result.message()).isEqualTo("cancellation requested for op-stream");
    }

    @Test
    void ctrlCStopsWaitingForASubmittedWriteWithoutClaimingRollback() {
        TuiOperation write = TuiOperation.submittedWrite("op-write", "pipeline.start");

        TuiOperation.CtrlCResult result = write.onCtrlC();

        assertThat(result.action()).isEqualTo(TuiOperation.CtrlCAction.STOP_WAITING);
        assertThat(result.operation().status()).isEqualTo(TuiOperation.Status.WAITING_STOPPED);
        assertThat(result.remoteCancellationRequested()).isFalse();
        assertThat(result.message()).isEqualTo("stopped waiting for submitted write op-write; outcome may still be in progress");
    }

    @Test
    void ctrlCDoesNothingAfterAnOperationHasFinished() {
        TuiOperation completed = TuiOperation.stream("op-done", "logs --follow")
                .complete();

        TuiOperation.CtrlCResult result = completed.onCtrlC();

        assertThat(result.action()).isEqualTo(TuiOperation.CtrlCAction.NOOP);
        assertThat(result.operation()).isSameAs(completed);
        assertThat(result.remoteCancellationRequested()).isFalse();
    }

    @Test
    void operationIdentityAndDescriptionAreRequired() {
        assertThatThrownBy(() -> TuiOperation.stream("", "status --watch"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TuiOperation.stream("op-1", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writeLockRejectsContextSwitchUntilTheWriteCompletionClearsIt() {
        TuiContextSessionState state = TuiContextSessionReducer.reduce(TuiContextSessionState.initial(),
                new TuiContextSessionAction.Initialize(context("dev")));
        state = TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.SetWriteInFlight("op-write"));

        TuiContextSessionState blocked = TuiContextSessionReducer.reduce(state,
                new TuiContextSessionAction.SwitchContext(context("prod")));

        assertThat(blocked.context().name()).isEqualTo("dev");
        assertThat(blocked.writeOperationId()).isEqualTo("op-write");

        TuiContextSessionState cleared = TuiContextSessionReducer.reduce(blocked,
                new TuiContextSessionAction.ClearWriteInFlight());
        TuiContextSessionState switched = TuiContextSessionReducer.reduce(cleared,
                new TuiContextSessionAction.SwitchContext(context("prod")));

        assertThat(switched.context().name()).isEqualTo("prod");
        assertThat(switched.writeOperationId()).isNull();
        assertThat(switched.firstWriteConfirmationRequired()).isTrue();
    }

    private static ResolvedContext.Named context(String name) {
        return new ResolvedContext.Named(name, new ContextDefinition(UUID.randomUUID(),
                List.of(URI.create("http://127.0.0.1:8081")), new ContextTls(true), UUID.randomUUID()),
                ResolvedContext.Source.WORKSPACE_BINDING);
    }
}
