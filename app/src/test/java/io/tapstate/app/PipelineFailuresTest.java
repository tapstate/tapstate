package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.runtime.engine.EngineError;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The mapping from the Throwable that killed a pipeline's run to the coded failure its observation
 * carries. A run dies for two kinds of reason and they must not be flattened into one: a diagnosable
 * fault the product already coded at its throw site (a sink that refused a write, a connector that could
 * not read) keeps that code, because it is the one that tells the user what to fix; anything else is
 * reported as the generic engine failure rather than being laundered into a specific code it does not
 * deserve.
 */
class PipelineFailuresTest {

    @Test
    void aCodedCauseKeepsItsOwnCodeAndArguments() {
        TapstateException coded = new TapstateException(
                EngineError.NO_SUCH_JOB, Map.of("pipeline", "orders"), null);

        ObservationFailure failure = PipelineFailures.of("orders", coded);

        assertThat(failure.code()).isEqualTo("engine.no-such-job");
        assertThat(failure.params()).containsOnly(entry("pipeline", "orders"));
    }

    @Test
    void aCodedCauseIsFoundThroughTheWrappersTheRuntimeAddedAroundIt() {
        // The data plane wraps what a sink or a connector threw before it reaches the converge loop. The
        // wrapper carries no diagnosis of its own, so the coded cause underneath is the one to report --
        // otherwise every real fault would read as the generic engine failure.
        TapstateException coded = new TapstateException(
                EngineError.NO_SUCH_JOB, Map.of("pipeline", "orders"), null);
        Throwable wrapped = new IllegalStateException("job execution failed", new RuntimeException(coded));

        assertThat(PipelineFailures.of("orders", wrapped).code()).isEqualTo("engine.no-such-job");
    }

    @Test
    void anUncodedCauseBecomesTheGenericEngineFailureCarryingItsMessage() {
        ObservationFailure failure = PipelineFailures.of("orders", new IllegalStateException("ring buffer closed"));

        assertThat(failure.code()).isEqualTo("engine.job-failed");
        assertThat(failure.params())
                .containsOnly(entry("pipeline", "orders"), entry("cause", "ring buffer closed"));
    }

    @Test
    void anUncodedCauseWithNoMessageFallsBackToItsType() {
        // Every declared placeholder must have a value; a message-less throwable still has to say something
        // more useful than an empty string.
        ObservationFailure failure = PipelineFailures.of("orders", new NullPointerException());

        assertThat(failure.params()).containsEntry("cause", "NullPointerException");
    }

    @Test
    void everyDeclaredPlaceholderOfTheGenericFailureIsSupplied() {
        ObservationFailure failure = PipelineFailures.of("orders", new IllegalStateException("boom"));

        assertThat(failure.params().keySet()).containsExactlyInAnyOrderElementsOf(EngineError.JOB_FAILED.placeholders());
    }

    @Test
    void aCodedCausesArgumentsAreBoundedTheSameWayAsAnUncodedDescription() {
        // A coded exception's arguments are not all product-authored: a sink's write failure carries the
        // driver's own message as its detail, which can be as long and as multiline as any uncoded cause.
        // Whatever rides into the persisted observation and out through the CLI is bounded, coded or not.
        String driverDump = "Deadlock found when trying to get lock\n\tat com.mysql.cj.jdbc.exceptions"
                + "\n\tat com.mysql.cj.jdbc.ClientPreparedStatement";
        TapstateException coded = new TapstateException(
                EngineError.NO_SUCH_JOB, Map.of("pipeline", driverDump), null);

        ObservationFailure failure = PipelineFailures.of("orders", coded);

        String detail = failure.params().get("pipeline");
        assertThat(detail).isEqualTo("Deadlock found when trying to get lock …");
        assertThat(detail).doesNotContain("\n");
    }

    @Test
    void aMultiLineCauseIsCutToItsFirstLine() {
        // The shape Jet hands back once a job's live context is gone and it can only reconstruct a mock
        // throwable from stored text: the "message" is a full printStackTrace() dump, several lines long.
        String stackTraceShaped = "FakeCodedException: connector.write-failed"
                + "\n\tat com.hazelcast.jet.impl.execution.TaskletExecutionService.handleTaskletExecutionError"
                + "\n\tat com.hazelcast.jet.impl.execution.TaskletExecutionService.access$0";
        ObservationFailure failure = PipelineFailures.of("orders", new RuntimeException(stackTraceShaped));

        String cause = failure.params().get("cause");
        assertThat(cause).isEqualTo("FakeCodedException: connector.write-failed …");
        assertThat(cause).doesNotContain("\n").doesNotContain("TaskletExecutionService");
    }

    @Test
    void aVeryLongSingleLineCauseIsCappedToABoundedLength() {
        String longMessage = "x".repeat(500);

        ObservationFailure failure = PipelineFailures.of("orders", new RuntimeException(longMessage));

        String cause = failure.params().get("cause");
        // Capped length plus the truncation marker, not the full 500 characters.
        assertThat(cause).hasSize(200 + 2).endsWith(" …");
    }

    @Test
    void aQualifiedRemoteSourceFailureKeepsItsOriginalCodeAndNamedArguments() {
        var owner = new io.tapstate.spi.store.WorkloadOwner("a", "boot-a");
        var pipeline = new io.tapstate.spi.store.WorkloadClaimFence(new io.tapstate.spi.store.WorkloadClaimKey(
                "cluster", io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION, "orders"), owner, 1, 2, 7, 1);
        var capture = new io.tapstate.spi.store.WorkloadClaimFence(new io.tapstate.spi.store.WorkloadClaimKey(
                "cluster", io.tapstate.spi.store.WorkloadClaimType.CAPTURE, "capture"), owner, 2, 0, 7, 1);
        var time = java.time.Instant.parse("2026-10-10T06:00:00Z");
        var position = new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(1, 4), "old");
        var witness = new io.tapstate.spi.store.CaptureResumeWitness("source", "mongo", "chain", "consumer",
                io.tapstate.core.model.ReadMode.CDC_ONLY, true, java.util.List.of("orders"), true, 1,
                position, true, false, java.util.List.of(), null, 0, null, null, Map.of());
        var attempt = new io.tapstate.spi.store.CaptureReadAttempt("chain", 1, 1, capture, java.util.List.of("orders"),
                io.tapstate.spi.store.CaptureReadAttempt.Kind.RESUME, "old", null, time);
        var params = Map.<String, Object>of("requested", "old", "earliest", "head", "retention", "2h");
        var state = new io.tapstate.spi.store.CaptureReadState(attempt, null, null, null, true,
                io.tapstate.runtime.srs.CaptureError.START_FROM_OUTSIDE_WINDOW.code(), params, "Inspect the retained source position", time);
        var proof = new io.tapstate.spi.store.CaptureStartupFailure(pipeline, witness,
                witness.requestedPosition("capture").orElseThrow(), time, state);

        ObservationFailure failure = PipelineFailures.of("orders", new io.tapstate.runtime.srs.CaptureStartupException(proof));

        assertThat(failure.code()).isEqualTo(io.tapstate.runtime.srs.CaptureError.START_FROM_OUTSIDE_WINDOW.code());
        assertThat(failure.params()).containsOnly(entry("requested", "old"), entry("earliest", "head"), entry("retention", "2h"));
    }
}
