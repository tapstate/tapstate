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

        assertThat(failure.code()).isEqualTo("TS0440001");
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

        assertThat(PipelineFailures.of("orders", wrapped).code()).isEqualTo("TS0440001");
    }

    @Test
    void anUncodedCauseBecomesTheGenericEngineFailureCarryingItsMessage() {
        ObservationFailure failure = PipelineFailures.of("orders", new IllegalStateException("ring buffer closed"));

        assertThat(failure.code()).isEqualTo("TS0440008");
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
}
