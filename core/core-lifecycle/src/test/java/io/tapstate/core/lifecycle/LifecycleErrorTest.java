package io.tapstate.core.lifecycle;

import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LifecycleErrorTest {

    @Test
    void everyCodeIsInTheLifecycleDomainAndErrorSeverity() {
        for (LifecycleError e : LifecycleError.values()) {
            assertThat(e.code()).matches("TS028(?!0000)[0-9]{4}");
            assertThat(e.severity()).isEqualTo(Severity.ERROR);
        }
    }

    @Test
    void carriesTheLifecycleVocabularyCodes() {
        assertThat(LifecycleError.values()).extracting(LifecycleError::code).containsExactlyInAnyOrder(
                "TS0280001",
                // start/resume refused because the pipeline's revision is not the latest applied one
                "TS0280002",
                // a stop that did not say whether to clear what the pipeline has accumulated
                "TS0280003",
                // a start or resume named a draft without an executable source-to-output path
                "TS0280004",
                // a lifecycle verb named a pipeline that was never applied
                "TS0280005",
                // a start its start checks stopped: to ask a question, or to refuse it outright
                "TS0280006",
                "TS0280007",
                // a start whose answer names something its start checks do not offer
                "TS0280008");
    }

    @Test
    void declaresThePlaceholderContractPerCode() {
        assertThat(LifecycleError.ILLEGAL_TRANSITION.placeholders())
                .containsExactlyInAnyOrder("from", "verb");
        // requested = the revision the start/resume would run at; latest = the latest applied revision
        assertThat(LifecycleError.INCOMPATIBLE_REVISION.placeholders())
                .containsExactlyInAnyOrder("requested", "latest");
        // pipeline = the id the caller named
        assertThat(LifecycleError.UNKNOWN_PIPELINE.placeholders())
                .containsExactlyInAnyOrder("pipeline");
        // pipeline = the id the stop was aimed at
        assertThat(LifecycleError.PURGE_STATE_NOT_STATED.placeholders())
                .containsExactlyInAnyOrder("pipeline");
        assertThat(LifecycleError.PIPELINE_NOT_RUNNABLE.placeholders())
                .containsExactlyInAnyOrder("pipeline");
    }
}
