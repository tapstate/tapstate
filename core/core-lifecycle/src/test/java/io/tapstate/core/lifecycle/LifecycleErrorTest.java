package io.tapstate.core.lifecycle;

import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LifecycleErrorTest {

    @Test
    void everyCodeIsInTheLifecycleDomainAndErrorSeverity() {
        for (LifecycleError e : LifecycleError.values()) {
            assertThat(e.code()).startsWith("lifecycle.");
            assertThat(e.severity()).isEqualTo(Severity.ERROR);
        }
    }

    @Test
    void carriesTheLifecycleVocabularyCodes() {
        assertThat(LifecycleError.values()).extracting(LifecycleError::code).containsExactlyInAnyOrder(
                "lifecycle.illegal-transition",
                // start/resume refused because the pipeline's revision is not the latest applied one
                "lifecycle.incompatible-revision",
                // a stop that did not say whether to clear what the pipeline has accumulated
                "lifecycle.purge-state-not-stated",
                // a lifecycle verb named a pipeline that was never applied
                "lifecycle.unknown-pipeline");
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
    }
}
