package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRunState.*;
import static org.assertj.core.api.Assertions.*;

class ValidationRunStateTest {
    @Test void fullRunCanCompleteOnlyAfterRunningAndStoppedRunsCannotReopen() {
        STARTING.requireTransitionTo(RUNNING, ValidationMode.FULL);
        RUNNING.requireTransitionTo(COMPLETED, ValidationMode.FULL);
        assertThatThrownBy(() -> STARTING.requireTransitionTo(COMPLETED, ValidationMode.FULL))
                .isInstanceOf(TapstateException.class);
        for (var terminal : Set.of(COMPLETED, STOPPED, FAILED)) {
            for (var next : values()) assertThat(terminal.canTransitionTo(next, ValidationMode.FULL)).isFalse();
        }
    }

    @Test void incrementalEmptyQueueNeverAuthorizesCompletion() {
        assertThatThrownBy(() -> RUNNING.requireTransitionTo(COMPLETED, ValidationMode.INCREMENTAL))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code()).isEqualTo(ValidationError.ILLEGAL_TRANSITION);
                    assertThat(e.args()).containsExactlyInAnyOrderEntriesOf(
                            Map.of("from", "RUNNING", "to", "COMPLETED", "mode", "INCREMENTAL"));
                });
        RUNNING.requireTransitionTo(STOPPING, ValidationMode.INCREMENTAL);
        STOPPING.requireTransitionTo(STOPPED, ValidationMode.INCREMENTAL);
    }

    @Test void cancellationCannotJumpTheStoppingFenceAndFailuresPreserveTerminals() {
        assertThat(STARTING.canTransitionTo(STOPPED, ValidationMode.FULL)).isFalse();
        assertThat(RUNNING.canTransitionTo(STOPPED, ValidationMode.FULL)).isFalse();
        STARTING.requireTransitionTo(STOPPING, ValidationMode.FULL);
        STOPPING.requireTransitionTo(FAILED, ValidationMode.FULL);
        assertThat(COMPLETED.canTransitionTo(FAILED, ValidationMode.FULL)).isFalse();
    }

    @Test void incrementalIsAlwaysFullColumnButFullMethodsRemainDistinct() {
        assertThat(ValidationMethod.FULL_COLUMN.supports(ValidationMode.INCREMENTAL)).isTrue();
        assertThat(ValidationMethod.COUNT.supports(ValidationMode.INCREMENTAL)).isFalse();
        assertThat(ValidationMethod.UNIQUE_KEY.supports(ValidationMode.INCREMENTAL)).isFalse();
        for (var method : ValidationMethod.values()) assertThat(method.supports(ValidationMode.FULL)).isTrue();
    }
}
