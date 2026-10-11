package io.tapstate.adapters.mongostore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceConfigKeyringAcknowledgementRetryTest {
    @Test
    void bothRetryWindowsFitWithinTheBoundedWaitBudget() {
        for (int sample = 0; sample < 100; sample++) {
            long first = SourceConfigKeyringStore.acknowledgementRetryDelayMillis(0);
            long second = SourceConfigKeyringStore.acknowledgementRetryDelayMillis(1);
            assertThat(first).isBetween(50L, 100L);
            assertThat(second).isBetween(100L, 200L);
            assertThat(first + second).isLessThanOrEqualTo(300L);
        }
    }

    @Test
    void aPauseOutsideTheTwoRetryWindowsIsAProgrammingError() {
        assertThatThrownBy(() -> SourceConfigKeyringStore.acknowledgementRetryDelayMillis(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SourceConfigKeyringStore.acknowledgementRetryDelayMillis(2))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
