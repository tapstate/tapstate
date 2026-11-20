package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MonitorErrorTest {

    @Test
    void everyCodeIsInTheMonitorDomainAndErrorSeverity() {
        for (MonitorError e : MonitorError.values()) {
            assertThat(e.code()).startsWith("TS040");
            assertThat(e.severity()).isEqualTo(Severity.ERROR);
        }
    }

    @Test
    void carriesTheMonitorVocabularyCodes() {
        assertThat(MonitorError.values()).extracting(MonitorError::code).containsExactlyInAnyOrder(
                // a status/metrics/snapshot read of a pipeline that has published no observation
                "TS0400001",
                "TS0400002",
                "TS0400003",
                "TS0400004");
    }

    @Test
    void declaresThePlaceholderContractPerCode() {
        // pipeline = the id the caller asked to observe
        assertThat(MonitorError.NO_OBSERVATION.placeholders())
                .containsExactlyInAnyOrder("pipeline");
        assertThat(MonitorError.INVALID_CURSOR.placeholders())
                .containsExactlyInAnyOrder("operation", "reason");
        assertThat(MonitorError.CURSOR_EXPIRED.placeholders()).containsExactly("operation");
        assertThat(MonitorError.QUERY_BUDGET_EXCEEDED.placeholders())
                .containsExactlyInAnyOrder("operation", "budget", "limit");
    }
}
