package io.tapstate.core.logging;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class LogLineTest {

    @Test
    void oversizedDiagnosticKeepsTheOriginalLineAndMarksTheAttachmentTruncated() {
        var params = new java.util.LinkedHashMap<String, Object>();
        for (int i = 0; i < 32; i++) params.put("field" + i, "x".repeat(4096));
        var line = new LogLine(7L, "ERROR", "original", java.util.Map.of("params", params));
        assertThat(line.message()).isEqualTo("original");
        assertThat(line.diagnostic()).isEqualTo(java.util.Map.of("truncated", true));
    }

    @Test
    void holdsTheTimestampLevelAndMessage() {
        LogLine line = new LogLine(1_700_000_000_000L, "INFO", "converged orders_sync to RUNNING");

        assertThat(line.timestampMillis()).isEqualTo(1_700_000_000_000L);
        assertThat(line.level()).isEqualTo("INFO");
        assertThat(line.message()).isEqualTo("converged orders_sync to RUNNING");
    }

    @Test
    void requiresLevelAndMessage() {
        assertThatNullPointerException().isThrownBy(() -> new LogLine(0L, null, "m"));
        assertThatNullPointerException().isThrownBy(() -> new LogLine(0L, "INFO", null));
    }
}
